import asyncio
import json
import re
import uuid
from collections import deque
from datetime import date, datetime, timezone, timedelta

_PATH_ID_RE = re.compile(r"/\d{6,}")

import psycopg
from psycopg_pool import AsyncConnectionPool

from config import DATABASE_URL
from ingestion.rotation_scheduler import ET, PREMARKET_START


class GuildNotSetUpError(Exception):
    """Raised when a guild tries /watch add|remove before /setup has created its row."""

    def __init__(self, guild_id: str):
        self.guild_id = guild_id
        super().__init__(f"Guild {guild_id} has no alert channel configured — run /setup first")


_pool: AsyncConnectionPool | None = None
_pool_lock = asyncio.Lock()


async def _get_pool() -> AsyncConnectionPool:
    """Lazily opens a single shared connection pool on first use. Lazy (rather than opened at
    import time) because opening a pool requires a running event loop, and this module is
    imported by both the FastAPI admin app and the discord bot before either loop starts."""
    global _pool
    if _pool is None:
        async with _pool_lock:
            if _pool is None:
                pool = AsyncConnectionPool(DATABASE_URL, open=False)
                await pool.open()
                _pool = pool
    return _pool


async def get_all_tickers() -> list[dict]:
    """Every ticker, active or not — used by the admin ticker-management view, which needs to
    offer the full universe regardless of current polling status."""
    pool = await _get_pool()
    async with pool.connection() as db:
        cur = await db.execute("SELECT ticker, title, is_active FROM tickers ORDER BY ticker")
        rows = await cur.fetchall()
    return [{"ticker": r[0], "title": r[1], "is_active": bool(r[2])} for r in rows]


async def get_active_tickers() -> list[dict]:
    """Only tickers with is_active = 1 — the actual polling watchlist. A ticker becomes active
    when it gets its first guild subscriber (add_guild_tickers) and inactive again once its
    last one drops it (remove_guild_tickers/delete_user) — this keeps SEC/yfinance request
    volume tied to real demand instead of the full ~500-ticker calendar universe."""
    pool = await _get_pool()
    async with pool.connection() as db:
        cur = await db.execute("SELECT ticker, title FROM tickers WHERE is_active = 1 ORDER BY ticker")
        rows = await cur.fetchall()
    return [{"ticker": r[0], "title": r[1]} for r in rows]


async def _deactivate_orphaned_tickers(db: psycopg.AsyncConnection, candidates: set[str]) -> None:
    """For each candidate ticker, set is_active = 0 if no subscription references it anymore.
    Called with the tickers a change *might* have orphaned — not every ticker in the DB — so
    callers must pass only the ones actually affected by their delete/replace."""
    if not candidates:
        return
    placeholders = ",".join(["%s"] * len(candidates))
    await db.execute(
        f"""UPDATE tickers SET is_active = 0
            WHERE ticker IN ({placeholders})
              AND ticker NOT IN (SELECT ticker FROM subscriptions)""",
        tuple(candidates),
    )


async def _guild_exists(db: psycopg.AsyncConnection, guild_id: str) -> bool:
    cur = await db.execute("SELECT 1 FROM users WHERE id = %s", (guild_id,))
    return await cur.fetchone() is not None


async def upsert_guild_channel(guild_id: str, name: str, webhook_url: str) -> None:
    """Insert or update a guild's alert channel — used by /setup. `id` holds the real Discord
    guild ID (not a random uuid4) so re-running /setup against a different channel updates the
    same row instead of creating an orphan. Deliberately does not touch `subscriptions` —
    rerunning /setup must never clear the watchlist, only change where alerts go."""
    now = datetime.now(timezone.utc).isoformat()
    pool = await _get_pool()
    async with pool.connection() as db:
        await db.execute(
            """INSERT INTO users (id, name, webhook_url, created_at)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT(id) DO UPDATE SET name = excluded.name,
                                             webhook_url = excluded.webhook_url""",
            (guild_id, name, webhook_url, now),
        )
        await db.commit()


async def add_guild_tickers(guild_id: str, tickers: list[str]) -> dict[str, list[str]]:
    """Adds tickers to a guild's watchlist. Returns {"added", "already_watched", "unknown"} so
    the /watch add command can report each outcome distinctly. Raises GuildNotSetUpError if the
    guild hasn't run /setup yet."""
    if not tickers:
        return {"added": [], "already_watched": [], "unknown": []}
    tickers = list(dict.fromkeys(tickers))  # de-dupe, preserve order

    now = datetime.now(timezone.utc).isoformat()
    placeholders = ",".join(["%s"] * len(tickers))
    pool = await _get_pool()
    async with pool.connection() as db:
        if not await _guild_exists(db, guild_id):
            raise GuildNotSetUpError(guild_id)

        cur = await db.execute(f"SELECT ticker FROM tickers WHERE ticker IN ({placeholders})", tickers)
        known = {r[0] for r in await cur.fetchall()}
        unknown = [t for t in tickers if t not in known]

        cur = await db.execute(
            f"""SELECT ticker FROM subscriptions
                WHERE user_id = %s AND ticker IN ({placeholders})""",
            (guild_id, *tickers),
        )
        already_watched = {r[0] for r in await cur.fetchall()}

        to_add = [t for t in tickers if t in known and t not in already_watched]

        async with db.cursor() as cur:
            await cur.executemany(
                "INSERT INTO subscriptions (user_id, ticker, subscribed_at) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                [(guild_id, t, now) for t in to_add],
            )
            await cur.executemany(
                "UPDATE tickers SET is_active = 1 WHERE ticker = %s",
                [(t,) for t in to_add],
            )
        await db.commit()

    return {
        "added": to_add,
        "already_watched": [t for t in tickers if t in already_watched],
        "unknown": unknown,
    }


async def remove_guild_tickers(guild_id: str, tickers: list[str]) -> list[str]:
    """Removes tickers from a guild's watchlist, deactivating any that no longer have any
    subscriber left. Returns the tickers actually removed. Raises GuildNotSetUpError if the
    guild hasn't run /setup yet."""
    if not tickers:
        return []

    placeholders = ",".join(["%s"] * len(tickers))
    pool = await _get_pool()
    async with pool.connection() as db:
        if not await _guild_exists(db, guild_id):
            raise GuildNotSetUpError(guild_id)

        cur = await db.execute(
            f"""SELECT ticker FROM subscriptions
                WHERE user_id = %s AND ticker IN ({placeholders})""",
            (guild_id, *tickers),
        )
        removed = [r[0] for r in await cur.fetchall()]

        if removed:
            async with db.cursor() as cur:
                await cur.executemany(
                    "DELETE FROM subscriptions WHERE user_id = %s AND ticker = %s",
                    [(guild_id, t) for t in removed],
                )
            await _deactivate_orphaned_tickers(db, set(removed))
        await db.commit()

    return removed


async def get_guild_watchlist(guild_id: str) -> list[str]:
    pool = await _get_pool()
    async with pool.connection() as db:
        cur = await db.execute(
            "SELECT ticker FROM subscriptions WHERE user_id = %s ORDER BY ticker", (guild_id,)
        )
        return [r[0] for r in await cur.fetchall()]


_calendar_cache: dict[float, tuple[list, datetime]] = {}
_CALENDAR_TTL = 60.0  # seconds — safe since the scheduler only refreshes once a day


async def get_upcoming_calendar(hours: float = 24) -> list[dict]:
    """Every company in earnings_calendar expected to report within the next `hours` (ET) —
    backs /calendar. Mirrors ingestion/rotation_scheduler.py's CalendarHotSet.upcoming_companies
    (display-only there too) but async, via the shared pool. Deliberately unscoped by guild
    watchlist and by tickers.is_active — /calendar is a lookup across the full ~500-ticker
    calendar universe, not a "what am I subscribed to" view (that's /watch list). Unknown
    earnings_time falls back to the start of the premarket window for ordering, flagged via
    "time_known": False."""
    now_utc = datetime.now(timezone.utc)
    cached = _calendar_cache.get(hours)
    if cached is not None:
        result, cached_at = cached
        if (now_utc - cached_at).total_seconds() < _CALENDAR_TTL:
            return result

    now_et = datetime.now(ET)
    horizon = now_et + timedelta(hours=hours)
    dates = sorted({now_et.date(), horizon.date()})

    placeholders = ",".join(["%s"] * len(dates))
    pool = await _get_pool()
    async with pool.connection() as db:
        cur = await db.execute(
            f"""SELECT ec.ticker, t.title, ec.earnings_date, ec.earnings_time
                FROM earnings_calendar ec
                JOIN tickers t ON t.ticker = ec.ticker
                WHERE ec.earnings_date IN ({placeholders})""",
            tuple(d.isoformat() for d in dates),
        )
        rows = await cur.fetchall()

    upcoming = []
    for ticker, title, date_str, time_str in rows:
        d = date.fromisoformat(date_str)
        t = datetime.strptime(time_str, "%H:%M:%S").time() if time_str else PREMARKET_START
        target = datetime.combine(d, t, tzinfo=ET)
        if now_et <= target <= horizon:
            upcoming.append({
                "ticker": ticker,
                "title": title,
                "target": target.isoformat(),
                "time_known": bool(time_str),
            })

    upcoming.sort(key=lambda c: c["target"])
    _calendar_cache[hours] = (upcoming, now_utc)
    return upcoming


async def delete_user(user_id: str) -> bool:
    """Delete a guild's row outright (cascades subscriptions). Used by on_guild_remove and by
    the dead-webhook cleanup path in delivery/router.py. Returns False if not found. Does NOT
    touch banned_guilds — a kicked/removed bot must not clear a ban."""
    pool = await _get_pool()
    async with pool.connection() as db:
        cur = await db.execute("SELECT id FROM users WHERE id = %s", (user_id,))
        if not await cur.fetchone():
            return False

        cur = await db.execute("SELECT ticker FROM subscriptions WHERE user_id = %s", (user_id,))
        affected = {r[0] for r in await cur.fetchall()}

        await db.execute("DELETE FROM users WHERE id = %s", (user_id,))
        await _deactivate_orphaned_tickers(db, affected)
        await db.commit()
    return True


async def delete_ticker(ticker: str) -> bool:
    """Remove a ticker from the universe entirely. Cascades to its subscriptions and earnings
    calendar row (ON DELETE CASCADE), so any guild watching it silently loses that subscription
    — same tradeoff as delete_user. Returns False if the ticker doesn't exist."""
    pool = await _get_pool()
    async with pool.connection() as db:
        cur = await db.execute("SELECT 1 FROM tickers WHERE ticker = %s", (ticker,))
        if not await cur.fetchone():
            return False
        await db.execute("DELETE FROM tickers WHERE ticker = %s", (ticker,))
        await db.commit()
    return True


async def set_ticker_active(ticker: str, active: bool) -> None:
    """Admin override for a ticker's polling status, independent of subscriber count."""
    pool = await _get_pool()
    async with pool.connection() as db:
        await db.execute(
            "UPDATE tickers SET is_active = %s WHERE ticker = %s", (1 if active else 0, ticker)
        )
        await db.commit()


async def set_all_tickers_active(active: bool) -> None:
    """Bulk admin override — activate/deactivate every ticker regardless of subscribers."""
    pool = await _get_pool()
    async with pool.connection() as db:
        await db.execute("UPDATE tickers SET is_active = %s", (1 if active else 0,))
        await db.commit()


async def create_ticker(ticker: str, cik: str, title: str) -> bool:
    """Add a new company to the ticker universe. Active by default — an admin adding one by
    hand means they want it polled. Returns False if the ticker already exists."""
    ticker = ticker.strip().upper()
    cik = str(int(cik)).zfill(10)
    pool = await _get_pool()
    async with pool.connection() as db:
        cur = await db.execute("SELECT 1 FROM tickers WHERE ticker = %s", (ticker,))
        if await cur.fetchone():
            return False
        await db.execute(
            "INSERT INTO tickers (ticker, cik, title, is_active) VALUES (%s, %s, %s, 1)",
            (ticker, cik, title),
        )
        await db.commit()
    return True


async def get_subscribers_for_ticker(ticker: str) -> list[dict]:
    """Returns [{id, webhook_url}] for all guilds subscribed to ticker."""
    pool = await _get_pool()
    async with pool.connection() as db:
        cur = await db.execute(
            """SELECT u.id, u.webhook_url
               FROM users u
               JOIN subscriptions s ON u.id = s.user_id
               WHERE s.ticker = %s""",
            (ticker,),
        )
        rows = await cur.fetchall()
    return [{"id": r[0], "webhook_url": r[1]} for r in rows]


async def save_alert(ticker: str, alert: dict) -> None:
    """Persist a processed alert so new subscribers can receive it as a catch-up."""
    now = datetime.now(timezone.utc).isoformat()
    pool = await _get_pool()
    async with pool.connection() as db:
        await db.execute(
            "INSERT INTO alerts (id, ticker, alert_json, detected_at, created_at) VALUES (%s, %s, %s, %s, %s)",
            (str(uuid.uuid4()), ticker, json.dumps(alert), alert.get("detected_at", now), now),
        )
        await db.commit()


async def get_analysis(ticker: str, accession: str) -> dict | None:
    """Pre-flight cache lookup for the single-process pipeline (main.py::_process_one) — returns
    the stored successful analysis for this exact (ticker, accession), or None. Scoped to
    status = 'ok' only: a prior 'error' row must not count as a cache hit, so a filing that
    failed transiently (rate limit, network blip) still gets retried on the next delivery of
    that message instead of being permanently stuck."""
    pool = await _get_pool()
    async with pool.connection() as db:
        cur = await db.execute(
            """SELECT alert_json, extraction_json, sentiment_json, risk_json, detected_at
               FROM filing_analysis
               WHERE ticker = %s AND accession = %s AND status = 'ok'""",
            (ticker, accession),
        )
        row = await cur.fetchone()
    if not row:
        return None
    return {
        "alert": json.loads(row[0]),
        "extraction": json.loads(row[1]) if row[1] else None,
        "sentiment": json.loads(row[2]) if row[2] else None,
        "risk": json.loads(row[3]) if row[3] else None,
        "detected_at": row[4],
    }


async def save_analysis(
    ticker: str,
    accession: str,
    status: str,
    alert: dict | None = None,
    extraction: dict | None = None,
    sentiment: dict | None = None,
    risk: dict | None = None,
    error_message: str | None = None,
    detected_at: str | None = None,
) -> None:
    """Upsert the analysis result for (ticker, accession) — success or failure, and regardless
    of whether the alert is ever delivered to a subscriber (delivery is a separate concern,
    handled by router.fanout after this is called). ON CONFLICT keeps a redelivered/reprocessed
    message from producing a duplicate row."""
    now = datetime.now(timezone.utc).isoformat()
    pool = await _get_pool()
    async with pool.connection() as db:
        await db.execute(
            """INSERT INTO filing_analysis
               (id, ticker, accession, status, alert_json, extraction_json, sentiment_json,
                risk_json, error_message, detected_at, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (ticker, accession) DO UPDATE SET
                   status          = excluded.status,
                   alert_json      = excluded.alert_json,
                   extraction_json = excluded.extraction_json,
                   sentiment_json  = excluded.sentiment_json,
                   risk_json       = excluded.risk_json,
                   error_message   = excluded.error_message,
                   detected_at     = excluded.detected_at,
                   created_at      = excluded.created_at""",
            (
                str(uuid.uuid4()), ticker, accession, status,
                json.dumps(alert) if alert is not None else None,
                json.dumps(extraction) if extraction is not None else None,
                json.dumps(sentiment) if sentiment is not None else None,
                json.dumps(risk) if risk is not None else None,
                error_message, detected_at, now,
            ),
        )
        await db.commit()


async def get_recent_alerts(ticker: str, hours: int = 1) -> list[dict]:
    """Return alerts for ticker detected within the last N hours, newest first."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    pool = await _get_pool()
    async with pool.connection() as db:
        cur = await db.execute(
            """SELECT alert_json FROM alerts
               WHERE ticker = %s AND detected_at > %s
               ORDER BY detected_at DESC""",
            (ticker, cutoff),
        )
        rows = await cur.fetchall()
    return [json.loads(r[0]) for r in rows]


# --- Bot visibility + ban/unban kill switch --------------------------------

async def log_command(
    guild_id: str | None, discord_user_id: str, command: str, args: str, success: bool,
    duration_ms: int = 0,
) -> None:
    """One row per slash-command invocation — the only source for "active users" and "commands
    per day", neither of which is derivable from current-state tables. Called from a single
    wrapper around every command handler, not duplicated per-command."""
    now = datetime.now(timezone.utc).isoformat()
    pool = await _get_pool()
    async with pool.connection() as db:
        await db.execute(
            """INSERT INTO bot_command_log
               (id, guild_id, discord_user_id, command, args, success, duration_ms, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (str(uuid.uuid4()), guild_id, discord_user_id, command, args, 1 if success else 0, duration_ms, now),
        )
        await db.commit()


# In-memory buffer — rows accumulate here and are flushed to Postgres in one
# batch every 5 seconds by flush_api_log_loop(). This collapses hundreds of
# individual INSERT+COMMIT calls/second (brutal under load) into one
# executemany per flush interval, cutting WAL pressure dramatically.
_api_log_buffer: deque = deque()


def log_api_request(method: str, path: str, status_code: int, duration_ms: int) -> None:
    """Append one row to the in-memory buffer. Synchronous — no DB call here."""
    normalized_path = _PATH_ID_RE.sub("/{id}", path)
    _api_log_buffer.append((
        str(uuid.uuid4()), method, normalized_path, status_code, duration_ms,
        datetime.now(timezone.utc).isoformat(),
    ))


async def flush_api_log_loop(interval: float = 5.0) -> None:
    """Background task started by the FastAPI lifespan. Drains the buffer into
    Postgres every `interval` seconds in a single batched executemany.
    Also prunes rows older than 7 days every ~5 minutes to keep the table bounded."""
    import logging
    _flog = logging.getLogger("db.flush_api_log")
    _flog.info("flush_api_log_loop started")
    prune_counter = 0
    while True:
        await asyncio.sleep(interval)
        if _api_log_buffer:
            rows = []
            while _api_log_buffer:
                rows.append(_api_log_buffer.popleft())
            try:
                pool = await _get_pool()
                async with pool.connection() as db:
                    async with db.cursor() as cur:
                        await cur.executemany(
                            """INSERT INTO api_request_log
                               (id, method, path, status_code, duration_ms, created_at)
                               VALUES (%s, %s, %s, %s, %s, %s)""",
                            rows,
                        )
                    await db.commit()
                _flog.debug("flushed %d api log rows", len(rows))
            except Exception as exc:
                _flog.error("flush failed (%s), %d rows returned to buffer", exc, len(rows))
                _api_log_buffer.extendleft(reversed(rows))

        prune_counter += 1
        if prune_counter >= 60:  # every ~5 min
            prune_counter = 0
            cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
            try:
                pool = await _get_pool()
                async with pool.connection() as db:
                    await db.execute(
                        "DELETE FROM api_request_log WHERE created_at < %s", (cutoff,)
                    )
                    await db.commit()
            except Exception as exc:
                _flog.error("prune failed: %s", exc)


# Granularities that can use simple substr() on the TEXT created_at column —
# these hit the existing text index directly with no casting overhead.
_GRAN_SUBSTR: dict[str, tuple[int, timedelta]] = {
    "1h":  (13, timedelta(hours=48)),   # "YYYY-MM-DDTHH"
    "day": (10, timedelta(days=30)),    # "YYYY-MM-DD"
}

# Granularities that need CAST + date_trunc — use the functional index on
# CAST(created_at AS TIMESTAMPTZ) added by the latest db/init.sql migration.
_GRAN_CAST: dict[str, tuple[str, timedelta]] = {
    "5min": (
        "date_trunc('hour', ts) + INTERVAL '5 minutes' * (EXTRACT(MINUTE FROM ts)::int / 5)",
        timedelta(hours=6),
    ),
    "15min": (
        "date_trunc('hour', ts) + INTERVAL '15 minutes' * (EXTRACT(MINUTE FROM ts)::int / 15)",
        timedelta(hours=24),
    ),
    "3h": (
        "date_trunc('hour', ts) - MAKE_INTERVAL(hours => EXTRACT(HOUR FROM ts)::int %% 3)",
        timedelta(days=7),
    ),
}

# 30s result cache — keyed by "api:<gran>" / "cmd:<gran>". The frontend
# auto-refreshes every 30s, so repeated hits within the same window are served
# from memory instead of re-running expensive queries.
_stats_cache: dict[str, tuple[dict, datetime]] = {}
_STATS_CACHE_TTL = 30.0


def _stats_cached(key: str) -> dict | None:
    entry = _stats_cache.get(key)
    if entry and (datetime.now(timezone.utc) - entry[1]).total_seconds() < _STATS_CACHE_TTL:
        return entry[0]
    return None


def _stats_store(key: str, value: dict) -> dict:
    _stats_cache[key] = (value, datetime.now(timezone.utc))
    return value


async def _bucket_query(table: str, granularity: str) -> tuple[list[dict], str]:
    """Returns (by_bucket_rows, since_iso) using the cheapest available strategy."""
    if granularity in _GRAN_SUBSTR:
        substr_len, window = _GRAN_SUBSTR[granularity]
        since = (datetime.now(timezone.utc) - window).isoformat()
        pool = await _get_pool()
        async with pool.connection() as db:
            cur = await db.execute(
                f"""SELECT substr(created_at, 1, {substr_len}) AS bucket, COUNT(*)
                    FROM {table} WHERE created_at >= %s
                    GROUP BY bucket ORDER BY bucket""",
                (since,),
            )
            # Pad to full ISO so JS new Date() parses unambiguously:
            #   day  → "2024-01-15"          → append "T00:00:00Z"
            #   1h   → "2024-01-15T14"       → append ":00:00Z"
            suffix = "T00:00:00Z" if substr_len == 10 else ":00:00Z"
            rows = [{"day": r[0] + suffix, "count": r[1]} for r in await cur.fetchall()]
        return rows, since

    bucket_sql, window = _GRAN_CAST[granularity]
    since = (datetime.now(timezone.utc) - window).isoformat()
    pool = await _get_pool()
    async with pool.connection() as db:
        cur = await db.execute(
            f"""WITH d AS (
                    SELECT CAST(created_at AS TIMESTAMPTZ) AS ts
                    FROM {table} WHERE created_at >= %s
                )
                SELECT {bucket_sql} AS bucket, COUNT(*)
                FROM d GROUP BY bucket ORDER BY bucket""",
            (since,),
        )
        rows = [{"day": r[0].isoformat(), "count": r[1]} for r in await cur.fetchall()]
    return rows, since


async def get_api_stats(granularity: str = "1h") -> dict:
    """API request counts per bucket and avg/p95 latency per endpoint."""
    cache_key = f"api:{granularity}"
    if (hit := _stats_cached(cache_key)) is not None:
        return hit

    by_day, since = await _bucket_query("api_request_log", granularity)
    pool = await _get_pool()
    async with pool.connection() as db:
        cur = await db.execute(
            """SELECT method,
                      regexp_replace(path, '/[0-9]{6,}', '/{id}', 'g') AS norm_path,
                      COUNT(*) AS total,
                      ROUND(AVG(duration_ms)) AS avg_ms,
                      PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_ms
               FROM api_request_log
               WHERE created_at >= %s
               GROUP BY method, norm_path
               ORDER BY total DESC""",
            (since,),
        )
        by_endpoint = [
            {
                "endpoint": f"{r[0]} {r[1]}",
                "total": r[2],
                "avg_ms": float(r[3]) if r[3] is not None else 0.0,
                "p95_ms": float(r[4]) if r[4] is not None else 0.0,
            }
            for r in await cur.fetchall()
        ]

    return _stats_store(cache_key, {"by_day": by_day, "by_endpoint": by_endpoint})


async def get_command_stats(granularity: str = "1h") -> dict:
    """Bucketed Discord command counts and per-command latency."""
    cache_key = f"cmd:{granularity}"
    if (hit := _stats_cached(cache_key)) is not None:
        return hit

    by_day, since = await _bucket_query("bot_command_log", granularity)
    pool = await _get_pool()
    async with pool.connection() as db:
        cur = await db.execute(
            """SELECT command,
                      COUNT(*) AS total,
                      ROUND(AVG(duration_ms)) AS avg_ms,
                      PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_ms,
                      SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) AS failures
               FROM bot_command_log
               WHERE created_at >= %s AND duration_ms IS NOT NULL
               GROUP BY command
               ORDER BY avg_ms DESC NULLS LAST""",
            (since,),
        )
        by_command = [
            {
                "command": r[0],
                "total": r[1],
                "avg_ms": float(r[2]) if r[2] is not None else 0.0,
                "p95_ms": float(r[3]) if r[3] is not None else 0.0,
                "failures": r[4],
            }
            for r in await cur.fetchall()
        ]

    return _stats_store(cache_key, {"by_day": by_day, "by_command": by_command})


async def is_guild_banned(guild_id: str) -> bool:
    pool = await _get_pool()
    async with pool.connection() as db:
        cur = await db.execute("SELECT 1 FROM banned_guilds WHERE guild_id = %s", (guild_id,))
        return await cur.fetchone() is not None


async def ban_guild(guild_id: str, reason: str = "") -> None:
    """Separate from `users` on purpose — must survive the guild's row being deleted (kicked bot,
    on_guild_remove) so re-inviting to dodge a ban doesn't work. /setup checks this first."""
    now = datetime.now(timezone.utc).isoformat()
    pool = await _get_pool()
    async with pool.connection() as db:
        await db.execute(
            """INSERT INTO banned_guilds (guild_id, reason, banned_at) VALUES (%s, %s, %s)
               ON CONFLICT(guild_id) DO UPDATE SET reason = excluded.reason,
                                                    banned_at = excluded.banned_at""",
            (guild_id, reason, now),
        )
        await db.commit()


async def unban_guild(guild_id: str) -> None:
    pool = await _get_pool()
    async with pool.connection() as db:
        await db.execute("DELETE FROM banned_guilds WHERE guild_id = %s", (guild_id,))
        await db.commit()


async def get_bot_stats(active_window_days: int = 30) -> dict:
    """Guild count, "active" users (ran a command in the window — a bot has no way to see who
    merely read an alert, with or without privileged intents), commands today, and a day-by-day
    breakdown for the admin stats panel."""
    since = (datetime.now(timezone.utc) - timedelta(days=active_window_days)).isoformat()
    today = datetime.now(timezone.utc).date().isoformat()
    pool = await _get_pool()
    async with pool.connection() as db:
        cur = await db.execute("SELECT COUNT(*) FROM users")
        guild_count = (await cur.fetchone())[0]

        cur = await db.execute(
            "SELECT COUNT(DISTINCT discord_user_id) FROM bot_command_log WHERE created_at >= %s",
            (since,),
        )
        active_users = (await cur.fetchone())[0]

        cur = await db.execute(
            "SELECT COUNT(*) FROM bot_command_log WHERE substr(created_at, 1, 10) = %s", (today,)
        )
        commands_today = (await cur.fetchone())[0]

        cur = await db.execute(
            """SELECT substr(created_at, 1, 10) AS day, COUNT(*)
               FROM bot_command_log WHERE created_at >= %s GROUP BY day ORDER BY day""",
            (since,),
        )
        commands_by_day = [{"day": r[0], "count": r[1]} for r in await cur.fetchall()]

    return {
        "guild_count": guild_count,
        "active_users": active_users,
        "commands_today": commands_today,
        "commands_by_day": commands_by_day,
    }


async def list_guilds() -> list[dict]:
    """Real guilds with their alert channel, watchlist size, and ban status — for the admin
    visibility view."""
    pool = await _get_pool()
    async with pool.connection() as db:
        cur = await db.execute("SELECT id, name, webhook_url, created_at FROM users ORDER BY created_at DESC")
        guilds = [
            {"guild_id": r[0], "name": r[1], "webhook_url": r[2], "created_at": r[3]}
            for r in await cur.fetchall()
        ]
        cur = await db.execute("SELECT user_id, ticker FROM subscriptions")
        watch_count: dict[str, int] = {}
        for guild_id, _ticker in await cur.fetchall():
            watch_count[guild_id] = watch_count.get(guild_id, 0) + 1
        cur = await db.execute("SELECT guild_id FROM banned_guilds")
        banned = {r[0] for r in await cur.fetchall()}

    for g in guilds:
        g["watchlist_size"] = watch_count.get(g["guild_id"], 0)
        g["banned"] = g["guild_id"] in banned
    return guilds
