import json
import uuid
from datetime import datetime, timezone, timedelta

import aiosqlite

from config import DB_PATH


class GuildNotSetUpError(Exception):
    """Raised when a guild tries /watch add|remove before /setup has created its row."""

    def __init__(self, guild_id: str):
        self.guild_id = guild_id
        super().__init__(f"Guild {guild_id} has no alert channel configured — run /setup first")


async def get_all_tickers() -> list[dict]:
    """Every ticker, active or not — used by the admin ticker-management view, which needs to
    offer the full universe regardless of current polling status."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT ticker, title, is_active FROM tickers ORDER BY ticker"
        ) as cur:
            rows = await cur.fetchall()
    return [{"ticker": r[0], "title": r[1], "is_active": bool(r[2])} for r in rows]


async def get_active_tickers() -> list[dict]:
    """Only tickers with is_active = 1 — the actual polling watchlist. A ticker becomes active
    when it gets its first guild subscriber (add_guild_tickers) and inactive again once its
    last one drops it (remove_guild_tickers/delete_user) — this keeps SEC/yfinance request
    volume tied to real demand instead of the full ~500-ticker calendar universe."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT ticker, title FROM tickers WHERE is_active = 1 ORDER BY ticker"
        ) as cur:
            rows = await cur.fetchall()
    return [{"ticker": r[0], "title": r[1]} for r in rows]


async def _deactivate_orphaned_tickers(db: aiosqlite.Connection, candidates: set[str]) -> None:
    """For each candidate ticker, set is_active = 0 if no subscription references it anymore.
    Called with the tickers a change *might* have orphaned — not every ticker in the DB — so
    callers must pass only the ones actually affected by their delete/replace."""
    if not candidates:
        return
    placeholders = ",".join("?" * len(candidates))
    await db.execute(
        f"""UPDATE tickers SET is_active = 0
            WHERE ticker IN ({placeholders})
              AND ticker NOT IN (SELECT ticker FROM subscriptions)""",
        tuple(candidates),
    )


async def _guild_exists(db: aiosqlite.Connection, guild_id: str) -> bool:
    async with db.execute("SELECT 1 FROM users WHERE id = ?", (guild_id,)) as cur:
        return await cur.fetchone() is not None


async def upsert_guild_channel(guild_id: str, name: str, webhook_url: str) -> None:
    """Insert or update a guild's alert channel — used by /setup. `id` holds the real Discord
    guild ID (not a random uuid4) so re-running /setup against a different channel updates the
    same row instead of creating an orphan. Deliberately does not touch `subscriptions` —
    rerunning /setup must never clear the watchlist, only change where alerts go."""
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO users (id, name, webhook_url, created_at)
               VALUES (?, ?, ?, ?)
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
    placeholders = ",".join("?" * len(tickers))
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys = ON")
        if not await _guild_exists(db, guild_id):
            raise GuildNotSetUpError(guild_id)

        async with db.execute(
            f"SELECT ticker FROM tickers WHERE ticker IN ({placeholders})", tickers
        ) as cur:
            known = {r[0] for r in await cur.fetchall()}
        unknown = [t for t in tickers if t not in known]

        async with db.execute(
            f"""SELECT ticker FROM subscriptions
                WHERE user_id = ? AND ticker IN ({placeholders})""",
            (guild_id, *tickers),
        ) as cur:
            already_watched = {r[0] for r in await cur.fetchall()}

        to_add = [t for t in tickers if t in known and t not in already_watched]

        await db.executemany(
            "INSERT OR IGNORE INTO subscriptions (user_id, ticker, subscribed_at) VALUES (?, ?, ?)",
            [(guild_id, t, now) for t in to_add],
        )
        await db.executemany(
            "UPDATE tickers SET is_active = 1 WHERE ticker = ?",
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

    placeholders = ",".join("?" * len(tickers))
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys = ON")
        if not await _guild_exists(db, guild_id):
            raise GuildNotSetUpError(guild_id)

        async with db.execute(
            f"""SELECT ticker FROM subscriptions
                WHERE user_id = ? AND ticker IN ({placeholders})""",
            (guild_id, *tickers),
        ) as cur:
            removed = [r[0] for r in await cur.fetchall()]

        if removed:
            await db.executemany(
                "DELETE FROM subscriptions WHERE user_id = ? AND ticker = ?",
                [(guild_id, t) for t in removed],
            )
            await _deactivate_orphaned_tickers(db, set(removed))
        await db.commit()

    return removed


async def get_guild_watchlist(guild_id: str) -> list[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT ticker FROM subscriptions WHERE user_id = ? ORDER BY ticker", (guild_id,)
        ) as cur:
            return [r[0] for r in await cur.fetchall()]


async def delete_user(user_id: str) -> bool:
    """Delete a guild's row outright (cascades subscriptions). Used by on_guild_remove and by
    the dead-webhook cleanup path in delivery/router.py. Returns False if not found. Does NOT
    touch banned_guilds — a kicked/removed bot must not clear a ban."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys = ON")

        async with db.execute("SELECT id FROM users WHERE id = ?", (user_id,)) as cur:
            if not await cur.fetchone():
                return False

        async with db.execute(
            "SELECT ticker FROM subscriptions WHERE user_id = ?", (user_id,)
        ) as cur:
            affected = {r[0] for r in await cur.fetchall()}

        await db.execute("DELETE FROM users WHERE id = ?", (user_id,))
        await _deactivate_orphaned_tickers(db, affected)
        await db.commit()
    return True


async def delete_ticker(ticker: str) -> bool:
    """Remove a ticker from the universe entirely. Cascades to its subscriptions and earnings
    calendar row (ON DELETE CASCADE), so any guild watching it silently loses that subscription
    — same tradeoff as delete_user. Returns False if the ticker doesn't exist."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys = ON")
        async with db.execute("SELECT 1 FROM tickers WHERE ticker = ?", (ticker,)) as cur:
            if not await cur.fetchone():
                return False
        await db.execute("DELETE FROM tickers WHERE ticker = ?", (ticker,))
        await db.commit()
    return True


async def set_ticker_active(ticker: str, active: bool) -> None:
    """Admin override for a ticker's polling status, independent of subscriber count."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE tickers SET is_active = ? WHERE ticker = ?", (1 if active else 0, ticker)
        )
        await db.commit()


async def set_all_tickers_active(active: bool) -> None:
    """Bulk admin override — activate/deactivate every ticker regardless of subscribers."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE tickers SET is_active = ?", (1 if active else 0,))
        await db.commit()


async def create_ticker(ticker: str, cik: str, title: str) -> bool:
    """Add a new company to the ticker universe. Active by default — an admin adding one by
    hand means they want it polled. Returns False if the ticker already exists."""
    ticker = ticker.strip().upper()
    cik = str(int(cik)).zfill(10)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM tickers WHERE ticker = ?", (ticker,)) as cur:
            if await cur.fetchone():
                return False
        await db.execute(
            "INSERT INTO tickers (ticker, cik, title, is_active) VALUES (?, ?, ?, 1)",
            (ticker, cik, title),
        )
        await db.commit()
    return True


async def get_subscribers_for_ticker(ticker: str) -> list[dict]:
    """Returns [{id, webhook_url}] for all guilds subscribed to ticker."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """SELECT u.id, u.webhook_url
               FROM users u
               JOIN subscriptions s ON u.id = s.user_id
               WHERE s.ticker = ?""",
            (ticker,),
        ) as cur:
            rows = await cur.fetchall()
    return [{"id": r[0], "webhook_url": r[1]} for r in rows]


async def save_alert(ticker: str, alert: dict) -> None:
    """Persist a processed alert so new subscribers can receive it as a catch-up."""
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO alerts (id, ticker, alert_json, detected_at, created_at) VALUES (?, ?, ?, ?, ?)",
            (str(uuid.uuid4()), ticker, json.dumps(alert), alert.get("detected_at", now), now),
        )
        await db.commit()


async def get_recent_alerts(ticker: str, hours: int = 1) -> list[dict]:
    """Return alerts for ticker detected within the last N hours, newest first."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """SELECT alert_json FROM alerts
               WHERE ticker = ? AND detected_at > ?
               ORDER BY detected_at DESC""",
            (ticker, cutoff),
        ) as cur:
            rows = await cur.fetchall()
    return [json.loads(r[0]) for r in rows]


# --- Bot visibility + ban/unban kill switch --------------------------------

async def log_command(
    guild_id: str | None, discord_user_id: str, command: str, args: str, success: bool
) -> None:
    """One row per slash-command invocation — the only source for "active users" and "commands
    per day", neither of which is derivable from current-state tables. Called from a single
    wrapper around every command handler, not duplicated per-command."""
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO bot_command_log
               (id, guild_id, discord_user_id, command, args, success, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (str(uuid.uuid4()), guild_id, discord_user_id, command, args, 1 if success else 0, now),
        )
        await db.commit()


async def is_guild_banned(guild_id: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM banned_guilds WHERE guild_id = ?", (guild_id,)
        ) as cur:
            return await cur.fetchone() is not None


async def ban_guild(guild_id: str, reason: str = "") -> None:
    """Separate from `users` on purpose — must survive the guild's row being deleted (kicked bot,
    on_guild_remove) so re-inviting to dodge a ban doesn't work. /setup checks this first."""
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO banned_guilds (guild_id, reason, banned_at) VALUES (?, ?, ?)
               ON CONFLICT(guild_id) DO UPDATE SET reason = excluded.reason,
                                                    banned_at = excluded.banned_at""",
            (guild_id, reason, now),
        )
        await db.commit()


async def unban_guild(guild_id: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM banned_guilds WHERE guild_id = ?", (guild_id,))
        await db.commit()


async def get_bot_stats(active_window_days: int = 30) -> dict:
    """Guild count, "active" users (ran a command in the window — a bot has no way to see who
    merely read an alert, with or without privileged intents), commands today, and a day-by-day
    breakdown for the admin stats panel."""
    since = (datetime.now(timezone.utc) - timedelta(days=active_window_days)).isoformat()
    today = datetime.now(timezone.utc).date().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as cur:
            guild_count = (await cur.fetchone())[0]

        async with db.execute(
            "SELECT COUNT(DISTINCT discord_user_id) FROM bot_command_log WHERE created_at >= ?",
            (since,),
        ) as cur:
            active_users = (await cur.fetchone())[0]

        async with db.execute(
            "SELECT COUNT(*) FROM bot_command_log WHERE substr(created_at, 1, 10) = ?", (today,)
        ) as cur:
            commands_today = (await cur.fetchone())[0]

        async with db.execute(
            """SELECT substr(created_at, 1, 10) AS day, COUNT(*)
               FROM bot_command_log WHERE created_at >= ? GROUP BY day ORDER BY day""",
            (since,),
        ) as cur:
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
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, name, webhook_url, created_at FROM users ORDER BY created_at DESC"
        ) as cur:
            guilds = [
                {"guild_id": r[0], "name": r[1], "webhook_url": r[2], "created_at": r[3]}
                for r in await cur.fetchall()
            ]
        async with db.execute("SELECT user_id, ticker FROM subscriptions") as cur:
            watch_count: dict[str, int] = {}
            for guild_id, _ticker in await cur.fetchall():
                watch_count[guild_id] = watch_count.get(guild_id, 0) + 1
        async with db.execute("SELECT guild_id FROM banned_guilds") as cur:
            banned = {r[0] for r in await cur.fetchall()}

    for g in guilds:
        g["watchlist_size"] = watch_count.get(g["guild_id"], 0)
        g["banned"] = g["guild_id"] in banned
    return guilds
