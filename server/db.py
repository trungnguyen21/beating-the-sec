import json
import uuid
from datetime import datetime, timezone, timedelta

import aiosqlite

from config import DB_PATH


async def get_all_tickers() -> list[dict]:
    """Every ticker, active or not — used by the subscribe UI, which needs to offer the full
    universe of tickers regardless of current polling status."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT ticker, title FROM tickers ORDER BY ticker") as cur:
            rows = await cur.fetchall()
    return [{"ticker": r[0], "title": r[1]} for r in rows]


async def get_active_tickers() -> list[dict]:
    """Only tickers with is_active = 1 — the actual polling watchlist. A ticker becomes active
    when it gets its first subscriber (upsert_user_subscriptions) and inactive again once its
    last subscriber unsubscribes (delete_subscriptions_by_webhook) — this keeps SEC/yfinance
    request volume tied to real demand instead of the full ~500-ticker calendar universe."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT ticker, title FROM tickers WHERE is_active = 1 ORDER BY ticker"
        ) as cur:
            rows = await cur.fetchall()
    return [{"ticker": r[0], "title": r[1]} for r in rows]


async def upsert_user_subscriptions(name: str, webhook_url: str, tickers: list[str]) -> str:
    """
    Insert or update a user by webhook_url, then replace their subscriptions.
    Returns the user's id.
    """
    now = datetime.now(timezone.utc).isoformat()
    new_id = str(uuid.uuid4())

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys = ON")

        # Upsert user: on webhook_url conflict just update the name.
        # sqlite RETURNING is available from 3.35 — use a separate SELECT to be safe.
        await db.execute(
            """INSERT INTO users (id, name, webhook_url, created_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(webhook_url) DO UPDATE SET name = excluded.name""",
            (new_id, name, webhook_url, now),
        )
        async with db.execute(
            "SELECT id FROM users WHERE webhook_url = ?", (webhook_url,)
        ) as cur:
            row = await cur.fetchone()
        user_id = row[0]

        # Replace subscriptions for this user
        async with db.execute(
            "SELECT ticker FROM subscriptions WHERE user_id = ?", (user_id,)
        ) as cur:
            previous_tickers = [r[0] for r in await cur.fetchall()]

        await db.execute("DELETE FROM subscriptions WHERE user_id = ?", (user_id,))
        await db.executemany(
            "INSERT OR IGNORE INTO subscriptions (user_id, ticker, subscribed_at) VALUES (?, ?, ?)",
            [(user_id, t, now) for t in tickers],
        )

        # A subscribed ticker must be pollable — flip it active regardless of its prior state.
        await db.executemany(
            "UPDATE tickers SET is_active = 1 WHERE ticker = ?",
            [(t,) for t in tickers],
        )
        # A ticker this user dropped, with no other subscriber left, stops being pollable.
        dropped = set(previous_tickers) - set(tickers)
        await _deactivate_orphaned_tickers(db, dropped)

        await db.commit()

    return user_id


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


async def delete_subscriptions_by_webhook(webhook_url: str) -> bool:
    """Remove all subscriptions for a user identified by webhook URL. Returns True if found."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys = ON")
        async with db.execute(
            "SELECT id FROM users WHERE webhook_url = ?", (webhook_url,)
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return False
        user_id = row[0]

        async with db.execute(
            "SELECT ticker FROM subscriptions WHERE user_id = ?", (user_id,)
        ) as cur:
            affected = {r[0] for r in await cur.fetchall()}

        await db.execute("DELETE FROM subscriptions WHERE user_id = ?", (user_id,))
        await _deactivate_orphaned_tickers(db, affected)
        await db.commit()
    return True


async def get_subscribers_for_ticker(ticker: str) -> list[dict]:
    """Returns [{id, webhook_url}] for all users subscribed to ticker."""
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
