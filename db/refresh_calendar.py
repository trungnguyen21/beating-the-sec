"""
Daily earnings-calendar refresh. Reads every ticker from the SQLite `tickers` table (NOT
watchlist.json directly — run `db/seed.py` first/periodically to keep `tickers` in sync from
watchlist.json), sequentially fetches each one's next earnings date via yfinance, and upserts
the result into `earnings_calendar`. Sequential and slow on purpose — see docs/checklist.md
Phase 6 for why concurrent fetching was rejected (undocumented Yahoo rate limiting risk).

ingestion/rotation_scheduler.py's CalendarHotSet reads this table to decide which companies
are "hot" (actively polled) on a given day — see docs/checklist.md Phase 6c.

Meant to run once a day via external cron, in ET:
    0 5 * * * cd <repo> && python -m db.refresh_calendar

The schedule's timezone is the cron daemon's responsibility — this script doesn't need to know
what day it is when invoked, it just refreshes the whole {ticker: date} mapping regardless.

Run manually with:
    python -m db.refresh_calendar
    python -m db.refresh_calendar --db path/to/other.db
"""

import argparse
import asyncio
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from ingestion.estimates import fetch_next_earnings_datetime, ticker_exists
from utils.logger import get_logger

logger = get_logger(__name__)

ROOT = Path(__file__).parent.parent


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=str(ROOT / "bsec.db"))
    return p.parse_args()


def _ensure_schema(db_path: str) -> None:
    schema = (ROOT / "db" / "init.sql").read_text()
    conn = sqlite3.connect(db_path)
    conn.executescript(schema)
    # CREATE TABLE IF NOT EXISTS is a no-op on an already-existing earnings_calendar table
    # (e.g. from before earnings_time was added) — migrate it explicitly.
    columns = {row[1] for row in conn.execute("PRAGMA table_info(earnings_calendar)")}
    if "earnings_time" not in columns:
        conn.execute("ALTER TABLE earnings_calendar ADD COLUMN earnings_time TEXT")
    conn.commit()
    conn.close()


def _load_tickers(db_path: str) -> list[str]:
    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT ticker FROM tickers ORDER BY ticker").fetchall()
    conn.close()
    return [r[0] for r in rows]


def _remove_tickers(db_path: str, tickers: list[str]) -> None:
    """Deletes from `tickers` — cascades (ON DELETE CASCADE) to earnings_calendar and
    subscriptions, since a ticker yfinance can't find at all has no business staying in the
    polling list or the calendar. `alerts` is untouched — it's a historical log, not a FK."""
    conn = sqlite3.connect(db_path)
    conn.executemany("DELETE FROM tickers WHERE ticker = ?", [(t,) for t in tickers])
    conn.commit()
    conn.close()


def _upsert_calendar(db_path: str, results: dict[str, tuple[str | None, str | None]]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    conn.executemany(
        """INSERT INTO earnings_calendar (ticker, earnings_date, earnings_time, updated_at)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(ticker) DO UPDATE SET earnings_date = excluded.earnings_date,
                                              earnings_time = excluded.earnings_time,
                                              updated_at = excluded.updated_at""",
        [(ticker, date, time_, now) for ticker, (date, time_) in results.items()],
    )
    conn.commit()
    conn.close()


async def refresh(db_path: str) -> None:
    _ensure_schema(db_path)
    tickers = _load_tickers(db_path)
    logger.info(f"Refreshing earnings calendar for {len(tickers)} ticker(s) — sequential, this will take a while")

    results: dict[str, tuple[str | None, str | None]] = {}
    invalid: list[str] = []
    found = 0
    missing = 0

    for i, ticker in enumerate(tickers, start=1):
        date, time_ = await fetch_next_earnings_datetime(ticker)
        if date is None:
            # Only tickers with no upcoming row are candidates for "not found" — a valid
            # ticker just not reporting soon must never be flagged here.
            if not await ticker_exists(ticker):
                invalid.append(ticker)
                missing += 1
                continue
        results[ticker] = (date, time_)
        if date:
            found += 1
        else:
            missing += 1
        if i % 50 == 0:
            logger.info(f"  progress: {i}/{len(tickers)} ({found} found, {missing} no date)")

    _upsert_calendar(db_path, results)

    if invalid:
        _remove_tickers(db_path, invalid)
        logger.warning(f"Removed {len(invalid)} not-found ticker(s) from tickers/calendar: {invalid}")

    logger.info(f"Calendar refresh complete: {found} with a date, {missing} without, {len(tickers)} total")


def main() -> None:
    args = _parse_args()
    asyncio.run(refresh(args.db))


if __name__ == "__main__":
    main()
