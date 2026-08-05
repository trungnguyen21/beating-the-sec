"""
Daily earnings-calendar refresh. Reads every ticker from the Postgres `tickers` table (NOT
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
    python -m db.refresh_calendar --database-url postgresql://...
"""

import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path

import psycopg

from config import DATABASE_URL
from db.schema import ensure_schema
from ingestion.estimates import fetch_next_earnings_datetime, ticker_exists
from utils.logger import get_logger

logger = get_logger(__name__)

ROOT = Path(__file__).parent.parent


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--database-url", default=DATABASE_URL)
    return p.parse_args()


def _load_tickers(database_url: str) -> list[str]:
    with psycopg.connect(database_url) as conn:
        rows = conn.execute("SELECT ticker FROM tickers ORDER BY ticker").fetchall()
    return [r[0] for r in rows]


def _remove_tickers(database_url: str, tickers: list[str]) -> None:
    """Deletes from `tickers` — cascades (ON DELETE CASCADE) to earnings_calendar and
    subscriptions, since a ticker yfinance can't find at all has no business staying in the
    polling list or the calendar. `alerts` is untouched — it's a historical log, not a FK."""
    with psycopg.connect(database_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.executemany("DELETE FROM tickers WHERE ticker = %s", [(t,) for t in tickers])


def _upsert_calendar(database_url: str, results: dict[str, tuple[str | None, str | None]]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with psycopg.connect(database_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO earnings_calendar (ticker, earnings_date, earnings_time, updated_at)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT(ticker) DO UPDATE SET earnings_date = excluded.earnings_date,
                                                  earnings_time = excluded.earnings_time,
                                                  updated_at = excluded.updated_at""",
            [(ticker, date, time_, now) for ticker, (date, time_) in results.items()],
        )


async def refresh(database_url: str) -> None:
    ensure_schema(database_url)
    tickers = _load_tickers(database_url)
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

    _upsert_calendar(database_url, results)

    if invalid:
        _remove_tickers(database_url, invalid)
        logger.warning(f"Removed {len(invalid)} not-found ticker(s) from tickers/calendar: {invalid}")

    logger.info(f"Calendar refresh complete: {found} with a date, {missing} without, {len(tickers)} total")


def main() -> None:
    args = _parse_args()
    asyncio.run(refresh(args.database_url))


if __name__ == "__main__":
    main()
