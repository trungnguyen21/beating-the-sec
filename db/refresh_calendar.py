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

from ingestion.estimates import fetch_next_earnings_date
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
    conn.commit()
    conn.close()


def _load_tickers(db_path: str) -> list[str]:
    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT ticker FROM tickers ORDER BY ticker").fetchall()
    conn.close()
    return [r[0] for r in rows]


def _upsert_calendar(db_path: str, results: dict[str, str | None]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    conn.executemany(
        """INSERT INTO earnings_calendar (ticker, earnings_date, updated_at)
           VALUES (?, ?, ?)
           ON CONFLICT(ticker) DO UPDATE SET earnings_date = excluded.earnings_date,
                                              updated_at = excluded.updated_at""",
        [(ticker, date, now) for ticker, date in results.items()],
    )
    conn.commit()
    conn.close()


async def refresh(db_path: str) -> None:
    _ensure_schema(db_path)
    tickers = _load_tickers(db_path)
    logger.info(f"Refreshing earnings calendar for {len(tickers)} ticker(s) — sequential, this will take a while")

    results: dict[str, str | None] = {}
    found = 0
    missing = 0

    for i, ticker in enumerate(tickers, start=1):
        date = await fetch_next_earnings_date(ticker)
        results[ticker] = date
        if date:
            found += 1
        else:
            missing += 1
        if i % 50 == 0:
            logger.info(f"  progress: {i}/{len(tickers)} ({found} found, {missing} no date)")

    _upsert_calendar(db_path, results)
    logger.info(f"Calendar refresh complete: {found} with a date, {missing} without, {len(tickers)} total")


def main() -> None:
    args = _parse_args()
    asyncio.run(refresh(args.db))


if __name__ == "__main__":
    main()
