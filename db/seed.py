"""
Run once (or any time) to initialise the DB and sync tickers from watchlist.json.

    python db/seed.py
    python db/seed.py --watchlist path/to/watchlist.json
"""
import argparse
import json
from pathlib import Path

import psycopg

from config import DATABASE_URL
from db.schema import ensure_schema

ROOT = Path(__file__).parent.parent


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--watchlist", default=str(ROOT / "watchlist.json"))
    p.add_argument("--database-url", default=DATABASE_URL)
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    with open(args.watchlist) as f:
        companies = json.load(f)

    ensure_schema(args.database_url)

    rows = [
        (
            item["ticker"],
            str(int(item["cik"])).zfill(10),
            item["title"],
        )
        for item in companies
    ]
    with psycopg.connect(args.database_url, autocommit=True) as conn, conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO tickers (ticker, cik, title) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            rows,
        )

    print(f"DB ready: {args.database_url}")
    print(f"Tickers seeded: {len(rows)}")


if __name__ == "__main__":
    main()
