"""
Run once (or any time) to initialise the DB and sync tickers from watchlist.json.

    python db/seed.py
    python db/seed.py --watchlist path/to/watchlist.json
"""
import argparse
import json
import sqlite3
from pathlib import Path

from db.schema import ensure_schema

ROOT = Path(__file__).parent.parent


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--watchlist", default=str(ROOT / "watchlist.json"))
    p.add_argument("--db", default=str(ROOT / "bsec.db"))
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    with open(args.watchlist) as f:
        companies = json.load(f)

    ensure_schema(args.db)
    conn = sqlite3.connect(args.db)

    rows = [
        (
            item["ticker"],
            str(int(item["cik"])).zfill(10),
            item["title"],
        )
        for item in companies
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO tickers (ticker, cik, title) VALUES (?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()

    print(f"DB ready: {args.db}")
    print(f"Tickers seeded: {len(rows)}")


if __name__ == "__main__":
    main()
