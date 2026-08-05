"""
Applies db/init.sql idempotently against DATABASE_URL — schema only, no ticker seeding.

db/seed.py already calls ensure_schema() too, but it's bundled with re-seeding `tickers` from
watchlist.json. This is the schema-only equivalent, for pushing a schema change (e.g. the new
filing_analysis table) to an already-running deployment without touching ticker data.

Safe to re-run anytime — every statement in db/init.sql is idempotent (CREATE TABLE IF NOT
EXISTS / ADD COLUMN IF NOT EXISTS).

Run locally:
    python -m db.migrate

Run against the prod droplet's stack without rebuilding or restarting any already-running
service — reuses the existing `migrate` one-off's image/config from docker-compose.yml, just
overriding its command:
    docker compose run --rm migrate python -m db.migrate
"""
import argparse

from config import DATABASE_URL
from db.schema import ensure_schema


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--database-url", default=DATABASE_URL)
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    ensure_schema(args.database_url)
    print(f"Schema migration applied: {args.database_url}")


if __name__ == "__main__":
    main()
