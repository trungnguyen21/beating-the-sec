"""
Shared schema application logic for the Postgres DB, used by both db/seed.py and
db/refresh_calendar.py so the two entry points can't drift out of sync.

init.sql is written to be safely re-run against an existing DB: CREATE TABLE IF NOT EXISTS is a
no-op against a table that already exists, and columns added to the schema after a DB was first
created (earnings_time, is_active) use ALTER TABLE ... ADD COLUMN IF NOT EXISTS instead of a
separate migration step.
"""

from pathlib import Path

import psycopg

ROOT = Path(__file__).parent.parent


def ensure_schema(database_url: str) -> None:
    schema = (ROOT / "db" / "init.sql").read_text()
    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute(schema)
