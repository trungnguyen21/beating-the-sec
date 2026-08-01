"""
Shared schema application + migration logic for bsec.db, used by both db/seed.py and
db/refresh_calendar.py so the two entry points can't drift out of sync.

CREATE TABLE IF NOT EXISTS (in init.sql) is a no-op against a table that already exists —
so columns added to the schema after a DB was first created (earnings_time, is_active) need
an explicit ALTER TABLE migration here, gated on PRAGMA table_info.
"""

import sqlite3
from pathlib import Path

ROOT = Path(__file__).parent.parent


def ensure_schema(db_path: str) -> None:
    schema = (ROOT / "db" / "init.sql").read_text()
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(schema)

        ec_columns = {row[1] for row in conn.execute("PRAGMA table_info(earnings_calendar)")}
        if "earnings_time" not in ec_columns:
            conn.execute("ALTER TABLE earnings_calendar ADD COLUMN earnings_time TEXT")

        t_columns = {row[1] for row in conn.execute("PRAGMA table_info(tickers)")}
        if "is_active" not in t_columns:
            conn.execute("ALTER TABLE tickers ADD COLUMN is_active INTEGER NOT NULL DEFAULT 0")
            # Backfill: a ticker that already has subscribers should stay pollable rather than
            # silently going dark the moment this migration runs.
            conn.execute(
                """UPDATE tickers SET is_active = 1
                   WHERE ticker IN (SELECT DISTINCT ticker FROM subscriptions)"""
            )

        conn.commit()
    finally:
        conn.close()
