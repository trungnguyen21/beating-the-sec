CREATE TABLE IF NOT EXISTS tickers (
    ticker    TEXT PRIMARY KEY,
    cik       TEXT NOT NULL,
    title     TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 0  -- only active tickers get polled; set by subscription
);

CREATE TABLE IF NOT EXISTS users (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    webhook_url TEXT NOT NULL UNIQUE,
    created_at  TEXT NOT NULL
);

-- Many-to-many: one user can subscribe to many tickers
CREATE TABLE IF NOT EXISTS subscriptions (
    user_id       TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    ticker        TEXT NOT NULL REFERENCES tickers(ticker) ON DELETE CASCADE,
    subscribed_at TEXT NOT NULL,
    PRIMARY KEY (user_id, ticker)
);

-- Stores processed alerts so new subscribers can receive recent ones on sign-up
CREATE TABLE IF NOT EXISTS alerts (
    id          TEXT PRIMARY KEY,
    ticker      TEXT NOT NULL,
    alert_json  TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_alerts_ticker_detected ON alerts(ticker, detected_at);
CREATE INDEX IF NOT EXISTS idx_subscriptions_ticker   ON subscriptions(ticker);

-- Daily-refreshed next-earnings-date snapshot, used by ingestion/rotation_scheduler.py's
-- CalendarHotSet to decide which companies are "hot" today. Nullable earnings_date — a ticker
-- with no known upcoming date just never enters the hot set, not an error.
CREATE TABLE IF NOT EXISTS earnings_calendar (
    ticker        TEXT PRIMARY KEY REFERENCES tickers(ticker) ON DELETE CASCADE,
    earnings_date TEXT,
    earnings_time TEXT,  -- ET "HH:MM:SS", NULL if yfinance gave no usable time
    updated_at    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_earnings_calendar_date ON earnings_calendar(earnings_date);

-- One row per Discord slash-command invocation. Not FK'd to users/guilds on purpose: a guild
-- can be deleted or banned and its history should still be queryable, and a command (e.g.
-- /setup) can arrive before that guild has a users row yet.
CREATE TABLE IF NOT EXISTS bot_command_log (
    id              TEXT PRIMARY KEY,
    guild_id        TEXT,
    discord_user_id TEXT NOT NULL,
    command         TEXT NOT NULL,
    args            TEXT,
    success         INTEGER NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bot_command_log_created ON bot_command_log(created_at);
CREATE INDEX IF NOT EXISTS idx_bot_command_log_user     ON bot_command_log(discord_user_id);

-- Separate from `users` on purpose: a ban must survive the banned guild's row being deleted
-- (e.g. they kick the bot to dodge it, or on_guild_remove fires after the ban) — /setup checks
-- this table independently before it's allowed to (re)create a guild's row.
CREATE TABLE IF NOT EXISTS banned_guilds (
    guild_id  TEXT PRIMARY KEY,
    reason    TEXT,
    banned_at TEXT NOT NULL
);

-- Columns added after a DB was first created — IF NOT EXISTS keeps this idempotent against a
-- fresh DB (where init.sql's CREATE TABLE already includes them) as well as an existing one.
ALTER TABLE earnings_calendar ADD COLUMN IF NOT EXISTS earnings_time TEXT;
ALTER TABLE tickers ADD COLUMN IF NOT EXISTS is_active INTEGER NOT NULL DEFAULT 0;

-- Backfill: a ticker that already has subscribers should stay pollable rather than silently
-- going dark the moment this migration runs. Re-running this UPDATE is a no-op once every
-- subscribed ticker is already active, so it's safe to leave in the idempotent schema script.
UPDATE tickers SET is_active = 1
WHERE ticker IN (SELECT DISTINCT ticker FROM subscriptions) AND is_active = 0;
