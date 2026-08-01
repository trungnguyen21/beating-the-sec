PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

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
