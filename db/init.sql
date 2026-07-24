PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS tickers (
    ticker TEXT PRIMARY KEY,
    cik    TEXT NOT NULL,
    title  TEXT NOT NULL
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
