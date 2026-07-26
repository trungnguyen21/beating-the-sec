# beatingTheSEC

Low-latency SEC 8-K earnings filing analyzer. Detects new filings as fast as possible, runs parallel AI analysis, and fans out structured alerts to subscribers via Discord.

---

## What it does

1. Watches a list of companies for new 8-K filings simultaneously
2. Races two detection paths per company (SEC EDGAR poller + RSS feed) — first hit wins
3. Fetches the press release (EX-99.1 exhibit), normalizes it to plain text
4. Runs three AI workers in parallel: financial extraction, sentiment, risk
5. Scores the result (bullish / neutral / bearish) and sends a Discord alert to every subscriber of that ticker

Detection-to-alert latency: ~15–30 seconds from SEC acceptance.

---

## Architecture

```
watchlist.json + users.json
        │
        ▼
┌─────────────────────────────┐
│   ingestion/watcher.py      │  one watch_company task per ticker, runs forever
│   ┌──────────────────────┐  │
│   │ EDGAR poller         │  │  polls filing index URL directly
│   │ SEC RSS feed         │  │  polls Atom feed every 1s
│   └──────────┬───────────┘  │
└──────────────┼──────────────┘
               │ DetectorResult
               ▼
        pipeline/queue.py          InMemoryFilingQueue (dev) or KafkaFilingQueue (prod)
               │
               ▼
        pipeline/normalizer.py     HTML/PDF → clean text → split sections
               │
               ▼
        ingestion/deduplicator.py  SHA-256 hash check — skip if seen
               │
               ▼
        pipeline/dispatcher.py     asyncio.gather → 3 AI workers in parallel
          ┌────┴─────┬──────────┐
          ▼          ▼          ▼
     extraction  sentiment   risk
          └────┬─────┘──────────┘
               ▼
        aggregator/aggregator.py   signal score (0.0–1.0) → label + alert dict
               │
               ▼
        delivery/router.py         SubscriptionRouter → fan-out to per-user Discord webhooks
```

---

## Running the app

```bash
# Install dependencies
uv sync

# Copy and fill in credentials
cp .env.example .env

# Dev mode — single process, in-memory queue, no Kafka needed
python main.py

# Production — three independent stages (requires Confluent Cloud)
python main.py --stage ingestion   # terminal 1: detect filings → Kafka
python main.py --stage ai-worker   # terminal 2: AI analysis → Kafka
python main.py --stage delivery    # terminal 3: fan-out → Discord
```

---

## Configuration files

| File | Purpose |
|---|---|
| `.env` | API keys and Confluent credentials (never commit) |
| `watchlist.json` | Company registry — ticker + CIK for every company the system can watch |
| `users.json` | Per-user config — which tickers they watch + their Discord webhook URL (never commit) |
| `config.py` | Shared constants: age limits, cooldown, Kafka topic names, concurrency |

---

## Key design decisions

- **Two detection paths race per company** — RSS and EDGAR poller run concurrently via `asyncio.wait(FIRST_COMPLETED)`. The loser is cancelled and awaited to prevent task leaks.
- **`seen` set lives in `watch_company`** — persists across detection races so the same filing is never returned twice in the same session.
- **Age filter is per-poll, not at startup** — filings older than `MAX_FILING_AGE_HOURS` are skipped each poll cycle. Configurable so testing with recent-but-not-fresh filings still works.
- **AI workers run in parallel** — `asyncio.gather` over extraction + sentiment + risk. Total time = slowest worker, not sum.
- **Fan-out is per-ticker** — `SubscriptionRouter` looks up only the users subscribed to the detected ticker. One AI pipeline run serves all subscribers.
- **`InMemoryFilingQueue` in dev, `KafkaFilingQueue` in prod** — same `FilingQueue` protocol, swap at startup via `--stage` flag.

---

## Directory map

| Directory | What lives there |
|---|---|
| `ingestion/` | Filing detection: EDGAR poller, RSS feed, watcher orchestration, deduplicator, estimates fetch |
| `pipeline/` | Queue abstraction, normalizer, AI dispatcher, Kafka consumer/producer |
| `workers/` | AI prompt workers: extraction, sentiment, risk + Gemini client |
| `aggregator/` | Signal scoring and alert dict construction |
| `delivery/` | Discord webhook sender, per-ticker subscription router |
| `models/` | Pydantic/dataclass data models: DetectorResult, User, Company |
| `utils/` | Logger, Kafka config helper, user config loader |
| `tests/` | E2E and unit tests — see `tests/README.md` for how to run each |
| `docs/` | Planning docs, Kafka scaling write-up, multi-user checklist |
| `data/` | Sample filing HTML files used by tests |
