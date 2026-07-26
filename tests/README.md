# tests/

Two categories: unit tests (fast, no external calls) and e2e tests (real API calls, real Discord messages).

---

## E2E Tests

### `test_e2e_parsing_pipeline.py` — single-process e2e
Tests the full pipeline without Kafka: normalize → AI analysis → aggregate → Discord fan-out.

```bash
# Uses data/DELL.html, sends alert to NVDA subscribers in users.json
python tests/test_e2e_parsing_pipeline.py

# Use a specific filing file and ticker
python tests/test_e2e_parsing_pipeline.py path/to/filing.html TICKER
```

Steps: load filing → normalize → fetch estimates → 3 AI workers → aggregate → `SubscriptionRouter.fanout()`

**Requires**: `GEMINI_API_KEY` in `.env`, real Discord webhook in `users.json`, ticker must be in a user's watchlist.

**What it proves**: normalization, AI analysis, scoring, and Discord delivery all work end-to-end. Does not touch Kafka.

---

### `test_e2e_kafka.py` — Kafka integration test
Tests the full three-stage Kafka pipeline: publish to `raw-filings` → AI worker consumes → `filing-results` → delivery consumer → Discord.

```bash
python tests/test_e2e_kafka.py

# Custom filing and ticker
python tests/test_e2e_kafka.py path/to/filing.html TICKER
```

**Mocked**: `fetch_filing_document` (returns `data/DELL.html`), `fetch_estimates` (returns `{}`)

**Real**: Confluent Kafka broker, Gemini AI, Discord webhook

**Important — consumer ordering**: Workers start first, sleep 5 seconds to connect, then the message is published. With `auto_offset_reset="latest"` (aiokafka default), publishing before the consumer connects means the message is missed entirely.

**Timeout**: 120 seconds. On timeout, diagnostic output shows which stage stalled:
```
AI consumed message : YES/NO
Discord delivered   : YES/NO
```

**Requires**: `CONFLUENT_BOOTSTRAP`, `CONFLUENT_API_KEY`, `CONFLUENT_API_SECRET` in `.env`, Confluent cluster active (Basic clusters auto-pause — resume in the UI if needed), `raw-filings` and `filing-results` topics created.

---

### `test_e2e_rss_detection.py` — RSS detection test
Tests the SEC RSS feed detection path live against real SEC data.

```bash
python tests/test_e2e_rss_detection.py
```

---

## Unit Tests

### `test_normalizer.py`
Tests `pipeline/normalizer.py` — HTML stripping, PDF extraction, section splitting.

### `test_rss.py`
Tests `ingestion/sec_rss.py` — feed parsing, age filtering, `seen` set deduplication.

### `test_workers.py`
Tests the AI workers with fixture text (mocked Gemini client).

---

## Sample data

`data/DELL.html` — Dell Technologies earnings press release. Used as the default filing input for both e2e tests when no file path is provided.
