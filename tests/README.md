# tests/

Uses `pytest` + `pytest-asyncio` (`asyncio_mode = "auto"` in `pyproject.toml` — `async def
test_...` functions just work, no `@pytest.mark.asyncio` needed).

Two categories, split by the `e2e` marker (registered in `pyproject.toml`):

- **Unit / mocked tests** — fast, no external calls. Run by default.
- **`@pytest.mark.e2e`** — hits a real external service (SEC EDGAR, Gemini, Kafka, Discord).
  Excluded by default via `addopts = "-m 'not e2e'"` in `pyproject.toml`.

```bash
# Default: unit/mocked tests only
pytest

# Everything, including e2e
pytest -m e2e

# A specific e2e test
pytest -m e2e tests/test_rss.py
```

---

## E2E Tests (`@pytest.mark.e2e`)

### `test_e2e_parsing_pipeline.py`
Full pipeline without Kafka: normalize → AI analysis → aggregate → Discord fan-out.
Override the sample filing via `SAMPLE_FILING_PATH` / `SAMPLE_FILING_TICKER` env vars
(defaults: `data/DELL.html`, ticker `NVDA`).

Steps: load filing → normalize → fetch estimates → 3 AI workers → aggregate → `SubscriptionRouter.fanout()`

**Requires**: `GEMINI_API_KEY` in `.env`. Skips (doesn't fail) if the ticker has no
subscribers in `users.json` — the alert is still built and asserted on, just not sent.

**What it proves**: normalization, AI analysis, scoring, and Discord delivery all work
end-to-end. Does not touch Kafka.

---

### `test_e2e_kafka.py`
Full three-stage Kafka pipeline: publish to `raw-filings` → AI worker consumes →
`filing-results` → delivery consumer → Discord. Override via `SAMPLE_FILING_PATH` /
`SAMPLE_FILING_TICKER` env vars (defaults: `data/DELL.html`, ticker `DELL`).

**Mocked**: `fetch_filing_document` (returns `data/DELL.html`), `fetch_estimates` (returns `{}`)

**Real**: Confluent Kafka broker, Gemini AI, Discord webhook

**Important — consumer ordering**: Workers start first, sleep 5 seconds to connect, then the
message is published. With `auto_offset_reset="latest"` (aiokafka default), publishing before
the consumer connects means the message is missed entirely.

**Timeout**: 120 seconds. On timeout, `pytest.fail` includes a diagnosis of which stage
stalled (AI consumed? Discord delivered?).

**Requires**: `CONFLUENT_BOOTSTRAP`, `CONFLUENT_API_KEY`, `CONFLUENT_API_SECRET` in `.env`,
an active Confluent cluster (Basic clusters auto-pause — resume in the UI if needed),
`raw-filings` and `filing-results` topics created. Skips if the ticker has no subscribers.

---

### `test_rss.py`
Hits live SEC EDGAR — confirms the RSS feed parses and at least one matching entry's exhibit
document can be fetched and is non-empty.

### `test_rotation_scheduler.py`
Integration test for `ingestion/rotation_scheduler.py` against live SEC EDGAR — confirms
rotation cycling (every company still gets polled at least once when `detection_budget <
company count`) and per-company error isolation (one company's forced failure never blocks
the others in the same tick).

### `test_workers.py`
Hits the real Gemini client — `extraction_worker` / `sentiment_worker` / `risk_worker`
against fixture press-release text.

---

## Unit Tests (no marker — run by default)

### `test_normalizer.py`
Tests `pipeline/normalizer.py` — HTML stripping, section splitting. `test_pdf_parser` is
skipped (`pytest.mark.skip`) — no bundled sample PDF in this repo.

### `test_e2e_rss_detection.py`
Fully mocked (no real network) — despite the filename, this one doesn't carry the `e2e`
marker. Verifies the RSS-feed-vs-EDGAR-poller race, `DetectorResult` field extraction,
document fetch, and normalization/section-splitting via `unittest.mock`.

### `test_rotation_cluster_simulation.py`
Pure scheduling-logic simulation (no real network, seeded RNG for determinism) — seeds a
30-company cluster, mocks `poll_company_once` to fire randomly, confirms the rotation
scheduler's worst-case-latency formula and cooldown-based dedup hold up under load.

---

## Sample data

`data/DELL.html` — Dell Technologies earnings press release. Used as the default filing input
for the parsing-pipeline and Kafka e2e tests when no override path is given.
