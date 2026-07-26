# ingestion/

Responsible for detecting new 8-K filings as fast as possible and pushing them into the queue. Nothing here touches AI or delivery — it only finds filings.

---

## Files

### `watcher.py`
Top-level orchestration. Entry point is `run_watcher(watchlist, queue)`.

- Spawns one `watch_company` task per company via `asyncio.gather` — all companies are watched in parallel
- `watch_company` loops forever: detect → enqueue → cooldown → repeat
- After each detection it waits `POST_FILING_COOLDOWN_HOURS` (config) before watching again
- Owns a `seen: set[str]` per company — persists across detection races to prevent the same filing being returned twice

**`_detect_filing(company, client, seen)`** — races two detectors:
```
asyncio.wait([edgar_poller_task, rss_task], FIRST_COMPLETED)
```
The winning task returns a `DetectorResult`. The losing task is cancelled and awaited (important — skipping the await leaks tasks).

### `edgar_poller.py`
Polls the SEC EDGAR filing index directly.

- `get_last_accession(cik)` — fetches the company's latest accession number from the SEC submissions API
- `next_accession(accession)` — predicts the next accession number by incrementing the sequence
- `poll_for_filing(cik, predicted_accession)` — hits the predicted index URL every second until it returns 200
- `fetch_filing_document(index_url)` — parses the filing index page and returns the raw HTML of the EX-99.1 exhibit (the press release). Matches on `"EX-99.1" in doc_type` — substring match, not equality, because SEC sometimes appends suffixes.

**Why predict the accession?** The EDGAR submissions API has a slight lag. By predicting the next accession and polling the index URL directly, we can detect the filing the moment it's accepted — before the submissions API reflects it.

### `sec_rss.py`
Polls the SEC EDGAR Atom feed as a parallel detection path.

- `detect_via_rss(company, client, seen, max_age_hours)` — polls `browse-edgar?action=getcompany&type=8-K&output=atom` every 1 second
- Per-poll soft age check: entries older than `max_age_hours` are skipped (not blocked on) — keeps the loop live for testing with recent filings
- `seen` is passed in from the caller (`watch_company`) so it persists across races
- Classifies the `items-desc` field via `classifier.py` before returning — skips non-actionable 8-Ks immediately

### `classifier.py`
Maps SEC item numbers to pipeline types.

```
"2.02" → EARNINGS
"5.02" → LEADERSHIP_CHANGE
"2.01" → ACQUISITION
"4.02" → RESTATEMENT
"1.03" → BANKRUPTCY
"2.05" → RESTRUCTURING
```

`classify_items(items_desc)` parses the RSS `items-desc` field (e.g. `"2.02, 9.01"`) and returns only the actionable pipelines. If none match, the filing is skipped before any network fetch.

### `deduplicator.py`
Prevents the same filing content from being processed twice.

- `Deduplicator.is_new(content)` — SHA-256 of the first 2000 characters. Returns `True` on first call, `False` on duplicates.
- Instance method — each consumer owns its own `Deduplicator`. In the Kafka multi-worker path, use Redis-backed dedup instead (see `docs/checklist.md` Phase 10).

### `estimates.py`
Fetches analyst consensus estimates for a ticker (EPS and revenue). Used to enrich the AI extraction prompt so the worker can compare actuals against expectations.

---

## Data flow out

Each detection path returns a `DetectorResult` (see `models/model_detectors.py`):
- `ticker`, `cik`, `accession`, `index_url` — filing identity
- `detected_at` — ISO-8601 UTC string (display only)
- `filed_at` — tz-aware `datetime` (used for age arithmetic)
- `source` — `RSS_POLLER` or `EDGAR_POLLER`
- `raw_text` — populated by `watch_company` after detection, before enqueue
- `estimates` — populated in parallel with `raw_text` fetch

---

## Key constraints

- **SEC rate limit**: 10 requests/second. The poller respects this with `asyncio.sleep(1)` between polls.
- **CIKs must be zero-padded to 10 digits** in archive URLs (e.g. `0001045810`). The `Company` Pydantic model auto-pads on load via a `field_validator`.
- **EX-99.1 must be matched by substring**, not equality — `"EX-99.1" in doc_type`. The `8-K` row appears first in the index and points to the XBRL viewer, not the press release.
