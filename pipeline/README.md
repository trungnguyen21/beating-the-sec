# pipeline/

Sits between ingestion and AI analysis. Handles queueing, text normalization, AI dispatch, and Kafka producers/consumers.

---

## Files

### `queue.py`
Defines the `FilingQueue` protocol and two implementations.

**Protocol** — any queue implementation must have:
```python
async def enqueue(filing) -> None
async def consume() -> AsyncIterator[tuple[str, filing]]
async def ack(msg_id: str) -> None
```

**`InMemoryFilingQueue`** — asyncio queue backed by `asyncio.Queue`. No persistence. Used in dev mode (`python main.py` with no `--stage` flag). Fast, zero dependencies.

**`RedisFilingQueue`** — Redis Streams backed queue. Durable, survives process restarts. Consumer group aware. Written but not currently the default — available as a stepping stone before Kafka.

### `normalizer.py`
Converts raw filing content to clean, structured plain text.

- `normalize(content, content_type)` — strips HTML tags (BeautifulSoup) or extracts PDF text (PyMuPDF). Returns clean plain text.
- `split_sections(text)` — heuristically splits the normalized text into named sections:
  - `header` — company name, fiscal period, report date
  - `financials` — EPS, revenue, margin figures
  - `guidance` — forward-looking statements
  - `full` — full text (fallback)

The dispatcher sends each section to the most relevant AI worker. Guidance lines are routed to `sections["guidance"]` — if they're missing from the extraction worker's input, guidance will always be `null` in the alert.

### `dispatcher.py`
Runs all three AI workers concurrently and merges results.

```python
extraction, sentiment, risk = await asyncio.gather(
    run_extraction(financials + guidance, client, estimates),
    run_sentiment(header or full, client),
    run_risk(full, client),
    return_exceptions=True,
)
```

- Each worker gets the section most relevant to its task — keeps prompts focused and token-efficient
- `return_exceptions=True` — one worker failing doesn't kill the others; the failed result is replaced with `{}`
- Total time = slowest worker, not sum

### `kafka_consumer.py`
All Kafka-side logic: producer queue for ingestion, AI worker consumer, delivery consumer.

**`KafkaFilingQueue`** — async context manager. Wraps an `AIOKafkaProducer` and publishes slim filing metadata to the `raw_filings` topic. Used when running `--stage ingestion`.

```python
async with KafkaFilingQueue() as queue:
    await run_watcher(unified_watchlist, queue)
```

Only metadata is published (ticker, cik, accession, index_url, detected_at). Raw text is intentionally excluded — the AI worker fetches the document itself in Stage 2.

**`run_ai_worker(worker_id)`** — Kafka consumer on `raw_filings`. For each message:
1. Fetches filing document from SEC
2. Normalizes and deduplicates
3. Runs AI analysis
4. Publishes result to `filing_results`
5. On failure: publishes original message to `filing_dlq` (dead-letter)

Lifecycle: `await consumer.start()` / `await producer.start()` before the loop, both stopped in `finally`. `GeminiClient` and `httpx.AsyncClient` are created as async context managers inside the function — not at module level.

**`run_delivery_consumer()`** — Kafka consumer on `filing_results`. For each message, looks up subscribers by ticker (per-message against SQLite, no startup config needed) and calls `DiscordWebhook.send()` for each. Error-isolated per subscriber.

---

## Kafka topics

| Topic | Written by | Read by | Retention |
|---|---|---|---|
| `raw_filings` | `KafkaFilingQueue` (ingestion) | `run_ai_worker` | 7 days |
| `filing_results` | `run_ai_worker` | `run_delivery_consumer` | 7 days |
| `filing_dlq` | `run_ai_worker` (on failure) | manual inspection / replay | 30 days |

---

## Running modes

**Dev (no Kafka)**
```
main.py → run_watcher → InMemoryFilingQueue → process_filings → router.fanout
```
All in one process. InMemoryFilingQueue connects watcher output directly to the consumer loop.

**Prod (Kafka)**
```
main.py --stage ingestion  → run_watcher → KafkaFilingQueue → raw_filings topic
main.py --stage ai-worker  → run_ai_worker → filing_results topic
main.py --stage delivery   → run_delivery_consumer → Discord
```
Three independent processes. Scale `ai-worker` horizontally — Kafka consumer groups handle partition assignment automatically.
