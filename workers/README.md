# workers/

AI prompt workers. Each worker receives a section of the normalized filing text, calls Gemini, and returns a structured JSON dict. All three run in parallel via `pipeline/dispatcher.py`.

---

## Files

### `client.py`
`AIClient` protocol + `GeminiClient` implementation.

- Model: `gemini-2.5-flash` (configurable at init)
- `invoke(prompt, text, max_tokens)` — appends `text[:8000]` to the prompt, calls Gemini with `response_mime_type="application/json"`, parses the response
- Strips markdown code fences if Gemini wraps its JSON in them despite the mime type hint
- Returns `{}` on any error — workers are expected to handle empty dicts gracefully
- Implements async context manager (`__aenter__` / `__aexit__`) so it can be used as `async with GeminiClient() as client`

**Required env var**: `GEMINI_API_KEY`

### `extraction_worker.py`
Extracts structured financial figures.

Input: `sections["financials"] + sections["guidance"]` (combined — guidance must be included or `guidance_direction` will always be null)

Output fields:
```
eps_actual, eps_estimate, eps_beat (beat/miss/in-line)
revenue_actual_millions, revenue_estimate_millions, revenue_beat
guidance_direction (raised/lowered/maintained/withdrawn)
gross_margin, net_income_millions
fiscal_period, report_date
```

Also receives `estimates` dict from `ingestion/estimates.py` — injected into the prompt so the worker can compare actuals against analyst consensus.

### `sentiment_worker.py`
Infers management tone and messaging style.

Input: `sections["header"]` or `sections["full"]`

Output fields:
```
overall_tone (bullish/neutral/cautious/bearish)
guidance_tone
yoy_trend
headline_signal (one-line summary)
notable_quotes []
confidence_score (0.0–1.0)
```

### `risk_worker.py`
Identifies material risk signals from the full filing text.

Input: `sections["full"]`

Output fields:
```
risk_level (low/medium/high/critical)
guidance_cut_detected (bool)
going_concern_detected (bool)
layoffs_detected (bool)
restatement_detected (bool)
flags: [{ type, description }]
```

---

## Adding a new worker

1. Create `workers/your_worker.py` with `async def run_your_worker(text, client) -> dict`
2. Add it to `pipeline/dispatcher.py`'s `asyncio.gather` call
3. Add its output key to `aggregator/aggregator.py`'s `build_alert`

The dispatcher's `return_exceptions=True` means new workers fail safely — a broken worker returns `{}` without crashing the pipeline.
