# models/

Shared data models used across the pipeline. All models are typed — no raw dicts passed between major components.

---

## Files

### `model_detectors.py`
Models for the ingestion layer output.

**`DetectorSource`** (enum)
```
RSS_POLLER    — detected via SEC EDGAR Atom feed
EDGAR_POLLER  — detected via direct index URL polling
```

**`ItemPipeline`** (enum)
```
EARNINGS, LEADERSHIP_CHANGE, ACQUISITION, RESTATEMENT, BANKRUPTCY, RESTRUCTURING
```

**`DetectorResult`** (dataclass) — the object produced by `ingestion/watcher.py` and enqueued into the filing queue.

| Field | Type | Notes |
|---|---|---|
| `ticker` | `str` | e.g. `"NVDA"` |
| `cik` | `str` | zero-padded 10-digit string e.g. `"0001045810"` |
| `accession` | `Optional[str]` | SEC accession number |
| `index_url` | `str` | URL to the filing index page |
| `detected_at` | `str` | UTC ISO-8601 string — display only, not for arithmetic |
| `filed_at` | `Optional[datetime]` | tz-aware UTC datetime — used for age checks |
| `raw_text` | `Optional[str]` | populated after fetch, before enqueue |
| `estimates` | `Optional[dict]` | analyst estimates fetched in parallel |

`detected_at` is a string because it's only ever displayed (in Discord embeds, logs). `filed_at` is a `datetime` because the age filter does arithmetic on it (`datetime.now(utc) - filed_at`). Do not conflate them.

### `users.py`
Models for the multi-user config layer.

**`Company`** (Pydantic BaseModel)
```
ticker: str
cik:    str   ← auto-padded to 10 digits by field_validator regardless of input format
title:  str
```

The `pad_cik` validator handles all three input formats:
- `1045810` (int in JSON) → `"0001045810"`
- `"1045810"` (unpadded string) → `"0001045810"`
- `"0001045810"` (already padded) → `"0001045810"`

**`User`** (Pydantic BaseModel)
```
id:        str
name:      str
watchlist: Optional[List[Company]]
webhook:   str   ← Discord webhook URL
```

---

## Config file schemas

**`watchlist.json`** — company registry (source of truth for CIK mappings):
```json
[
  { "ticker": "NVDA", "cik": 1045810, "title": "NVIDIA CORP" }
]
```

**`users.json`** — per-user config (gitignored — contains webhook secrets):
```json
[
  {
    "id": "alice",
    "name": "alice",
    "watchlist": ["NVDA", "MSFT"],
    "webhook": "https://discord.com/api/webhooks/..."
  }
]
```
`watchlist` in `users.json` is a list of ticker strings — the loader resolves them against `watchlist.json` to get full `Company` objects.
