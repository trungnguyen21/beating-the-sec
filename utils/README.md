# utils/

Shared utilities: logging, Kafka connection config, and user config loading.

---

## Files

### `logger.py`
`get_logger(name)` — returns a structured logger with timestamp, level, and module name. Used by every module: `logger = get_logger(__name__)`.

Log level is controlled by `LOGGING_LEVEL` in `config.py` (default: `INFO`).

### `kafka.py`
`kafka_client_config()` — returns the connection dict needed by every `AIOKafkaProducer` and `AIOKafkaConsumer` to connect to Confluent Cloud:

```python
{
    "bootstrap_servers": CONFLUENT_BOOTSTRAP,
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "PLAIN",
    "sasl_plain_username": CONFLUENT_API_KEY,
    "sasl_plain_password": CONFLUENT_API_SECRET,
    "ssl_context": ssl.create_default_context(),
}
```

The `ssl_context` is required by `aiokafka` when using `SASL_SSL` — without it the client raises `ValueError` at init time.

All three env vars (`CONFLUENT_BOOTSTRAP`, `CONFLUENT_API_KEY`, `CONFLUENT_API_SECRET`) are read from `.env` via `config.py`. Every producer/consumer calls `**kafka_client_config()` so the SASL/SSL config is defined in exactly one place.

### `load_user.py`
`UserConfig` — loads and cross-references `watchlist.json` and `users.json` at startup.

```python
user_config = UserConfig()
user_config.load_configs("watchlist.json", "users.json")

user_config.get_companies()  # → { "NVDA": Company, "MSFT": Company, ... }
user_config.get_users()      # → [ User(id="alice", watchlist=[Company, ...]), ... ]
```

**`load_companies(path)`** — reads `watchlist.json`, validates each entry via `Company.model_validate(dict)`, stores as `{ ticker: Company }`.

**`load_user(path)`** — reads `users.json`, resolves each ticker string against the loaded companies dict, builds `User` objects with full `Company` objects in `watchlist`. Raises `ValueError` immediately if a ticker in `users.json` doesn't exist in `watchlist.json` — fail fast at startup rather than silently watching nothing.

**Used in `main.py` to derive:**
- `unified_watchlist` — deduplicated list of `{ticker, cik}` dicts for `run_watcher`
- `subscription_map` — `{ticker: [User, ...]}` for `SubscriptionRouter`
