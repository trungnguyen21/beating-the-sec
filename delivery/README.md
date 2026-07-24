# delivery/

Sends alert dicts to users. Handles both the low-level HTTP transport (webhook) and the per-ticker fan-out routing (router).

---

## Files

### `webhook.py`
Two delivery implementations, both sharing the `AlertDelivery` protocol (`async def send(alert)`).

**`DiscordWebhook`** — formats the alert as a Discord embed and POSTs it to a webhook URL.

Embed fields: Signal label, Tone, Risk level, EPS actual/estimate/result, Revenue actual/estimate/result, Guidance, Risk flags (if any). Color-coded by signal: green (Bullish), yellow (Neutral), red (Bearish).

Footer shows `detected_at` and `alerted_at` timestamps.

**`WebhookDelivery`** — generic signed HTTP webhook. Sends raw JSON with an HMAC-SHA256 `X-Signature` header and `X-Timestamp`. Use this if you want to receive alerts in your own service.

### `router.py`
`SubscriptionRouter` — maps tickers to subscribers and fans out alerts.

```python
router = SubscriptionRouter(subscription_map)
# subscription_map: { "NVDA": [User, User], "MSFT": [User] }

await router.fanout("NVDA", alert)
# → sends to all users subscribed to NVDA
# → skips users subscribed to other tickers
# → one user failing doesn't block others
```

`fanout(ticker, alert)`:
1. Looks up `subscription_map[ticker]`
2. Logs a warning if no subscribers (doesn't crash)
3. Iterates subscribers sequentially — calls `DiscordWebhook(sub.webhook).send(alert)` for each
4. Catches per-user exceptions individually — one bad webhook doesn't block the rest

---

## How the subscription map is built

In `main.py` at startup:

```python
subscription_map = {}
for user in user_config.get_users():
    for company in user.watchlist:
        subscription_map.setdefault(company.ticker, []).append(user)
```

Result: `{ "NVDA": [trung], "MSFT": [trung], "MU": [trung] }`

---

## Adding a new delivery channel

1. Implement the `AlertDelivery` protocol: `async def send(self, alert: dict) -> None`
2. In the Kafka path: add a new independent consumer group on the `filing-results` topic in `pipeline/kafka_consumer.py` — nothing upstream needs to change
3. In the single-process path: call it alongside `router.fanout` in `main.py`'s `_process_one`
