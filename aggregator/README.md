# aggregator/

Combines the three AI worker outputs into a single alert dict with a numeric signal score and human-readable label.

---

## Files

### `aggregator.py`

**`compute_signal_score(results)`** — starts at 0.5 (neutral) and adjusts based on:

| Signal | Adjustment |
|---|---|
| EPS beat | +0.15 |
| EPS miss | −0.15 |
| Revenue beat | +0.10 |
| Revenue miss | −0.10 |
| Guidance raised | +0.15 |
| Guidance lowered | −0.20 |
| Guidance withdrawn | −0.10 |
| Bullish tone (scaled by confidence) | up to +0.15 |
| Bearish tone (scaled by confidence) | up to −0.20 |
| Guidance cut detected (risk) | −0.15 |
| Going concern detected (risk) | −0.30 |
| Risk level high/critical | −0.10 / −0.25 |

Score is clamped to `[0.0, 1.0]`.

**`build_alert(ticker, results, detected_at)`** — returns the final alert dict:

```python
{
    "ticker": "NVDA",
    "detected_at": "...",        # from DetectorResult
    "alerted_at": "...",         # UTC now
    "signal_score": 0.78,
    "signal_label": "🟢 Bullish",  # ≥0.65 Bullish, <0.40 Bearish, else Neutral
    "headline": "...",
    "eps": { "actual", "estimate", "result" },
    "revenue": { "actual_m", "estimate_m", "result" },
    "guidance": "raised",
    "risk_level": "low",
    "risk_flags": ["..."],
    "tone": "bullish",
}
```

---

## Thresholds

| Score range | Label |
|---|---|
| ≥ 0.65 | 🟢 Bullish |
| 0.40 – 0.64 | 🟡 Neutral |
| < 0.40 | 🔴 Bearish |

To tune the thresholds or weights, edit `compute_signal_score` in `aggregator.py`. The scoring logic is intentionally simple and transparent — no model, no training data.
