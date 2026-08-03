# tests/alert_helpers.py
"""
Shared alert fixtures + schema contract, reused by test_aggregator.py (fast, no network) and
the e2e tests so the alert everyone sends to Discord is always a real product of
aggregator.build_alert() rather than a hand-copied dict that can silently drift out of sync
with the real schema.
"""

# Worker output schemas — see workers/extraction_worker.py, sentiment_worker.py, risk_worker.py
# for the prompts these mirror.
_DEFAULT_EXTRACTION = {
    "eps_actual": 1.85,
    "eps_estimate": 1.70,
    "eps_beat": "beat",
    "revenue_actual_millions": 94500.0,
    "revenue_estimate_millions": 91200.0,
    "revenue_beat": "beat",
    "gross_margin_pct": 62.3,
    "net_income_millions": 12000.0,
    "guidance_next_quarter_low": 95000.0,
    "guidance_next_quarter_high": 98000.0,
    "guidance_direction": "raised",
    "fiscal_period": "Q3 2026",
    "report_date": "2026-08-03",
}

_DEFAULT_SENTIMENT = {
    "overall_tone": "bullish",
    "confidence_score": 0.9,
    "guidance_tone": "optimistic",
    "ceo_language_flags": ["unprecedented demand"],
    "yoy_trend": "improving",
    "headline_signal": "Beats on EPS and revenue, raises guidance.",
    "notable_quotes": ["We are thrilled with our strong performance this quarter."],
}

_DEFAULT_RISK = {
    "risk_level": "low",
    "flags": [],
    "guidance_cut_detected": False,
    "restructuring_detected": False,
    "legal_language_detected": False,
    "going_concern_detected": False,
}


def sample_results(extraction: dict = None, sentiment: dict = None, risk: dict = None) -> dict:
    """A full {"extraction", "sentiment", "risk"} results dict, matching what
    pipeline/dispatcher.py::analyze_filing returns. Defaults to a clean bullish case;
    pass partial overrides for any of the three to build other scenarios, e.g.:

        sample_results(extraction={"eps_beat": "miss"}, sentiment={"overall_tone": "bearish"})
    """
    return {
        "extraction": {**_DEFAULT_EXTRACTION, **(extraction or {})},
        "sentiment": {**_DEFAULT_SENTIMENT, **(sentiment or {})},
        "risk": {**_DEFAULT_RISK, **(risk or {})},
    }


# The canonical set of keys build_alert() must return. Update this alongside aggregator.py
# whenever the schema changes on purpose — that's the point: a change here should be a
# deliberate, reviewable edit, not something that happens silently.
ALERT_SCHEMA = {
    "ticker": str,
    "filing_url": (str, type(None)),
    "filed_at": (str, type(None)),
    "detected_at": str,
    "alerted_at": str,
    "feed_to_detection_seconds": (int, float, type(None)),
    "signal_score": float,
    "signal_label": str,
    "headline": str,
    "eps": dict,
    "revenue": dict,
    "guidance": (str, type(None)),
    "risk_level": (str, type(None)),
    "risk_flags": list,
    "tone": (str, type(None)),
}


def assert_valid_alert(alert: dict) -> None:
    """Fails loudly (missing/extra/mistyped key) the moment an alert stops matching
    ALERT_SCHEMA — the single place both the unit test and the e2e tests check the contract,
    instead of each asserting on a different partial subset of fields."""
    missing = ALERT_SCHEMA.keys() - alert.keys()
    assert not missing, f"alert missing expected field(s): {missing}"

    extra = alert.keys() - ALERT_SCHEMA.keys()
    assert not extra, (
        f"alert has field(s) not declared in ALERT_SCHEMA: {extra} — "
        f"add them to alert_helpers.ALERT_SCHEMA if this is intentional"
    )

    for field, expected_type in ALERT_SCHEMA.items():
        value = alert[field]
        assert isinstance(value, expected_type), (
            f"alert[{field!r}] = {value!r} has type {type(value).__name__}, "
            f"expected {expected_type}"
        )

    assert 0.0 <= alert["signal_score"] <= 1.0, "signal_score out of [0, 1]"
    assert alert["signal_label"] in {"🟢 Bullish", "🟡 Neutral", "🔴 Bearish"}, "signal_label invalid"
    for eps_key in ("actual", "estimate", "result"):
        assert eps_key in alert["eps"], f"alert['eps'] missing {eps_key!r}"
    for rev_key in ("actual_m", "estimate_m", "result"):
        assert rev_key in alert["revenue"], f"alert['revenue'] missing {rev_key!r}"
