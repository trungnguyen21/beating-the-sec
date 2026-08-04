# tests/test_aggregator.py
"""
Fast, no-network contract tests for aggregator.build_alert() / compute_signal_score(). These
run by default (no e2e marker) so a schema change is caught in under a second, before anyone
gets to a slow/opt-in e2e test — see alert_helpers.py for the shared fixtures and the
ALERT_SCHEMA contract this asserts against.
"""
from aggregator.aggregator import build_alert, compute_signal_score
from tests.alert_helpers import assert_valid_alert, sample_results


def test_build_alert_matches_schema():
    alert = build_alert(
        ticker="NVDA",
        results=sample_results(),
        detected_at="2026-08-03T18:00:00+00:00",
        filed_at="2026-08-03T17:55:00+00:00",
        filing_url="https://www.sec.gov/Archives/edgar/data/example-index.htm",
    )
    assert_valid_alert(alert)
    assert alert["ticker"] == "NVDA"
    assert alert["filing_url"] == "https://www.sec.gov/Archives/edgar/data/example-index.htm"


def test_build_alert_optional_fields_default_to_none():
    alert = build_alert(
        ticker="NVDA",
        results=sample_results(),
        detected_at="2026-08-03T18:00:00+00:00",
    )
    assert_valid_alert(alert)
    assert alert["filing_url"] is None
    assert alert["filed_at"] is None
    assert alert["feed_to_detection_seconds"] is None


def test_feed_to_detection_seconds_computed_from_filed_and_detected():
    alert = build_alert(
        ticker="NVDA",
        results=sample_results(),
        detected_at="2026-08-03T18:00:10+00:00",
        filed_at="2026-08-03T18:00:00+00:00",
    )
    assert alert["feed_to_detection_seconds"] == 10.0


def test_bullish_case_scores_high_and_labels_bullish():
    score = compute_signal_score(sample_results())
    assert score >= 0.65

    alert = build_alert("NVDA", sample_results(), "2026-08-03T18:00:00+00:00")
    assert alert["signal_label"] == "🟢 Bullish"


def test_bearish_case_scores_low_and_labels_bearish():
    results = sample_results(
        extraction={"eps_beat": "miss", "revenue_beat": "miss", "guidance_direction": "lowered"},
        sentiment={"overall_tone": "bearish", "confidence_score": 0.9},
        risk={"risk_level": "critical", "guidance_cut_detected": True, "going_concern_detected": True},
    )
    score = compute_signal_score(results)
    assert score < 0.40

    alert = build_alert("NVDA", results, "2026-08-03T18:00:00+00:00")
    assert alert["signal_label"] == "🔴 Bearish"
    assert alert["risk_level"] == "critical"


def test_neutral_defaults_produce_neutral_baseline():
    results = sample_results(
        extraction={"eps_beat": None, "revenue_beat": None, "guidance_direction": None},
        sentiment={"overall_tone": "neutral", "confidence_score": 0.5},
        risk={"risk_level": "medium"},
    )
    assert compute_signal_score(results) == 0.5


def test_score_always_clamped_to_unit_interval():
    worst = sample_results(
        extraction={"eps_beat": "miss", "revenue_beat": "miss", "guidance_direction": "lowered"},
        sentiment={"overall_tone": "bearish", "confidence_score": 1.0},
        risk={"risk_level": "critical", "guidance_cut_detected": True, "going_concern_detected": True},
    )
    assert compute_signal_score(worst) == 0.0

    best = sample_results(
        extraction={"eps_beat": "beat", "revenue_beat": "beat", "guidance_direction": "raised"},
        sentiment={"overall_tone": "bullish", "confidence_score": 1.0},
        risk={"risk_level": "low"},
    )
    assert compute_signal_score(best) <= 1.0


def test_risk_flags_extracted_as_description_strings():
    results = sample_results(
        risk={
            "flags": [
                {"type": "layoffs", "description": "Cutting 5% of workforce.", "severity": "medium"},
                {"type": "debt", "description": "Increased leverage ratio.", "severity": "low"},
            ]
        }
    )
    alert = build_alert("NVDA", results, "2026-08-03T18:00:00+00:00")
    assert alert["risk_flags"] == ["Cutting 5% of workforce.", "Increased leverage ratio."]
