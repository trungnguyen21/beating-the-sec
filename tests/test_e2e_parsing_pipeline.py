"""
E2E test: SEC filing -> parse -> spawn 3 AI workers -> aggregate -> Discord fan-out.

Requires GEMINI_API_KEY in .env. Uses the bundled DELL.html sample and ticker "NVDA" by
default (override via SAMPLE_FILING_PATH / SAMPLE_FILING_TICKER env vars).
"""

import json
import os
import time

import pytest
from dotenv import load_dotenv

load_dotenv()

from pipeline.normalizer import normalize, split_sections
from pipeline.dispatcher import analyze_filing
from aggregator.aggregator import build_alert
from workers.client import GeminiClient
from ingestion.estimates import fetch_estimates
from delivery.router import SubscriptionRouter
from utils.load_user import UserConfig
from main import _build_subscription_map

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLE_HTML = os.path.join(REPO_ROOT, "data", "DELL.html")


@pytest.mark.e2e
async def test_e2e_parsing_pipeline():
    html_path = os.environ.get("SAMPLE_FILING_PATH", SAMPLE_HTML)
    ticker = os.environ.get("SAMPLE_FILING_TICKER", "NVDA").upper()

    with open(html_path) as f:
        content = f.read()

    clean_text = normalize(content, "html")
    assert isinstance(clean_text, str) and len(clean_text) > 0, "Normalizer returned empty string"

    sections = split_sections(clean_text)

    estimates = await fetch_estimates(ticker)

    async with GeminiClient() as client:
        t0 = time.perf_counter()
        results = await analyze_filing(sections, client, estimates)
        elapsed = time.perf_counter() - t0
    print(f"All 3 agents finished in {elapsed:.2f}s")

    extraction = results.get("extraction", {})
    assert isinstance(extraction, dict), "Extraction result is not a dict"

    sentiment = results.get("sentiment", {})
    assert isinstance(sentiment, dict), "Sentiment result is not a dict"

    risk = results.get("risk", {})
    assert isinstance(risk, dict), "Risk result is not a dict"

    alert = build_alert(
        ticker=ticker,
        results=results,
        detected_at="2024-11-20T21:00:00Z",
    )
    print(json.dumps(alert, indent=2))

    assert alert.get("ticker") == ticker, "alert has wrong ticker"
    assert 0.0 <= alert.get("signal_score", -1) <= 1.0, "signal_score out of [0, 1]"
    assert alert.get("signal_label") in {"🟢 Bullish", "🟡 Neutral", "🔴 Bearish"}, "signal_label missing/invalid"
    assert isinstance(alert.get("eps"), dict), "eps dict missing"
    assert isinstance(alert.get("revenue"), dict), "revenue dict missing"
    assert all(not isinstance(v, Exception) for v in results.values()), "one or more workers raised"

    user_config = UserConfig()
    user_config.load_configs("watchlist.json", "data/users.json")
    subscription_map = _build_subscription_map(user_config)
    router = SubscriptionRouter(subscription_map)

    subscribers = subscription_map.get(ticker, [])
    if not subscribers:
        pytest.skip(f"No subscribers for {ticker} in users.json — alert built but not sent")
    await router.fanout(ticker, alert)
