"""
Manual end-to-end test: SEC filing -> parse PDF -> spawn 3 agents -> gather results.

Run with:
    python tests/test_e2e_pipeline.py [path/to/filing.pdf]

Requires GEMINI_API_KEY in .env. If no PDF is provided, falls back to a
bundled HTML sample so the test is runnable without a real filing on hand.
"""

import asyncio
import json
import os
import sys
import time

# Allow running from repo root or tests/ directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from pprint import pprint

from pipeline.normalizer import normalize, split_sections
from pipeline.dispatcher import analyze_filing
from aggregator.aggregator import build_alert
from workers.client import GeminiClient
from ingestion.estimates import fetch_estimates
from delivery.router import SubscriptionRouter
from utils.load_user import UserConfig
from main import _build_subscription_map


def _section(title: str) -> None:
    width = 60
    print(f"\n{'=' * width}")
    print(f"  {title}")
    print(f"{'=' * width}")


SAMPLE_HTML = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "DELL.html")

def _load_filing(html_path: str | None) -> tuple[str, str]:
    """Return (content, content_type) for the filing to test."""
    path = html_path or SAMPLE_HTML
    print(f"Loading filing from: {path}")
    with open(path) as f:
        return f.read(), "html"


async def run() -> None:
    # Usage: python test_e2e_parsing_pipeline.py [path/to/filing.html] [TICKER]
    # Defaults: data/DELL.html, ticker NVDA (must be in your users.json watchlist)
    html_path = sys.argv[1] if len(sys.argv) > 1 else None
    ticker_override = sys.argv[2] if len(sys.argv) > 2 else None

    # ------------------------------------------------------------------
    # STEP 1: Load filing
    # ------------------------------------------------------------------
    _section("STEP 1 — Load SEC Filing")
    content, content_type = _load_filing(html_path)
    source_label = html_path or "bundled HTML sample"
    print(f"Source      : {source_label}")
    print(f"Format      : {content_type.upper()}")
    print(f"Raw size    : {len(content):,} chars")

    # ------------------------------------------------------------------
    # STEP 2: Parse / normalize
    # ------------------------------------------------------------------
    _section("STEP 2 — Parse & Normalize")
    t0 = time.perf_counter()
    clean_text = normalize(content, content_type)
    elapsed = time.perf_counter() - t0

    assert isinstance(clean_text, str) and len(clean_text) > 0, "Normalizer returned empty string"
    print(f"Clean text  : {len(clean_text):,} chars  ({elapsed * 1000:.1f} ms)")
    print(f"\nFirst 300 chars:\n{clean_text[:300]}")
    sections = split_sections(clean_text)
    print("Splitted into sections!")
    pprint(sections)

    # ------------------------------------------------------------------
    # STEP 3: Spawn 3 agents concurrently
    # ------------------------------------------------------------------
    _section("STEP 3 — Fetch Analyst Estimates")
    if ticker_override:
        ticker = ticker_override.upper()
    elif html_path:
        ticker = os.path.basename(html_path).split(".")[0].upper()
    else:
        ticker = "NVDA"  # default — must be in users.json watchlist to receive the alert
    estimates = await fetch_estimates(ticker)
    print(f"Estimates for {ticker}: {estimates}")

    _section("STEP 4 — Spawn 3 AI Agents (concurrent)")
    print("Launching: extraction_worker | sentiment_worker | risk_worker")
    async with GeminiClient() as client:
        t0 = time.perf_counter()
        results = await analyze_filing(sections, client, estimates)
        elapsed = time.perf_counter() - t0
    print(f"All 3 agents finished in {elapsed:.2f}s")

    # ------------------------------------------------------------------
    # STEP 4: Display individual agent results
    # ------------------------------------------------------------------
    _section("STEP 5 — Agent Results")

    print("\n[extraction_worker]")
    extraction = results.get("extraction", {})
    assert isinstance(extraction, dict), "Extraction result is not a dict"
    print(json.dumps(extraction, indent=2))

    print("\n[sentiment_worker]")
    sentiment = results.get("sentiment", {})
    assert isinstance(sentiment, dict), "Sentiment result is not a dict"
    print(json.dumps(sentiment, indent=2))

    print("\n[risk_worker]")
    risk = results.get("risk", {})
    assert isinstance(risk, dict), "Risk result is not a dict"
    print(json.dumps(risk, indent=2))

    # ------------------------------------------------------------------
    # STEP 5: Aggregate into alert
    # ------------------------------------------------------------------
    _section("STEP 6 — Aggregate Alert")
    alert = build_alert(
        ticker=ticker,
        results=results,
        detected_at="2024-11-20T21:00:00Z",
    )
    print(json.dumps(alert, indent=2))

    # ------------------------------------------------------------------
    # STEP 6: Sanity assertions
    # ------------------------------------------------------------------
    _section("STEP 7 — Sanity Checks")

    checks = [
        ("alert has ticker",           alert.get("ticker") == ticker),
        ("signal_score in [0, 1]",     0.0 <= alert.get("signal_score", -1) <= 1.0),
        ("signal_label present",        alert.get("signal_label") in {"🟢 Bullish", "🟡 Neutral", "🔴 Bearish"}),
        ("eps dict present",            isinstance(alert.get("eps"), dict)),
        ("revenue dict present",        isinstance(alert.get("revenue"), dict)),
        ("no unhandled exceptions",     all(not isinstance(v, Exception) for v in results.values())),
    ]

    all_passed = True
    for label, ok in checks:
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {label}")
        if not ok:
            all_passed = False

    print()
    if all_passed:
        print("All checks passed. End-to-end pipeline is healthy.")
    else:
        print("One or more checks failed — see above.")
        sys.exit(1)

    _section("STEP 8 — Fan-out via SubscriptionRouter")
    user_config = UserConfig()
    user_config.load_configs("watchlist.json", "users.json")
    subscription_map = _build_subscription_map(user_config)
    router = SubscriptionRouter(subscription_map)

    subscribers = subscription_map.get(ticker, [])
    if not subscribers:
        print(f"  ⚠  No subscribers for {ticker} in users.json — alert built but not sent.")
        print(f"     Add '{ticker}' to a user's watchlist to receive the Discord message.")
    else:
        print(f"  Sending to {len(subscribers)} subscriber(s) for {ticker}...")
        await router.fanout(ticker, alert)
        print("  Done — check your Discord channel.")


if __name__ == "__main__":
    asyncio.run(run())
