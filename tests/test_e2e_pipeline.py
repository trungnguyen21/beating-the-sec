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

from pipeline.normalizer import normalize
from pipeline.dispatcher import analyze_filing
from aggregator.aggregator import build_alert
from workers.client import GeminiClient

# ---------------------------------------------------------------------------
# Bundled HTML sample (NVDA Q3 2025 earnings excerpt)
# ---------------------------------------------------------------------------
SAMPLE_HTML = """
<html><body>
<h1>NVIDIA Announces Financial Results for Third Quarter Fiscal 2025</h1>
<p>SANTA CLARA, Calif., Nov. 20, 2024 (GLOBE NEWSWIRE) --
NVIDIA (NASDAQ: NVDA) today reported revenue for the third quarter ended October 27, 2024,
of $35.08 billion, up 17% from the previous quarter and up 94% from a year ago.</p>

<p>GAAP earnings per diluted share for the quarter were $0.78, up 16% from the previous
quarter and up 111% from a year ago. Non-GAAP earnings per diluted share were $0.81,
up 19% from the previous quarter and up 103% from a year ago.</p>

<p>"The age of AI is in full steam, propelling a global shift to NVIDIA computing,"
said Jensen Huang, founder and CEO of NVIDIA.</p>

<p>For the fourth quarter of fiscal 2025, NVIDIA expects revenue to be $37.5 billion,
plus or minus 2%. This is above the prior outlook of $32.5 billion, representing
raised guidance on strong data center demand.</p>

<p>Gross margin is expected to be 73.5% and 73.0% on a GAAP and non-GAAP basis,
respectively, plus or minus 50 basis points.</p>

<p>Data Center revenue was $30.8 billion, up 17% sequentially and up 112% year on year,
driven by strong demand for NVIDIA Hopper GPU computing platform.</p>

<footer>Copyright 2024 NVIDIA Corporation</footer>
</body></html>
"""


def _section(title: str) -> None:
    width = 60
    print(f"\n{'=' * width}")
    print(f"  {title}")
    print(f"{'=' * width}")


def _load_filing(pdf_path: str | None) -> tuple[bytes | str, str]:
    """Return (content, content_type) for the filing to test."""
    if pdf_path:
        with open(pdf_path, "rb") as f:
            return f.read(), "pdf"
    print("No PDF provided — using bundled HTML sample (NVDA Q3 FY2025 earnings).")
    return SAMPLE_HTML, "html"


async def run() -> None:
    pdf_path = sys.argv[1] if len(sys.argv) > 1 else None

    # ------------------------------------------------------------------
    # STEP 1: Load filing
    # ------------------------------------------------------------------
    _section("STEP 1 — Load SEC Filing")
    content, content_type = _load_filing(pdf_path)
    source_label = pdf_path or "bundled HTML sample"
    print(f"Source      : {source_label}")
    print(f"Format      : {content_type.upper()}")
    print(f"Raw size    : {len(content):,} {'bytes' if isinstance(content, bytes) else 'chars'}")

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

    # ------------------------------------------------------------------
    # STEP 3: Spawn 3 agents concurrently
    # ------------------------------------------------------------------
    _section("STEP 3 — Spawn 3 AI Agents (concurrent)")
    print("Launching: extraction_worker | sentiment_worker | risk_worker")
    client = GeminiClient()
    t0 = time.perf_counter()
    results = await analyze_filing(clean_text, client)
    elapsed = time.perf_counter() - t0
    print(f"All 3 agents finished in {elapsed:.2f}s")

    # ------------------------------------------------------------------
    # STEP 4: Display individual agent results
    # ------------------------------------------------------------------
    _section("STEP 4 — Agent Results")

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
    _section("STEP 5 — Aggregate Alert")
    ticker = "NVDA" if not pdf_path else os.path.basename(pdf_path).split(".")[0].upper()
    alert = build_alert(
        ticker=ticker,
        text=clean_text,
        results=results,
        detected_at="2024-11-20T21:00:00Z",
    )
    print(json.dumps(alert, indent=2))

    # ------------------------------------------------------------------
    # STEP 6: Sanity assertions
    # ------------------------------------------------------------------
    _section("STEP 6 — Sanity Checks")

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


if __name__ == "__main__":
    asyncio.run(run())
