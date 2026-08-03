"""
E2E test: SEC filing index URL -> fetch -> parse -> spawn 3 AI workers -> aggregate ->
Discord fan-out. No detection/polling involved — the index URL is hardcoded below, matching
the same "{accession}-index.htm" page format ingestion/edgar_poller.build_index_url and the
RSS feed's entry.link both produce (see ingestion/sec_rss.py).

Requires GEMINI_API_KEY in .env. Targets NVDA's Q1 FY27 earnings 8-K (item 2.02, EX-99.1
press release exhibit) by default — override via SAMPLE_FILING_INDEX_URL / SAMPLE_FILING_TICKER
env vars to point at a different filing.

Discord delivery is opt-in and never targets real subscriber webhooks: set
TEST_DISCORD_WEBHOOK_URL to a dedicated test channel's webhook to exercise it. Without that
var set, the test builds and asserts on the alert but skips the actual send — it does NOT fall
back to fanning out to whatever real users happen to be configured in data/users.json / the DB,
since that would post a fabricated test alert into a real subscriber's channel. When it does
send, the message is prefixed with a 🧪 TEST marker so it's unmistakable even in a shared channel.
"""

import json
import os
import time
from datetime import datetime, timezone

import feedparser
import httpx
import pytest
from dotenv import load_dotenv

load_dotenv()

from config import HEADERS
from ingestion.edgar_poller import fetch_filing_document
from pipeline.normalizer import normalize, split_sections
from pipeline.dispatcher import analyze_filing
from aggregator.aggregator import build_alert
from workers.client import GeminiClient
from ingestion.estimates import fetch_estimates
from delivery.router import SubscriptionRouter
from models.users import User
from tests.alert_helpers import assert_valid_alert

SAMPLE_FILING_TICKER = "NVDA"
SAMPLE_FILING_CIK = "1045810"
SAMPLE_FILING_ACCESSION = "0001045810-26-000051"
SAMPLE_FILING_INDEX_URL = (
    "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000051/0001045810-26-000051-index.htm"
)
TEST_DISCORD_WEBHOOK_URL = os.environ.get("TEST_DISCORD_WEBHOOK_URL")


async def _fetch_filed_at(cik: str, accession: str, client: httpx.AsyncClient) -> str | None:
    """Look up the real RSS-feed publish time for a filing — since this test fetches by index
    URL directly (no polling), it has to go find that entry itself on the live atom feed.
    entry.date is feedparser's raw ISO-8601 string (e.g. "2026-07-02T09:23:16-04:00"), so no
    struct_time conversion is needed here."""
    rss_url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcompany&CIK={cik}&type=8-K&output=atom"
    )
    r = await client.get(rss_url, headers=HEADERS, timeout=10)
    feed = feedparser.parse(r.text)
    for entry in feed.entries:
        if entry.get("accession-number") == accession:
            return entry.get("date")
    return None


@pytest.mark.e2e
async def test_e2e_parsing_pipeline():
    index_url = SAMPLE_FILING_INDEX_URL

    async with httpx.AsyncClient() as http_client:
        content = await fetch_filing_document(SAMPLE_FILING_INDEX_URL, http_client)
        filed_at = await _fetch_filed_at(SAMPLE_FILING_CIK, SAMPLE_FILING_ACCESSION, http_client)
    assert content, f"No EX-99.1 exhibit found at {index_url}"

    clean_text = normalize(content, "html")
    assert isinstance(clean_text, str) and len(clean_text) > 0, "Normalizer returned empty string"

    sections = split_sections(clean_text)

    estimates = await fetch_estimates(SAMPLE_FILING_TICKER)

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
        ticker=SAMPLE_FILING_TICKER,
        results=results,
        detected_at=datetime.now(timezone.utc).isoformat(),
        filed_at=filed_at,
        filing_url=index_url,
    )
    print(json.dumps(alert, indent=2))

    # Checks the alert's full schema against tests/alert_helpers.py's ALERT_SCHEMA — the same
    # contract test_aggregator.py enforces — so a schema change surfaces here too, against a
    # real filing and real AI output, not just the mocked unit-test fixtures.
    assert_valid_alert(alert)
    assert alert["ticker"] == SAMPLE_FILING_TICKER, "alert has wrong ticker"
    assert alert["filing_url"] == index_url, "filing_url not threaded through"
    assert all(not isinstance(v, Exception) for v in results.values()), "one or more workers raised"

    if not TEST_DISCORD_WEBHOOK_URL:
        pytest.skip(
            "Set TEST_DISCORD_WEBHOOK_URL to a dedicated test channel's webhook to verify "
            "Discord delivery end-to-end — alert built and asserted on, but not sent, since "
            "this test never fans out to real subscriber webhooks"
        )

    alert["headline"] = f"🧪 TEST ALERT — DO NOT ACT ON THIS — {alert.get('headline', '')}".strip()

    test_subscriber = User(id="test-e2e-parsing-pipeline", name="test", watchlist=None, webhook=TEST_DISCORD_WEBHOOK_URL)
    router = SubscriptionRouter({SAMPLE_FILING_TICKER: [test_subscriber]})
    await router.fanout(SAMPLE_FILING_TICKER, alert)
