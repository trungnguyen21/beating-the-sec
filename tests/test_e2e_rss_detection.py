"""
RSS detection pipeline test (fully mocked — no real network, no e2e marker needed).

Verifies that:
  1. The RSS feed and EDGAR poller race correctly via asyncio.wait(FIRST_COMPLETED)
  2. When the EDGAR poller never finds the filing (HEAD always 404), RSS wins
  3. The RSS feed is empty on the first poll, then updated to include a fresh 8-K
  4. Filing metadata (accession, index_url) is extracted correctly onto DetectorResult
  5. The exhibit document is fetched, normalized, and split into sections

Stops before AI analysis — no API key required.
"""

import asyncio
import time

import pytest
from dotenv import load_dotenv

load_dotenv()

from unittest.mock import AsyncMock, MagicMock, patch

# Saved before any patching. Used inside mock HTTP handlers to call
# sleep(0) — the standard idiom for yielding to the event loop without waiting.
# This is necessary because AsyncMock coroutines complete without suspending,
# so a patched asyncio.sleep never yields — causing the EDGAR poller to run as
# a tight synchronous loop that starves the RSS task entirely.
_real_sleep = asyncio.sleep

from ingestion.watcher import _detect_filing
from ingestion.edgar_poller import fetch_filing_document
from ingestion.classifier import classify_items
from pipeline.normalizer import normalize, split_sections
from models.model_detectors import DetectorSource

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

COMPANY = {"ticker": "NVDA", "cik": "1045810"}

LAST_ACCESSION      = "0001213900-26-000001"
PREDICTED_ACCESSION = "0001213900-26-000002"
ITEMS_DESC          = "Item 2.02: Results of Operations and Financial Condition"

# Must match build_index_url("1045810", "0001213900-26-000002")
INDEX_URL = (
    "https://www.sec.gov/Archives/edgar/data/1045810/"
    "000121390026000002/0001213900-26-000002-index.htm"
)

MOCK_INDEX_HTML = """\
<html><body>
<table><tr><td>1</td><td>EX-99.1</td><td><a href="/Archives/edgar/data/1045810/pr.htm">pr.htm</a></td></tr></table>
</body></html>"""

MOCK_PRESS_RELEASE = """\
<html><body>
<h1>NVIDIA Announces Financial Results for First Quarter Fiscal 2027</h1>
<p>SANTA CLARA, Calif. — NVIDIA (NASDAQ: NVDA) reported revenue of $44.1 billion for
Q1 FY2027, up 69% year over year and above analyst expectations of $43.3 billion.</p>
<p>GAAP diluted earnings per share were $0.96. Non-GAAP diluted earnings per share
were $0.98, beating the consensus estimate of $0.89.</p>
<p>Data Center revenue was $39.1 billion, a new quarterly record, up 73% year on year.</p>
<p>Gross margin was 78.4% on a GAAP basis and 78.9% on a non-GAAP basis.</p>
<p>For Q2 FY2027, NVIDIA expects revenue to be $45.0 billion, plus or minus 2%,
reflecting continued strong demand for Blackwell GPU computing platforms.</p>
<table>
  <tr><th>Metric</th><th>Q1 FY2027</th><th>Q1 FY2026</th></tr>
  <tr><td>Revenue</td><td>$44.1B</td><td>$26.0B</td></tr>
  <tr><td>Non-GAAP EPS</td><td>$0.98</td><td>$0.61</td></tr>
  <tr><td>Gross Margin (non-GAAP)</td><td>78.9%</td><td>77.0%</td></tr>
</table>
</body></html>"""

# ---------------------------------------------------------------------------
# Mock builders
# ---------------------------------------------------------------------------

def _feedparser_result(has_entry: bool) -> MagicMock:
    """
    Build a feedparser-style feed object.
    Patching feedparser.parse avoids namespace-parsing uncertainty in SEC Atom feeds.
    """
    feed = MagicMock()
    if not has_entry:
        feed.entries = []
        return feed

    entry = MagicMock()
    entry.title          = "NVIDIA 8-K (Earnings)"
    entry.link           = INDEX_URL
    entry.updated_parsed = time.gmtime()  # current time — passes the recency check
    entry.get = lambda key, default="": {
        "items-desc":       ITEMS_DESC,
        "accession-number": PREDICTED_ACCESSION,
    }.get(key, default)

    feed.entries = [entry]
    return feed


def _make_mock_client() -> MagicMock:
    """
    Mock httpx.AsyncClient:
      - RSS GET  : empty feed on call #1, fresh 8-K entry on call #2+
      - HEAD     : always 404 so the EDGAR poller never wins
      - index GET: returns mock filing index page
      - doc GET  : returns mock press-release HTML
    """
    rss_calls = 0

    async def mock_get(url, **kwargs):
        nonlocal rss_calls
        # sleep(0) yields to the event loop so both racing tasks interleave correctly.
        # Without this, the EDGAR poller's tight loop would starve the RSS task.
        await _real_sleep(0)
        resp = MagicMock()
        resp.status_code = 200

        if "browse-edgar" in url:
            rss_calls += 1
            # sentinel text — feedparser.parse is patched so the value doesn't matter
            resp.text = f"rss_response_{rss_calls}"
            return resp

        if "index.htm" in url:
            resp.text = MOCK_INDEX_HTML
            return resp

        # Exhibit / press release
        resp.text = MOCK_PRESS_RELEASE
        return resp

    async def mock_head(url, **kwargs):
        await _real_sleep(0)  # yield so the RSS task gets scheduled
        resp = MagicMock()
        resp.status_code = 404  # EDGAR poller never finds the filing
        return resp

    client = MagicMock()
    client.get  = mock_get
    client.head = mock_head
    return client


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

async def test_rss_detection_pipeline():
    client = _make_mock_client()
    feedparser_responses = [
        _feedparser_result(has_entry=False),  # first poll: no actionable entry
        _feedparser_result(has_entry=True),   # second poll: 8-K with 2.02
    ]

    with (
        # Avoid real SEC API call for last accession number
        patch("ingestion.watcher.get_last_accession",
              new=AsyncMock(return_value=LAST_ACCESSION)),
        # Control what feedparser sees from the RSS HTTP response
        patch("ingestion.sec_rss.feedparser.parse",
              side_effect=feedparser_responses),
        # Skip wait loops — both detectors run as fast as possible
        patch("asyncio.sleep", new=AsyncMock()),
    ):
        result, source = await _detect_filing(COMPANY, client, seen=set())

    assert source == "rss", f"Expected RSS to win, got {source!r}"
    assert result.ticker == "NVDA", "ticker mismatch"
    assert result.source == DetectorSource.RSS_POLLER, "source field wrong"
    assert result.accession == PREDICTED_ACCESSION, "accession mismatch"
    assert result.index_url == INDEX_URL, "index_url mismatch"
    assert result.detected_at, "detected_at missing"

    # DetectorResult itself carries no `pipelines` field — poll_company_once computes it
    # internally (via classify_items) purely to decide whether to return a hit at all, then
    # discards it. Verify the classification independently against the same items-desc the
    # mock feed provided, rather than asserting on a field that doesn't exist on the result.
    assert "earnings" in classify_items(ITEMS_DESC)

    # ------------------------------------------------------------------
    # Fetch filing document (EX-99.1 exhibit)
    # ------------------------------------------------------------------
    raw_text = await fetch_filing_document(result.index_url, client)
    assert raw_text, "fetch_filing_document returned empty string"

    # ------------------------------------------------------------------
    # Normalize & split sections
    # ------------------------------------------------------------------
    clean_text = normalize(raw_text, "html")
    assert clean_text, "normalize() returned empty string"

    sections = split_sections(clean_text)
    assert "full" in sections, "split_sections missing 'full' key"
    assert sections["full"], "'full' section is empty"
    assert sections["financials"], "financials section is empty — keyword matching may be broken"
    assert sections["guidance"], "guidance section is empty — keyword matching may be broken"
