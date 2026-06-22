"""
E2E test: RSS detection pipeline.

Verifies that:
  1. The RSS feed and EDGAR poller race correctly via asyncio.wait(FIRST_COMPLETED)
  2. When the EDGAR poller never finds the filing (HEAD always 404), RSS wins
  3. The RSS feed is empty on the first poll, then updated to include a fresh 8-K
  4. Filing metadata (accession, pipelines, index_url) is extracted correctly
  5. The exhibit document is fetched, normalized, and split into sections

Stops before AI analysis — no API key required.

Run with:
    python tests/test_e2e_rss_detection.py
"""

import asyncio
import os
import sys
import time as time_mod
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from pprint import pprint
from unittest.mock import AsyncMock, MagicMock, patch

# Saved before any patching. Used inside mock HTTP handlers to call
# sleep(0) — the standard idiom for yielding to the event loop without waiting.
# This is necessary because AsyncMock coroutines complete without suspending,
# so a patched asyncio.sleep never yields — causing the EDGAR poller to run as
# a tight synchronous loop that starves the RSS task entirely.
_real_sleep = asyncio.sleep

from ingestion.watcher import _detect_filing
from ingestion.edgar_poller import fetch_filing_document
from pipeline.normalizer import normalize, split_sections

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

COMPANY = {"ticker": "NVDA", "cik": "1045810"}

LAST_ACCESSION      = "0001213900-26-000001"
PREDICTED_ACCESSION = "0001213900-26-000002"

# Must match build_index_url("1045810", "0001213900-26-000002")
INDEX_URL = (
    "https://www.sec.gov/Archives/edgar/data/1045810/"
    "000121390026000002/0001213900-26-000002-index.htm"
)

MOCK_INDEX_HTML = f"""\
<?xml version="1.0" encoding="ISO-8859-1" ?>
  <feed xmlns="http://www.w3.org/2005/Atom">
    <author>
      <email>webmaster@sec.gov</email>
      <name>Webmaster</name>
    </author>
    <company-info>
      <addresses>
        <address type="mailing">
          <city>DUBLIN</city>
          <state>L2</state>
          <street1>1 GRAND CANAL SQUARE</street1>
          <street2>GRAND CANAL HARBOUR</street2>
          <zip>D2</zip>
        </address>
        <address type="business">
          <city>DUBLIN</city>
          <phone>353-1-646-2000</phone>
          <state>L2</state>
          <street1>1 GRAND CANAL SQUARE</street1>
          <street2>GRAND CANAL HARBOUR</street2>
          <zip>D2</zip>
        </address>
      </addresses>
      <assigned-sic>7389</assigned-sic>
      <assigned-sic-desc>SERVICES-BUSINESS SERVICES, NEC</assigned-sic-desc>
      <assigned-sic-href>https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&amp;SIC=7389&amp;owner=include&amp;count=40</assigned-sic-href>
      <cik>0001467373</cik>
      <cik-href>https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&amp;CIK=0001467373&amp;owner=include&amp;count=40</cik-href>
      <conformed-name>Accenture plc</conformed-name>
      <fiscal-year-end>0831</fiscal-year-end>
      <formerly-names count="1">
        <names>
          <date>2026-06-08</date>
          <name>Accenture plc</name>
        </names>
      </formerly-names>
      <state-location>L2</state-location>
      <state-location-href>https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&amp;State=L2&amp;owner=include&amp;count=40</state-location-href>
      <state-of-incorporation>L2</state-of-incorporation>
    </company-info>
    <entry>
      <category label="form type" scheme="https://www.sec.gov/" term="8-K" />
      <content type="text/xml">
        <accession-number>0001467373-26-000019</accession-number>
        <act>34</act>
        <file-number>001-34448</file-number>
        <file-number-href>https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&amp;filenum=001-34448&amp;owner=include&amp;count=40</file-number-href>
        <filing-date>2026-04-24</filing-date>
        <filing-href>https://www.sec.gov/Archives/edgar/data/1467373/000146737326000019/0001467373-26-000019-index.htm</filing-href>
        <filing-type>8-K</filing-type>
        <film-number>26890399</film-number>
        <form-name>Current report</form-name>
        <items-desc>items 1.01, 1.02, 2.03and9.01</items-desc>
        <size>105 MB</size>
        <xbrl_href>https://www.sec.gov/cgi-bin/viewer?action=view&amp;cik=1467373&amp;accession_number=0001467373-26-000019&amp;xbrl_type=v</xbrl_href>
      </content>
      <id>urn:tag:sec.gov,2008:accession-number=0001467373-26-000019</id>
      <link href="https://www.sec.gov/Archives/edgar/data/1467373/000146737326000019/0001467373-26-000019-index.htm" rel="alternate" type="text/html" />
      <summary type="html"> &lt;b&gt;Filed:&lt;/b&gt; 2026-04-24 &lt;b&gt;AccNo:&lt;/b&gt; 0001467373-26-000019 &lt;b&gt;Size:&lt;/b&gt; 105 MB&lt;br&gt;Item 1.01: Entry into a Material Definitive Agreement&lt;br&gt;Item 1.02: Termination of a Material Definitive Agreement&lt;br&gt;Item 2.03: Creation of a Direct Financial Obligation or an Obligation under an Off-Balance Sheet Arrangement of a Registrant&lt;br&gt;Item 9.01: Financial Statements and Exhibits</summary>
      <title>8-K  - Current report</title>
      <updated>2026-04-24T06:35:41-04:00</updated>
    </entry>
    <id>https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&amp;CIK=0001467373</id>
    <link href="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&amp;CIK=0001467373&amp;type=8-K&amp;start=0&amp;count=40" rel="alternate" type="text/html" />
    <link href="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&amp;CIK=0001467373&amp;type=8-K&amp;start=0&amp;count=40&amp;output=atom" rel="self" type="application/atom+xml" />
    <link href="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&amp;CIK=0001467373&amp;type=8-K%25&amp;datea=&amp;dateb=&amp;owner=include&amp;count=40&amp;output=atom&amp;start=40" rel="next" type="application/atom+xml" />
    <title>Accenture plc  (0001467373)</title>
    <updated>2026-06-17T22:31:44-04:00</updated>
  </feed>"""

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
    entry.updated_parsed = time.gmtime()  # current time — passes the 24 h recency check
    entry.get = lambda key, default="": {
        "items-desc":       "Item 2.02: Results of Operations and Financial Condition",
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
# Helpers
# ---------------------------------------------------------------------------

def _section(title: str) -> None:
    print(f"\n{'=' * 60}\n  {title}\n{'=' * 60}")


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

async def run() -> None:
    _section("STEP 1 — Mock setup")
    print("  RSS feed  : empty on poll #1, fresh 8-K (item 2.02) on poll #2")
    print("  Poller    : HEAD always 404 — will not win the race")
    print("  feedparser: patched to return controlled feed objects")

    client = _make_mock_client()
    feedparser_responses = [
        _feedparser_result(has_entry=False),   # first poll: no actionable entry
        _feedparser_result(has_entry=True),    # second poll: 8-K with 2.02
    ]

    # ------------------------------------------------------------------
    _section("STEP 2 — Race detectors (asyncio.wait FIRST_COMPLETED)")
    # ------------------------------------------------------------------
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
        t0 = time_mod.perf_counter()
        result, source = await _detect_filing(COMPANY, client)
        elapsed = time_mod.perf_counter() - t0

    print(f"Race resolved in {elapsed:.3f}s")
    print(f"Winning detector : {source}")
    print("Filing metadata  :")
    pprint(result)

    assert source == "rss",                       f"Expected RSS to win, got {source!r}"
    assert result["ticker"] == "NVDA",            "ticker mismatch"
    assert result["source"] == "sec_rss",         "source field wrong"
    assert result["accession"] == PREDICTED_ACCESSION, "accession mismatch"
    assert result["index_url"] == INDEX_URL,      "index_url mismatch"
    assert "detected_at" in result,               "detected_at missing"
    assert "pipelines" in result,                 "pipelines missing"
    assert "earnings" in result["pipelines"], (
        f"expected 'earnings' pipeline, got {result['pipelines']}"
    )
    print("\n[PASS] Detection assertions")

    # ------------------------------------------------------------------
    _section("STEP 3 — Fetch filing document (EX-99.1 exhibit)")
    # ------------------------------------------------------------------
    t0 = time_mod.perf_counter()
    raw_text = await fetch_filing_document(result["index_url"], client)
    elapsed = time_mod.perf_counter() - t0

    assert raw_text, "fetch_filing_document returned empty string"
    print(f"Fetched in {elapsed:.3f}s  ({len(raw_text):,} chars)")
    print(f"\nFirst 200 chars:\n{raw_text[:200]}")
    print("\n[PASS] Document fetch")

    # ------------------------------------------------------------------
    _section("STEP 4 — Normalize & split sections")
    # ------------------------------------------------------------------
    clean_text = normalize(raw_text, "html")
    assert clean_text, "normalize() returned empty string"
    print(f"Clean text: {len(clean_text):,} chars")
    print(f"\nFirst 300 chars:\n{clean_text[:300]}")

    sections = split_sections(clean_text)
    assert "full" in sections,  "split_sections missing 'full' key"
    assert sections["full"],    "'full' section is empty"

    print("\nSections:")
    for key, val in sections.items():
        status = f"{len(val):,} chars" if val else "(empty)"
        print(f"  {key:12s}: {status}")

    assert sections["financials"], "financials section is empty — keyword matching may be broken"
    assert sections["guidance"],   "guidance section is empty — keyword matching may be broken"
    print("\n[PASS] Normalisation")

    # ------------------------------------------------------------------
    _section("STEP 5 — Summary  (AI workers NOT invoked)")
    # ------------------------------------------------------------------
    print(f"  Ticker      : {result['ticker']}")
    print(f"  Source      : {source}")
    print(f"  Accession   : {result['accession']}")
    print(f"  Pipelines   : {result['pipelines']}")
    print(f"  Detected at : {result['detected_at']}")
    print(f"  Content     : {len(clean_text):,} chars cleaned  "
          f"({len(sections['financials']):,} financials / "
          f"{len(sections['guidance']):,} guidance)")
    print()
    print("All checks passed. RSS detection pipeline is healthy.")


if __name__ == "__main__":
    asyncio.run(run())
