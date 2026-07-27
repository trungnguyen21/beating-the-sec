import httpx
import feedparser
import pytest

from config import HEADERS
from ingestion.edgar_poller import fetch_filing_document


@pytest.mark.e2e
async def test_sec_rss_feed_and_exhibit_fetch():
    """Hits live SEC EDGAR — confirms the RSS feed parses and at least one matching
    entry's exhibit document can be fetched and is non-empty."""
    company_cik = "0000320193"  # Apple Inc.

    rss_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={company_cik}&type=8-K&output=atom"

    async with httpx.AsyncClient() as client:
        r = await client.get(rss_url, headers=HEADERS, timeout=10)
        assert r.status_code == 200, "Failed to fetch RSS feed"

        feed = feedparser.parse(r.text)
        assert len(feed.entries) > 0, "Expected at least one entry in the feed"

        matches = [
            entry for entry in feed.entries[:5]
            if any(kw in entry.title.upper() for kw in ("8-K", "EARNINGS", "PRESS RELEASE"))
        ]
        assert matches, "Expected at least one matching entry in the latest 5"

        text = await fetch_filing_document(matches[0].link, client)
        assert text, "Could not extract exhibit document text from the matching entry"
