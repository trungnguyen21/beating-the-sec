import asyncio
import hashlib
import httpx
import feedparser
from datetime import datetime, timezone
from utils.logger import get_logger
from config import HEADERS
from ingestion.classifier import classify_items

logger = get_logger(__name__)


async def monitor_nasdaq_rss(company: dict, queue, seen: set):
    """
    Poll the SEC EDGAR RSS/Atom feed for a specific company.
    Classifies items-desc immediately to skip non-actionable 8-Ks before fetching.
    """
    ticker = company["ticker"]
    rss_url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcompany&CIK={company['cik']}&type=8-K&output=atom"
    )

    async with httpx.AsyncClient() as client:
        while True:
            try:
                r = await client.get(rss_url, headers=HEADERS, timeout=10)
                feed = feedparser.parse(r.text)

                for entry in feed.entries[:1]:
                    # Classify immediately — no network fetch needed yet
                    items_desc = entry.get('items-desc', '')
                    pipelines = classify_items(items_desc)

                    if not pipelines:
                        logger.debug(f"RSS: {ticker} — no actionable items in '{items_desc}', skipping")
                        continue

                    # Check recency (skip filings older than 24 hours)
                    is_new = True
                    if hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                        import calendar
                        from datetime import timedelta
                        timestamp = calendar.timegm(entry.updated_parsed)
                        entry_date = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                        if datetime.now(timezone.utc) - entry_date > timedelta(hours=24):
                            is_new = False

                    if not is_new:
                        continue

                    link = entry.link
                    link_hash = hashlib.sha256(link.encode()).hexdigest()

                    if link_hash not in seen:
                        seen.add(link_hash)
                        logger.info(f"📡 RSS hit for {ticker}: {entry.title} — pipelines: {pipelines}")

                        await queue.enqueue({
                            "source": "sec_rss",
                            "ticker": ticker,
                            "cik": company["cik"],
                            "accession": entry.get('accession-number', ''),
                            "index_url": link,
                            "pipelines": pipelines,
                            "detected_at": datetime.now(timezone.utc).isoformat(),
                        })

            except Exception as e:
                logger.error(f"RSS monitor error ({ticker}): {e}")

            await asyncio.sleep(3)
