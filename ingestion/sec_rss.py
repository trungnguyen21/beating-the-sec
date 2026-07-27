import asyncio
import calendar
import hashlib
import httpx
import feedparser
from datetime import datetime, timedelta, timezone
from utils.logger import get_logger
from config import HEADERS
from ingestion.classifier import classify_items
from models.model_detectors import DetectorResult, DetectorSource

logger = get_logger(__name__)


async def poll_company_once(
    company: dict,
    client: httpx.AsyncClient,
    seen: set[str],
    max_age_hours: float = 3.0,
) -> DetectorResult | None:
    """
    Single attempt: fetch the company's SEC RSS feed and return a DetectorResult if a new
    actionable 8-K is found, else None. Does not loop or sleep — callers control cadence
    (detect_via_rss's own loop, or an external scheduler like rotation_scheduler.py).
    `seen` is owned by the caller so it persists across calls.
    Per-poll soft age check: entries older than max_age_hours are skipped, not blocked on.
    Raises on request/parse errors — callers decide how to handle a failed poll.
    """
    ticker = company["ticker"]
    rss_url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcompany&CIK={company['cik']}&type=8-K&output=atom"
    )

    r = await client.get(rss_url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    feed = feedparser.parse(r.text)
    for entry in feed.entries[:1]:
        items_desc = entry.get('items-desc', '')
        pipelines = classify_items(items_desc)
        if not pipelines:
            continue

        filed_at = None
        if hasattr(entry, 'updated_parsed') and entry.updated_parsed:
            filed_at = datetime.fromtimestamp(
                calendar.timegm(entry.updated_parsed), tz=timezone.utc
            )
            age = datetime.now(timezone.utc) - filed_at
            if age > timedelta(hours=max_age_hours):
                logger.debug(f"RSS: {ticker} — entry {age} old, skipping")
                continue

        link_hash = hashlib.sha256(entry.link.encode()).hexdigest()
        if link_hash in seen:
            continue
        seen.add(link_hash)

        logger.info(f"📡 RSS hit for {ticker}: {entry.title} — {pipelines} - {entry['filing-href']}")
        return DetectorResult(
            ticker=company["ticker"],
            cik=company["cik"],
            accession=entry.get('accession-number', ''),
            index_url=entry.link,
            detected_at=datetime.now(timezone.utc).isoformat(),
            source=DetectorSource.RSS_POLLER,
            filed_at=filed_at,
        )

    return None


async def detect_via_rss(
    company: dict,
    client: httpx.AsyncClient,
    seen: set[str],
    max_age_hours: float = 3.0,
) -> DetectorResult:
    """Poll the SEC RSS feed until a new actionable 8-K appears.
    Thin wrapper around poll_company_once for callers that want "poll this one company
    forever" behavior (e.g. local dev/testing without the rotation scheduler).
    """
    ticker = company["ticker"]
    logger.info(f"Watching the RSS feed for {ticker}")

    while True:
        try:
            result = await poll_company_once(company, client, seen, max_age_hours)
            if result:
                return result
        except Exception as e:
            logger.error(f"RSS poll error ({ticker}): {e}")
            # Continue looping — a transient error shouldn't kill the detector.

        await asyncio.sleep(1)


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
