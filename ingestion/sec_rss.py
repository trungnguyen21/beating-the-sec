import asyncio
import hashlib
import httpx
import feedparser
from datetime import datetime, timezone
from utils.logger import get_logger
from config import HEADERS

logger = get_logger(__name__)

async def monitor_nasdaq_rss(company: dict, queue, seen: set):
    """
    Poll the Nasdaq RSS feed for a specific company.
    Used as a last resort fallback in the ingestion pipeline.
    """
    ticker = company["ticker"]
    
    # Nasdaq's general outbound RSS feed per symbol
    # rss_url = f"https://www.nasdaq.com/feed/rssoutbound?symbol={ticker}"
    
    rss_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={company['cik']}&type=8-K&output=atom"

    async with httpx.AsyncClient() as client:
        while True:
            try:
                r = await client.get(rss_url, headers=HEADERS, timeout=10)
                
                # Parse the RSS/Atom feed using feedparser
                feed = feedparser.parse(r.text)
                
                for entry in feed.entries[:1]:
                    title = entry.title.upper()
                    
                    # Check if the report is new (published within the last 24 hours)
                    is_new = True
                    if hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                        import calendar
                        from datetime import timedelta
                        
                        # feedparser returns time.struct_time in UTC for updated_parsed
                        timestamp = calendar.timegm(entry.updated_parsed)
                        entry_date = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                        
                        if datetime.now(timezone.utc) - entry_date > timedelta(hours=24):
                            is_new = False
                    
                    # Filter for new earnings or 8-K related items
                    if is_new and ("8-K" in title or "EARNINGS" in title or "PRESS RELEASE" in title):
                        link = entry.link
                        link_hash = hashlib.sha256(link.encode()).hexdigest()
                        
                        if link_hash not in seen:
                            seen.add(link_hash)
                            logger.info(f"📡 RSS fallback hit for {ticker}: {entry.title}")
                            
                            # Enqueue the finding
                            await queue.enqueue({
                                "source": "nasdaq_rss",
                                "ticker": ticker,
                                "url": link,
                                "title": entry.title,
                                "detected_at": datetime.now(timezone.utc).isoformat(),
                            })
                            # Typically you only want the newest one, so break here if feeds are chronological
                            # break
                            
            except Exception as e:
                logger.error(f"RSS monitor error ({ticker}): {e}")
            
            # Poll every 10 seconds for the fallback
            await asyncio.sleep(5)
