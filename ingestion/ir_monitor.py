# ingestion/ir_monitor.py
import asyncio
import hashlib
import httpx
from bs4 import BeautifulSoup

async def monitor_ir_page(company: dict, client: httpx.AsyncClient, queue, seen: set):
    """
    Poll a company's IR press release page every 5 seconds.
    Many companies post here simultaneously with or just before SEC filing.
    """
    url = company["ir_url"]
    ticker = company["ticker"]
    
    while True:
        try:
            r = await client.get(url, timeout=10)
            soup = BeautifulSoup(r.text, "html.parser")
            
            # Use the company-specific selector from watchlist, or fallback to a default
            selector = company.get("ir_selector", ".press-release-item a, .news-list a")
            latest = soup.select_one(selector)
            if latest:
                link_hash = hashlib.sha256(latest["href"].encode()).hexdigest()
                if link_hash not in seen:
                    seen.add(link_hash)
                    print(f"📰 IR page hit: {ticker}")
                    await queue.enqueue({
                        "source": "ir_website",
                        "ticker": ticker,
                        "url": latest["href"],
                        "detected_at": ...,
                    })
        except Exception as e:
            print(f"IR monitor error ({ticker}): {e}")
        
        await asyncio.sleep(5)