import asyncio
import httpx
import feedparser
import sys
import os

# Add root directory to python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import HEADERS
from ingestion.edgar_poller import fetch_filing_document

async def test_sec_rss():
    company_cik = "0000320193" # Apple Inc.
    ticker = "AAPL"
    
    rss_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={company_cik}&type=8-K&output=atom"
    print(f"📡 Polling SEC RSS feed for {ticker}...")
    print(f"URL: {rss_url}")
    
    async with httpx.AsyncClient() as client:
        r = await client.get(rss_url, headers=HEADERS, timeout=10)
        print(f"Status Code: {r.status_code}")
        
        if r.status_code != 200:
            print("Failed to fetch RSS. Check headers or network.")
            return

        feed = feedparser.parse(r.text)
        print(f"Found {len(feed.entries)} total entries in the feed.\n")
        
        print("--- Latest 5 Entries ---")
        for i, entry in enumerate(feed.entries[:5]):
            title = entry.title.upper()
            is_match = "8-K" in title or "EARNINGS" in title or "PRESS RELEASE" in title
            
            print(f"[{i+1}] Title: {entry.title}")
            print(f"    Link: {entry.link}")
            print(f"    Matches Filter?: {'✅ YES' if is_match else '❌ NO'}")
            
            if is_match:
                print("    Fetching actual document text...")
                try:
                    text = await fetch_filing_document(entry.link, client)
                    if text:
                        print(f"    ✅ Successfully extracted exhibit! Length: {len(text)} characters.")
                        print(f"    Preview: {text[:150].strip()}...\n")
                    else:
                        print("    ❌ Could not find the main document link on the index page.\n")
                except Exception as e:
                    print(f"    ❌ Error fetching document: {e}\n")
            else:
                print()

if __name__ == "__main__":
    asyncio.run(test_sec_rss())
