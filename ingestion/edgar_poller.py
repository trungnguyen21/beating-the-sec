# ingestion/edgar_poller.py
"""
Credit: John Friedman - The Fastest Way to get SEC filings
https://github.com/john-friedman/The-fastest-way-to-get-SEC-filings
"""

import asyncio
import httpx
import json
from datetime import datetime, timezone

from utils.logger import get_logger
from config import HEADERS

logger = get_logger(__name__)

EDGAR_BASE = "https://www.sec.gov/Archives/edgar/data"
SUBMISSIONS_API = "https://data.sec.gov/submissions/CIK{cik}.json"

HEADERS['Accept-Encoding'] = "gzip, deflate"

async def get_last_accession(cik: str, client: httpx.AsyncClient) -> str:
    """Fetch the company's most recent accession number from SEC submissions API."""
    url = SUBMISSIONS_API.format(cik=cik.zfill(10))
    r = await client.get(url, headers=HEADERS)
    data = r.json()
    # Most recent filing is index 0
    recent = data["filings"]["recent"]
    accession = recent["accessionNumber"][0]  # format: 0001213900-25-125488
    logger.info(f"Last accession number found: {accession}")
    return accession

def next_accession(accession: str) -> str:
    """Increment the sequential part of an accession number."""
    prefix, year, seq = accession.split("-")
    next_accession = f"{prefix}-{year}-{str(int(seq) + 1).zfill(6)}"
    logger.info(f"Next accession number: {next_accession}")
    return next_accession

def build_index_url(company_cik: str, accession: str) -> str:
    accession_nodash = accession.replace("-", "")
    index_url = f"{EDGAR_BASE}/{company_cik}/{accession_nodash}/{accession}-index.htm"
    logger.info(f"Index URL: {index_url}")
    return index_url

async def poll_for_filing(company_cik: str, predicted_accession: str,
                           client: httpx.AsyncClient, timeout: int = 3600) -> dict | None:
    """
    Poll the predicted index URL every 1 seconds until it returns 200.
    Returns filing metadata dict when found.
    """
    url = build_index_url(company_cik, predicted_accession)
    deadline = asyncio.get_event_loop().time() + timeout
    
    while asyncio.get_event_loop().time() < deadline:
        try:
            r = await client.head(url, headers=HEADERS, timeout=5)
            if r.status_code == 200:
                detected_at = datetime.now(timezone.utc).isoformat()
                logger.info(f"✅ Filing detected: {predicted_accession} at {detected_at}")
                return {
                    "accession": predicted_accession,
                    "company_cik": company_cik,
                    "index_url": url,
                    "detected_at": detected_at,
                }
        except Exception as e:
            logger.error(f"Poll error: {e}")
        
        await asyncio.sleep(1)  # Poll every 1 seconds (stay under SEC 10 req/s limit)
    
    return None

async def fetch_filing_document(index_url: str, client: httpx.AsyncClient) -> str:
    """
    Parse the index page to find the actual earnings document (8-K exhibit).
    Returns the raw text content.
    """
    r = await client.get(index_url, headers=HEADERS)
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(r.text, "html.parser")
    
    # Find the primary document link (usually the press release exhibit)
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) >= 3:
            doc_type = cells[1].text.strip()
            if doc_type in ("EX-99.1", "8-K"):  # Earnings press release
                href = cells[2].find("a")["href"]
                doc_url = f"https://www.sec.gov{href}"
                doc_r = await client.get(doc_url, headers=HEADERS)
                return doc_r.text
    
    return ""

async def watch_company(company: dict, client: httpx.AsyncClient, queue):
    """Watch a single company for new earnings filings."""
    cik = company["cik"]
    ticker = company["ticker"]
    
    print(f"👁  Watching {ticker} (CIK: {cik})")
    
    last_accession = await get_last_accession(cik, client)
    predicted = next_accession(last_accession)
    
    result = await poll_for_filing(cik, predicted, client)
    if result:
        result["ticker"] = ticker
        await queue.enqueue(result)  # Send to AI pipeline

async def run_watcher(watchlist_path: str, queue):
    with open(watchlist_path) as f:
        watchlist = json.load(f)
    
    async with httpx.AsyncClient() as client:
        tasks = [watch_company(company, client, queue) for company in watchlist]
        await asyncio.gather(*tasks)