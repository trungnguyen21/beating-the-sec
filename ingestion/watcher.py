# ingestion/watcher.py
import asyncio
import json
import httpx

from utils.logger import get_logger
from ingestion.edgar_poller import get_last_accession, next_accession, poll_for_filing, fetch_filing_document
from ingestion.sec_rss import detect_via_rss
from ingestion.estimates import fetch_estimates

logger = get_logger(__name__)


async def _detect_filing(company: dict, client: httpx.AsyncClient) -> tuple[dict, str]:
    """
    Race all available detectors for a single company.
    Returns (filing_metadata, source_name) from whichever wins.
    Add new detectors here as additional tasks.
    """
    cik = company["cik"]
    last_accession = await get_last_accession(cik, client)
    predicted = next_accession(last_accession)

    detectors = {
        asyncio.create_task(poll_for_filing(cik, predicted, client), name="poller"): "poller",
        asyncio.create_task(detect_via_rss(company, client), name="rss"): "rss",
    }

    done, pending = await asyncio.wait(detectors, return_when=asyncio.FIRST_COMPLETED)

    for task in pending:
        task.cancel()

    winner = done.pop()
    if winner.exception():
        raise winner.exception()

    source = detectors[winner]
    return winner.result(), source


async def watch_company(company: dict, client: httpx.AsyncClient, queue) -> None:
    """
    Coordinate detection and exhibit fetching for a single company.
    Races all detectors, fetches the exhibit from the winner, and enqueues.
    """
    ticker = company["ticker"]
    logger.info(f"👁  Watching {ticker} (CIK: {company['cik']})")

    try:
        result, source = await _detect_filing(company, client)
    except Exception as e:
        logger.error(f"Detection failed for {ticker}: {e}")
        return

    if not result:
        return

    logger.info(f"🏁 {ticker} detected via {source}")
    result["ticker"] = ticker

    raw_text, estimates = await asyncio.gather(
        fetch_filing_document(result["index_url"], client),
        fetch_estimates(ticker),
    )

    if not raw_text:
        logger.warning(f"No exhibit found for {ticker} — skipping")
        return

    result["raw_text"] = raw_text
    result["estimates"] = estimates
    await queue.enqueue(result)


async def run_watcher(watchlist_path: str, queue) -> None:
    with open(watchlist_path) as f:
        watchlist = json.load(f)

    async with httpx.AsyncClient() as client:
        await asyncio.gather(*[watch_company(company, client, queue) for company in watchlist])
