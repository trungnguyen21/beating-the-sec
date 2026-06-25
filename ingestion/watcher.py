# ingestion/watcher.py
import asyncio
import json
import httpx

from utils.logger import get_logger
from ingestion.edgar_poller import get_last_accession, next_accession, poll_for_filing, fetch_filing_document
from ingestion.sec_rss import detect_via_rss
from ingestion.estimates import fetch_estimates
from models.model_detectors import DetectorResult

MAX_FILING_AGE_HOURS = 2       # ignore filings older than this
POST_FILING_COOLDOWN_HOURS = 4  # wait this long before watching for the next filing

logger = get_logger(__name__)


async def _detect_filing(
    company: dict, client: httpx.AsyncClient, seen: set[str]
) -> tuple[DetectorResult, str]:
    """
    Race all available detectors for a single company.
    `seen` is passed through to detect_via_rss so it persists across calls.
    """
    cik = company["cik"]
    last_accession = await get_last_accession(cik, client)
    predicted = next_accession(last_accession)

    detectors = {
        asyncio.create_task(poll_for_filing(cik, predicted, client), name="poller"): "poller",
        asyncio.create_task(
            detect_via_rss(company, client, seen, max_age_hours=MAX_FILING_AGE_HOURS),
            name="rss",
        ): "rss",
    }

    done, pending = await asyncio.wait(detectors, return_when=asyncio.FIRST_COMPLETED)

    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    winner = done.pop()
    if winner.exception():
        raise winner.exception()

    source = detectors[winner]
    return winner.result(), source


async def watch_company(company: dict, client: httpx.AsyncClient, queue) -> None:
    """
    Continuously watch a single company. After each detected filing is enqueued,
    loops back to wait for the next one — runs until cancelled.
    """
    ticker = company["ticker"]
    logger.info(f"👁  Watching {ticker} (CIK: {company['cik']})")

    # Persists across detection races so already-seen filings are never re-returned.
    seen: set[str] = set()

    while True:
        try:
            result, source = await _detect_filing(company, client, seen)
        except Exception as e:
            logger.error(f"Detection failed for {ticker}: {e}")
            await asyncio.sleep(5)
            continue

        if not result:
            await asyncio.sleep(5)
            continue

        logger.info(f"🏁 {ticker} detected via {source}")
        result.ticker = ticker

        raw_text, estimates = await asyncio.gather(
            fetch_filing_document(result.index_url, client),
            fetch_estimates(ticker),
        )

        if not raw_text:
            logger.warning(f"No exhibit found for {ticker} — skipping")
        else:
            result.raw_text = raw_text
            result.estimates = estimates
            await queue.enqueue(result)

        logger.info(f"💤 {ticker} — cooling down for {POST_FILING_COOLDOWN_HOURS}h")
        await asyncio.sleep(POST_FILING_COOLDOWN_HOURS * 3600)


async def run_watcher(watchlist_path: str, queue) -> None:
    with open(watchlist_path) as f:
        watchlist = json.load(f)

    async with httpx.AsyncClient() as client:
        await asyncio.gather(*[watch_company(company, client, queue) for company in watchlist])
