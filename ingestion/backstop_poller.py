"""
Low-cadence secondary confirmation for companies currently in the hot set — a backstop in
case the RSS-based rotation scheduler (ingestion/rotation_scheduler.py) misses a filing.

Deliberately NOT a per-company 1s hot loop (that was the original budget collision this whole
redesign exists to fix). Each cycle does at most one HEAD request per hot company, spaced
`interval_seconds` apart, checking whether the predicted next accession number has appeared —
reusing the accession-prediction building blocks from edgar_poller.py rather than its own
internal 60s polling loop (poll_for_filing), which isn't a single-attempt primitive and would
be awkward to budget cleanly against a shared rotation.
"""

import asyncio
from datetime import datetime, timezone

import httpx

from config import HEADERS, BACKSTOP_POLL_INTERVAL_SECONDS
from ingestion.edgar_poller import get_last_accession, next_accession, build_index_url
from ingestion.rotation_scheduler import HotSetProvider, OnDetected
from models.model_detectors import DetectorResult, DetectorSource
from utils.logger import get_logger

logger = get_logger(__name__)


async def _backstop_check_once(
    company: dict,
    client: httpx.AsyncClient,
    predicted_cache: dict[str, str],
) -> DetectorResult | None:
    """Single HEAD check against this company's predicted next accession index URL.
    Caches the predicted accession per ticker so a hit isn't required to re-fetch
    get_last_accession every cycle — only refreshed once the predicted accession is
    consumed (i.e. after a detection, the caller drops the ticker from the cache)."""
    ticker = company["ticker"]
    cik = company["cik"]

    if ticker not in predicted_cache:
        last_accession = await get_last_accession(cik, client)
        predicted_cache[ticker] = next_accession(last_accession)

    predicted = predicted_cache[ticker]
    url = build_index_url(cik, predicted)
    r = await client.head(url, headers=HEADERS, timeout=5)

    if r.status_code == 200:
        return DetectorResult(
            ticker=ticker,
            cik=cik,
            accession=predicted,
            index_url=url,
            detected_at=datetime.now(timezone.utc).isoformat(),
            source=DetectorSource.EDGAR_POLLER,
        )
    return None


async def run_backstop_poller(
    hot_set_provider: HotSetProvider,
    client: httpx.AsyncClient,
    on_detected: OnDetected,
    interval_seconds: float = BACKSTOP_POLL_INTERVAL_SECONDS,
) -> None:
    """
    Runs forever. Every `interval_seconds`, checks every company currently in the hot set
    once. A per-company failure is logged and skipped — it never blocks the rest of the
    cycle. On a hit, calls `on_detected` and drops the ticker's cached prediction so the next
    time it re-enters the hot set (e.g. next quarter) it re-derives a fresh prediction.
    """
    predicted_cache: dict[str, str] = {}

    while True:
        for company in hot_set_provider.current_hot_companies():
            ticker = company["ticker"]
            try:
                result = await _backstop_check_once(company, client, predicted_cache)
            except Exception as e:
                logger.error(f"Backstop poll failed for {ticker}: {e}")
                continue

            if result:
                predicted_cache.pop(ticker, None)
                maybe_awaitable = on_detected(company, result)
                if maybe_awaitable is not None:
                    await maybe_awaitable

        await asyncio.sleep(interval_seconds)
