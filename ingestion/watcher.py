# ingestion/watcher.py
import asyncio
import httpx

from utils.logger import get_logger
from ingestion.edgar_poller import get_last_accession, next_accession, poll_for_filing, fetch_filing_document
from ingestion.sec_rss import detect_via_rss
from ingestion.estimates import fetch_estimates
from ingestion.rotation_scheduler import CalendarHotSet, CooldownAwareHotSet, run_rotation
from models.model_detectors import DetectorResult
from config import MAX_FILING_AGE_HOURS, POST_FILING_COOLDOWN_HOURS, DETECTION_BUDGET

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
    Continuously watch a single company via its own independent detector race.
    Superseded by run_watcher's rotation-scheduler-based orchestration below for actual
    production use (see docs/checklist.md Phase 4) — kept here as the primitive
    tests/test_e2e_rss_detection.py exercises directly, and as a reference implementation of
    the per-company race semantics the rotation scheduler now runs at fleet scale instead.
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


async def run_watcher(watchlist: list, queue) -> None:
    """
    Fleet-scale orchestration (docs/checklist.md Phase 4): instead of one independent
    detector race per company (watch_company, above), all companies are driven through a
    shared budget-capped rotation scheduler.

    run_backstop_poller (ingestion/backstop_poller.py) is deliberately NOT wired in here.
    Its accession-prediction trick (next_accession + build_index_url) assumes accession
    numbers increment predictably and that every filer's index URL follows the same naming
    convention — observed in practice to be unreliable across enough companies (different
    filing agents assign accession numbers differently) to not trust as a live detection
    path. Kept in the codebase for a future, more robust per-filer approach — not deleted,
    just not called.

    Hot-set source is CalendarHotSet (docs/checklist.md Phase 6): a company is only hot if
    it's expected to report earnings today (per the daily db/refresh_calendar.py snapshot) AND
    the current ET time is within ±15min of that company's own estimated report time (Phase
    6d) — or, if yfinance gave no usable time for it, inside the broader premarket/aftermarket
    block as a fallback. Fully empty otherwise, which is the whole point of this design (SEC
    request volume no longer scales with watchlist size). `watchlist` is passed through as a
    ticker_filter so the calendar doesn't waste budget polling companies nobody is actually
    subscribed to. CooldownAwareHotSet still applies POST_FILING_COOLDOWN_HOURS on top so a
    company that just fired isn't immediately re-synced back into rotation next tick.
    """
    async with httpx.AsyncClient() as client:
        watchlist_tickers = {c["ticker"] for c in watchlist}
        base_hot_set = CalendarHotSet(ticker_filter=watchlist_tickers)
        hot_set = CooldownAwareHotSet(base_hot_set, POST_FILING_COOLDOWN_HOURS)

        async def on_detected(company: dict, result: DetectorResult) -> None:
            ticker = company["ticker"]
            result.ticker = ticker
            logger.info(f"🏁 {ticker} detected")

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

            hot_set.mark_detected(ticker)
            logger.info(f"💤 {ticker} — cooling down for {POST_FILING_COOLDOWN_HOURS}h")

        await run_rotation(
            hot_set, client, on_detected,
            detection_budget=DETECTION_BUDGET,
            max_age_hours=MAX_FILING_AGE_HOURS,
            describe_upcoming=base_hot_set.upcoming_companies,
        )
