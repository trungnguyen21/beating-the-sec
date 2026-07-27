"""
Budget-capped rotation scheduler for SEC EDGAR detection polling.

Instead of one independent polling loop per watched company (which blows SEC's ~10 req/sec
fair-access budget once the watchlist grows), this drives a shared "hot set" of companies
through a single loop that never polls more than `detection_budget` companies per second.
A company that fires a detection is not requeued — that immediately frees its slot for the
rest of the hot set, shrinking worst-case latency for everyone still waiting as the tick
progresses.

Calendar-based hot-set gating (deciding *which* companies are "hot" on a given day) is a
separate, deferred concern — see docs/checklist.md Phase 6. This module only consumes
whatever `hot_set_provider.current_hot_companies()` returns.
"""

import asyncio
import math
import sqlite3
from collections import deque
from datetime import date, datetime, time, timedelta, timezone
from typing import Awaitable, Callable, Protocol
from zoneinfo import ZoneInfo

import httpx

from config import DB_PATH, DETECTION_BUDGET
from ingestion.sec_rss import poll_company_once
from models.model_detectors import DetectorResult
from utils.logger import get_logger

logger = get_logger(__name__)

OnDetected = Callable[[dict, DetectorResult], Awaitable[None] | None]


class HotSetProvider(Protocol):
    def current_hot_companies(self) -> list[dict]: ...


class FixedHotSet:
    """Trivial stub hot-set provider for testing/local dev ahead of the real calendar-based
    provider (docs/checklist.md Phase 6)."""

    def __init__(self, companies: list[dict]):
        self._companies = companies

    def current_hot_companies(self) -> list[dict]:
        return self._companies


class CooldownAwareHotSet:
    """Wraps another hot-set provider, filtering out companies currently in a post-detection
    cooldown window so a company that just fired isn't immediately re-synced back into the
    rotation queue next tick (the base provider — e.g. FixedHotSet, or the real Phase 6
    calendar provider later — has no notion of "just detected, give it a rest")."""

    def __init__(self, base_provider: HotSetProvider, cooldown_hours: float):
        self._base = base_provider
        self._cooldown = timedelta(hours=cooldown_hours)
        self._cooldown_until: dict[str, datetime] = {}

    def mark_detected(self, ticker: str) -> None:
        self._cooldown_until[ticker] = datetime.now(timezone.utc) + self._cooldown

    def current_hot_companies(self) -> list[dict]:
        now = datetime.now(timezone.utc)
        return [
            c for c in self._base.current_hot_companies()
            if self._cooldown_until.get(c["ticker"], now) <= now
        ]


ET = ZoneInfo("America/New_York")
PREMARKET_START = time(6, 0)
PREMARKET_END = time(9, 30)
AFTERMARKET_START = time(16, 0)
AFTERMARKET_END = time(18, 0)


def _in_polling_window(now_et: datetime) -> bool:
    t = now_et.time()
    return (PREMARKET_START <= t <= PREMARKET_END) or (AFTERMARKET_START <= t <= AFTERMARKET_END)


class CalendarHotSet:
    """Real hot-set provider (docs/checklist.md Phase 6) — a company is "hot" only if it's
    expected to report earnings today (exact date match, no ±1 day hedge — a wrong predicted
    date is a full miss for that day, by design) AND the current ET time is inside the
    premarket (~6:00-9:30) or aftermarket (~4:00-6:00pm) window. Outside those windows the hot
    set is fully empty, even for companies confirmed reporting today — zero SEC requests
    during midday/overnight is the actual point of this class, not a compromise.

    `now_fn` is injectable so tests can simulate specific times without waiting on real
    windows, matching the injectable-provider pattern established for FixedHotSet/on_detected.

    `ticker_filter`, if given, restricts the calendar-driven result to just these tickers.
    Without it, every company in the DB reporting earnings today is hot — including ones
    nobody is actually subscribed to, which wastes SEC request budget on companies nobody
    cares about. `run_watcher` passes its `watchlist` param here for exactly that reason.
    """

    def __init__(
        self,
        db_path: str = DB_PATH,
        now_fn: Callable[[], datetime] = None,
        ticker_filter: set[str] | None = None,
    ):
        self._db_path = db_path
        self._now_fn = now_fn or (lambda: datetime.now(ET))
        self._ticker_filter = ticker_filter
        self._cached_date: date | None = None
        self._cached_companies: list[dict] = []

    def _query_hot_companies(self, today: date) -> list[dict]:
        conn = sqlite3.connect(self._db_path)
        try:
            rows = conn.execute(
                """SELECT ec.ticker, t.cik, t.title
                   FROM earnings_calendar ec
                   JOIN tickers t ON t.ticker = ec.ticker
                   WHERE ec.earnings_date = ?""",
                (today.isoformat(),),
            ).fetchall()
        finally:
            conn.close()
        companies = [{"ticker": r[0], "cik": r[1], "title": r[2]} for r in rows]
        if self._ticker_filter is not None:
            companies = [c for c in companies if c["ticker"] in self._ticker_filter]
        return companies

    def current_hot_companies(self) -> list[dict]:
        now_et = self._now_fn()

        if not _in_polling_window(now_et):
            return []

        today = now_et.date()
        if today != self._cached_date:
            self._cached_companies = self._query_hot_companies(today)
            self._cached_date = today

        return self._cached_companies


def _sync_queue(queue: deque, current_hot: list[dict]) -> None:
    """Add newly-hot companies, drop companies no longer in the hot set.
    Only touches the deque — in-flight companies mid-batch aren't affected, since a batch is
    already popped out of the deque by the time this runs again next tick."""
    current_tickers = {c["ticker"] for c in current_hot}
    queued_tickers = {c["ticker"] for c in queue}

    # Drop companies that fell out of the hot set.
    for company in [c for c in queue if c["ticker"] not in current_tickers]:
        queue.remove(company)

    # Add newly-hot companies not already queued.
    for company in current_hot:
        if company["ticker"] not in queued_tickers:
            queue.append(company)


async def run_rotation(
    hot_set_provider: HotSetProvider,
    client: httpx.AsyncClient,
    on_detected: OnDetected,
    detection_budget: int = DETECTION_BUDGET,
    max_age_hours: float = 3.0,
) -> None:
    """
    Runs forever. Each tick:
      1. Re-sync the rotation queue against the current hot set.
      2. Pop up to `detection_budget` companies and poll them concurrently.
      3. A detected company is handed to `on_detected` and NOT requeued (frees its slot).
      4. A miss or a per-company failure is requeued for the next tick — one company's
         failure never blocks or cancels the rest of the batch.
    """
    queue: deque = deque(hot_set_provider.current_hot_companies())
    if detection_budget <= 0:
        raise ValueError("detection_budget must be >= 1")
    seen_map: dict[str, set[str]] = {}
    while True:
        _sync_queue(queue, hot_set_provider.current_hot_companies())

        if queue:
            worst_case_seconds = math.ceil(len(queue) / detection_budget)
            logger.info(
                f"Rotation: {len(queue)} hot company(ies), budget={detection_budget}/s, "
                f"worst-case detection lag ~{worst_case_seconds}s"
            )

        batch = [queue.popleft() for _ in range(min(detection_budget, len(queue)))]

        if batch:
            results = await asyncio.gather(
                *[
                    poll_company_once(
                        company, client, seen_map.setdefault(company["ticker"], set()), max_age_hours
                    )
                    for company in batch
                ],
                return_exceptions=True,
            )

            for company, result in zip(batch, results):
                if isinstance(result, Exception):
                    logger.error(f"Rotation poll failed for {company['ticker']}: {result}")
                    queue.append(company)  # treat like a miss, retry next tick
                elif result:
                    try:
                        maybe_awaitable = on_detected(company, result)
                        if maybe_awaitable is not None:
                            await maybe_awaitable
                    except Exception as e:
                        logger.error(f"Rotation on_detected failed for {company['ticker']}: {e}")
                        queue.append(company)

        await asyncio.sleep(1)  # TODO(tick-drift): compensate for per-tick work time once
                                 # Phase 5 has real numbers to tune against.
