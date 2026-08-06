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
from collections import deque
from datetime import date, datetime, timedelta, timezone
from typing import Awaitable, Callable, Protocol

import httpx
import psycopg

from config import DATABASE_URL, DETECTION_BUDGET, ROTATION_POLL_TIMEOUT_SECONDS
from config import OUTER_SHORT_CIRCUIT_END, OUTER_SHORT_CIRCUIT_START, PREMARKET_START, PREMARKET_END, AFTERMARKET_START, AFTERMARKET_END, TIME_WINDOW_PAD, ET
from ingestion.sec_rss import poll_company_once
from models.model_detectors import DetectorResult
from utils.logger import get_logger

logger = get_logger(__name__)

OnDetected = Callable[[dict, DetectorResult], Awaitable[None] | None]

IDLE_HEARTBEAT_TICKS = 300  # ~5 minutes at the 1s/tick sleep below


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


def _maybe_active_now(now_et: datetime) -> bool:
    t = now_et.time()
    return OUTER_SHORT_CIRCUIT_START <= t <= OUTER_SHORT_CIRCUIT_END


def _in_company_window(now_et: datetime, earnings_time: str | None) -> bool:
    """earnings_time is ET "HH:MM:SS" or None. With a time, hot only within ±TIME_WINDOW_PAD
    of it. Without one (yfinance gave no usable time for this company), fall back to the
    full premarket+aftermarket block — same safety net as before Phase 6d."""
    if not earnings_time:
        t = now_et.time()
        return (PREMARKET_START <= t <= PREMARKET_END) or (AFTERMARKET_START <= t <= AFTERMARKET_END)
    try:
        target_time = datetime.strptime(earnings_time, "%H:%M:%S").time()
    except ValueError:
        logger.warning(f"Unparseable earnings_time {earnings_time!r} — falling back to session window")
        t = now_et.time()
        return (PREMARKET_START <= t <= PREMARKET_END) or (AFTERMARKET_START <= t <= AFTERMARKET_END)
    target = now_et.replace(
        hour=target_time.hour, minute=target_time.minute, second=target_time.second, microsecond=0
    )
    return abs(now_et - target) <= TIME_WINDOW_PAD


class CalendarHotSet:
    """Real hot-set provider (docs/checklist.md Phase 6) — a company is "hot" only if it's
    expected to report earnings today (exact date match, no ±1 day hedge — a wrong predicted
    date is a full miss for that day, by design) AND the current ET time is within
    ±TIME_WINDOW_PAD of its own estimated report time (Phase 6d) — or, if no report time is
    known, within the broader premarket (~6:00-9:30) or aftermarket (~4:00-6:00pm) block.
    Outside those windows the hot set is fully empty, even for companies confirmed reporting
    today — zero SEC requests during midday/overnight is the actual point of this class, not
    a compromise.

    `now_fn` is injectable so tests can simulate specific times without waiting on real
    windows, matching the injectable-provider pattern established for FixedHotSet/on_detected.

    `ticker_filter`, if given, restricts the calendar-driven result to just these tickers.
    Without it, every company in the DB reporting earnings today is hot — including ones
    nobody is actually subscribed to, which wastes SEC request budget on companies nobody
    cares about. `run_watcher` passes its `watchlist` param here for exactly that reason.

    `cache_ttl` bounds how stale the underlying "who's reporting today" query can get — this
    instance lives for the lifetime of the pipeline process (run_watcher builds one and hands
    it to run_rotation, which loops forever), so without a TTL a same-day db/refresh_calendar.py
    run would never be picked up until the calendar date itself rolled over. Re-querying is
    cheap (small table, once per TTL window, not per tick) — the per-tick live behavior
    (_in_company_window filtering below) is unaffected either way.
    """

    def __init__(
        self,
        database_url: str = DATABASE_URL,
        now_fn: Callable[[], datetime] = None,
        ticker_filter: set[str] | None = None,
        cache_ttl: timedelta = timedelta(minutes=5),
    ):
        self._database_url = database_url
        self._now_fn = now_fn or (lambda: datetime.now(ET))
        self._ticker_filter = ticker_filter
        self._cache_ttl = cache_ttl
        self._cached_date: date | None = None
        self._cached_at: datetime | None = None
        self._cached_companies: list[dict] = []

    def _query_hot_companies(self, today: date) -> list[dict]:
        with psycopg.connect(self._database_url) as conn:
            rows = conn.execute(
                """SELECT ec.ticker, t.cik, t.title, ec.earnings_time
                   FROM earnings_calendar ec
                   JOIN tickers t ON t.ticker = ec.ticker
                   WHERE ec.earnings_date = %s""",
                (today.isoformat(),),
            ).fetchall()
        companies = [{"ticker": r[0], "cik": r[1], "title": r[2], "earnings_time": r[3]} for r in rows]
        if self._ticker_filter is not None:
            companies = [c for c in companies if c["ticker"] in self._ticker_filter]
        return companies

    def current_hot_companies(self) -> list[dict]:
        now_et = self._now_fn()

        if not _maybe_active_now(now_et):
            return []

        today = now_et.date()
        stale = self._cached_at is None or (now_et - self._cached_at) >= self._cache_ttl
        if today != self._cached_date or stale:
            self._cached_companies = self._query_hot_companies(today)
            self._cached_date = today
            self._cached_at = now_et

        return [c for c in self._cached_companies if _in_company_window(now_et, c["earnings_time"])]

    def upcoming_companies(self, hours: float = 24) -> list[dict]:
        """Companies expected to report within the next `hours` — for display/observability
        only, not used for actual polling decisions (that's current_hot_companies above).
        Ignores ticker cooldown state on purpose: this shows what the calendar has coming up,
        not real-time poll eligibility. Unknown earnings_time falls back to the start of the
        premarket window for ordering/display purposes, flagged via "time_known": False."""
        now_et = self._now_fn()
        horizon = now_et + timedelta(hours=hours)
        dates = sorted({now_et.date(), horizon.date()})

        placeholders = ",".join(["%s"] * len(dates))
        with psycopg.connect(self._database_url) as conn:
            rows = conn.execute(
                f"""SELECT ec.ticker, t.title, ec.earnings_date, ec.earnings_time
                    FROM earnings_calendar ec
                    JOIN tickers t ON t.ticker = ec.ticker
                    WHERE ec.earnings_date IN ({placeholders})""",
                tuple(d.isoformat() for d in dates),
            ).fetchall()

        upcoming = []
        for ticker, title, date_str, time_str in rows:
            if self._ticker_filter is not None and ticker not in self._ticker_filter:
                continue
            d = date.fromisoformat(date_str)
            t = datetime.strptime(time_str, "%H:%M:%S").time() if time_str else PREMARKET_START
            target = datetime.combine(d, t, tzinfo=ET)
            if now_et <= target <= horizon:
                upcoming.append({
                    "ticker": ticker, "title": title, "target": target, "time_known": bool(time_str),
                })

        upcoming.sort(key=lambda c: c["target"])
        return upcoming


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


def _format_upcoming(companies: list[dict]) -> str:
    now_et = datetime.now(ET)
    lines = []
    for c in companies:
        delta = c["target"] - now_et
        total_minutes = max(int(delta.total_seconds() // 60), 0)
        h, m = divmod(total_minutes, 60)
        time_note = "" if c["time_known"] else " (time unknown, estimated)"
        lines.append(
            f"  {c['ticker']:<6} {c['target'].strftime('%Y-%m-%d %H:%M')} ET "
            f"(in {h}h{m:02d}m){time_note} — {c['title']}"
        )
    return "\n".join(lines)


async def run_rotation(
    hot_set_provider: HotSetProvider,
    client: httpx.AsyncClient,
    on_detected: OnDetected,
    detection_budget: int = DETECTION_BUDGET,
    max_age_hours: float = 3.0,
    describe_upcoming: Callable[[], list[dict]] | None = None,
) -> None:
    """
    Runs forever. Each tick:
      1. Re-sync the rotation queue against the current hot set.
      2. Pop up to `detection_budget` companies and poll them concurrently.
      3. A detected company is handed to `on_detected` and NOT requeued (frees its slot).
      4. A miss or a per-company failure is requeued for the next tick — one company's
         failure never blocks or cancels the rest of the batch.

    `describe_upcoming`, if given (typically CalendarHotSet.upcoming_companies), is logged
    once at startup and again on every idle heartbeat — display/observability only, doesn't
    affect polling.
    """
    queue: deque = deque(hot_set_provider.current_hot_companies())
    if detection_budget <= 0:
        raise ValueError("detection_budget must be >= 1")
    seen_map: dict[str, set[str]] = {}
    idle_ticks = 0

    def _log_upcoming() -> None:
        if describe_upcoming is None:
            return
        upcoming = describe_upcoming()
        if upcoming:
            logger.info(
                f"📅 {len(upcoming)} compan{'y' if len(upcoming) == 1 else 'ies'} "
                f"expected to report in the next 24h:\n{_format_upcoming(upcoming)}"
            )
        else:
            logger.info("📅 No companies expected to report in the next 24h")

    _log_upcoming()

    while True:
        _sync_queue(queue, hot_set_provider.current_hot_companies())

        if queue:
            idle_ticks = 0
            worst_case_seconds = math.ceil(len(queue) / detection_budget)
            logger.info(
                f"Rotation: {len(queue)} hot company(ies), budget={detection_budget}/s, "
                f"worst-case detection lag ~{worst_case_seconds}s"
            )
        else:
            # Empty queue is the normal state most of the day (docs/checklist.md Phase 6) —
            # but total silence for hours looks indistinguishable from a hung process. Log a
            # throttled heartbeat instead of nothing.
            idle_ticks += 1
            if idle_ticks % IDLE_HEARTBEAT_TICKS == 1:
                logger.info("Rotation: idle, no hot companies right now — waiting for next window")
                _log_upcoming()

        batch = [queue.popleft() for _ in range(min(detection_budget, len(queue)))]

        if batch:
            results = await asyncio.gather(
                *[
                    asyncio.wait_for(
                        poll_company_once(
                            company, client, seen_map.setdefault(company["ticker"], set()), max_age_hours
                        ),
                        timeout=ROTATION_POLL_TIMEOUT_SECONDS,
                    )
                    for company in batch
                ],
                return_exceptions=True,
            )

            for company, result in zip(batch, results):
                if isinstance(result, Exception):
                    logger.error(
                        f"Rotation poll failed for {company['ticker']}: "
                        f"{type(result).__name__}: {result}"
                    )
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
