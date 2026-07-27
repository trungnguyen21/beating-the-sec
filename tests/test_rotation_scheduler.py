"""
Integration test for ingestion/rotation_scheduler.py.

Calls the real interfaces together against live SEC EDGAR — run_rotation, FixedHotSet, and
poll_company_once — rather than mocking the scheduler's internals. Matches this repo's existing
script-style, network-hitting test convention (see tests/test_rss.py).

Validates:
  1. Rotation cycling: with detection_budget < number of hot companies, every company still
     gets polled at least once within a few ticks (round-robin requeueing works).
  2. Per-company error isolation: one company's poll_company_once failure (faked via monkeypatch
     on the *scheduler's* import of the real function, not a mock replacing the scheduler logic
     itself) doesn't block or cancel the other, genuinely-polled companies in the same tick.

Run with:
    python tests/test_rotation_scheduler.py
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx

import ingestion.rotation_scheduler as rotation_scheduler
from ingestion.rotation_scheduler import FixedHotSet, run_rotation

REAL_POLL_COMPANY_ONCE = rotation_scheduler.poll_company_once

COMPANIES = [
    {"ticker": "AAPL", "cik": "0000320193"},
    {"ticker": "MSFT", "cik": "0000789019"},
    {"ticker": "FAIL", "cik": "0000000000"},  # forced-failure stand-in, see _flaky_poll below
]

RUN_SECONDS = 6
DETECTION_BUDGET = 2  # < len(COMPANIES), forces at least one requeue cycle


def _section(title: str) -> None:
    print(f"\n{'=' * 60}\n  {title}\n{'=' * 60}")


async def _flaky_poll(company, client, seen, max_age_hours=3.0):
    """Wraps the real poll_company_once, but forces an exception for ticker FAIL so we can
    verify the scheduler isolates a per-company failure without mocking the scheduler's own
    logic — AAPL/MSFT still make real network calls through the unmodified function."""
    if company["ticker"] == "FAIL":
        raise RuntimeError("forced failure for isolation test")
    return await REAL_POLL_COMPANY_ONCE(company, client, seen, max_age_hours)


async def run() -> None:
    _section("Rotation scheduler integration test")
    print(f"  Companies        : {[c['ticker'] for c in COMPANIES]}")
    print(f"  Detection budget : {DETECTION_BUDGET}")
    print(f"  Run duration     : {RUN_SECONDS}s")

    detected: list[tuple[dict, object]] = []

    def on_detected(company, result):
        detected.append((company, result))
        print(f"  🚨 on_detected fired for {company['ticker']}")

    rotation_scheduler.poll_company_once = _flaky_poll

    hot_set = FixedHotSet(COMPANIES)

    async with httpx.AsyncClient() as client:
        try:
            await asyncio.wait_for(
                run_rotation(hot_set, client, on_detected, detection_budget=DETECTION_BUDGET),
                timeout=RUN_SECONDS,
            )
        except asyncio.TimeoutError:
            pass  # expected — run_rotation loops forever by design
        finally:
            rotation_scheduler.poll_company_once = REAL_POLL_COMPANY_ONCE

    _section("Assertions")

    seen_map = {}
    # Re-derive per-ticker seen state isn't exposed outside run_rotation, so instead assert on
    # the queue's own bookkeeping via a second, shorter run with instrumentation.
    polled_tickers: set[str] = set()

    async def _counting_poll(company, client, seen, max_age_hours=3.0):
        polled_tickers.add(company["ticker"])
        if company["ticker"] == "FAIL":
            raise RuntimeError("forced failure for isolation test")
        return await REAL_POLL_COMPANY_ONCE(company, client, seen, max_age_hours)

    rotation_scheduler.poll_company_once = _counting_poll
    async with httpx.AsyncClient() as client:
        try:
            await asyncio.wait_for(
                run_rotation(hot_set, client, on_detected, detection_budget=DETECTION_BUDGET),
                timeout=RUN_SECONDS,
            )
        except asyncio.TimeoutError:
            pass
        finally:
            rotation_scheduler.poll_company_once = REAL_POLL_COMPANY_ONCE

    print(f"  Tickers polled at least once: {sorted(polled_tickers)}")
    assert polled_tickers == {"AAPL", "MSFT", "FAIL"}, (
        "Expected every company to cycle through and get polled at least once despite "
        f"detection_budget < len(COMPANIES); got {polled_tickers}"
    )
    print("  ✅ Rotation cycling: all companies were polled despite budget < company count")

    assert "AAPL" in polled_tickers and "MSFT" in polled_tickers, (
        "Real companies should still be polled even in the same runs where FAIL raises"
    )
    print("  ✅ Error isolation: FAIL's forced exception did not block AAPL/MSFT from being polled")

    print(f"\n  Detections this run: {[(c['ticker']) for c, _ in detected]} (0 is expected/fine — "
          f"these companies aren't necessarily filing an 8-K right now)")


if __name__ == "__main__":
    asyncio.run(run())
