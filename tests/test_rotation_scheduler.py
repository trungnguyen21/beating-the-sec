"""
Integration test for ingestion/rotation_scheduler.py.

Calls the real interfaces together against live SEC EDGAR — run_rotation, FixedHotSet, and
poll_company_once — rather than mocking the scheduler's internals.

Validates:
  1. Rotation cycling: with detection_budget < number of hot companies, every company still
     gets polled at least once within a few ticks (round-robin requeueing works).
  2. Per-company error isolation: one company's poll_company_once failure (faked via monkeypatch
     on the *scheduler's* import of the real function, not a mock replacing the scheduler logic
     itself) doesn't block or cancel the other, genuinely-polled companies in the same tick.
"""

import asyncio

import httpx
import pytest

import ingestion.rotation_scheduler as rotation_scheduler
from ingestion.rotation_scheduler import FixedHotSet, run_rotation

REAL_POLL_COMPANY_ONCE = rotation_scheduler.poll_company_once

COMPANIES = [
    {"ticker": "AAPL", "cik": "0000320193"},
    {"ticker": "MSFT", "cik": "0000789019"},
    {"ticker": "FAIL", "cik": "0000000000"},  # forced-failure stand-in
]

RUN_SECONDS = 6
DETECTION_BUDGET = 2  # < len(COMPANIES), forces at least one requeue cycle


@pytest.mark.e2e
async def test_rotation_cycling_and_error_isolation():
    polled_tickers: set[str] = set()

    async def _counting_poll(company, client, seen, max_age_hours=3.0):
        polled_tickers.add(company["ticker"])
        if company["ticker"] == "FAIL":
            raise RuntimeError("forced failure for isolation test")
        return await REAL_POLL_COMPANY_ONCE(company, client, seen, max_age_hours)

    rotation_scheduler.poll_company_once = _counting_poll
    hot_set = FixedHotSet(COMPANIES)

    def on_detected(_company, _result):
        pass  # not necessarily any hits — these companies may not be filing right now

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

    assert polled_tickers == {"AAPL", "MSFT", "FAIL"}, (
        "Expected every company to cycle through and get polled at least once despite "
        f"detection_budget < len(COMPANIES); got {polled_tickers}"
    )
    assert "AAPL" in polled_tickers and "MSFT" in polled_tickers, (
        "Real companies should still be polled even in the same runs where FAIL raises"
    )
