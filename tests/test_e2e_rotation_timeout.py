"""
Real-network rotation timing probe for ingestion/rotation_scheduler.py.

No mocking anywhere: run_rotation, poll_company_once, and httpx.AsyncClient all hit live SEC
EDGAR, driven against a real slice of watchlist.json. The point is to reproduce and measure the
tick-drift/timeout behavior seen in production logs (a single slow/timed-out company in a batch
stalls the whole tick for up to its timeout, since asyncio.gather waits for the slowest task).

Wraps the real poll_company_once (not a fake) purely to record per-call wall-clock duration and
exception type/message — the underlying call and its real network behavior are untouched.
"""

import asyncio
import time

import httpx
import pytest

import ingestion.rotation_scheduler as rotation_scheduler
from ingestion.rotation_scheduler import FixedHotSet, run_rotation
from config import DETECTION_BUDGET, HEADERS

REAL_POLL_COMPANY_ONCE = rotation_scheduler.poll_company_once

# Fixed slice of real companies (first 15 entries of watchlist.json at the time this test was
# written) — hardcoded rather than read from watchlist.json so the test doesn't drift silently
# if the watchlist is edited later.
COMPANIES = [
    {"ticker": "NVDA", "cik": "1045810"},
    {"ticker": "GOOGL", "cik": "1652044"},
    {"ticker": "AAPL", "cik": "320193"},
    {"ticker": "MSFT", "cik": "789019"},
    {"ticker": "AMZN", "cik": "1018724"},
    {"ticker": "AVGO", "cik": "1730168"},
    {"ticker": "TSLA", "cik": "1318605"},
    {"ticker": "META", "cik": "1326801"},
    {"ticker": "MU", "cik": "723125"},
    {"ticker": "BRK-B", "cik": "1067983"},
    {"ticker": "LLY", "cik": "59478"},
    {"ticker": "WMT", "cik": "104169"},
    {"ticker": "JPM", "cik": "19617"},
    {"ticker": "AMD", "cik": "2488"},
    {"ticker": "ASML", "cik": "937966"},
]

RUN_SECONDS = 20
PER_REQUEST_TIMEOUT = 10  # must match the hardcoded timeout in ingestion/sec_rss.py


@pytest.mark.e2e
async def test_rotation_real_network_timing():
    """
    Polls real companies' real SEC RSS feeds through the real rotation scheduler for
    RUN_SECONDS, recording per-call latency and per-tick wall-clock duration. Prints a
    diagnostic report at the end rather than asserting hard timing bounds — real network
    latency is inherently variable, so this is primarily a reproduction/measurement tool for
    the tick-drift issue, not a pass/fail gate.
    """
    companies = COMPANIES
    call_log: list[dict] = []
    tick_timestamps: list[float] = []

    async def _timed_poll(company, client, seen, max_age_hours=3.0):
        tick_timestamps.append(time.monotonic())
        start = time.monotonic()
        try:
            result = await REAL_POLL_COMPANY_ONCE(company, client, seen, max_age_hours)
            call_log.append({
                "ticker": company["ticker"],
                "duration": time.monotonic() - start,
                "error": None,
            })
            return result
        except Exception as e:
            call_log.append({
                "ticker": company["ticker"],
                "duration": time.monotonic() - start,
                "error": f"{type(e).__name__}: {e}",
            })
            raise

    rotation_scheduler.poll_company_once = _timed_poll
    hot_set = FixedHotSet(companies)

    def on_detected(_company, _result):
        pass  # a real hit here would be a genuine breaking filing — not required for this probe

    async with httpx.AsyncClient(headers=HEADERS) as client:
        try:
            await asyncio.wait_for(
                run_rotation(hot_set, client, on_detected, detection_budget=DETECTION_BUDGET),
                timeout=RUN_SECONDS,
            )
        except asyncio.TimeoutError:
            pass  # expected — run_rotation loops forever by design
        finally:
            rotation_scheduler.poll_company_once = REAL_POLL_COMPANY_ONCE

    # --- Diagnostic report ---------------------------------------------------
    assert call_log, "No polls executed at all — check network/DNS before trusting timing results"

    durations = [c["duration"] for c in call_log]
    errors = [c for c in call_log if c["error"]]
    near_timeout = [c for c in call_log if c["duration"] >= PER_REQUEST_TIMEOUT * 0.8]

    gaps = [b - a for a, b in zip(tick_timestamps, tick_timestamps[1:])]
    max_gap = max(gaps, default=0.0)

    print(f"\n--- Rotation real-network timing report ({len(call_log)} calls) ---")
    print(f"companies polled: {sorted({c['ticker'] for c in call_log})}")
    print(f"duration min/avg/max: {min(durations):.2f}s / {sum(durations)/len(durations):.2f}s / {max(durations):.2f}s")
    print(f"errors: {len(errors)}")
    for c in errors:
        print(f"  {c['ticker']}: {c['error']} (after {c['duration']:.2f}s)")
    if near_timeout:
        print(f"calls within 80% of the {PER_REQUEST_TIMEOUT}s timeout: {len(near_timeout)}")
        for c in near_timeout:
            print(f"  {c['ticker']}: {c['duration']:.2f}s error={c['error']}")
    print(f"largest single gap between consecutive poll invocations: {max_gap:.2f}s")

    # Soft signal, not a hard failure: flag (don't fail the suite) when we actually reproduced
    # the tick-stall pattern discussed from prod logs, so it's visible in CI output either way.
    if max_gap > 2.0:
        print(
            f"NOTE: observed a {max_gap:.2f}s gap between poll invocations — consistent with "
            "one slow/timed-out request stalling the whole batch (asyncio.gather waits for "
            "the slowest task in run_rotation)."
        )
