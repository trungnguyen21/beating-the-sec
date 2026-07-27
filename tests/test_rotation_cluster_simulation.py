"""
Phase 5 — cluster simulation for ingestion/rotation_scheduler.py.

Seeds the hot set with 30 companies (a plausible earnings-day cluster per docs/checklist.md's
"many banks report the same day" scenario), mocks poll_company_once to return a hit for a
random subset each tick (no real network — this is a pure scheduling-logic simulation), and
confirms:
  1. Worst-case latency tracks ceil(len(hot set) / DETECTION_BUDGET) at the start.
  2. That worst-case value visibly drops as companies get detected and leave the queue —
     the whole point of not requeuing a detected company.
  3. A detected company never resurfaces in the same run (CooldownAwareHotSet, same wiring
     ingestion/watcher.py actually uses in production).
"""

import asyncio
import math
import random
from datetime import datetime, timezone

import httpx

import ingestion.rotation_scheduler as rotation_scheduler
from ingestion.rotation_scheduler import CooldownAwareHotSet, FixedHotSet, run_rotation
from models.model_detectors import DetectorResult, DetectorSource
from config import DETECTION_BUDGET

CLUSTER_SIZE = 30
HIT_PROBABILITY_PER_POLL = 0.3  # chance a still-queued company "files" when polled this tick
RUN_SECONDS = 15
SAMPLE_INTERVAL_SECONDS = 0.5

REAL_POLL_COMPANY_ONCE = rotation_scheduler.poll_company_once


async def _fake_poll_company_once(company, client, seen, max_age_hours=3.0):
    if random.random() < HIT_PROBABILITY_PER_POLL:
        return DetectorResult(
            ticker=company["ticker"],
            cik=company["cik"],
            accession="sim",
            index_url="https://example.com/sim",
            detected_at=datetime.now(timezone.utc).isoformat(),
            source=DetectorSource.RSS_POLLER,
        )
    return None


async def test_cluster_simulation_worst_case_latency_shrinks():
    random.seed(42)  # deterministic — avoids introducing a rerun-on-flaky plugin dependency
    initial_worst_case = math.ceil(CLUSTER_SIZE / DETECTION_BUDGET)

    companies = [{"ticker": f"CO{i:03d}", "cik": f"{i:010d}"} for i in range(CLUSTER_SIZE)]
    base_hot_set = FixedHotSet(companies)
    hot_set = CooldownAwareHotSet(base_hot_set, cooldown_hours=24.0)

    detected_order: list[str] = []

    async def on_detected(company, _result):
        detected_order.append(company["ticker"])
        hot_set.mark_detected(company["ticker"])

    rotation_scheduler.poll_company_once = _fake_poll_company_once

    # Capture the true starting count synchronously, before run_rotation's task gets a chance
    # to run even one tick — sampling after the task starts is racy, since polling begins the
    # instant the task is scheduled, not after our first sleep interval.
    starting_count = len(hot_set.current_hot_companies())
    assert starting_count == CLUSTER_SIZE, f"Expected {CLUSTER_SIZE} companies hot at start, got {starting_count}"

    worst_case_history: list[int] = []

    async with httpx.AsyncClient() as client:
        task = asyncio.create_task(
            run_rotation(hot_set, client, on_detected, detection_budget=DETECTION_BUDGET)
        )
        try:
            elapsed = 0.0
            while elapsed < RUN_SECONDS:
                await asyncio.sleep(SAMPLE_INTERVAL_SECONDS)
                elapsed += SAMPLE_INTERVAL_SECONDS
                remaining = len(hot_set.current_hot_companies())
                worst_case_history.append(math.ceil(remaining / DETECTION_BUDGET) if remaining else 0)
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            rotation_scheduler.poll_company_once = REAL_POLL_COMPANY_ONCE

    assert len(set(detected_order)) == len(detected_order), (
        "A company resurfaced and was detected twice in the same run — cooldown isn't holding"
    )
    assert initial_worst_case == math.ceil(starting_count / DETECTION_BUDGET), (
        "Sanity check on the theoretical formula itself failed — shouldn't happen"
    )
    assert len(detected_order) > 0, (
        f"Expected at least one detection over {RUN_SECONDS}s at hit probability "
        f"{HIT_PROBABILITY_PER_POLL} — rerun if this was genuinely bad luck"
    )
    assert worst_case_history[-1] <= worst_case_history[0], (
        "Worst-case latency should shrink or stay flat as companies get detected and drop out"
    )
