"""
Storage-layer tests for filing_analysis (server/db.py::get_analysis / save_analysis).

Hits a real, live Postgres via DATABASE_URL — no mocking of psycopg, since these functions are
thin enough that mocking them would just assert their own SQL back at themselves. Marked
@pytest.mark.e2e because it requires a reachable Postgres (e.g. `docker compose up -d postgres`
locally + `python -m db.migrate`), not because it hits a third-party service.

Each test gets a unique, never-reused ticker (uuid-suffixed) so test data can never collide with
anything real in the DB, and a fixture teardown deletes its rows afterward regardless of
pass/fail.
"""
import uuid

import psycopg
import pytest

from config import DATABASE_URL
from server.db import get_analysis, save_analysis


@pytest.fixture
async def ticker():
    t = f"TDB{uuid.uuid4().hex[:8].upper()}"
    yield t
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        conn.execute("DELETE FROM filing_analysis WHERE ticker = %s", (t,))


@pytest.mark.e2e
async def test_cache_miss_returns_none(ticker):
    assert await get_analysis(ticker, "acc-1") is None


@pytest.mark.e2e
async def test_save_ok_round_trips_all_fields(ticker):
    await save_analysis(
        ticker=ticker,
        accession="acc-1",
        status="ok",
        alert={"ticker": ticker, "signal_label": "🟡 Neutral"},
        extraction={"eps_beat": "beat"},
        sentiment={"overall_tone": "bullish"},
        risk={"risk_level": "low"},
        detected_at="2026-01-01T00:00:00+00:00",
    )

    hit = await get_analysis(ticker, "acc-1")
    assert hit is not None
    assert hit["alert"] == {"ticker": ticker, "signal_label": "🟡 Neutral"}
    assert hit["extraction"] == {"eps_beat": "beat"}
    assert hit["sentiment"] == {"overall_tone": "bullish"}
    assert hit["risk"] == {"risk_level": "low"}
    assert hit["detected_at"] == "2026-01-01T00:00:00+00:00"


@pytest.mark.e2e
async def test_save_ok_with_no_worker_output_is_allowed(ticker):
    """A cache hit only needs alert_json to be reusable — extraction/sentiment/risk are nullable
    so a caller isn't forced to have them all on hand (e.g. reconstructing from a partial
    source)."""
    await save_analysis(ticker=ticker, accession="acc-1", status="ok", alert={"ticker": ticker})

    hit = await get_analysis(ticker, "acc-1")
    assert hit is not None
    assert hit["alert"] == {"ticker": ticker}
    assert hit["extraction"] is None
    assert hit["sentiment"] is None
    assert hit["risk"] is None


@pytest.mark.e2e
async def test_upsert_does_not_duplicate_and_overwrites(ticker):
    await save_analysis(ticker=ticker, accession="acc-1", status="ok", alert={"v": 1})
    await save_analysis(ticker=ticker, accession="acc-1", status="ok", alert={"v": 2})

    hit = await get_analysis(ticker, "acc-1")
    assert hit["alert"] == {"v": 2}, "second save should overwrite, not stack, the first"

    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        (count,) = conn.execute(
            "SELECT COUNT(*) FROM filing_analysis WHERE ticker = %s AND accession = %s",
            (ticker, "acc-1"),
        ).fetchone()
    assert count == 1, "upsert must not create a duplicate row for the same (ticker, accession)"


@pytest.mark.e2e
async def test_error_status_excluded_from_cache_hit(ticker):
    await save_analysis(ticker=ticker, accession="acc-1", status="error", error_message="boom")

    assert await get_analysis(ticker, "acc-1") is None, (
        "an error row must not count as a cache hit — a transient failure (rate limit, network "
        "blip) should still be retried on the next attempt, not permanently stuck"
    )


@pytest.mark.e2e
async def test_error_row_can_be_superseded_by_a_later_success(ticker):
    await save_analysis(ticker=ticker, accession="acc-1", status="error", error_message="boom")
    await save_analysis(ticker=ticker, accession="acc-1", status="ok", alert={"ticker": ticker})

    hit = await get_analysis(ticker, "acc-1")
    assert hit is not None
    assert hit["alert"] == {"ticker": ticker}


@pytest.mark.e2e
async def test_different_accessions_are_independent_rows(ticker):
    await save_analysis(ticker=ticker, accession="acc-1", status="ok", alert={"v": "first"})
    await save_analysis(ticker=ticker, accession="acc-2", status="ok", alert={"v": "second"})

    assert (await get_analysis(ticker, "acc-1"))["alert"] == {"v": "first"}
    assert (await get_analysis(ticker, "acc-2"))["alert"] == {"v": "second"}
