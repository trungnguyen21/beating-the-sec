"""
Kafka end-to-end integration test.

Publishes a fake filing event to the live Confluent raw_filings topic, then runs
run_ai_worker + run_delivery_consumer and waits for a real Discord alert to land.

Mocked:
  - fetch_filing_document  → returns data/DELL.html so we don't hit SEC
  - fetch_estimates        → returns {} so we don't need a market-data API key
  - DiscordWebhook.send    → intercepted so no real HTTP call is made to the webhook URL

Real:
  - Confluent Kafka broker (raw_filings → filing_results topics)
  - Gemini AI analysis
  - SQLite subscriber lookup (server/db.py) — the actual datastore run_delivery_consumer
    reads from in production, NOT data/users.json (that's a separate, legacy config path
    used only by main.py's no-flag dev mode). The test seeds its own guild subscriber row
    directly via upsert_guild_channel/add_guild_tickers so it's self-contained and doesn't
    silently pass or hang depending on whatever happens to already be in bsec.db.

Requires CONFLUENT_BOOTSTRAP / CONFLUENT_API_KEY / CONFLUENT_API_SECRET in .env, an active
Confluent cluster, and raw_filings/filing_results topics created. Requires the ticker used
(SAMPLE_FILING_TICKER, default DELL) to already exist in the `tickers` table (FK constraint
on subscriptions.ticker) — true for anything in watchlist.json once the server has been run
at least once to seed it.
"""

import asyncio
import os
import time
from unittest.mock import AsyncMock, patch

import pytest
from dotenv import load_dotenv

load_dotenv()

from pipeline.kafka_consumer import KafkaFilingQueue, run_ai_worker, run_delivery_consumer
from delivery.webhook import DiscordWebhook
from server.db import add_guild_tickers, delete_user, upsert_guild_channel

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLE_HTML = os.path.join(REPO_ROOT, "data", "DELL.html")
TIMEOUT = 120  # seconds — covers Kafka poll delay + AI analysis time
TEST_WEBHOOK_URL = "https://example.com/test-e2e-kafka-webhook"  # never actually called — DiscordWebhook.send is patched
TEST_GUILD_ID = "test-e2e-kafka-guild"  # fake Discord guild ID, not a real one


class _FakeFiling:
    """Minimal stand-in for DetectorResult — only the fields KafkaFilingQueue.enqueue reads."""
    def __init__(self, ticker: str):
        self.ticker = ticker
        self.cik = "0001045810"
        self.accession = "test-0001"
        self.index_url = "https://example.com/fake-index"  # intercepted by mock
        self.detected_at = "2026-07-10T00:00:00+00:00"
        self.filed_at = None


@pytest.mark.e2e
async def test_e2e_kafka_pipeline():
    test_ticker = os.environ.get("SAMPLE_FILING_TICKER", "DELL").upper()
    html_path = os.environ.get("SAMPLE_FILING_PATH", SAMPLE_HTML)

    # Seed a real guild subscriber row in the SQLite DB the delivery consumer actually reads
    # from, rather than depending on whatever happens to already be there.
    await upsert_guild_channel(TEST_GUILD_ID, "test-e2e-kafka", TEST_WEBHOOK_URL)
    await add_guild_tickers(TEST_GUILD_ID, [test_ticker])

    with open(html_path) as f:
        sample_html = f.read()

    ai_consumed_event = asyncio.Event()   # fires when AI worker picks up the message
    delivery_event = asyncio.Event()      # fires when Discord webhook is called
    async def _intercepted_send(self, alert: dict) -> None:
        delivery_event.set()

    async def _mock_fetch_filing_document(*_args, **_kwargs) -> str:
        ai_consumed_event.set()
        return sample_html

    try:
        with patch("pipeline.kafka_consumer.fetch_filing_document",
                   new=AsyncMock(side_effect=_mock_fetch_filing_document)), \
             patch("pipeline.kafka_consumer.fetch_estimates",
                   new=AsyncMock(return_value={})), \
             patch.object(DiscordWebhook, "send", _intercepted_send):

            ai_task = asyncio.create_task(run_ai_worker("test-worker"))
            delivery_task = asyncio.create_task(run_delivery_consumer())

            # Consumers need time to connect before we publish — with auto_offset_reset="latest"
            # (aiokafka default), a consumer joining after the message is written misses it entirely.
            await asyncio.sleep(5)

            async with KafkaFilingQueue() as queue:
                await queue.enqueue(_FakeFiling(test_ticker))

            t_start = time.perf_counter()
            try:
                await asyncio.wait_for(asyncio.shield(delivery_event.wait()), timeout=TIMEOUT)
            except asyncio.TimeoutError:
                elapsed = time.perf_counter() - t_start
                diagnosis = (
                    f"AI consumed message : {'YES' if ai_consumed_event.is_set() else 'NO — check raw_filings topic exists and AI API key is valid'}\n"
                    f"Discord delivered   : {'YES' if delivery_event.is_set() else 'NO — check subscriber row was seeded correctly and filing_results topic exists'}"
                )
                pytest.fail(f"Timed out after {elapsed:.0f}s\n{diagnosis}")
            finally:
                ai_task.cancel()
                delivery_task.cancel()
                await asyncio.gather(ai_task, delivery_task, return_exceptions=True)

        assert delivery_event.is_set()
    finally:
        # Don't leave the seeded test guild in the shared bsec.db.
        await delete_user(TEST_GUILD_ID)
