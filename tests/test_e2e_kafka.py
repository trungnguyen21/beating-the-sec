"""
Kafka end-to-end integration test.

Publishes a fake filing event to the live Confluent raw-filings topic, then runs
run_ai_worker + run_delivery_consumer and waits for a real Discord alert to land.

Mocked:
  - fetch_filing_document  → returns data/DELL.html so we don't hit SEC
  - fetch_estimates        → returns {} so we don't need a market-data API key

Real:
  - Confluent Kafka broker (raw-filings → filing-results topics)
  - Gemini AI analysis
  - Discord webhook delivery

Requires CONFLUENT_BOOTSTRAP / CONFLUENT_API_KEY / CONFLUENT_API_SECRET in .env, an active
Confluent cluster, and raw-filings/filing-results topics created.
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
from main import _build_subscription_map
from utils.load_user import UserConfig

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLE_HTML = os.path.join(REPO_ROOT, "data", "DELL.html")
TIMEOUT = 120  # seconds — covers Kafka poll delay + AI analysis time


class _FakeFiling:
    """Minimal stand-in for DetectorResult — only the fields KafkaFilingQueue.enqueue reads."""
    def __init__(self, ticker: str):
        self.ticker = ticker
        self.cik = "0001045810"
        self.accession = "test-0001"
        self.index_url = "https://example.com/fake-index"  # intercepted by mock
        self.detected_at = "2026-07-10T00:00:00+00:00"


@pytest.mark.e2e
async def test_e2e_kafka_pipeline():
    test_ticker = os.environ.get("SAMPLE_FILING_TICKER", "DELL").upper()
    html_path = os.environ.get("SAMPLE_FILING_PATH", SAMPLE_HTML)

    user_config = UserConfig()
    user_config.load_configs("watchlist.json", "data/users.json")
    subscription_map = _build_subscription_map(user_config)

    subscribers = subscription_map.get(test_ticker, [])
    if not subscribers:
        pytest.skip(f"No subscribers for {test_ticker} in users.json — add one to run this test")

    with open(html_path) as f:
        sample_html = f.read()

    ai_consumed_event = asyncio.Event()   # fires when AI worker picks up the message
    delivery_event = asyncio.Event()      # fires when Discord webhook is called
    original_send = DiscordWebhook.send

    async def _intercepted_send(self, alert: dict) -> None:
        await original_send(self, alert)
        delivery_event.set()

    async def _mock_fetch_filing_document(*_args, **_kwargs) -> str:
        ai_consumed_event.set()
        return sample_html

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
                f"AI consumed message : {'YES' if ai_consumed_event.is_set() else 'NO — check raw-filings topic exists and AI API key is valid'}\n"
                f"Discord delivered   : {'YES' if delivery_event.is_set() else 'NO — check filing-results topic exists and webhook URL is correct'}"
            )
            pytest.fail(f"Timed out after {elapsed:.0f}s\n{diagnosis}")
        finally:
            ai_task.cancel()
            delivery_task.cancel()
            await asyncio.gather(ai_task, delivery_task, return_exceptions=True)

    assert delivery_event.is_set()
