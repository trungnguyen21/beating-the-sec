"""
Kafka end-to-end integration test.

Publishes a fake filing event to the live Confluent raw-filings topic, then
runs run_ai_worker + run_delivery_consumer and waits for a real Discord alert
to land. Requires the Confluent cluster to be reachable.

Mocked:
  - fetch_filing_document  → returns data/DELL.html so we don't hit SEC
  - fetch_estimates        → returns {} so we don't need a market-data API key

Real:
  - Confluent Kafka broker (raw-filings → filing-results topics)
  - Gemini AI analysis
  - Discord webhook delivery

Run with:
    python tests/test_e2e_kafka.py

Optional args:
    python tests/test_e2e_kafka.py [path/to/filing.html] [TICKER]
    Defaults: data/DELL.html, ticker NVDA
"""

import asyncio
import os
import sys
import time
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from pipeline.kafka_consumer import KafkaFilingQueue, run_ai_worker, run_delivery_consumer
from delivery.webhook import DiscordWebhook
from main import _build_subscription_map
from utils.load_user import UserConfig

REPO_ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLE_HTML  = os.path.join(REPO_ROOT, "data", "DELL.html")
TIMEOUT      = 60  # seconds — covers Kafka poll delay + AI analysis time


def _section(title: str) -> None:
    print(f"\n{'=' * 60}\n  {title}\n{'=' * 60}")


class _FakeFiling:
    """Minimal stand-in for DetectorResult — only the fields KafkaFilingQueue.enqueue reads."""
    def __init__(self, ticker: str):
        self.ticker      = ticker
        self.cik         = "0001045810"
        self.accession   = "test-0001"
        self.index_url   = "https://example.com/fake-index"  # intercepted by mock
        self.detected_at = "2026-07-10T00:00:00+00:00"


async def run() -> None:
    html_path       = sys.argv[1] if len(sys.argv) > 1 else SAMPLE_HTML
    test_ticker     = sys.argv[2].upper() if len(sys.argv) > 2 else "DELL"

    # ── Step 1: config ────────────────────────────────────────────────
    _section("STEP 1 — Load user config")
    user_config = UserConfig()
    user_config.load_configs("watchlist.json", "users.json")
    subscription_map = _build_subscription_map(user_config)

    subscribers = subscription_map.get(test_ticker, [])
    if not subscribers:
        print(f"  ✗ No subscribers for {test_ticker} in users.json.")
        print(f"    Add '{test_ticker}' to a user's watchlist and re-run.")
        sys.exit(1)
    print(f"  Ticker       : {test_ticker}")
    print(f"  Subscribers  : {[s.id for s in subscribers]}")
    print(f"  Webhooks     : {[s.webhook for s in subscribers]}")

    # ── Step 2: load sample filing ────────────────────────────────────
    _section("STEP 2 — Load sample filing")
    with open(html_path) as f:
        sample_html = f.read()
    print(f"  File   : {html_path}")
    print(f"  Size   : {len(sample_html):,} chars")

    # ── Step 3: start workers first, then publish ─────────────────────
    # Workers must be running BEFORE the message is published.
    # With auto_offset_reset="latest" (aiokafka default), a consumer that
    # joins after the message is written will miss it entirely.
    _section(f"STEP 3 — Start workers then publish (timeout: {TIMEOUT}s)")

    ai_consumed_event = asyncio.Event()   # fires when AI worker picks up the message
    delivery_event    = asyncio.Event()   # fires when Discord webhook is called
    original_send     = DiscordWebhook.send

    async def _intercepted_send(self, alert: dict) -> None:
        await original_send(self, alert)
        print(f"\n  🚨 Discord delivery confirmed — {alert.get('ticker')} alert sent")
        delivery_event.set()

    async def _mock_fetch_filing_document(*_args, **_kwargs) -> str:
        ai_consumed_event.set()
        return sample_html

    with patch("pipeline.kafka_consumer.fetch_filing_document",
               new=AsyncMock(side_effect=_mock_fetch_filing_document)), \
         patch("pipeline.kafka_consumer.fetch_estimates",
               new=AsyncMock(return_value={})), \
         patch.object(DiscordWebhook, "send", _intercepted_send):

        ai_task       = asyncio.create_task(run_ai_worker("test-worker"))
        delivery_task = asyncio.create_task(run_delivery_consumer(subscription_map))

        # Give consumers time to connect and start polling before we publish
        print("  Waiting for consumers to connect...")
        await asyncio.sleep(5)

        # ── Step 4: publish ───────────────────────────────────────────
        _section("STEP 4 — Publish filing to raw-filings topic")
        async with KafkaFilingQueue() as queue:
            await queue.enqueue(_FakeFiling(test_ticker))
        print("  Published ✓  — waiting for AI worker to consume it...")

        t_start = time.perf_counter()

        try:
            # Phase A: wait for AI worker to process (covers most of the timeout)
            await asyncio.wait_for(
                asyncio.shield(delivery_event.wait()),
                timeout=TIMEOUT,
            )
            elapsed = time.perf_counter() - t_start
            print(f"  Completed in {elapsed:.1f}s")

        except asyncio.TimeoutError:
            elapsed = time.perf_counter() - t_start
            print(f"\n  ✗ FAILED after {elapsed:.0f}s — timeout reached")
            print()
            print("  Diagnosis:")
            print(f"    AI consumed message : {'YES' if ai_consumed_event.is_set() else 'NO — check raw-filings topic exists in Confluent and AI API key is valid'}")
            print(f"    Discord delivered   : {'YES' if delivery_event.is_set() else 'NO — check filing-results topic exists and webhook URL is correct'}")
            sys.exit(1)
        finally:
            ai_task.cancel()
            delivery_task.cancel()
            await asyncio.gather(ai_task, delivery_task, return_exceptions=True)
            print("  Workers cancelled cleanly")

    # ── Result ────────────────────────────────────────────────────────
    _section("RESULT")
    print("  ✓ Kafka integration test passed.")
    print("  Check your Discord channel for the alert.")


if __name__ == "__main__":
    asyncio.run(run())
