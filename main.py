# main.py
import argparse
import asyncio
import traceback
from ingestion.watcher import run_watcher
from ingestion.deduplicator import Deduplicator
from pipeline.queue import FilingQueue, InMemoryFilingQueue
from pipeline.kafka_consumer import KafkaFilingQueue, run_ai_worker, run_delivery_consumer
from pipeline.normalizer import normalize, split_sections
from pipeline.dispatcher import analyze_filing
from aggregator.aggregator import build_alert
from workers.client import AIClient, GeminiClient
from delivery.router import SubscriptionRouter
from utils.load_user import UserConfig
from config import CONSUMER_CONCURRENCY

from dotenv import load_dotenv
load_dotenv()


def _build_subscription_map(user_config: UserConfig) -> dict:
    subscription_map: dict = {}
    for user in user_config.get_users():
        for company in (user.watchlist or []):
            subscription_map.setdefault(company.ticker, []).append(user)
    return subscription_map


def _build_unified_watchlist(user_config: UserConfig) -> list:
    seen: set[str] = set()
    watchlist = []
    for user in user_config.get_users():
        for company in (user.watchlist or []):
            if company.ticker not in seen:
                seen.add(company.ticker)
                watchlist.append({"ticker": company.ticker, "cik": company.cik})
    return watchlist


async def _process_one(msg_id: str, filing, queue: FilingQueue,
                        deduplicator: Deduplicator, ai_client: AIClient,
                        router: SubscriptionRouter) -> None:
    try:
        print(f"📥 Processing: {filing.ticker} {filing.accession}")

        raw_text = filing.raw_text
        if not raw_text:
            await queue.ack(msg_id)
            return

        clean_text = normalize(raw_text, "html")

        if not deduplicator.is_new(clean_text):
            print(f"⏭  Duplicate skipped: {filing.ticker}")
            await queue.ack(msg_id)
            return

        sections = split_sections(clean_text)
        results = await analyze_filing(sections, ai_client, filing.estimates)

        alert = build_alert(
            ticker=filing.ticker,
            results=results,
            detected_at=filing.detected_at,
        )

        print(f"🚨 Alert: {alert['signal_label']} — {alert['ticker']}")
        await router.fanout(filing.ticker, alert)
        await queue.ack(msg_id)

    except Exception as e:
        filing_ctx = {"ticker": filing.ticker, "accession": filing.accession, "msg_id": msg_id}
        print(f"Error processing filing {filing_ctx}: {e}")
        print(traceback.format_exc())
        await queue.ack(msg_id)


async def process_filings(queue: FilingQueue, deduplicator: Deduplicator,
                          ai_client: AIClient, router: SubscriptionRouter) -> None:
    """
    Consumer loop: dispatch each filing as an independent task so multiple filings
    (e.g. simultaneous earnings) are analyzed concurrently instead of serially.
    A semaphore caps concurrency to avoid blowing through AI rate limits.
    """
    semaphore = asyncio.Semaphore(CONSUMER_CONCURRENCY)

    async def bounded(msg_id, filing):
        async with semaphore:
            await _process_one(msg_id, filing, queue, deduplicator, ai_client, router)

    async for msg_id, filing in queue.consume():
        asyncio.create_task(bounded(msg_id, filing))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SEC filing alert pipeline")
    parser.add_argument(
        "--stage",
        choices=["ingestion", "ai-worker", "delivery"],
        default=None,
        help=(
            "Run a single pipeline stage independently. "
            "Omit to run all stages in one process (dev mode)."
        ),
    )
    return parser.parse_args()


async def main():
    args = _parse_args()

    user_config = UserConfig()
    user_config.load_configs("watchlist.json", "users.json")

    subscription_map = _build_subscription_map(user_config)
    unified_watchlist = _build_unified_watchlist(user_config)
    router = SubscriptionRouter(subscription_map)

    if args.stage == "ingestion":
        async with KafkaFilingQueue() as queue:
            await run_watcher(unified_watchlist, queue)

    elif args.stage == "ai-worker":
        await run_ai_worker("worker-1")

    elif args.stage == "delivery":
        await run_delivery_consumer(subscription_map)

    else:
        # Dev mode: all three stages run in a single process using an in-memory queue.
        # No Kafka required — swap InMemoryFilingQueue for KafkaFilingQueue to go multi-process.
        queue = InMemoryFilingQueue()
        deduplicator = Deduplicator()
        async with GeminiClient() as ai_client:
            await asyncio.gather(
                run_watcher(unified_watchlist, queue),
                process_filings(queue, deduplicator, ai_client, router),
            )


if __name__ == "__main__":
    asyncio.run(main())
