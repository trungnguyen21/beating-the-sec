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
from delivery.router import SQLiteSubscriptionRouter
from server.db import get_active_tickers
from config import CONSUMER_CONCURRENCY

from dotenv import load_dotenv
load_dotenv()


async def _process_one(msg_id: str, filing, queue: FilingQueue,
                        deduplicator: Deduplicator, ai_client: AIClient,
                        router: SQLiteSubscriptionRouter) -> None:
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
            filed_at=filing.filed_at,
            filing_url=filing.index_url,
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
                          ai_client: AIClient, router: SQLiteSubscriptionRouter) -> None:
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
        choices=["ingestion", "ai-worker", "delivery", "server", "discord-bot"],
        default=None,
        help=(
            "Run a single pipeline stage independently. "
            "Omit to run all stages in one process (dev mode)."
        ),
    )
    return parser.parse_args()


async def main():
    args = _parse_args()

    if args.stage == "ingestion":
        # Only poll tickers with is_active = 1 (i.e. at least one guild watching it) — see
        # server/db.py's get_active_tickers/add_guild_tickers for how that flag is kept in
        # sync with the subscriptions table.
        watchlist = await get_active_tickers()
        async with KafkaFilingQueue() as queue:
            await run_watcher(watchlist, queue)

    elif args.stage == "ai-worker":
        await run_ai_worker("worker-1")

    elif args.stage == "delivery":
        # Subscription lookup is done per-message against SQLite — no startup config needed.
        await run_delivery_consumer()

    elif args.stage == "server":
        import uvicorn
        from server.app import app as web_app
        config = uvicorn.Config(web_app, host="0.0.0.0", port=8000)
        await uvicorn.Server(config).serve()

    elif args.stage == "discord-bot":
        from discord_bot.bot import run_bot
        await run_bot()

    else:
        # Dev mode: single-process, in-memory queue, SQLite for both the ticker list
        # (tickers table, filtered to is_active) and delivery routing (subscriptions table) —
        # same source of truth as the multi-stage production path, just collapsed into one
        # process.
        watchlist = await get_active_tickers()
        print(f"🚀 Starting dev mode — {len(watchlist)} active ticker(s) loaded from SQLite")
        router = SQLiteSubscriptionRouter()

        queue = InMemoryFilingQueue()
        deduplicator = Deduplicator()
        async with GeminiClient() as ai_client:
            await asyncio.gather(
                run_watcher(watchlist, queue),
                process_filings(queue, deduplicator, ai_client, router),
            )


if __name__ == "__main__":
    asyncio.run(main())
