# main.py
import asyncio
import os
import traceback
from ingestion.watcher import run_watcher
from ingestion.deduplicator import Deduplicator
from pipeline.queue import FilingQueue, InMemoryFilingQueue
from pipeline.normalizer import normalize, split_sections
from pipeline.dispatcher import analyze_filing
from aggregator.aggregator import build_alert
from workers.client import AIClient, GeminiClient
from delivery.webhook import DiscordWebhook
from config import CONSUMER_CONCURRENCY

from dotenv import load_dotenv
load_dotenv()

async def _process_one(msg_id: str, filing, queue: FilingQueue,
                        deduplicator: Deduplicator, ai_client: AIClient) -> None:
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

        discord_url = os.environ.get("DISCORD_WEBHOOK_URL")
        if discord_url:
            await DiscordWebhook(discord_url).send(alert)

        await queue.ack(msg_id)

    except Exception as e:
        filing_ctx = {"ticker": filing.ticker, "accession": filing.accession, "msg_id": msg_id}
        print(f"Error processing filing {filing_ctx}: {e}")
        print(traceback.format_exc())
        await queue.ack(msg_id)


async def process_filings(queue: FilingQueue, deduplicator: Deduplicator, ai_client: AIClient) -> None:
    """
    Consumer loop: dispatch each filing as an independent task so multiple filings
    (e.g. simultaneous earnings) are analyzed concurrently instead of serially.
    A semaphore caps concurrency to avoid blowing through AI rate limits.
    """
    semaphore = asyncio.Semaphore(CONSUMER_CONCURRENCY)

    async def bounded(msg_id, filing):
        async with semaphore:
            await _process_one(msg_id, filing, queue, deduplicator, ai_client)

    async for msg_id, filing in queue.consume():
        asyncio.create_task(bounded(msg_id, filing))


async def main():
    queue = InMemoryFilingQueue()
    deduplicator = Deduplicator()

    async with GeminiClient() as ai_client:
        await asyncio.gather(
            run_watcher("watchlist.json", queue),
            process_filings(queue, deduplicator, ai_client),
        )


if __name__ == "__main__":
    asyncio.run(main())
