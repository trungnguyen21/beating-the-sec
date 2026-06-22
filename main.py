# main.py
import asyncio
import os
from ingestion.watcher import run_watcher
from ingestion.deduplicator import Deduplicator
from pipeline.queue import FilingQueue, InMemoryFilingQueue
from pipeline.normalizer import normalize, split_sections
from pipeline.dispatcher import analyze_filing
from aggregator.aggregator import build_alert
from workers.client import AIClient, GeminiClient
from delivery.webhook import DiscordWebhook

from dotenv import load_dotenv
load_dotenv()

async def process_filings(queue: FilingQueue, deduplicator: Deduplicator, ai_client: AIClient):
    """Consumer loop: read from queue, normalize, split sections, analyze, alert."""
    async for msg_id, filing in queue.consume():
        try:
            print(f"📥 Processing: {filing['ticker']} {filing.get('accession', '')}")

            raw_text = filing.get("raw_text", "")
            if not raw_text:
                await queue.ack(msg_id)
                continue

            clean_text = normalize(raw_text, "html")

            if not deduplicator.is_new(clean_text):
                print(f"⏭  Duplicate skipped: {filing['ticker']}")
                await queue.ack(msg_id)
                continue

            sections = split_sections(clean_text)

            results = await analyze_filing(sections, ai_client, filing.get("estimates"))

            alert = build_alert(
                ticker=filing["ticker"],
                results=results,
                detected_at=filing["detected_at"],
            )

            print(f"🚨 Alert: {alert['signal_label']} — {alert['ticker']}")

            discord_url = os.environ.get("DISCORD_WEBHOOK_URL")
            if discord_url:
                await DiscordWebhook(discord_url).send(alert)

            await queue.ack(msg_id)

        except Exception as e:
            print(f"Error processing filing: {e}")
            await queue.ack(msg_id)


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
