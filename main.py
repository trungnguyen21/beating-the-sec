# main.py
import asyncio
import json
from ingestion.edgar_poller import run_watcher
from ingestion.deduplicator import Deduplicator
from pipeline.queue import FilingQueue
from pipeline.normalizer import normalize
from pipeline.dispatcher import analyze_filing
from aggregator.aggregator import build_alert
import os

async def process_filings(queue: FilingQueue, deduplicator: Deduplicator):
    """Consumer loop: read from queue, normalize, analyze, alert."""
    async for msg_id, filing in queue.consume():
        try:
            print(f"📥 Processing: {filing['ticker']} {filing['accession']}")
            
            # Fetch and normalize document
            # (fetch_filing_document imported from edgar_poller)
            raw_text = filing.get("raw_text", "")
            if not raw_text:
                continue
            
            clean_text = normalize(raw_text, "html")
            
            # Deduplicate
            if not deduplicator.is_new(clean_text):
                print(f"⏭  Duplicate skipped: {filing['ticker']}")
                await queue.ack(msg_id)
                continue
            
            # Run AI workers in parallel
            results = await analyze_filing(clean_text)
            
            # Build and send alert
            alert = build_alert(
                ticker=filing["ticker"],
                text=clean_text,
                results=results,
                detected_at=filing["detected_at"],
            )
            
            print(f"🚨 Alert: {alert['signal_label']} — {alert['ticker']}")
            await queue.ack(msg_id)
            
        except Exception as e:
            print(f"Error processing filing: {e}")
            await queue.ack(msg_id)

async def main():
    queue = FilingQueue()
    deduplicator = Deduplicator()
    
    # Run ingestion watcher and consumer loop concurrently
    await asyncio.gather(
        run_watcher("watchlist.json", queue),
        process_filings(queue, deduplicator),
    )

if __name__ == "__main__":
    asyncio.run(main())