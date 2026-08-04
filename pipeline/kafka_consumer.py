import json

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import httpx

from pipeline.normalizer import normalize, split_sections
from pipeline.dispatcher import analyze_filing
from aggregator.aggregator import build_alert
from ingestion.deduplicator import Deduplicator
from ingestion.edgar_poller import fetch_filing_document
from ingestion.estimates import fetch_estimates
from config import KAFKA_RAW_TOPIC, KAFKA_RESULTS_TOPIC, KAFKA_DLQ_TOPIC, KAFKA_AI_GROUP, KAFKA_DELIVERY_GROUP
from workers.client import GeminiClient
from delivery.webhook import DiscordWebhook, WebhookGoneError
from server.db import delete_user, get_subscribers_for_ticker, save_alert
from utils.kafka import kafka_client_config
from utils.logger import get_logger

logger = get_logger(__name__)


class KafkaFilingQueue:
    """Producer-side queue: serialises DetectorResult metadata into raw_filings topic.
    Raw text is intentionally excluded — Stage 2 (run_ai_worker) fetches it directly."""

    async def __aenter__(self):
        self._producer = AIOKafkaProducer(**kafka_client_config())
        await self._producer.start()
        logger.info("Kafka ingestion producer started")
        return self

    async def enqueue(self, filing) -> None:
        payload = {
            "ticker": filing.ticker,
            "cik": filing.cik,
            "accession": filing.accession,
            "index_url": filing.index_url,
            "detected_at": filing.detected_at,
            "filed_at": filing.filed_at.isoformat() if filing.filed_at else None,
        }
        await self._producer.send(KAFKA_RAW_TOPIC, json.dumps(payload).encode())
        logger.info(f"→ Kafka: {filing.ticker} enqueued to {KAFKA_RAW_TOPIC}")

    async def ack(self, _msg_id: str) -> None:
        pass  # offsets are committed by the consumer, not the producer

    async def __aexit__(self, *_):
        await self._producer.stop()
        logger.info("Kafka ingestion producer stopped")


async def run_ai_worker(worker_id: str):
    consumer = AIOKafkaConsumer(
        KAFKA_RAW_TOPIC,
        **kafka_client_config(),
        group_id=KAFKA_AI_GROUP,
        enable_auto_commit=False,
    )
    producer = AIOKafkaProducer(**kafka_client_config())
    deduplicator = Deduplicator()

    await consumer.start()
    await producer.start()
    logger.info(f"AI worker {worker_id} started")

    try:
        async with httpx.AsyncClient() as http_client:
            async with GeminiClient() as ai_client:
                async for msg in consumer:
                    filing = json.loads(msg.value)
                    try:
                        raw_text = await fetch_filing_document(filing["index_url"], http_client)
                        estimates = await fetch_estimates(filing["ticker"])
                        clean_text = normalize(raw_text, "html")

                        if not deduplicator.is_new(clean_text):
                            logger.debug(f"Duplicate skipped: {filing['ticker']}")
                            await consumer.commit()
                            continue

                        sections = split_sections(clean_text)
                        results = await analyze_filing(sections, ai_client, estimates)
                        alert = build_alert(
                            filing["ticker"], results, filing["detected_at"], filing.get("filed_at"),
                            filing_url=filing.get("index_url"),
                        )

                        await save_alert(filing["ticker"], alert)
                        await producer.send(KAFKA_RESULTS_TOPIC, json.dumps(alert).encode())
                        await consumer.commit()
                        logger.info(f"AI worker {worker_id} processed {filing['ticker']}")

                    except Exception as e:
                        logger.error(f"AI worker {worker_id} failed for {filing['ticker']}: {e}")
                        await producer.send(KAFKA_DLQ_TOPIC, msg.value)
                        await consumer.commit()
    finally:
        await consumer.stop()
        await producer.stop()
        logger.info(f"AI worker {worker_id} stopped")


async def run_delivery_consumer():
    consumer = AIOKafkaConsumer(
        KAFKA_RESULTS_TOPIC,
        **kafka_client_config(),
        group_id=KAFKA_DELIVERY_GROUP,
        enable_auto_commit=False,
    )

    await consumer.start()
    logger.info("Delivery consumer started")

    try:
        async for msg in consumer:
            alert = json.loads(msg.value)
            ticker = alert["ticker"]
            subscribers = await get_subscribers_for_ticker(ticker)

            if not subscribers:
                logger.warning(f"No subscribers for {ticker}")
                await consumer.commit()
                continue

            for sub in subscribers:
                try:
                    await DiscordWebhook(sub["webhook_url"]).send(alert)
                    logger.info(f"  ✓ {sub['id']}")
                except WebhookGoneError:
                    logger.warning(f"  ✗ {sub['id']}: webhook gone — deactivating guild")
                    try:
                        await delete_user(sub["id"])
                    except Exception as e:
                        logger.error(
                            f"  ✗ {sub['id']}: failed to delete guild after webhook gone: {e}"
                        )
                except Exception as e:
                    logger.error(f"  ✗ {sub['id']}: {e}")

            await consumer.commit()
    finally:
        await consumer.stop()
        logger.info("Delivery consumer stopped")
