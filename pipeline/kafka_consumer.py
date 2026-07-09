import json

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from httpx import AsyncClient

from pipeline.normalizer import normalize, split_sections
from pipeline.dispatcher import analyze_filing
from aggregator.aggregator import build_alert
from ingestion.deduplicator import Deduplicator
from ingestion.watcher import fetch_estimates, fetch_filing_document
from config import KAFKA_RAW_TOPIC, KAFKA_RESULTS_TOPIC, KAFKA_DLQ_TOPIC, KAFKA_AI_GROUP, KAFKA_DELIVERY_GROUP
from workers.client import GeminiClient
from delivery.webhook import DiscordWebhook
from utils.kafka import kafka_client_config
from utils.logger import get_logger

logger = get_logger(__name__)
client = AsyncClient()
ai_client = GeminiClient()

async def run_ai_worker(worker_id: str):
    consumer = AIOKafkaConsumer(
        KAFKA_RAW_TOPIC,
        **kafka_client_config(),
        group_id=KAFKA_AI_GROUP,
        enable_auto_commit=False,
    )
    producer = AIOKafkaProducer(**kafka_client_config())
    async for msg in consumer:
        filing = json.loads(msg.value)
        try:
            raw_text = await fetch_filing_document(filing["index_url"], client)
            estimates = await fetch_estimates(filing["ticker"])
            clean_text = normalize(raw_text, "html")

            if not Deduplicator.is_new(clean_text):   # see dedup note below
                await consumer.commit()
                continue

            sections = split_sections(clean_text)
            results = await analyze_filing(sections, ai_client, estimates)
            alert = build_alert(filing["ticker"], results, filing["detected_at"])

            await producer.send(KAFKA_RESULTS_TOPIC, json.dumps(alert).encode())
            await consumer.commit()
        except Exception as e:
            logger.error(f"AI worker failed for {filing['ticker']}: {e}")
            await producer.send(KAFKA_DLQ_TOPIC, msg.value)  # preserve original
            await consumer.commit()

async def run_delivery_consumer(subscription_map: dict):
    consumer = AIOKafkaConsumer(
        KAFKA_RESULTS_TOPIC,
        **kafka_client_config(),
        group_id=KAFKA_DELIVERY_GROUP,
        enable_auto_commit=False,
    )
    async for msg in consumer:
        alert = json.loads(msg.value)
        ticker = alert["ticker"]
        subscribers = subscription_map.get(ticker, [])
        if not subscribers:
            logger.warning(f"No subscribers for {ticker}")
            await consumer.commit()
            continue

        for sub in subscribers:
            try:
                await DiscordWebhook(sub["webhook_url"]).send(alert)
                logger.info(f"  ✓ {sub['id']}")
            except Exception as e:
                logger.error(f"  ✗ {sub['id']}: {e}")

        await consumer.commit()