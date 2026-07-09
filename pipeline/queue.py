# pipeline/queue.py
import asyncio
import json

from typing import AsyncIterator, Protocol
import redis.asyncio as redis
from aiokafka import AIOKafkaProducer

from config import KAFKA_RAW_TOPIC
from utils.kafka import kafka_client_config

class FilingQueue(Protocol):
    async def enqueue(self, filing: dict) -> None: ...
    def consume(self) -> AsyncIterator[tuple[str, dict]]: ...
    async def ack(self, msg_id: str) -> None: ...

class KafkaFilingQueue:
    async def __aenter__(self):
        self._producer = AIOKafkaProducer(**kafka_client_config())
        await self._producer.start()
        return self

    async def enqueue(self, filing) -> None:
        await self._producer.send(KAFKA_RAW_TOPIC, json.dumps(filing.__dict__).encode())

    async def ack(self, _msg_id: str) -> None:
        pass  # Kafka offsets are committed by the consumer, not the producer

    async def __aexit__(self, *_):
        await self._producer.stop()

class RedisFilingQueue:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.client = redis.from_url(redis_url)
        self.stream = "filings"

    async def enqueue(self, filing: dict) -> None:
        await self.client.xadd(self.stream, {"data": json.dumps(filing)})

    async def consume(self, group: str = "workers", consumer: str = "w1") -> AsyncIterator[tuple[str, dict]]:
        await self._ensure_group()
        while True:
            messages = await self.client.xreadgroup(
                group, consumer, {self.stream: ">"}, count=1, block=1000
            )
            if messages:
                _, entries = messages[0]
                for msg_id, fields in entries:
                    yield msg_id, json.loads(fields[b"data"])

    async def ack(self, msg_id: str) -> None:
        await self.client.xack(self.stream, "workers", msg_id)

    async def _ensure_group(self):
        try:
            await self.client.xgroup_create(self.stream, "workers", id="0", mkstream=True)
        except Exception:
            pass


class InMemoryFilingQueue:
    """Async in-memory queue — useful for testing and local runs without Redis."""

    def __init__(self):
        self._queue: asyncio.Queue[tuple[str, dict]] = asyncio.Queue()
        self._counter = 0

    async def enqueue(self, filing: dict) -> None:
        self._counter += 1
        msg_id = f"mem-{self._counter}"
        await self._queue.put((msg_id, filing))

    async def consume(self) -> AsyncIterator[tuple[str, dict]]:
        while True:
            msg_id, filing = await self._queue.get()
            yield msg_id, filing

    async def ack(self, _msg_id: str) -> None:
        self._queue.task_done()
