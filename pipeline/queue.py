# pipeline/queue.py
import redis.asyncio as redis
import json

class FilingQueue:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.client = redis.from_url(redis_url)
        self.stream = "filings"
    
    async def enqueue(self, filing: dict):
        await self.client.xadd(self.stream, {"data": json.dumps(filing)})
    
    async def consume(self, group: str = "workers", consumer: str = "w1"):
        await self._ensure_group()
        while True:
            messages = await self.client.xreadgroup(
                group, consumer, {self.stream: ">"}, count=1, block=1000
            )
            if messages:
                _, entries = messages[0]
                for msg_id, fields in entries:
                    yield msg_id, json.loads(fields[b"data"])
    
    async def ack(self, msg_id: str):
        await self.client.xack(self.stream, "workers", msg_id)
    
    async def _ensure_group(self):
        try:
            await self.client.xgroup_create(self.stream, "workers", id="0", mkstream=True)
        except Exception:
            pass  # Group already exists