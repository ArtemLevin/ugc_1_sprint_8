import aiokafka
from aiokafka import AIOKafkaProducer
import json
import asyncio
from app.core.config import KafkaSettings
from app.core.logger import get_logger
from app.core.tracing import add_event_attributes
from app.services.dlq_handler import DLQHandler

logger = get_logger(__name__)

class BufferedKafkaProducer:
    def __init__(self, bootstrap_server, topic, buffer_size=100, flush_interval=5, send_timeout=5):
        self.bootstrap_server = bootstrap_server
        self.topic = topic
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        self.send_timeout = send_timeout
        self.queue = asyncio.Queue()
        self.producer = None
        self.task = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            enable_idempotence=True
        )
        await self.producer.start()
        self.task = asyncio.create_task(self._flush_loop())

    async def stop(self):
        if self.task:
            self.task.cancel()
        if self.producer:
            await self.producer.stop()

    async def _flush_loop(self):
        while True:
            batch = []
            while not self.queue.empty() and len(batch) < self.buffer_size:
                batch.append(await self.queue.get())
            if batch:
                try:
                    await asyncio.wait_for(
                        self.producer.send_batch(
                            [(msg, None) for msg in batch],
                            topic=self.topic
                        ),
                        timeout=self.send_timeout
                    )
                    logger.debug(f"Sent {len(batch)} events to Kafka")
                except Exception as e:
                    logger.error("Kafka send error", error=str(e))
                    handler = DLQHandler(self.producer)
                    await handler.save_messages(batch)
            await asyncio.sleep(self.flush_interval)

    async def send(self, message):
        await self.queue.put(message)

    async def start_dlq_retry_task(self, interval=60):
        from app.services.dlq_handler import DLQHandler
        handler = DLQHandler(redis_client=await self.redis_pool.get_client())
        while True:
            try:
                await handler.retry_messages()
            except Exception as e:
                logger.error("DLQ retry task failed", error=str(e))
            await asyncio.sleep(interval)