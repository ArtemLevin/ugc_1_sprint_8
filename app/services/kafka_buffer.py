import asyncio
from typing import List, Dict, Any
from aiokafka import AIOKafkaProducer
from app.core.config import settings
from app.core.logger import get_logger
from app.utils.cache import RedisService
from app.services.dlq_handler import DLQHandler

logger = get_logger(__name__)

class KafkaBuffer:
    def __init__(
        self,
        max_size: int = None,
        flush_interval: int = None,
        redis_service: RedisService = None
    ):
        self.max_size = max_size or settings.kafka.kafka_max_batch_size
        self.flush_interval = flush_interval or settings.kafka.kafka_flush_interval
        self.queue = asyncio.Queue()
        self.task = None
        self.connection: AIOKafkaProducer | None = None
        self.topic: str | None = None
        self.dlq_handler = DLQHandler(redis_service or RedisService())
        logger.info("KafkaBuffer initialized", max_size=self.max_size, flush_interval=self.flush_interval)

    async def start(self, connection: AIOKafkaProducer, topic: str):
        self.connection = connection
        self.topic = topic
        self.task = asyncio.create_task(self._flush_loop())
        logger.info("KafkaBuffer started", topic=topic)

    async def stop(self):
        if self.task:
            self.task.cancel()
        await self._flush_queue()
        logger.info("KafkaBuffer stopped")

    async def _flush_loop(self):
        logger.info("Starting Kafka buffer flush loop")
        while True:
            try:
                batch = await self._collect_batch()
                if batch:
                    await self._send_batch(batch)
                await asyncio.sleep(self.flush_interval)
            except asyncio.CancelledError:
                logger.info("Flushing remaining messages before shutdown")
                await self._flush_queue()
                break
            except Exception as e:
                logger.error("Error in Kafka buffer loop", error=str(e))
                await asyncio.sleep(5)

    async def _collect_batch(self) -> List[Dict[str, Any]]:
        batch = []
        while not self.queue.empty() and len(batch) < self.max_size:
            batch.append(await self.queue.get())
        return batch

    async def _send_batch(self, batch: List[Dict[str, Any]]):
        try:
            await self.connection.send_batch(batch, self.topic)
            logger.info("Batch sent to Kafka", size=len(batch), topic=self.topic)
        except Exception as e:
            logger.warning("Failed to send batch to Kafka", error=str(e), size=len(batch))
            await self.dlq_handler.save_messages(batch)

    async def _flush_queue(self):
        batch = []
        while not self.queue.empty():
            batch.append(await self.queue.get())
        if not batch:
            return
        try:
            await self.connection.send_batch(batch, self.topic)
            logger.info("Remaining messages sent to Kafka", size=len(batch))
        except Exception as e:
            logger.warning("Failed to send remaining messages to Kafka", error=str(e))
            await self.dlq_handler.save_messages(batch)

    async def send(self, message: Dict[str, Any]):
        try:
            await self.queue.put(message)
            logger.debug("Message added to buffer", message_id=message.get("id"))
        except asyncio.QueueFull:
            logger.error("Message queue is full, dropping message", message=message)
            await self.dlq_handler.save_messages([message])