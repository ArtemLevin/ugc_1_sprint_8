from aiokafka import AIOKafkaProducer
from app.core.config import settings
from app.core.logger import get_logger
from app.utils.cache import RedisService
from app.services.dlq_handler import DLQHandler
from app.services.kafka_connection import KafkaConnection
from app.services.kafka_buffer import KafkaBuffer
import asyncio

logger = get_logger(__name__)

class BufferedKafkaProducer:
    def __init__(
        self,
        bootstrap_server: str | None = None,
        topic: str | None = None,
        buffer_size: int | None = None,
        flush_interval: int | None = None,
        send_timeout: int | None = None,
        redis_service: RedisService | None = None
    ):
        self.bootstrap_server = bootstrap_server or settings.kafka.kafka_bootstrap_server
        self.topic = topic or settings.kafka.kafka_topic
        self.buffer_size = buffer_size or settings.kafka.kafka_max_batch_size
        self.flush_interval = flush_interval or settings.kafka.kafka_flush_interval
        self.send_timeout = send_timeout or settings.kafka.kafka_send_timeout
        self.connection = KafkaConnection(self.bootstrap_server)
        self.buffer = KafkaBuffer(self.buffer_size, self.flush_interval)
        self.redis_service = redis_service or RedisService()
        self.dlq_handler = DLQHandler(self.redis_service)
        self.dlq_retry_task = None

    async def start(self):
        await self.connection.connect()
        await self.buffer.start(self.connection.producer, self.topic)
        logger.info("Starting DLQ retry task")
        self.dlq_retry_task = asyncio.create_task(self._dlq_retry_loop())

    async def stop(self):
        logger.info("Stopping Kafka producer")
        if self.dlq_retry_task:
            self.dlq_retry_task.cancel()
        await self.buffer.stop()
        await self.connection.disconnect()
        logger.info("Kafka producer stopped")

    async def _dlq_retry_loop(self, interval: int = 60):
        logger.info("DLQ retry loop started", interval=interval)
        while True:
            try:
                await self.dlq_handler.retry_messages()
            except Exception as e:
                logger.error("DLQ retry task failed", error=str(e))
            await asyncio.sleep(interval)

    async def send(self, message: dict):
        logger.debug("Adding message to buffer", message_id=message.get("id"))
        await self.buffer.send(message)

    async def send_immediately(self, message: dict):
        logger.debug("Sending message immediately", message_id=message.get("id"))
        try:
            await self.connection.producer.send(self.topic, value=message)
        except Exception as e:
            logger.error("Immediate Kafka send failed", error=str(e))
            await self.dlq_handler.save_messages([message])