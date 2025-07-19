from app.services.kafka_connection import KafkaConnection
from app.services.kafka_buffer import KafkaBuffer
from app.core.logger import get_logger

logger = get_logger(__name__)


class BufferedKafkaProducer:
    def __init__(self, bootstrap_server=None, topic=None, buffer_size=None, flush_interval=None, send_timeout=None):
        self.connection = KafkaConnection(bootstrap_server)
        self.buffer = KafkaBuffer(buffer_size, flush_interval)
        self.topic = topic or settings.kafka.kafka_topic
        self.send_timeout = send_timeout or settings.kafka.kafka_send_timeout
        self.dlq_retry_task = None

    async def start(self):
        await self.connection.connect()
        await self.buffer.start(self.connection.producer, self.topic)
        logger.info("Starting DLQ retry task")
        self.dlq_retry_task = asyncio.create_task(self._dlq_retry_loop())

    async def stop(self):
        await self.buffer.stop()
        await self.connection.disconnect()
        if self.dlq_retry_task:
            self.dlq_retry_task.cancel()
        logger.info("Kafka producer stopped")

    async def _dlq_retry_loop(self, interval=60):
        from app.services.dlq_handler import DLQHandler
        redis_pool = RedisPool()
        redis_client = await redis_pool.get_client()
        handler = DLQHandler(redis_client=redis_client)

        while True:
            try:
                await handler.retry_messages()
            except Exception as e:
                logger.error("DLQ retry task failed", error=str(e))
            await asyncio.sleep(interval)

    async def send(self, message):
        await self.buffer.send(message)