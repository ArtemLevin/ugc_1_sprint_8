import json
import asyncio
from typing import List
from aiokafka import AIOKafkaProducer
from app.core.config import settings
from app.core.logger import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential
from app.utils.retry import RetryHandler
from app.utils.cache import RedisService

logger = get_logger(__name__)
retry_handler = RetryHandler(max_retries=3, base_delay=1, max_delay=10)

class DLQHandler:
    def __init__(
        self,
        redis_service: RedisService,
        queue_key: str | None = None,
        max_retries: int = 3,
        retry_delay: int = 5,
        kafka_bootstrap_server: str | None = None,
        kafka_topic: str | None = None
    ):
        self.redis_service = redis_service
        self.queue_key = queue_key or settings.dlq.dlq_queue_key
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.kafka_bootstrap_server = kafka_bootstrap_server or settings.kafka.kafka_bootstrap_server
        self.kafka_topic = kafka_topic or settings.kafka.kafka_topic

    async def save_messages(self, messages: List[dict]):
        for msg in messages:
            msg["retry_attempt"] = msg.get("retry_attempt", 0) + 1
            await self.redis_service.rpush(self.queue_key, json.dumps(msg))
        queue_size = await self.redis_service.llen(self.queue_key)
        logger.warning(
            f"{len(messages)} messages saved to DLQ",
            queue_key=self.queue_key,
            queue_size=queue_size
        )

    @retry_handler
    async def retry_message(self, raw_msg: str) -> bool:
        """
        Повторяет попытку отправки сообщения в Kafka
        """
        msg = json.loads(raw_msg)
        attempt = msg.get("retry_attempt", 1)

        logger.info("Retrying message from DLQ", message_id=msg.get("id"), attempt=attempt)

        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_server,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            await producer.start()
            future = await producer.send(self.kafka_topic, value=msg)
            result = await future
            logger.info("Message successfully retried", message_id=msg.get("id"), offset=result.offset)
            await producer.stop()
            return True
        except Exception as e:
            logger.error("DLQ retry failed", error=str(e), message_id=msg.get("id"))
            await producer.stop()
            return False

    async def retry_messages(self):
        """
        Повторяет отправку всех сообщений в DLQ
        """
        retry_count = 0
        while True:
            raw_msg = await self.redis_service.lpop(self.queue_key)
            if not raw_msg:
                break
            success = await self.retry_message(raw_msg)
            if success:
                retry_count += 1
            else:
                logger.error("DLQ message failed after retries", message=raw_msg)
        if retry_count > 0:
            logger.info(f"Successfully retried {retry_count} messages from DLQ")

    async def start_retry_loop(self, interval: int = 60):
        """
        Запускает цикл автоматического ретрая сообщений
        """
        logger.info("Starting DLQ retry loop", interval=interval)
        while True:
            try:
                await self.retry_messages()
            except Exception as e:
                logger.error("DLQ retry loop failed", error=str(e))
            await asyncio.sleep(interval)