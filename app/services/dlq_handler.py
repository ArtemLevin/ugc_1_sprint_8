import json
from app.core.config import settings
from app.core.logger import get_logger

logger = get_logger(__name__)

class DLQHandler:
    def __init__(self, redis_client=None):
        self.redis = redis_client
        self.queue_key = settings.dlq_queue_key
        self.max_retries = 3

    async def save_messages(self, messages):
        for msg in messages:
            msg["retry_attempt"] = 0
            await self.redis.rpush(self.queue_key, json.dumps(msg))
        logger.warning(f"{len(messages)} messages saved to DLQ")

    async def retry_messages(self):
        from app.services.kafka_producer import BufferedKafkaProducer
        producer = BufferedKafkaProducer()
        try:
            await producer.start()
            while True:
                raw_msg = await self.redis.lpop(self.queue_key)
                if not raw_msg:
                    break
                try:
                    msg = json.loads(raw_msg)
                    attempt = msg.get("retry_attempt", 0)
                    if attempt >= self.max_retries:
                        logger.warning("Max retries exceeded for DLQ message")
                        continue
                    await producer.send(msg)
                    logger.info("Message retried from DLQ")
                except Exception as e:
                    logger.error("DLQ retry failed", error=str(e))
                    msg["retry_attempt"] = attempt + 1
                    await self.redis.rpush(self.queue_key, json.dumps(msg))
        finally:
            await producer.stop()