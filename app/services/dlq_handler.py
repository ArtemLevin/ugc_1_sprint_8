import json
from aiokafka import AIOKafkaProducer
from app.core.logger import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential

logger = get_logger(__name__)

class DLQHandler:
    def __init__(self, redis_client=None):
        self.redis = redis_client
        self.queue_key = "dlq_kafka"
        self.max_retries = 3

    async def save_messages(self, messages):
        for msg in messages:
            msg["retry_attempt"] = 0
            await self.redis.rpush(self.queue_key, json.dumps(msg))
        logger.warning(f"{len(messages)} messages saved to DLQ")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    async def retry_message(self, raw_msg):
        msg = json.loads(raw_msg)
        producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
        try:
            await producer.start()
            await producer.send("user_events", value=json.dumps(msg).encode("utf-8"))
            logger.info("Message retried from DLQ")
        except Exception as e:
            logger.error("DLQ retry failed", error=str(e))
            raise
        finally:
            await producer.stop()

    async def retry_messages(self):
        while True:
            raw_msg = await self.redis.lpop(self.queue_key)
            if not raw_msg:
                break
            try:
                await self.retry_message(raw_msg)
            except Exception as e:
                logger.error("DLQ retry failed after retries", error=str(e))