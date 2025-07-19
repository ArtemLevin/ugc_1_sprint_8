import json
from app.core.logger import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential

logger = get_logger(__name__)


class DLQHandler:
    def __init__(self, redis_client=None):
        self.redis = redis_client
        self.queue_key = settings.dlq.dlq_queue_key
        self.max_retries = 3

    async def save_messages(self, messages):
        for msg in messages:
            msg["retry_attempt"] = msg.get("retry_attempt", 0) + 1
            await self.redis.rpush(self.queue_key, json.dumps(msg))
        logger.warning(f"{len(messages)} messages saved to DLQ", queue_size=await self.redis.llen(self.queue_key))

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    async def retry_message(self, raw_msg):
        msg = json.loads(raw_msg)
        producer = AIOKafkaProducer(bootstrap_servers=settings.kafka.kafka_bootstrap_server)
        try:
            await producer.start()
            await producer.send(settings.kafka.kafka_topic, value=json.dumps(msg).encode("utf-8"))
            logger.info("Message retried from DLQ", message_id=msg.get('id'))
        except Exception as e:
            logger.error("DLQ retry failed", error=str(e), message_id=msg.get('id'))
            raise
        finally:
            await producer.stop()

    async def retry_messages(self):
        retry_count = 0
        while True:
            raw_msg = await self.redis.lpop(self.queue_key)
            if not raw_msg:
                break

            try:
                await self.retry_message(raw_msg)
                retry_count += 1
            except Exception as e:
                logger.error("DLQ retry failed after retries", error=str(e))

        if retry_count > 0:
            logger.info(f"Successfully retried {retry_count} messages from DLQ")