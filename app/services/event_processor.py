import hashlib
import json
from app.core.logger import get_logger
from app.core.tracing import add_event_attributes

logger = get_logger(__name__)


class EventProcessor:
    def __init__(self, redis_pool, rate_limiter, kafka_producer):
        self.redis_pool = redis_pool
        self.rate_limiter = rate_limiter
        self.kafka_producer = kafka_producer

    async def process_event(self, event):
        logger.info("Processing event", event_type=event.event_type, user_id=str(event.user_id))

        if await self._is_duplicate(event):
            logger.info("Duplicate event detected",
                        event_type=event.event_type,
                        user_id=str(event.user_id))
            return {"status": "duplicate"}, 200

        if not await self._check_rate_limit(event):
            logger.warning("Rate limit exceeded",
                           event_type=event.event_type,
                           user_id=str(event.user_id))
            return {"error": "Too many requests"}, 429

        try:
            await self.kafka_producer.send(event.model_dump())
            await self._cache_event(event)
            add_event_attributes(event)
            logger.info("Event sent to Kafka",
                        event_type=event.event_type,
                        user_id=str(event.user_id))
            return {"status": "accepted"}, 202
        except Exception as e:
            logger.error("Failed to send event to Kafka",
                         error=str(e),
                         event_type=event.event_type,
                         user_id=str(event.user_id))
            await self._handle_kafka_failure(event)
            return {"status": "saved to DLQ"}, 503

    async def _is_duplicate(self, event):
        user_id_str = str(event.user_id)
        event_dict = event.model_dump()
        content_hash = hashlib.sha256(json.dumps(event_dict).encode()).hexdigest()
        cache_key = f"user_actions:{user_id_str}:{content_hash}"
        return await self.redis_pool.get_client().get(cache_key)

    async def _check_rate_limit(self, event):
        user_id_str = str(event.user_id)
        return await self.rate_limiter.allow_request(f"{user_id_str}:{event.event_type}")

    async def _cache_event(self, event):
        user_id_str = str(event.user_id)
        event_dict = event.model_dump()
        content_hash = hashlib.sha256(json.dumps(event_dict).encode()).hexdigest()
        cache_key = f"user_actions:{user_id_str}:{content_hash}"
        await self.redis_pool.get_client().setex(cache_key, self.rate_limiter.window, "processed")

    async def _handle_kafka_failure(self, event):
        from app.services.dlq_handler import DLQHandler
        handler = DLQHandler(redis_client=await self.redis_pool.get_client())
        await handler.save_messages([event.model_dump()])