from app.core.logger import get_logger
from app.core.tracing import add_event_attributes

logger = get_logger(__name__)

class EventProcessor:
    def __init__(self, duplicate_checker, rate_limiter, kafka_sender, dlq_handler):
        self.duplicate_checker = duplicate_checker
        self.rate_limiter = rate_limiter
        self.kafka_sender = kafka_sender
        self.dlq_handler = dlq_handler

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
            await self.kafka_sender.send(event.model_dump())
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
        return await self.duplicate_checker.is_duplicate(event)

    async def _check_rate_limit(self, event):
        return await self.rate_limiter.check(event)

    async def _cache_event(self, event):
        await self.duplicate_checker.cache_event(event)

    async def _handle_kafka_failure(self, event):
        await self.dlq_handler.save_messages([event.model_dump()])