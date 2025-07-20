from abc import ABC, abstractmethod
from app.core.errors import DuplicateEvent, RateLimitExceeded
from app.core.logger import get_logger
from app.models.event import Event
from app.services.dlq_handler import DLQHandler
from app.services.duplicate_checker import DuplicateChecker
from app.services.kafka_producer import BufferedKafkaProducer
from app.services.rate_limiter_service import RateLimiterService

logger = get_logger(__name__)

class Handler(ABC):
    def __init__(self, next_handler=None):
        self.next_handler = next_handler

    @abstractmethod
    async def handle(self, event: Event) -> tuple[dict, int]:
        pass

class DuplicateCheckHandler(Handler):
    def __init__(self, duplicate_checker: DuplicateChecker, next_handler=None):
        super().__init__(next_handler)
        self.duplicate_checker = duplicate_checker

    async def handle(self, event: Event) -> tuple[dict, int]:
        logger.info("Checking for duplicate", event_type=event.event_type, user_id=str(event.user_id))
        if await self.duplicate_checker.is_duplicate(event):
            logger.info("Duplicate event detected", event_type=event.event_type, user_id=str(event.user_id))
            raise DuplicateEvent()
        if self.next_handler:
            return await self.next_handler.handle(event)
        return {"status": "accepted"}, 202

class RateLimitHandler(Handler):
    def __init__(self, rate_limiter: RateLimiterService, next_handler=None):
        super().__init__(next_handler)
        self.rate_limiter = rate_limiter

    async def handle(self, event: Event) -> tuple[dict, int]:
        logger.info("Checking rate limit", event_type=event.event_type, user_id=str(event.user_id))
        if not await self.rate_limiter.check(event):
            logger.warning("Rate limit exceeded", event_type=event.event_type, user_id=str(event.user_id))
            raise RateLimitExceeded()
        if self.next_handler:
            return await self.next_handler.handle(event)
        return {"status": "accepted"}, 202

class KafkaSendHandler(Handler):
    def __init__(self, kafka_sender: BufferedKafkaProducer, dlq_handler: DLQHandler, next_handler=None):
        super().__init__(next_handler)
        self.kafka_sender = kafka_sender
        self.dlq_handler = dlq_handler

    async def handle(self, event: Event) -> tuple[dict, int]:
        logger.info("Sending event to Kafka", event_type=event.event_type, user_id=str(event.user_id))
        try:
            await self.kafka_sender.send(event.model_dump())
            await self._cache_event(event)
            return {"status": "accepted"}, 202
        except Exception as e:
            logger.error("Failed to send event to Kafka", error=str(e))
            await self._handle_kafka_failure(event)
            return {"status": "saved to DLQ"}, 503

    async def _cache_event(self, event: Event):
        await event.duplicate_checker.cache_event(event)

    async def _handle_kafka_failure(self, event: Event):
        await self.dlq_handler.save_messages([event.model_dump()])

class EventProcessor:
    def __init__(
        self,
        duplicate_checker: DuplicateChecker,
        rate_limiter: RateLimiterService,
        kafka_sender: BufferedKafkaProducer,
        dlq_handler: DLQHandler
    ):
        self.chain = DuplicateCheckHandler(
            duplicate_checker,
            RateLimitHandler(
                rate_limiter,
                KafkaSendHandler(kafka_sender, dlq_handler)
            )
        )

    async def process_event(self, event: Event) -> tuple[dict, int]:
        logger.info("Processing event", event_type=event.event_type, user_id=str(event.user_id))
        try:
            return await self.chain.handle(event)
        except DuplicateEvent:
            return {"status": "duplicate"}, 200
        except RateLimitExceeded:
            return {"error": "Too many requests"}, 429
        except Exception as e:
            logger.error("Unexpected error", error=str(e))
            return {"error": "Internal server error"}, 500