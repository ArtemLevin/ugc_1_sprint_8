from flask import request, jsonify
from app.models.event import Event
from app.core.logger import get_logger
from app.services.event_processor import EventProcessor
from app.services.duplicate_checker import DuplicateChecker
from app.services.rate_limiter_service import RateLimiterService
from app.services.kafka_event_sender import KafkaEventSender
from app.services.dlq_handler import DLQHandler
from app.utils.cache import RedisService

logger = get_logger(__name__)

async def track_event_route(redis_pool, rate_limiter, kafka_producer):
    data = request.get_json()
    try:
        event = Event(**data)
    except Exception as e:
        logger.error("Invalid event format",
                     error=str(e),
                     raw_data=data)
        return jsonify({"error": "Invalid input"}), 400

    redis_service = RedisService()
    duplicate_checker = DuplicateChecker(redis_service)
    rate_limiter_service = RateLimiterService(rate_limiter)
    kafka_event_sender = KafkaEventSender(kafka_producer)
    dlq_handler = DLQHandler(redis_service)

    processor = EventProcessor(
        duplicate_checker,
        rate_limiter_service,
        kafka_event_sender,
        dlq_handler
    )

    return await processor.process_event(event)