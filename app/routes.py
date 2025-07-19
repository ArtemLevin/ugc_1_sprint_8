from flask import request, jsonify
from app.models.event import Event
from app.core.logger import get_logger
from app.services.event_processor import EventProcessor

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

    processor = EventProcessor(redis_pool, rate_limiter, kafka_producer)
    return await processor.process_event(event)