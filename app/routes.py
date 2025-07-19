from flask import request, jsonify
from app.models.event import Event
from app.core.logger import get_logger
from app.core.tracing import add_event_attributes
import hashlib
import json

logger = get_logger(__name__)

async def track_event(redis_pool, rate_limiter, kafka_producer):
    data = request.get_json()
    try:
        event = Event(**data)
    except Exception as e:
        logger.error("Invalid event format", error=str(e))
        return jsonify({"error": "Invalid input"}), 400

    user_id_str = str(event.user_id)
    event_dict = event.model_dump()
    content_hash = hashlib.sha256(json.dumps(event_dict).encode()).hexdigest()
    cache_key = f"user_actions:{user_id_str}:{content_hash}"

    cached = await redis_pool.get_client().get(cache_key)
    if cached:
        logger.info("Duplicate event", user_id=user_id_str, event_type=event.event_type)
        return jsonify({"status": "duplicate"}), 200

    allowed = await rate_limiter.allow_request(f"{user_id_str}:{event.event_type}")
    if not allowed:
        return jsonify({"error": "Too many requests"}), 429

    logger.info("Received event", event_type=event.event_type, user_id=user_id_str)
    try:
        await kafka_producer.send(event_dict)
        await redis_pool.get_client().setex(cache_key, rate_limiter.window, "processed")
        add_event_attributes(event)
        return jsonify({"status": "accepted"}), 202
    except Exception as e:
        logger.error("Failed to send event to Kafka", error=str(e))
        from app.services.dlq_handler import DLQHandler
        handler = DLQHandler(redis_client=await redis_pool.get_client())
        await handler.save_messages([event_dict])
        return jsonify({"status": "saved to DLQ"}), 503