from flask import Flask, request, jsonify
from flask.async import AsyncFlask
from app.models.event import Event
from app.services.kafka_producer import BufferedKafkaProducer
from app.services.rate_limiter import RedisLeakyBucketRateLimiter
from app.utils.cache import RedisPool
from app.core.config import settings
from app.core.logger import get_logger
from app.core.tracing import setup_tracing, add_event_attributes
from app.core.health import register_health_check
import asyncio
import hashlib
import json

app = AsyncFlask(__name__)
setup_tracing(app)
register_health_check(app)
logger = get_logger(__name__)

redis_pool = RedisPool()
producer = None
rate_limiter = None
redis_client = None

@app.before_first_request
async def initialize():
    global redis_client, rate_limiter, producer
    try:
        redis_client = await redis_pool.get_client()
        rate_limiter = RedisLeakyBucketRateLimiter(
            redis_client,
            rate=settings.rate_limit,
            capacity=settings.rate_limit * 2
        )
        producer = BufferedKafkaProducer(buffer_size=settings.kafka_max_batch_size)
        await producer.start()
        asyncio.create_task(start_dlq_retry_task())
        logger.info("Service initialized successfully")
    except Exception as e:
        logger.error("Initialization failed", error=str(e))
        raise

@app.route('/api/v1/events/track', methods=['POST'])
async def track_event():
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

    cached = await redis_client.get(cache_key)
    if cached:
        logger.info("Duplicate event", user_id=user_id_str, event_type=event.event_type)
        return jsonify({"status": "duplicate"}), 200

    allowed = await rate_limiter.allow_request(f"{user_id_str}:{event.event_type}")
    if not allowed:
        return jsonify({"error": "Too many requests"}), 429

    logger.info("Received event", event_type=event.event_type, user_id=user_id_str)

    try:
        await producer.send(event_dict)
        await redis_client.setex(cache_key, settings.rate_limit_window, "processed")
        add_event_attributes(event)
        return jsonify({"status": "accepted"}), 202
    except Exception as e:
        logger.error("Failed to send event to Kafka", error=str(e))
        from app.services.dlq_handler import DLQHandler
        handler = DLQHandler(redis_client=redis_client)
        await handler.save_messages([event_dict])
        return jsonify({"status": "saved to DLQ"}), 503

async def start_dlq_retry_task(interval=60):
    from app.services.dlq_handler import DLQHandler
    handler = DLQHandler(redis_client=redis_client)
    while True:
        try:
            await handler.retry_messages()
        except Exception as e:
            logger.error("DLQ retry task failed", error=str(e))
        await asyncio.sleep(interval)

@app.teardown_appcontext
async def shutdown_producer(exception):
    if producer:
        await producer.stop()
    if redis_client:
        await redis_client.close()
    logger.info("Gracefully shut down resources")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)