from flask import Flask
from flask.async import AsyncFlask
from app.core.config import KafkaSettings, RedisSettings, RateLimitSettings
from app.core.logger import get_logger
from app.core.tracing import setup_tracing
from app.core.health import register_health_check
from app.services.kafka_producer import BufferedKafkaProducer
from app.services.rate_limiter import RedisLeakyBucketRateLimiter
from app.utils.cache import RedisPool
from app.routes import track_event
import asyncio
import logging

def create_app():
    app = AsyncFlask(__name__)
    setup_tracing(app)
    register_health_check(app)

    # Setup logging
    logging.basicConfig(level=logging.INFO)

    # Load configs
    kafka_settings = KafkaSettings()
    redis_settings = RedisSettings()
    rate_limit_settings = RateLimitSettings()

    # Initialize services
    redis_pool = RedisPool(url=redis_settings.redis_url, max_connections=redis_settings.redis_max_connections)
    rate_limiter = RedisLeakyBucketRateLimiter(
        redis_pool=redis_pool,
        rate=rate_limit_settings.rate,
        capacity=rate_limit_settings.capacity
    )
    kafka_producer = BufferedKafkaProducer(
        bootstrap_server=kafka_settings.kafka_bootstrap_server,
        topic=kafka_settings.kafka_topic,
        buffer_size=kafka_settings.kafka_max_batch_size,
        send_timeout=kafka_settings.kafka_send_timeout
    )

    # Register routes
    @app.route('/api/v1/events/track', methods=['POST'])
    async def track_event_route():
        return await track_event(redis_pool, rate_limiter, kafka_producer)

    # Lifecycle
    @app.before_first_request
    async def initialize():
        await redis_pool.get_client()
        await kafka_producer.start()
        asyncio.create_task(kafka_producer.start_dlq_retry_task())

    @app.teardown_appcontext
    async def shutdown(exception):
        await kafka_producer.stop()
        await redis_pool.close()

    return app