from flask import Flask

from app.core.logger import get_logger
from flask.async import AsyncFlask
from app.core.config import settings
from app.core.tracing import setup_tracing
from app.core.health import register_health_check
from app.services.kafka_producer import BufferedKafkaProducer
from app.services.rate_limiter import RedisLeakyBucketRateLimiter
from app.utils.cache import RedisPool
from app.routes import track_event_route
import logging


def create_app(redis_pool=None, rate_limiter=None, kafka_producer=None):
    app = AsyncFlask(__name__)
    setup_tracing(app)
    register_health_check(app)

    logging.basicConfig(
        level=getattr(logging, settings.app.app_debug.upper()) if isinstance(settings.app.app_debug, str) else (
            logging.DEBUG if settings.app.app_debug else logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger = get_logger(__name__)

    if not redis_pool:
        redis_pool = RedisPool()

    if not rate_limiter:
        rate_limiter = RedisLeakyBucketRateLimiter(redis_pool)

    if not kafka_producer:
        kafka_producer = BufferedKafkaProducer(
            bootstrap_server=settings.kafka.kafka_bootstrap_server,
            topic=settings.kafka.kafka_topic
        )

    @app.route('/api/v1/events/track', methods=['POST'])
    async def track_event():
        return await track_event_route(redis_pool, rate_limiter, kafka_producer)

    @app.before_first_request
    async def initialize():
        logger.info("Initializing application")
        try:
            await redis_pool.get_client()
            await kafka_producer.start()
            logger.info("Application initialized successfully")
        except Exception as e:
            logger.error("Failed to initialize application", error=str(e))
            raise

    @app.teardown_appcontext
    async def shutdown(exception):
        logger.info("Shutting down application")
        await kafka_producer.stop()
        await redis_pool.close()

    return app