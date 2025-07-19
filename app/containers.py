from dependency_injector import containers, providers
from app.utils.cache import RedisPool
from app.services.rate_limiter import RedisLeakyBucketRateLimiter
from app.services.event_processor import EventProcessor
from app.services.duplicate_checker import DuplicateChecker
from app.services.dlq_handler import DLQHandler
from app.services.kafka_producer import BufferedKafkaProducer
from app.core.config import settings

class Container(containers.DeclarativeContainer):
    wiring_config = containers.WiringConfiguration(modules=["app.routes"])

    redis_pool = providers.Singleton(
        RedisPool,
        url=settings.redis.redis_url,
        max_connections=settings.redis.redis_max_connections,
        max_retries=settings.redis.redis_retry_attempts
    )

    rate_limiter = providers.Factory(
        RedisLeakyBucketRateLimiter,
        redis_pool=redis_pool,
        rate=settings.rate_limit.rate_limit,
        capacity=settings.rate_limit.rate_limit_window
    )

    dlq_handler = providers.Factory(
        DLQHandler,
        redis_client=redis_pool
    )

    duplicate_checker = providers.Factory(
        DuplicateChecker,
        redis_service=redis_pool
    )

    kafka_producer = providers.Singleton(
        BufferedKafkaProducer,
        bootstrap_server=settings.kafka.kafka_bootstrap_server,
        topic=settings.kafka.kafka_topic,
        buffer_size=settings.kafka.kafka_max_batch_size,
        flush_interval=settings.kafka.kafka_flush_interval,
        send_timeout=settings.kafka.kafka_send_timeout
    )

    event_processor = providers.Factory(
        EventProcessor,
        duplicate_checker=duplicate_checker,
        rate_limiter=rate_limiter,
        kafka_sender=kafka_producer,
        dlq_handler=dlq_handler
    )