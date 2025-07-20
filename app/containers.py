from dependency_injector import containers, providers
from app.utils.cache import RedisService
from app.services.rate_limiter import RedisLeakyBucketRateLimiter
from app.services.event_processor import EventProcessor
from app.services.duplicate_checker import DuplicateChecker
from app.services.dlq_handler import DLQHandler
from app.services.kafka_producer import BufferedKafkaProducer
from app.core.config import settings


class Container(containers.DeclarativeContainer):
    """
    Контейнер зависимостей, использующий dependency_injector для DI.
    Содержит все основные сервисы и зависимости приложения.
    """

    wiring_config = containers.WiringConfiguration(modules=["app.routes"])

    redis_service = providers.Singleton(
        RedisService,
        redis_settings=settings.redis
    )

    rate_limiter = providers.Factory(
        RedisLeakyBucketRateLimiter,
        redis_service=redis_service,
        rate=settings.rate_limit.rate_limit,
        capacity=settings.rate_limit.rate_limit_window
    )


    duplicate_checker = providers.Factory(
        DuplicateChecker,
        redis_service=redis_service,
        cache_ttl=settings.duplicate_checker.cache_ttl
    )



    dlq_handler = providers.Factory(
        DLQHandler,
        redis_service=redis_service,
        queue_key=settings.dlq.dlq_queue_key
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