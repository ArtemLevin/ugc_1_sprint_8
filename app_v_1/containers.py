from dependency_injector import containers, providers

from app_v_1.core.config import settings
from app_v_1.utils.cache import RedisService
from app_v_1.repositories.redis_repository import RedisRepository
from app_v_1.models.event import Event
from app_v_1.services.duplicate_checker import DuplicateChecker
from app_v_1.services.rate_limiter_service import RateLimiterService
from app_v_1.services.rate_limiter import RedisLeakyBucketRateLimiter
from app_v_1.services.kafka_producer import BufferedKafkaProducer
from app_v_1.services.dlq_handler import DLQHandler
from app_v_1.services.event_processor import EventProcessor


class Container(containers.DeclarativeContainer):
    """
    DI-контейнер для всех сквозных зависимостей.
    """

    wiring_config = containers.WiringConfiguration(
        modules=["app_v_1.routes"]
    )

    config = providers.Object(settings)

    # RedisService для кешей, rate-limiter и DLQ
    redis_service = providers.Singleton(RedisService)

    # Репозиторий для хранения обработанных событий
    event_repository = providers.Factory(
        RedisRepository,
        redis_service=redis_service,
        model=Event
    )

    # Сервис проверки дубликатов
    duplicate_checker = providers.Singleton(
        DuplicateChecker,
        cache_repository=event_repository,
        cache_ttl=3600  # или settings.redis.default_ttl
    )

    # Конкретный leaky-bucket-лимитер
    rate_limiter = providers.Singleton(
        RedisLeakyBucketRateLimiter,
        redis_service=redis_service,
        limit=settings.rate_limit.rate_limit,
        window=settings.rate_limit.window_seconds
    )

    # Сервис проверки rate-limit
    rate_limiter_service = providers.Singleton(
        RateLimiterService,
        rate_limiter=rate_limiter
    )

    # DLQ-handler для неуспешных отправок
    dlq_handler = providers.Singleton(
        DLQHandler,
        redis_service=redis_service,
        queue_key=settings.dlq.dlq_queue_key
    )

    # Буферизованный Kafka-продюсер
    kafka_producer = providers.Singleton(
        BufferedKafkaProducer,
        redis_service=redis_service
    )

    # Основной процессор событий
    event_processor = providers.Factory(
        EventProcessor,
        duplicate_checker=duplicate_checker,
        rate_limiter=rate_limiter_service,
        kafka_sender=kafka_producer,
        dlq_handler=dlq_handler
    )