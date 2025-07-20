"""
Модуль для настройки и управления зависимостями через DI-контейнер.

Содержит класс `Container`, использующий `dependency_injector` для внедрения зависимостей.
"""

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
    Контейнер зависимостей, использующий `dependency_injector` для внедрения зависимостей.

    Attributes:
        wiring_config: Конфигурация для автоматического подключения зависимостей.
        redis_service: Сервис для работы с Redis.
        rate_limiter: Сервис для проверки рейт-лимита.
        duplicate_checker: Сервис для проверки дубликатов событий.
        dlq_handler: Обработчик DLQ для сохранения неотправленных сообщений.
        kafka_producer: Продюсер Kafka с буферизацией.
        event_processor: Сервис для обработки событий.
    """

    wiring_config = containers.WiringConfiguration(modules=["app.routes"])

    redis_service: providers.Singleton[RedisService] = providers.Singleton(
        RedisService,
        redis_settings=settings.redis
    )

    rate_limiter: providers.Factory[RedisLeakyBucketRateLimiter] = providers.Factory(
        RedisLeakyBucketRateLimiter,
        redis_service=redis_service,
        rate=settings.rate_limit.rate_limit,
        capacity=settings.rate_limit.rate_limit_window
    )

    duplicate_checker: providers.Factory[DuplicateChecker] = providers.Factory(
        DuplicateChecker,
        redis_service=redis_service,
        cache_ttl=settings.duplicate_checker.cache_ttl
    )

    dlq_handler: providers.Factory[DLQHandler] = providers.Factory(
        DLQHandler,
        redis_service=redis_service,
        queue_key=settings.dlq.dlq_queue_key
    )

    kafka_producer: providers.Singleton[BufferedKafkaProducer] = providers.Singleton(
        BufferedKafkaProducer,
        bootstrap_server=settings.kafka.bootstrap_server,
        topic=settings.kafka.topic,
        buffer_size=settings.kafka.max_batch_size,
        flush_interval=settings.kafka.flush_interval,
        send_timeout=settings.kafka.send_timeout
    )

    event_processor: providers.Factory[EventProcessor] = providers.Factory(
        EventProcessor,
        duplicate_checker=duplicate_checker,
        rate_limiter=rate_limiter,
        kafka_sender=kafka_producer,
        dlq_handler=dlq_handler
    )