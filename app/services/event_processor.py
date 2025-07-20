"""
Модуль для обработки пользовательских событий.
Содержит цепочку обработчиков (handler chain) для обработки событий,
включая проверку дубликатов, рейт-лимит и отправку в Kafka.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Tuple
from app.core.errors import AppError
from app.core.logger import get_logger
from app.models.event import Event
from app.services.dlq_handler import DLQHandler
from app.services.duplicate_checker import DuplicateChecker
from app.services.kafka_producer import BufferedKafkaProducer
from app.services.rate_limiter_service import RateLimiterService

logger = get_logger(__name__)

logger = get_logger(__name__)


class EventHandler(ABC):
    """
    Абстрактный класс для обработчика событий.
    Определяет общий интерфейс для всех обработчиков в цепочке.
    """
    def __init__(self, next_handler: Optional["EventHandler"] = None):
        self.next_handler = next_handler

    @abstractmethod
    async def handle(self, event: Event) -> Tuple[Dict[str, Any], int]:
        """
        Обрабатывает событие.
        Args:
            event: Событие для обработки.
        Returns:
            Кортеж из словаря с результатом и HTTP-статус-кода.
        """
        pass

    def set_next(self, next_handler: "EventHandler") -> "EventHandler":
        """
        Устанавливает следующий обработчик в цепочке.
        Args:
            next_handler: Следующий обработчик.
        Returns:
            Следующий обработчик.
        """
        self.next_handler = next_handler
        return next_handler


class DuplicateCheckHandler(EventHandler):
    """
    Обработчик для проверки дубликатов событий.
    Attributes:
        duplicate_checker: Сервис для проверки дубликатов.
    """
    def __init__(self, duplicate_checker: DuplicateChecker, next_handler: EventHandler | None = None):
        super().__init__(next_handler)
        self.duplicate_checker = duplicate_checker

    async def handle(self, event: Event) -> Tuple[Dict[str, Any], int]:
        """
        Проверяет, не является ли событие дубликатом.
        Args:
            event: Событие для проверки.
        Returns:
            Кортеж из словаря с результатом и HTTP-статус-кода.
        """
        logger.info(
            "Checking for duplicate",
            event_type=event.event_type,
            user_id=str(event.user_id)
        )
        if await self.duplicate_checker.is_duplicate(event):
            logger.info(
                "Duplicate event detected",
                event_type=event.event_type,
                user_id=str(event.user_id)
            )
            return {"status": "duplicate"}, 200
        if self.next_handler:
            return await self.next_handler.handle(event)
        return {"status": "accepted"}, 202


class RateLimitHandler(EventHandler):
    """
    Обработчик для проверки лимитов на количество запросов.
    Attributes:
        rate_limiter: Сервис для проверки рейт-лимита.
    """
    def __init__(self, rate_limiter: RateLimiterService, next_handler: EventHandler | None = None):
        super().__init__(next_handler)
        self.rate_limiter = rate_limiter

    async def handle(self, event: Event) -> Tuple[Dict[str, Any], int]:
        """
        Проверяет, не превышен ли лимит запросов.
        Args:
            event: Событие для проверки.
        Returns:
            Кортеж из словаря с результатом и HTTP-статус-кода.
        """
        logger.info(
            "Checking rate limit",
            event_type=event.event_type,
            user_id=str(event.user_id)
        )
        if not await self.rate_limiter.check(event):
            logger.warning(
                "Rate limit exceeded",
                event_type=event.event_type,
                user_id=str(event.user_id)
            )
            return {"error": "Too many requests"}, 429
        if self.next_handler:
            return await self.next_handler.handle(event)
        return {"status": "accepted"}, 202


class KafkaSendHandler(EventHandler):
    """
    Обработчик для отправки события в Kafka и последующего кэширования через DuplicateChecker.
    """

    def __init__(
        self,
        kafka_sender: BufferedKafkaProducer,
        dlq_handler: DLQHandler,
        duplicate_checker: DuplicateChecker,
        next_handler: EventHandler | None = None
    ):
        super().__init__(next_handler)
        self.kafka_sender = kafka_sender
        self.dlq_handler = dlq_handler
        self.duplicate_checker = duplicate_checker

    async def handle(self, event: Event) -> Tuple[Dict[str, Any], int]:
        logger.info(
            "Sending event to Kafka",
            event_type=event.event_type,
            user_id=str(event.user_id)
        )

        try:
            # Буферизованная отправка
            await self.kafka_sender.send(event.model_dump())
            # Кэширование как «обработанное»
            await self.duplicate_checker.cache_event(event)
            return {"status": "accepted"}, 202

        except Exception as e:
            logger.error(
                "Failed to send event to Kafka",
                error=str(e),
                event_type=event.event_type,
                user_id=str(event.user_id)
            )
            # Сохраняем во «входящий мёртвый» DLQ
            await self._handle_kafka_failure(event)
            return {"status": "saved to DLQ"}, 503

    async def _handle_kafka_failure(self, event: Event):
        await self.dlq_handler.save_messages([event.model_dump()])


class EventProcessor:
    """
    Собирает цепочку обработки:
    1) DuplicateCheck
    2) RateLimit
    3) KafkaSend + cache_event
    """

    def __init__(
        self,
        duplicate_checker: DuplicateChecker,
        rate_limiter: RateLimiterService,
        kafka_sender: BufferedKafkaProducer,
        dlq_handler: DLQHandler
    ):
        # 1) Проверка дубликата
        dup = DuplicateCheckHandler(duplicate_checker)
        # 2) Рейт-лимит
        rate = RateLimitHandler(rate_limiter)
        # 3) Отправка в Kafka + кэш
        kafka = KafkaSendHandler(kafka_sender, dlq_handler, duplicate_checker)

        # Склеиваем цепочку
        dup.set_next(rate).set_next(kafka)
        self.chain = dup
        logger.info("EventProcessor chain initialized")

    async def process_event(self, event: Event) -> Tuple[Dict[str, Any], int]:
        try:
            return await self.chain.handle(event)
        except AppError as e:
            return e.to_response()
        except Exception as e:
            logger.error("Unexpected error during event processing", error=str(e), exc_info=True)
            return {"error": "Internal server error"}, 500