"""
Модуль для обработки пользовательских событий.

Содержит цепочку обработчиков (handler chain) для обработки событий,
включая проверку дубликатов, рейт-лимит и отправку в Kafka.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List, Tuple
from app.core.errors import AppError
from app.core.logger import get_logger
from app.models.event import Event
from app.services.dlq_handler import DLQHandler
from app.services.duplicate_checker import DuplicateChecker
from app.services.kafka_producer import BufferedKafkaProducer
from app.services.rate_limiter_service import RateLimiterService
from app.utils.cache import RedisService

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
    Обработчик для отправки события в Kafka.

    Attributes:
        kafka_sender: Буферизованный продюсер Kafka.
        dlq_handler: Обработчик DLQ для сохранения неотправленных событий.
    """

    def __init__(
            self,
            kafka_sender: BufferedKafkaProducer,
            dlq_handler: DLQHandler,
            next_handler: EventHandler | None = None
    ):
        super().__init__(next_handler)
        self.kafka_sender = kafka_sender
        self.dlq_handler = dlq_handler

    async def handle(self, event: Event) -> Tuple[Dict[str, Any], int]:
        """
        Отправляет событие в Kafka.

        Args:
            event: Событие для отправки.

        Returns:
            Кортеж из словаря с результатом и HTTP-статус-кода.
        """
        logger.info(
            "Sending event to Kafka",
            event_type=event.event_type,
            user_id=str(event.user_id)
        )

        try:
            await self.kafka_sender.send(event.model_dump())
            await self._cache_event(event)
            return {"status": "accepted"}, 202
        except Exception as e:
            logger.error(
                "Failed to send event to Kafka",
                error=str(e),
                event_type=event.event_type,
                user_id=str(event.user_id)
            )
            await self._handle_kafka_failure(event)
            return {"status": "saved to DLQ"}, 503

    async def _cache_event(self, event: Event):
        """
        Кэширует событие как обработанное.

        Args:
            event: Событие для кэширования.
        """
        await event.duplicate_checker.cache_event(event)

    async def _handle_kafka_failure(self, event: Event):
        """
        Обрабатывает ошибки при отправке в Kafka, сохраняя событие в DLQ.

        Args:
            event: Событие, которое не удалось отправить.
        """
        await self.dlq_handler.save_messages([event.model_dump()])


class EventProcessor:
    """
    Сервис для обработки пользовательских событий.

    Состоит из цепочки обработчиков:
    1. Проверка дубликатов
    2. Проверка рейт-лимита
    3. Отправка в Kafka
    """

    def __init__(
            self,
            duplicate_checker: DuplicateChecker,
            rate_limiter: RateLimiterService,
            kafka_sender: BufferedKafkaProducer,
            dlq_handler: DLQHandler
    ):
        """
        Инициализирует цепочку обработчиков.

        Args:
            duplicate_checker: Сервис для проверки дубликатов.
            rate_limiter: Сервис для проверки рейт-лимита.
            kafka_sender: Продюсер Kafka для отправки событий.
            dlq_handler: Обработчик DLQ для сохранения неотправленных событий.
        """
        self.chain = DuplicateCheckHandler(duplicate_checker)
        self.chain.set_next(RateLimitHandler(rate_limiter))
        self.chain.set_next(KafkaSendHandler(kafka_sender, dlq_handler))
        logger.info("EventProcessor chain initialized")

    async def process_event(self, event: Event) -> Tuple[Dict[str, Any], int]:
        """
        Обрабатывает событие, пропуская его через цепочку обработчиков.

        Args:
            event: Событие для обработки.

        Returns:
            Кортеж из словаря с результатом и HTTP-статус-кода.
        """
        logger.info(
            "Processing event",
            event_type=event.event_type,
            user_id=str(event.user_id)
        )

        try:
            return await self.chain.handle(event)
        except AppError as e:
            logger.warning(
                "Application error",
                error=e.message,
                status_code=e.status_code,
                event_type=event.event_type,
                user_id=str(event.user_id)
            )
            return e.to_response()
        except Exception as e:
            logger.error(
                "Unexpected error during event processing",
                error=str(e),
                event_type=event.event_type,
                user_id=str(event.user_id),
                exc_info=True
            )
            return {"error": "Internal server error"}, 500