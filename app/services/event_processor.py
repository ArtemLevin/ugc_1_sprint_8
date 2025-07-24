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


class EventHandler(ABC):
    """
    Абстрактный класс для обработчика событий.
    Определяет общий интерфейс для всех обработчиков в цепочке.

    Реализует паттерн Chain of Responsibility.
    Каждый обработчик решает, обработать ли событие или передать дальше.

    """

    def __init__(self, next_handler: Optional["EventHandler"] = None):
        """
        Инициализация обработчика.

        Args:
            next_handler: Следующий обработчик в цепочке.
        """
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

        Позволяет строить цепочку методом: handler1.set_next(handler2).set_next(handler3)

        Args:
            next_handler: Следующий обработчик.

        Returns:
            Следующий обработчик — позволяет строить цепочку в "fluent interface".

        Пример:
            dup.set_next(rate).set_next(kafka) — строит цепочку из трёх звеньев.
        """
        self.next_handler = next_handler
        return next_handler


class DuplicateCheckHandler(EventHandler):
    """
    Обработчик для проверки дубликатов событий.

    Использует DuplicateChecker для семантической проверки события.
    Если событие уже было обработано — возвращает 200 OK (idempotency).
    """

    def __init__(
        self,
        duplicate_checker: DuplicateChecker,
        next_handler: EventHandler | None = None
    ):
        super().__init__(next_handler)
        self.duplicate_checker = duplicate_checker

    async def handle(self, event: Event) -> Tuple[Dict[str, Any], int]:
        """
        Проверяет, не является ли событие дубликатом.

        Args:
            event: Событие для проверки.

        Returns:
            Кортеж из словаря с результатом и HTTP-статус-кода.

        Логика:
        1. Проверяет наличие события в Redis по хэшу (без id, timestamp, session_id);
        2. Если дубль — возвращает 200 OK;
        3. Если нет — передаёт управление следующему обработчику.

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

    Использует RateLimiterService, который в свою очередь использует
    RedisLeakyBucketRateLimiter с Lua-скриптом.
    """

    def __init__(
        self,
        rate_limiter: RateLimiterService,
        next_handler: EventHandler | None = None
    ):
        super().__init__(next_handler)
        self.rate_limiter = rate_limiter

    async def handle(self, event: Event) -> Tuple[Dict[str, Any], int]:
        """
        Проверяет, не превышен ли лимит запросов.

        Args:
            event: Событие для проверки.

        Returns:
            Кортеж из словаря с результатом и HTTP-статус-кода.

        Логика:
        1. Формирует ключ: user_id:event_type;
        2. Проверяет, не превышен ли лимит;
        3. Если превышен — возвращает 429 Too Many Requests;
        4. Если нет — передаёт дальше.

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

    Важно: кэширование происходит ТОЛЬКО после успешной отправки.
    Это гарантирует, что если Kafka упал, дубликат не будет закэширован.
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
        """
        Отправляет событие в Kafka.

        Args:
            event: Событие для отправки.

        Returns:
            Кортеж из словаря с результатом и HTTP-статус-кода.

        Логика:
        1. Отправляет событие в Kafka через BufferedKafkaProducer;
        2. Только при успехе — кэширует событие как "обработанное";
        3. При ошибке — сохраняет в DLQ и возвращает 503.

        Почему 503, а не 500?
        - 503 — Service Unavailable — означает, что сервис временно недоступен;
        - Это подсказка клиенту, что можно повторить запрос позже.

        Почему не 202?
        - 202 Accepted — означает, что запрос принят, но не обработан;
        - Здесь мы не можем гарантировать обработку, так как Kafka упал.
        """
        logger.info(
            "Sending event to Kafka",
            event_type=event.event_type,
            user_id=str(event.user_id)
        )

        try:
            # Отправка в буферизованный продюсер
            await self.kafka_sender.send(event.model_dump())

            # Кэшируем ТОЛЬКО при успехе
            # Это критично: если Kafka упал, мы не должны считать событие обработанным
            await self.duplicate_checker.cache_event(event)

            return {"status": "accepted"}, 202

        except Exception as e:
            logger.error(
                "Failed to send event to Kafka",
                error=str(e),
                event_type=event.event_type,
                user_id=str(event.user_id)
            )
            # Сохраняем в DLQ для повторной отправки
            await self._handle_kafka_failure(event)
            return {"status": "saved to DLQ"}, 503

    async def _handle_kafka_failure(self, event: Event):
        """
        Обрабатывает сбой отправки в Kafka.

        Сохраняет событие в DLQ (Redis), откуда позже будет повторно отправлено.

        Args:
            event: Событие, которое не удалось отправить.
        """
        # DLQ ожидает список сообщений
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
        """
        Инициализация EventProcessor.

        Создаёт и соединяет обработчики в цепочку:
        DuplicateCheck → RateLimit → KafkaSend

        Порядок важен:
        - Сначала дешёвые проверки (Redis);
        - Потом дорогие (Kafka).
        """
        dup = DuplicateCheckHandler(duplicate_checker)

        rate = RateLimitHandler(rate_limiter)

        kafka = KafkaSendHandler(kafka_sender, dlq_handler, duplicate_checker)

        # Строим цепочку: dup → rate → kafka
        # set_next возвращает следующий обработчик — позволяет строить цепочку
        dup.set_next(rate).set_next(kafka)

        # Сохраняем цепочку
        self.chain = dup

        logger.info("EventProcessor chain initialized")

    async def process_event(self, event: Event) -> Tuple[Dict[str, Any], int]:
        """
        Обрабатывает событие через цепочку обработчиков.

        Args:
            event: Событие для обработки.

        Returns:
            Кортеж из словаря с результатом и HTTP-статус-кода.

        Обработка ошибок:
        - AppError — ожидаемые ошибки (дубликат, рейт-лимит) → возвращаем понятный ответ;
        - Exception — неожиданные ошибки → 500.
        """
        try:
            return await self.chain.handle(event)

        except AppError as e:
            # Ожидаемые ошибки: дубликат, рейт-лимит
            # Уже залогированы внутри to_response()
            return e.to_response()

        except Exception as e:
            # Неожиданная ошибка — логируем с traceback
            logger.error(
                "Unexpected error during event processing",
                error=str(e),
                event_type=event.event_type,
                user_id=str(event.user_id),
                exc_info=True
            )
            return {"error": "Internal server error"}, 500