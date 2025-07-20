"""
Модуль для проверки дубликатов событий.

Содержит класс `DuplicateChecker` для проверки, не было ли уже обработано аналогичное событие.
"""

import xxhash
import json
from typing import Any, Optional, Dict
from uuid import UUID
from pydantic import BaseModel
from app.core.logger import get_logger
from app.utils.cache import RedisService

logger = get_logger(__name__)


class DuplicateChecker:
    """
    Сервис для проверки дубликатов событий.

    Attributes:
        redis_service: Сервис для работы с Redis.
        cache_ttl: Время жизни ключа в Redis (в секундах).
        hash_algorithm: Алгоритм хэширования для генерации ключа.
    """

    DEFAULT_KEY_PREFIX = "user_actions"
    DEFAULT_HASH_ALGORITHM = xxhash.xxh64

    def __init__(
            self,
            redis_service: RedisService,
            cache_ttl: int = 3600,
            hash_algorithm: Any = xxhash.xxh64
    ):
        """
        Инициализирует DuplicateChecker.

        Args:
            redis_service: Сервис для работы с Redis.
            cache_ttl: Время жизни ключа в Redis (в секундах).
            hash_algorithm: Алгоритм хэширования для генерации ключа.
        """
        self.redis_service = redis_service
        self.cache_ttl = cache_ttl
        self.hash_algorithm = hash_algorithm
        logger.info(
            "DuplicateChecker initialized",
            cache_ttl=cache_ttl,
            hash_algorithm=hash_algorithm.__name__
        )

    async def is_duplicate(self, event: BaseModel) -> bool:
        """
        Проверяет, является ли событие дубликатом.

        Args:
            event: Событие для проверки.

        Returns:
            True, если событие является дубликатом, иначе False.
        """
        key = self._generate_key(event)
        result = await self._get(key)

        logger.debug(
            "Duplicate check",
            key=key,
            is_duplicate=bool(result),
            user_id=self._get_user_id(event)
        )

        return bool(result)

    async def cache_event(self, event: BaseModel) -> None:
        """
        Кэширует событие в Redis.

        Args:
            event: Событие для кэширования.
        """
        key = self._generate_key(event)
        result = await self._setex(key, self.cache_ttl, "processed")

        logger.debug(
            "Event cached",
            key=key,
            user_id=self._get_user_id(event),
            success=bool(result)
        )

    def _generate_key(self, event: BaseModel) -> str:
        """
        Генерирует уникальный ключ для события.

        Args:
            event: Событие, для которого нужно сгенерировать ключ.

        Returns:
            Уникальный ключ для события.
        """
        event_data = self._prepare_event_data(event)
        content = json.dumps(event_data, sort_keys=True)
        content_hash = self.hash_algorithm(content).hexdigest()

        return f"{self.DEFAULT_KEY_PREFIX}:{self._get_user_id(event)}:{content_hash}"

    def _prepare_event_data(self, event: BaseModel) -> Dict[str, Any]:
        """
        Подготавливает данные события для хэширования.

        Args:
            event: Событие для подготовки.

        Returns:
            Словарь с данными события, исключая поля, не влияющие на уникальность.
        """
        # Исключаем поля, которые не влияют на уникальность события
        exclude_fields = {"id", "timestamp", "session_id"}

        if hasattr(event, "model_dump"):
            return event.model_dump(exclude=exclude_fields)
        elif hasattr(event, "dict"):
            return event.dict(exclude=exclude_fields)
        else:
            raise TypeError("Unsupported event type for duplication check")

    def _get_user_id(self, event: BaseModel) -> Optional[str]:
        """
        Извлекает идентификатор пользователя из события.

        Args:
            event: Событие, из которого нужно извлечь идентификатор пользователя.

        Returns:
            Идентификатор пользователя в виде строки или None, если его нет.
        """
        if hasattr(event, "user_id"):
            user_id = getattr(event, "user_id")
            return str(user_id) if isinstance(user_id, UUID) else user_id
        return None

    async def _get(self, key: str) -> Optional[bytes]:
        """
        Получает значение по ключу из Redis.

        Args:
            key: Ключ, по которому нужно получить значение.

        Returns:
            Значение по ключу или None, если ключ не найден.
        """
        try:
            return await self.redis_service.get(key)
        except Exception as e:
            logger.warning(
                "Redis GET failed",
                key=key,
                error=str(e)
            )
            return None

    async def _setex(self, key: str, ttl: int, value: str) -> bool:
        """
        Сохраняет значение по ключу в Redis с TTL.

        Args:
            key: Ключ, по которому нужно сохранить значение.
            ttl: Время жизни ключа (в секундах).
            value: Значение для сохранения.

        Returns:
            True, если операция успешна, иначе False.
        """
        try:
            return await self.redis_service.setex(key, ttl, value)
        except Exception as e:
            logger.warning(
                "Redis SETEX failed",
                key=key,
                error=str(e)
            )
            return False