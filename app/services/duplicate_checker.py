import xxhash
import json
from typing import Any, Optional, Dict
from uuid import UUID
from pydantic import BaseModel
from app.core.logger import get_logger
from app.repositories.repository import CacheRepository
from app.core.config import settings

logger = get_logger(__name__)


class DuplicateChecker:
    """
    Сервис для проверки дубликатов событий через CacheRepository.
    Attributes:
        cache_repository: абстракция репозитория для кэширования события.
        cache_ttl: Время жизни ключа в Redis (в секундах).
        hash_algorithm: Алгоритм хэширования для генерации ключа.
        key_prefix: Префикс для ключей Redis.
    """
    DEFAULT_HASH_ALGORITHM = xxhash.xxh64

    def __init__(
        self,
        cache_repository: CacheRepository[BaseModel],
        cache_ttl: int | None = None,
        hash_algorithm: Any = xxhash.xxh64,
        key_prefix: str | None = None
    ):
        self.cache_repository = cache_repository
        self.cache_ttl = cache_ttl if cache_ttl is not None else settings.duplicate_checker.cache_ttl
        self.hash_algorithm = hash_algorithm
        self.key_prefix = key_prefix if key_prefix is not None else settings.duplicate_checker.key_prefix

        logger.info(
            "DuplicateChecker initialized",
            cache_ttl=self.cache_ttl,
            hash_algorithm=self.hash_algorithm.__name__,
            key_prefix=self.key_prefix # Логируем префикс
        )

    async def is_duplicate(self, event: BaseModel) -> bool:
        """
        Проверяет, является ли событие дубликатом.

        Генерирует ключ на основе хэша содержимого события (без метаданных)
        и проверяет его наличие в Redis.

        Args:
            event: Pydantic-модель события.

        Returns:
            True, если событие уже было обработано.
        """
        key = self._generate_key(event)

        cached = await self.cache_repository.get(key)

        is_dup = bool(cached)

        logger.debug(
            "Duplicate check",
            key=key,
            is_duplicate=is_dup,
            user_id=self._get_user_id(event)
        )

        return is_dup

    async def cache_event(self, event: BaseModel) -> None:
        """
        Сохраняет событие в Redis как "обработанное".

        Используется после успешной отправки в Kafka.
        Ключ удаляется через TTL (cache_ttl).

        Args:
            event: Pydantic-модель события.
        """
        key = self._generate_key(event)

        success = await self.cache_repository.setex(key, self.cache_ttl, event)

        logger.debug(
            "Event cached",
            key=key,
            user_id=self._get_user_id(event),
            success=bool(success)
        )

    def _generate_key(self, event: BaseModel) -> str:
        """
        Генерирует уникальный ключ для события.
        """
        event_data = self._prepare_event_data(event)
        content = json.dumps(event_data, sort_keys=True)
        content_hash = self.hash_algorithm(content).hexdigest()
        return f"{self.key_prefix}:{self._get_user_id(event)}:{content_hash}"

    def _prepare_event_data(self, event: BaseModel) -> Dict[str, Any]:
        """
        Подготавливает словарь события для хэширования.

        Исключает поля, которые не влияют на семантику события:
        - id: может быть разным, даже если событие одно и то же;
        - timestamp: всегда разный;
        - session_id: зависит от сессии.

        Поддерживает Pydantic v1 (dict) и v2 (model_dump).
        """
        exclude_fields = {"id", "timestamp", "session_id"}

        if hasattr(event, "model_dump"):
            # Pydantic v2
            return event.model_dump(exclude=exclude_fields)
        else:
            raise TypeError("Unsupported event type for duplication check")

    def _get_user_id(self, event: BaseModel) -> Optional[str]:
        """
        Извлекает идентификатор пользователя.

        Возвращает строковое представление user_id.

        """
        if hasattr(event, "user_id"):
            uid = getattr(event, "user_id")
            return str(uid) if isinstance(uid, UUID) else uid
        return None