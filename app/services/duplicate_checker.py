import xxhash
import json
from typing import Any, Optional, Dict
from uuid import UUID
from pydantic import BaseModel
from app.core.logger import get_logger
from app.repositories.repository import CacheRepository

logger = get_logger(__name__)

class DuplicateChecker:
    """
    Сервис для проверки дубликатов событий через CacheRepository.

    Attributes:
        cache_repository: абстракция репозитория для кэширования события.
        cache_ttl: Время жизни ключа в Redis (в секундах).
        hash_algorithm: Алгоритм хэширования для генерации ключа.
    """

    DEFAULT_KEY_PREFIX = "user_actions"
    DEFAULT_HASH_ALGORITHM = xxhash.xxh64

    def __init__(
        self,
        cache_repository: CacheRepository[BaseModel],
        cache_ttl: int = 3600,
        hash_algorithm: Any = xxhash.xxh64
    ):
        self.cache_repository = cache_repository
        self.cache_ttl = cache_ttl
        self.hash_algorithm = hash_algorithm
        logger.info(
            "DuplicateChecker initialized",
            cache_ttl=cache_ttl,
            hash_algorithm=hash_algorithm.__name__
        )

    async def is_duplicate(self, event: BaseModel) -> bool:
        """
        Проверяет, является ли событие дубликатом по ключу.
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
        Кэширует событие в репозитории.
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
        return f"{self.DEFAULT_KEY_PREFIX}:{self._get_user_id(event)}:{content_hash}"

    def _prepare_event_data(self, event: BaseModel) -> Dict[str, Any]:
        """
        Подготавливает словарь события для хэширования.
        """
        exclude_fields = {"id", "timestamp", "session_id"}
        if hasattr(event, "model_dump"):
            return event.model_dump(exclude=exclude_fields)
        elif hasattr(event, "dict"):
            return event.dict(exclude=exclude_fields)
        else:
            raise TypeError("Unsupported event type for duplication check")

    def _get_user_id(self, event: BaseModel) -> Optional[str]:
        """
        Извлекает идентификатор пользователя.
        """
        if hasattr(event, "user_id"):
            uid = getattr(event, "user_id")
            return str(uid) if isinstance(uid, UUID) else uid
        return None