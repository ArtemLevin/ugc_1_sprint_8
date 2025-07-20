import xxhash
import json
from typing import Any
from app.core.logger import get_logger
from app.utils.cache import RedisService

logger = get_logger(__name__)

class DuplicateChecker:
    def __init__(self, redis_service: RedisService, cache_ttl: int = 3600):
        self.redis_service = redis_service
        self.cache_ttl = cache_ttl

    async def is_duplicate(self, event: Any) -> bool:
        key = self._generate_key(event)
        result = await self.redis_service.get(key)
        logger.debug("Duplicate check", key=key, is_duplicate=bool(result))
        return bool(result)

    async def cache_event(self, event: Any) -> None:
        key = self._generate_key(event)
        await self.redis_service.setex(key, self.cache_ttl, "processed")
        logger.debug("Event cached", key=key)

    def _generate_key(self, event: Any) -> str:
        content = json.dumps(event.model_dump(), sort_keys=True)
        content_hash = xxhash.xxh64(content).hexdigest()
        return f"user_actions:{event.user_id}:{content_hash}"