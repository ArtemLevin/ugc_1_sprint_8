import hashlib
import json
from app.core.logger import get_logger

logger = get_logger(__name__)

class DuplicateChecker:
    def __init__(self, redis_service, cache_ttl=3600):
        self.redis_service = redis_service
        self.cache_ttl = cache_ttl

    async def is_duplicate(self, event):
        key = self._generate_key(event)
        return bool(await self.redis_service.get(key))

    async def cache_event(self, event):
        key = self._generate_key(event)
        await self.redis_service.setex(key, self.cache_ttl, "processed")

    def _generate_key(self, event):
        content_hash = hashlib.sha256(json.dumps(event.model_dump()).encode()).hexdigest()
        return f"user_actions:{event.user_id}:{content_hash}"