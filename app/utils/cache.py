import redis.asyncio as redis
from app.core.config import settings
from app.core.logger import get_logger

logger = get_logger(__name__)

class RedisPool:
    def __init__(self):
        self._client = None

    async def get_client(self):
        if not self._client:
            self._client = redis.from_url(str(settings.redis_url), max_connections=settings.redis_max_connections)
        return self._client