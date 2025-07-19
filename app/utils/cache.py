import redis.asyncio as redis
from app.core.config import settings
from app.core.logger import get_logger

logger = get_logger(__name__)

class RedisPool:
    def __init__(self):
        self._client = None

    async def get_client(self):
        if not self._client:
            try:
                self._client = redis.from_url(
                    str(settings.redis_url),
                    max_connections=settings.redis_max_connections,
                    socket_timeout=5
                )
                await self._client.ping()
            except Exception as e:
                logger.error("Failed to connect to Redis", error=str(e))
                raise
        return self._client