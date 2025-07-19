import redis.asyncio as redis
from app.core.logger import get_logger

logger = get_logger(__name__)

class RedisPool:
    def __init__(self, url="redis://redis:6379", max_connections=10):
        self.url = url
        self.max_connections = max_connections
        self._client = None

    async def get_client(self):
        if not self._client:
            try:
                self._client = redis.from_url(self.url, max_connections=self.max_connections)
                await self._client.ping()
            except Exception as e:
                logger.error("Failed to connect to Redis", error=str(e))
                raise
        return self._client

    async def close(self):
        if self._client:
            await self._client.close()