import redis.asyncio as redis

from app.core.config import settings
from app.core.logger import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential

from app.utils.retry import RetryHandler

logger = get_logger(__name__)

retry_handler = RetryHandler(max_retries=3, base_delay=1, max_delay=10)

class RedisService:
    def __init__(self, url=None, max_connections=10, max_retries=3):
        self.url = url or settings.redis.redis_url
        self.max_connections = max_connections or settings.redis.redis_max_connections
        self.max_retries = max_retries or settings.redis.redis_retry_attempts
        self._client = None

    @retry_handler
    async def get_client(self):
        if not self._client:
            try:
                self._client = redis.from_url(self.url, max_connections=self.max_connections)
                await self._client.ping()
            except Exception as e:
                logger.error("Failed to connect to Redis", error=str(e))
                raise
        return self._client

    async def get(self, key):
        client = await self.get_client()
        return await client.get(key)

    async def set(self, key, value):
        client = await self.get_client()
        return await client.set(key, value)

    async def setex(self, key, ttl, value):
        client = await self.get_client()
        return await client.setex(key, ttl, value)

    async def rpush(self, key, value):
        client = await self.get_client()
        return await client.rpush(key, value)

    async def lpop(self, key):
        client = await self.get_client()
        return await client.lpop(key)

    async def llen(self, key):
        client = await self.get_client()
        return await client.llen(key)

    async def close(self):
        if self._client:
            await self._client.close()