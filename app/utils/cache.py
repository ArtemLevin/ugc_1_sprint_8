from typing import Optional
from redis.asyncio import Redis
from redis.exceptions import RedisError
from app.core.config import settings


class RedisService:
    """
    Асинхронный сервис работы с Redis на базе redis-py v5+ (redis.asyncio).
    """

    def __init__(self):
        self._client: Optional[Redis] = None

    async def get_client(self) -> Redis:
        """
        Ленивая инициализация клиента.
        """
        if self._client is None:
            # settings.redis.url, e.g. "redis://cache:6379"
            # max_connections — число коннектов в пуле
            self._client = Redis.from_url(
                settings.redis.url,
                max_connections=settings.redis.max_connections,
                decode_responses=False  # если вам нужен байтовый ответ
            )
        return self._client

    async def close(self) -> None:
        """
        Закрывает соединение/пул.
        """
        if self._client:
            await self._client.close()

    async def get(self, key: str) -> Optional[str]:
        try:
            client = await self.get_client()
            return await client.get(key)
        except RedisError as e:
            # логируем или пробрасываем дальше
            raise

    async def set(self, key: str, value: str, ttl: Optional[int] = None) -> bool:
        try:
            client = await self.get_client()
            if ttl:
                return await client.setex(key, ttl, value)
            return await client.set(key, value)
        except RedisError as e:
            raise

    async def setex(self, key: str, ttl: int, value: str) -> bool:
        try:
            client = await self.get_client()
            return await client.setex(key, ttl, value)
        except RedisError as e:
            raise

    async def rpush(self, key: str, value: str) -> int:
        try:
            client = await self.get_client()
            return await client.rpush(key, value)
        except RedisError as e:
            raise

    async def lpop(self, key: str) -> Optional[str]:
        try:
            client = await self.get_client()
            return await client.lpop(key)
        except RedisError as e:
            raise

    async def llen(self, key: str) -> int:
        try:
            client = await self.get_client()
            return await client.llen(key)
        except RedisError as e:
            raise

    async def exists(self, key: str) -> bool:
        try:
            client = await self.get_client()
            return await client.exists(key) == 1
        except RedisError as e:
            raise