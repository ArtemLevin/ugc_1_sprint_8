"""
Модуль для работы с Redis через асинхронный клиент.
Содержит класс `RedisService` для взаимодействия с Redis с поддержкой retry-логики и кэширования.
"""
from functools import wraps
from typing import Callable
import redis
from pydantic import BaseModel
from app.core.config import settings
from app.core.logger import get_logger
from app.utils.retry import RetryHandler
from aioredis import RedisError
import logging
from tenacity import before_sleep_log, RetryError

logger = get_logger(__name__)


class RedisSettings(BaseModel):
    """
    Настройки для подключения к Redis.
    """
    url: str = settings.redis.url
    max_connections: int = settings.redis.max_connections
    retry_attempts: int = settings.redis.retry_attempts
    default_ttl: int = 3600  # 1 hour

    class Config:
        arbitrary_types_allowed = True


class RedisService:
    """
    Класс для работы с Redis через асинхронный клиент.
    Оставлены только используемые методы: get, set, setex.
    """
    def __init__(self, settings: RedisSettings | None = None):
        self.settings = settings or RedisSettings()
        self._client: redis.Redis | None = None
        logger.info("RedisService initialized", redis_url=self.settings.url)

    async def __aenter__(self):
        self._client = await self._create_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def _get_retry_handler(self, log_level: int = logging.WARNING) -> RetryHandler:
        return RetryHandler(
            max_retries=self.settings.retry_attempts,
            base_delay=1,
            max_delay=10,
            retryable_exceptions=(RedisError,),
            log_level=log_level
        )

    def _get_before_sleep(self, log_level: int = logging.WARNING):
        return before_sleep_log(logger, log_level, exc_info=True)

    async def _create_client(self) -> redis.Redis:
        if self._client:
            return self._client
        try:
            self._client = redis.from_url(
                self.settings.url,
                max_connections=self.settings.max_connections
            )
            await self._client.ping()
            logger.info("Connected to Redis", redis_url=self.settings.url)
            return self._client
        except RedisError as e:
            logger.error("Failed to connect to Redis", error=str(e))
            raise
        except Exception as e:
            logger.error("Unexpected error during Redis connection", error=str(e))
            raise

    def get_client(self) -> redis.Redis:
        if not self._client:
            raise RuntimeError("Redis client is not initialized")
        return self._client

    def _get_retryable_method(self, method_name: str) -> Callable:
        retry_handler = self._get_retry_handler()
        def decorator(async_func):
            @retry_handler
            @wraps(async_func)
            async def wrapper(*args, **kwargs):
                result = await async_func(*args, **kwargs)
                logger.debug(f"Redis {method_name}", *args, **kwargs, result=bool(result))
                return result
            return wrapper
        return decorator

    @property
    def get(self):
        @self._get_retryable_method("GET")
        async def _get(key: str) -> bytes | None:
            client = await self.get_client()
            return await client.get(key)
        return _get

    @property
    def set(self):
        @self._get_retryable_method("SET")
        async def _set(key: str, value: str, ttl: int | None = None) -> bool:
            client = await self.get_client()
            result = await client.set(key, value)
            if ttl is None:
                ttl = self.settings.default_ttl
            await client.expire(key, ttl)
            return result
        return _set

    @property
    def setex(self):
        @self._get_retryable_method("SETEX")
        async def _setex(key: str, ttl: int, value: str) -> bool:
            client = await self.get_client()
            return await client.setex(key, ttl, value)
        return _setex

    async def close(self):
        if self._client:
            await self._client.close()
            logger.info("Redis connection closed")
        else:
            logger.warning("Redis client was not initialized, nothing to close")