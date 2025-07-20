"""
Модуль для работы с Redis через асинхронный клиент.

Содержит класс `RedisService` для взаимодействия с Redis с поддержкой retry-логики и кэширования.
"""
from functools import wraps
from typing import Callable

import redis.asyncio as redis
from pydantic import BaseModel
from app.core.config import settings
from app.core.logger import get_logger
from app.utils.retry import RetryHandler
from aioredis import RedisError
import logging

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    retry_if_exception_type,
    RetryError
)

logger = get_logger(__name__)


class RedisSettings(BaseModel):
    """
    Настройки для подключения к Redis.

    Attributes:
        url: URL подключения к Redis.
        max_connections: Максимальное количество подключений.
        retry_attempts: Количество попыток при неудачном подключении.
        default_ttl: Время жизни ключей по умолчанию (в секундах).
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

    Attributes:
        settings: Настройки Redis.
        _client: Асинхронный клиент Redis.
    """

    def __init__(self, settings: RedisSettings | None = None):
        """
        Инициализирует RedisService.

        Args:
            settings: Настройки Redis. Если не переданы, используются настройки по умолчанию.
        """
        self.settings = settings or RedisSettings()
        self._client: redis.Redis | None  = None
        logger.info("RedisService initialized", redis_url=self.settings.url)

    async def __aenter__(self):
        """
        Контекстный менеджер для асинхронного подключения.
        """
        self._client = await self._create_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Контекстный менеджер для асинхронного закрытия соединения.
        """
        await self.close()

    def _get_retry_handler(self, log_level: int = logging.WARNING) -> RetryHandler:
        """
        Возвращает настроенный обработчик повторных попыток.

        Args:
            log_level: Уровень логирования для retry-логики.

        Returns:
            Обработчик повторных попыток.
        """
        return RetryHandler(
            max_retries=self.settings.retry_attempts,
            base_delay=1,
            max_delay=10,
            retryable_exceptions=(RedisError,),
            log_level=log_level
        )

    def _get_before_sleep(self, log_level: int = logging.WARNING):
        """
        Возвращает функцию для логирования перед повторной попыткой.

        Args:
            log_level: Уровень логирования.

        Returns:
            Функцию для логирования перед повторной попыткой.
        """
        return before_sleep_log(logger, log_level, exc_info=True)

    async def _create_client(self) -> redis.Redis:
        """
        Создает и проверяет подключение к Redis.

        Returns:
            Асинхронный клиент Redis.

        Raises:
            RedisError: Если не удалось подключиться к Redis.
        """
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
        """
        Возвращает активный клиент Redis.

        Returns:
            Асинхронный клиент Redis.
        """
        if not self._client:
            raise RuntimeError("Redis client is not initialized")
        return self._client

    def _get_retryable_method(self, method_name: str) -> Callable:
        """
        Возвращает метод с retry-логикой.

        Args:
            method_name: Название метода для логирования.

        Returns:
            Метод с retry-логикой.
        """
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
        """
        Возвращает метод GET с retry-логикой.
        """

        @self._get_retryable_method("GET")
        async def _get(key: str) -> bytes | None:
            """
            Получает значение по ключу.

            Args:
                key: Ключ, по которому нужно получить значение.

            Returns:
                Значение по ключу или None, если ключ не найден.
            """
            client = await self.get_client()
            return await client.get(key)

        return _get

    @property
    def set(self):
        """
        Возвращает метод SET с retry-логикой.
        """

        @self._get_retryable_method("SET")
        async def _set(key: str, value: str, ttl: int | None = None) -> bool:
            """
            Устанавливает значение по ключу с возможностью указания TTL.

            Args:
                key: Ключ, по которому нужно сохранить значение.
                value: Значение для сохранения.
                ttl: Время жизни ключа (в секундах).

            Returns:
                True, если операция успешна, иначе False.
            """
            client = await self.get_client()
            result = await client.set(key, value)
            if ttl is None:
                ttl = self.settings.default_ttl
            await client.expire(key, ttl)
            return result

        return _set

    @property
    def setex(self):
        """
        Возвращает метод SETEX с retry-логикой.
        """

        @self._get_retryable_method("SETEX")
        async def _setex(key: str, ttl: int, value: str) -> bool:
            """
            Устанавливает значение по ключу с указанием времени жизни.

            Args:
                key: Ключ, по которому нужно сохранить значение.
                ttl: Время жизни ключа (в секундах).
                value: Значение для сохранения.

            Returns:
                True, если операция успешна, иначе False.
            """
            client = await self.get_client()
            return await client.setex(key, ttl, value)

        return _setex

    @property
    def rpush(self):
        """
        Возвращает метод RPUSH с retry-логикой.
        """

        @self._get_retryable_method("RPUSH")
        async def _rpush(key: str, value: str) -> int:
            """
            Добавляет значение в список по ключу.

            Args:
                key: Ключ, по которому нужно добавить значение.
                value: Значение для добавления.

            Returns:
                Длина списка после добавления.
            """
            client = await self.get_client()
            return await client.rpush(key, value)

        return _rpush

    @property
    def lpop(self):
        """
        Возвращает метод LPOP с retry-логикой.
        """

        @self._get_retryable_method("LPOP")
        async def _lpop(key: str) -> Optional[bytes]:
            """
            Удаляет и возвращает первый элемент списка по ключу.

            Args:
                key: Ключ, из которого нужно удалить элемент.

            Returns:
                Удаленный элемент или None, если список пуст.
            """
            client = await self.get_client()
            return await client.lpop(key)

        return _lpop

    @property
    def llen(self):
        """
        Возвращает метод LLEN с retry-логикой.
        """

        @self._get_retryable_method("LLEN")
        async def _llen(key: str) -> int:
            """
            Возвращает длину списка по ключу.

            Args:
                key: Ключ, для которого нужно получить длину списка.

            Returns:
                Длина списка.
            """
            client = await self.get_client()
            return await client.llen(key)

        return _llen

    @property
    def exists(self):
        """
        Возвращает метод EXISTS с retry-логикой.
        """

        @self._get_retryable_method("EXISTS")
        async def _exists(key: str) -> bool:
            """
            Проверяет, существует ли ключ в Redis.

            Args:
                key: Ключ для проверки.

            Returns:
                True, если ключ существует, иначе False.
            """
            client = await self.get_client()
            return bool(await client.exists(key))

        return _exists

    async def close(self):
        """
        Закрывает соединение с Redis.
        """
        if self._client:
            await self._client.close()
            logger.info("Redis connection closed")
        else:
            logger.warning("Redis client was not initialized, nothing to close")