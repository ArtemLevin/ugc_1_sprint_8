"""
Модуль для реализации рейт-лимита с использованием Redis и Lua-скриптов.

Содержит абстрактный класс `RateLimiter` и его реализацию `RedisLeakyBucketRateLimiter`.
"""

import asyncio
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Callable
from app.core.config import settings
from app.core.logger import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from app.utils.retry import RetryHandler
from app.utils.cache import RedisService
from redis.exceptions import RedisError
from redis.asyncio import Redis

logger = get_logger(__name__)
LUA_SCRIPT_PATH = Path(__file__).parent.parent / "utils" / "lua" / "rate_limiting.lua"


class RateLimiter(ABC):
    """
    Абстрактный класс для реализации рейт-лимитера.

    Все реализации рейт-лимитера должны наследоваться от этого класса.
    """

    @abstractmethod
    async def allow_request(self, identifier: str) -> bool:
        """
        Проверяет, разрешен ли запрос для данного идентификатора.

        Args:
            identifier: Уникальный идентификатор для проверки лимита.

        Returns:
            True, если запрос разрешен, иначе False.
        """
        pass


class RedisLeakyBucketRateLimiter(RateLimiter):
    """
    Реализация рейт-лимитера на основе Redis и алгоритма "Leaky Bucket".

    Attributes:
        redis_service: Сервис для работы с Redis.
        rate: Скорость, с которой можно делать запросы (в req/sec).
        capacity: Максимальная вместимость "ведра".
        window: Временное окно для проверки лимита (в секундах).
        script_sha: SHA-хэш загруженного Lua-скрипта.
        script_loaded: Флаг, указывающий, загружен ли скрипт в Redis.
        lua_script: Содержимое Lua-скрипта.
    """

    def __init__(
            self,
            redis_service: RedisService,
            rate: int | None = None,
            capacity: int | None = None,
            window: int | None = None
    ):
        """
        Инициализирует RedisLeakyBucketRateLimiter.

        Args:
            redis_service: Сервис для работы с Redis.
            rate: Скорость, с которой можно делать запросы (в req/sec).
            capacity: Максимальная вместимость "ведра".
            window: Временное окно для проверки лимита (в секундах).
        """
        self.redis_service = redis_service
        self.rate = rate or settings.rate_limit.rate_limit
        self.capacity = capacity or settings.rate_limit.rate_limit
        self.window = window or settings.rate_limit.window_seconds
        self.script_sha = None
        self.script_loaded = False
        self.lua_script = self._read_script()
        logger.info(
            "RedisLeakyBucketRateLimiter initialized",
            rate=self.rate,
            capacity=self.capacity,
            window=self.window
        )

    def _read_script(self) -> str:
        """
        Читает Lua-скрипт из файла.

        Returns:
            Содержимое Lua-скрипта.

        Raises:
            Exception: Если не удалось прочитать файл.
        """
        try:
            with open(LUA_SCRIPT_PATH, 'r') as f:
                return f.read()
        except Exception as e:
            logger.error("Failed to read Lua script", error=str(e))
            raise

    def _get_retry_handler(self) -> RetryHandler:
        """
        Возвращает настроенный обработчик повторных попыток.

        Returns:
            Обработчик повторных попыток.
        """
        return RetryHandler(
            max_retries=3,
            base_delay=1,
            max_delay=10,
            retryable_exceptions=(RedisError,)
        )

    @property
    def load_script(self) -> Callable:
        """
        Возвращает функцию с настроенными retry-попытками.

        Returns:
            Функцию с retry-логикой.
        """
        retry_handler = self._get_retry_handler()

        @retry_handler
        async def _load_script():
            """
            Загружает Lua-скрипт в Redis.
            """
            if not self.script_loaded:
                client = await self.redis_service.get_client()
                self.script_sha = await client.script_load(self.lua_script)
                self.script_loaded = True

        return _load_script

    async def allow_request(self, identifier: str) -> bool:
        """
        Проверяет, разрешено ли выполнение запроса.

        Args:
            identifier: Уникальный идентификатор для проверки лимита.

        Returns:
            True, если запрос разрешен, иначе False.
        """
        await self.load_script()
        key = f"rate_limit:{identifier}"
        now = asyncio.get_event_loop().time() * 1000  # Текущее время в миллисекундах
        window_ms = self.window * 1000  # Окно в миллисекундах

        try:
            result = await self._execute_script(key, now, window_ms)
            logger.debug("Rate limit check", identifier=identifier, allowed=bool(int(result)))
            return bool(int(result))
        except RedisError as e:
            logger.warning("Redis error during rate limit check", error=str(e), identifier=identifier)
            return await self._fallback_rate_limit(identifier)
        except Exception as e:
            logger.error("Unexpected error during rate limit check", error=str(e), identifier=identifier)
            return await self._fallback_rate_limit(identifier)

    def _get_execute_script(self) -> Callable:
        """
        Возвращает функцию с retry-логикой для выполнения Lua-скрипта.

        Returns:
            Функцию с retry-логикой.
        """
        retry_handler = self._get_retry_handler()

        @retry_handler
        async def _execute_script(key: str, now: float, window: float) -> int:
            """
            Выполняет Lua-скрипт для проверки лимита.

            Args:
                key: Ключ Redis для идентификатора.
                now: Текущее время в миллисекундах.
                window: Окно проверки в миллисекундах.

            Returns:
                Результат выполнения скрипта.
            """
            client = await self.redis_service.get_client()
            result = await client.evalsha(
                self.script_sha,
                keys=[key],
                args=[self.rate, self.capacity, now, window]
            )
            return int(result)

        return _execute_script

    execute_script = property(_get_execute_script)

    async def _fallback_rate_limit(self, identifier: str) -> bool:
        """
        Резервная реализация рейт-лимита, используемая при сбое Redis.

        Args:
            identifier: Уникальный идентификатор для проверки лимита.

        Returns:
            True (разрешает запрос).
        """
        logger.warning("Fallback rate limit", identifier=identifier)
        return True