"""
Модуль для реализации рейт-лимита с использованием Redis и Lua-скриптов.
Содержит абстрактный класс `RateLimiter` и его реализацию `RedisLeakyBucketRateLimiter`.
"""

import asyncio
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable
from app_v_1.core.config import settings
from app_v_1.core.logger import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from app_v_1.utils.retry import RetryHandler
from app_v_1.utils.cache import RedisService
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
                        Обычно формируется как "user_id:event_type".

        Returns:
            True, если запрос разрешен, иначе False.
        """
        pass


class RedisLeakyBucketRateLimiter(RateLimiter):
    """
    Реализация рейт-лимитера на основе Redis и алгоритма "Leaky Bucket".
    Использует Lua-скрипт для атомарной проверки и обновления состояния.

    Алгоритм "Leaky Bucket":
    - "Ведро" имеет ограниченную ёмкость (capacity).
    - Скорость "утечки" — это rate (запросов в окно).
    - При каждом запросе проверяется, влезет ли он в ведро.
    - Если ведро переполнено — запрос отклоняется.

    Это более плавкий алгоритм, чем "Token Bucket", и лучше справляется с всплесками.
    """

    def __init__(
            self,
            redis_service: RedisService,
            rate: int | None = None,
            capacity: int | None = None,
            window: int | None = None
    ):
        """
        Инициализация рейт-лимитера.

        Args:
            redis_service: Сервис для работы с Redis (DI).
            rate: Скорость запросов (в req/sec). По умолчанию берётся из settings.
            capacity: Максимальная вместимость "ведра". По умолчанию = rate.
            window: Временное окно (в секундах). По умолчанию берётся из settings.

        """
        self.redis_service = redis_service
        self.rate = rate or settings.rate_limit.rate_limit
        self.capacity = capacity or settings.rate_limit.rate_limit
        self.window = window or settings.rate_limit.window_seconds

        # SHA-хэш загруженного Lua-скрипта. Нужен для быстрого вызова через evalsha.
        self.script_sha = None
        # Флаг, что скрипт уже загружен. Предотвращает повторную загрузку.
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
            Exception: Если не удалось прочитать файл (например, отсутствует или нет прав).
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

        RetryHandler — это кастомный декоратор на базе tenacity, позволяющий
        гибко настраивать retry-логику (макс. попытки, задержка, типы исключений).

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
        Возвращает функцию с настроенной retry-логикой для загрузки Lua-скрипта.

        Используется как property, чтобы каждый вызов возвращал новую обёрнутую функцию.
        Это позволяет использовать retry на уровне вызова, а не на уровне метода.

        Почему не загружать при старте?
        - Позволяет отложить загрузку до первого запроса (lazy).
        - Уменьшает время старта приложения.
        - Позволяет переподключиться, если Redis был недоступен.

        Returns:
            Функцию с retry-логикой.
        """
        retry_handler = self._get_retry_handler()

        @retry_handler
        async def _load_script():
            """
            Загружает Lua-скрипт в Redis.
            Используется script_load — возвращает SHA-хэш, который потом используется в evalsha.
            Это эффективнее, чем вызывать eval() каждый раз.
            """
            if not self.script_loaded:
                client = await self.redis_service.get_client()
                # script_load — отправляет скрипт в Redis и возвращает его SHA
                self.script_sha = await client.script_load(self.lua_script)
                self.script_loaded = True

        return _load_script

    async def allow_request(self, identifier: str) -> bool:
        """
        Проверяет, разрешено ли выполнение запроса.

        Args:
            identifier: Уникальный идентификатор (например, "user_id:event_type").

        Returns:
            True, если запрос разрешен, иначе False.

        Логика:
        1. Загружает Lua-скрипт (если ещё не загружен).
        2. Генерирует ключ в Redis: rate_limit:{identifier}.
        3. Выполняет Lua-скрипт через evalsha (с retry).
        4. В случае ошибки Redis — переходит в fallback.

        """
        await self.load_script()

        # Ключ в Redis: rate_limit:{user_id}:{event_type}
        key = f"rate_limit:{identifier}"

        # Текущее время в миллисекундах
        now = asyncio.get_event_loop().time() * 1000

        window_ms = self.window * 1000

        try:
            # Выполняем Lua-скрипт с retry-логикой
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

        Почему не использовать eval() напрямую?
        - evalsha() работает быстрее, так как Redis кэширует скрипт по SHA.
        - Это критично для high-load.

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
                Результат выполнения скрипта (1 — разрешить, 0 — отклонить).
            """
            client = await self.redis_service.get_client()

            # Выполняем скрипт по SHA (предварительно загружен через script_load)
            result = await client.evalsha(
                self.script_sha,
                keys=[key],
                args=[self.rate, self.capacity, now, window]
            )
            return int(result)

        return _execute_script

    # Свойство, чтобы вызывать _get_execute_script() каждый раз
    # Это позволяет иметь разные экземпляры retry-логики
    execute_script = property(_get_execute_script)

    async def _fallback_rate_limit(self, identifier: str) -> bool:
        """
        Резервная реализация рейт-лимита, используемая при сбое Redis.

        Возвращает True — разрешает запрос.

        Почему не блокировать?
        - Это fail-open поведение.
        - Лучше пропустить несколько запросов, чем уронить сервис.
        - В production можно добавить in-memory fallback (например, TokenBucket).

        Args:
            identifier: Уникальный идентификатор для проверки лимита.

        Returns:
            True (разрешает запрос).
        """
        logger.warning("Fallback rate limit", identifier=identifier)
        return True