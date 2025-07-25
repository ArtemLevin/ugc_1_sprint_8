"""
Redis буфер для временного хранения событий.

Используется для буферизации сообщений перед отправкой в ClickHouse.
"""

import redis.asyncio as redis
from typing import List
from ..core.config import config
from ..core.logger import logger


class RedisBuffer:
    """
    Асинхронный Redis буфер для хранения событий.

    Обеспечивает буферизацию сообщений перед отправкой в ClickHouse.
    """

    def __init__(self):
        """Инициализирует Redis клиент."""
        self.client = redis.Redis(
            host=config.redis.host,
            port=config.redis.port,
            db=config.redis.db,
            decode_responses=False,  # Для бинарных данных
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True
        )
        logger.info("Redis buffer initialized",
                    host=config.redis.host,
                    port=config.redis.port,
                    db=config.redis.db)

    async def push(self, key: str, data: str) -> bool:
        """
        Добавляет данные в Redis список (LPUSH).

        Args:
            key: Ключ списка в Redis
             Данные для добавления

        Returns:
            bool: True если успешно, False при ошибке
        """
        try:
            await self.client.lpush(key, data.encode('utf-8'))
            logger.debug("Data pushed to Redis buffer", key=key)
            return True
        except Exception as e:
            logger.error("Failed to push data to Redis",
                         key=key,
                         error=str(e))
            return False

    async def pop_batch(self, key: str, batch_size: int) -> List[str]:
        """
        Извлекает пакет данных из Redis списка.

        Args:
            key: Ключ списка в Redis
            batch_size: Размер пакета

        Returns:
            List[str]: Список извлеченных данных
        """
        try:
            # Извлекаем пакет данных атомарно
            pipe = self.client.pipeline()

            for _ in range(batch_size):
                pipe.lpop(key)

            results = await pipe.execute()

            # Фильтруем None значения и декодируем
            batch = []
            for item in results:
                if item is not None:
                    try:
                        batch.append(item.decode('utf-8'))
                    except UnicodeDecodeError:
                        logger.warning("Failed to decode Redis item", item=item)
                        continue

            logger.debug("Batch popped from Redis", key=key, count=len(batch))
            return batch

        except Exception as e:
            logger.error("Failed to pop batch from Redis",
                         key=key,
                         batch_size=batch_size,
                         error=str(e))
            return []

    async def get_length(self, key: str) -> int:
        """
        Получает количество элементов в списке.

        Args:
            key: Ключ списка в Redis

        Returns:
            int: Количество элементов
        """
        try:
            length = await self.client.llen(key)
            return length
        except Exception as e:
            logger.error("Failed to get Redis list length",
                         key=key,
                         error=str(e))
            return 0

    async def test_connection(self) -> bool:
        """
        Проверяет соединение с Redis.

        Returns:
            bool: True если соединение успешно
        """
        try:
            await self.client.ping()
            return True
        except Exception as e:
            logger.error("Redis connection failed", error=str(e))
            return False

    async def clear_key(self, key: str) -> bool:
        """
        Очищает ключ в Redis.

        Args:
            key: Ключ для очистки

        Returns:
            bool: True если успешно
        """
        try:
            await self.client.delete(key)
            logger.info("Redis key cleared", key=key)
            return True
        except Exception as e:
            logger.error("Failed to clear Redis key", key=key, error=str(e))
            return False