"""
Dead Letter Queue (DLQ) для обработки ошибочных сообщений.

Сохраняет сообщения, которые не удалось обработать, в Redis.
"""

from .buffer import RedisBuffer
from ..core.config import config
from ..core.logger import logger
from typing import List


class DLQHandler(RedisBuffer):
    """
    Обработчик Dead Letter Queue для Redis.

    Сохраняет сообщения, которые не удалось обработать.
    """

    def __init__(self):
        """Инициализирует DLQ обработчик."""
        super().__init__()
        self.dlq_key = config.redis.dlq_key
        logger.info("DLQ handler initialized", key=self.dlq_key)

    async def send_to_dlq(self, data: str) -> bool:
        """
        Отправляет данные в Dead Letter Queue.

        Args:
             Данные для отправки в DLQ

        Returns:
            bool: True если успешно, False при ошибке
        """
        try:
            result = await self.push(self.dlq_key, data)
            if result:
                logger.warning("Message sent to DLQ", key=self.dlq_key)
            return result
        except Exception as e:
            logger.error("Failed to send message to DLQ",
                         key=self.dlq_key,
                         error=str(e))
            return False

    async def get_dlq_length(self) -> int:
        """
        Получает количество сообщений в DLQ.

        Returns:
            int: Количество сообщений в очереди
        """
        return await self.get_length(self.dlq_key)

    async def reprocess_dlq(self, max_messages: int = 100) -> int:
        """
        Перемещает сообщения из DLQ обратно в основную очередь.

        Args:
            max_messages: Максимальное количество сообщений для перемещения

        Returns:
            int: Количество перемещенных сообщений
        """
        try:
            moved_count = 0
            for _ in range(max_messages):
                message = await self.client.lpop(self.dlq_key)
                if message is None:
                    break

                # Перемещаем в основную очередь
                await self.push(config.redis.events_key, message.decode('utf-8'))
                moved_count += 1

            logger.info("Messages reprocessed from DLQ", count=moved_count)
            return moved_count

        except Exception as e:
            logger.error("Failed to reprocess DLQ messages", error=str(e))
            return 0

    async def get_dlq_messages(self, count: int = 10) -> List[str]:
        """
        Получает сообщения из DLQ для инспекции.

        Args:
            count: Количество сообщений для получения

        Returns:
            List[str]: Список сообщений из DLQ
        """
        try:
            # Получаем сообщения без их удаления
            messages = await self.client.lrange(self.dlq_key, 0, count - 1)
            return [msg.decode('utf-8') for msg in messages]
        except Exception as e:
            logger.error("Failed to get DLQ messages", error=str(e))
            return []