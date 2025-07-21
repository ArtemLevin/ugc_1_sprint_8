"""
Модуль для буферизации сообщений перед отправкой в Kafka.

Содержит класс `KafkaBuffer`, реализующий буферизацию и асинхронную отправку сообщений.
"""

import asyncio
from typing import List, Dict, Any, Optional
from aiokafka import AIOKafkaProducer
from app.core.config import settings
from app.core.logger import get_logger
from app.utils.cache import RedisService
from app.services.dlq_handler import DLQHandler

logger = get_logger(__name__)


class KafkaBuffer:
    """
    Класс для буферизации сообщений перед отправкой в Kafka.

    Attributes:
        max_size: Максимальный размер батча для отправки.
        flush_interval: Интервал отправки (в секундах).
        queue: Асинхронная очередь для хранения сообщений.
        task: Задача asyncio для периодической отправки.
        connection: Активное соединение с Kafka.
        topic: Топик Kafka для отправки сообщений.
        dlq_handler: Обработчик DLQ для сохранения неотправленных сообщений.
    """

    def __init__(
            self,
            max_size: int | None = None,
            flush_interval: int | None = None,
            redis_service: RedisService | None  = None
    ):
        """
        Инициализирует буфер для сообщений Kafka.

        Args:
            max_size: Максимальный размер батча. Если не указан, используется значение из настроек.
            flush_interval: Интервал отправки (в секундах). Если не указан, используется значение из настроек.
            redis_service: Сервис для работы с Redis для DLQ. Если не указан, создается новый.
        """
        self.max_size = max_size or settings.kafka.max_batch_size
        self.flush_interval = flush_interval or settings.kafka.flush_interval
        self.queue = asyncio.Queue()
        self.task = None
        self.connection: AIOKafkaProducer | None = None
        self.topic: str | None = None
        self.dlq_handler = DLQHandler(redis_service or RedisService())

        logger.info(
            "KafkaBuffer initialized",
            max_size=self.max_size,
            flush_interval=self.flush_interval
        )

    async def start(self, connection: "KafkaConnection", topic: str):
        self.connection = connection
        self.topic = topic
        self.task = asyncio.create_task(self._flush_loop())

    async def stop(self):
        """
        Останавливает цикл отправки сообщений и отправляет оставшиеся сообщения.
        """
        if self.task:
            self.task.cancel()
        await self._flush_queue()
        logger.info("KafkaBuffer stopped")

    async def _flush_loop(self):
        """
        Основной цикл отправки сообщений.
        """
        logger.info("Starting Kafka buffer flush loop")
        while True:
            try:
                batch = await self._collect_batch()
                if batch:
                    await self._send_batch(batch)
                await asyncio.sleep(self.flush_interval)
            except asyncio.CancelledError:
                logger.info("Flushing remaining messages before shutdown")
                await self._flush_queue()
                break
            except Exception as e:
                logger.error("Error in Kafka buffer loop", error=str(e))
                await asyncio.sleep(5)

    async def _collect_batch(self) -> List[Dict[str, Any]]:
        """
        Собирает сообщения из очереди в батч.

        Returns:
            Список сообщений для отправки.
        """
        batch = []
        while not self.queue.empty() and len(batch) < self.max_size:
            batch.append(await self.queue.get())
        return batch

    async def _send_batch(self, batch: List[Dict[str, Any]]):
        try:
            await self.connection.send_batch(batch, self.topic)
            logger.info("Batch sent to Kafka", size=len(batch), topic=self.topic)
        except Exception as e:
            logger.warning("Failed to send batch to Kafka", error=str(e), size=len(batch))
            await self.dlq_handler.save_messages(batch)

    async def _flush_queue(self):
        """
        Отправляет все оставшиеся сообщения в очереди.
        """
        batch = []
        while not self.queue.empty():
            batch.append(await self.queue.get())

        if not batch:
            return

        try:
            await self.connection.send_batch(batch, self.topic)
            logger.info("Remaining messages sent to Kafka", size=len(batch))
        except Exception as e:
            logger.warning(
                "Failed to send remaining messages to Kafka",
                error=str(e)
            )
            await self.dlq_handler.save_messages(batch)

    async def send(self, message: Dict[str, Any]):
        """
        Добавляет сообщение в очередь на отправку.

        Args:
            message: Сообщение для добавления.
        """
        try:
            await self.queue.put(message)
            logger.debug("Message added to buffer", message_id=message.get("id"))
        except asyncio.QueueFull:
            logger.error("Message queue is full, dropping message", message=message)
            await self.dlq_handler.save_messages([message])