"""
Модуль для асинхронной отправки сообщений в Kafka с буферизацией.

Содержит класс `BufferedKafkaProducer`, реализующий буферизацию и отправку сообщений в Kafka.
"""

from aiokafka import AIOKafkaProducer
from typing import Optional, Dict, Any
from aiokafka.errors import KafkaError
from app.core.config import settings
from app.core.logger import get_logger
from app.utils.cache import RedisService
from app.services.dlq_handler import DLQHandler
from app.services.kafka_connection import KafkaConnection
from app.services.kafka_buffer import KafkaBuffer
import asyncio

logger = get_logger(__name__)


class BufferedKafkaProducer:
    """
    Класс для асинхронной отправки сообщений в Kafka с буферизацией.

    Attributes:
        bootstrap_server: Адрес Kafka-брокера.
        topic: Топик Kafka для отправки сообщений.
        buffer_size: Максимальный размер батча.
        flush_interval: Интервал отправки (в секундах).
        send_timeout: Таймаут для отправки сообщения.
        connection: Активное соединение с Kafka.
        buffer: Буфер для сообщений.
        redis_service: Сервис для работы с Redis.
        dlq_handler: Обработчик DLQ для сохранения неотправленных сообщений.
        dlq_retry_task: Задача asyncio для автоматического ретрая сообщений.
    """

    def __init__(
            self,
            bootstrap_server: str | None = None,
            topic: str | None = None,
            buffer_size: int | None = None,
            flush_interval: int | None = None,
            send_timeout: int | None = None,
            redis_service: RedisService | None = None
    ):
        """
        Инициализирует BufferedKafkaProducer.

        Args:
            bootstrap_server: Адрес Kafka-брокера. Если не указан, используется значение из настроек.
            topic: Топик Kafka для отправки. Если не указан, используется значение из настроек.
            buffer_size: Максимальный размер батча. Если не указан, используется значение из настроек.
            flush_interval: Интервал отправки (в секундах). Если не указан, используется значение из настроек.
            send_timeout: Таймаут для отправки сообщения. Если не указан, используется значение из настроек.
            redis_service: Сервис для работы с Redis. Если не указан, создается новый.
        """
        self.bootstrap_server = bootstrap_server or settings.kafka.bootstrap_server
        self.topic = topic or settings.kafka.topic
        self.buffer_size = buffer_size or settings.kafka.max_batch_size
        self.flush_interval = flush_interval or settings.kafka.flush_interval
        self.send_timeout = send_timeout or settings.kafka.send_timeout
        self.connection = KafkaConnection(
            bootstrap_server=self.bootstrap_server,
            max_request_size=settings.kafka.request_size
        )
        self.buffer = KafkaBuffer(
            max_size=self.buffer_size,
            flush_interval=self.flush_interval,
            redis_service=redis_service
        )
        self.redis_service = redis_service or RedisService()
        self.dlq_handler = DLQHandler(
            redis_service=self.redis_service,
            queue_key=settings.dlq.dlq_queue_key
        )
        self.dlq_retry_task = None
        logger.info(
            "BufferedKafkaProducer initialized",
            bootstrap_server=self.bootstrap_server,
            topic=self.topic,
            buffer_size=self.buffer_size,
            flush_interval=self.flush_interval
        )

    async def start(self):
        """
        Запускает продюсера и буфер для отправки сообщений.
        """
        logger.info("Starting Kafka producer")
        await self.connection.connect()
        await self.buffer.start(self.connection.producer, self.topic)
        logger.info("Starting DLQ retry task")
        self.dlq_retry_task = asyncio.create_task(self._dlq_retry_loop())

    async def stop(self):
        """
        Останавливает продюсера и очищает ресурсы.
        """
        logger.info("Stopping Kafka producer")
        if self.dlq_retry_task:
            self.dlq_retry_task.cancel()
        await self.buffer.stop()
        await self.connection.disconnect()
        logger.info("Kafka producer stopped")

    async def _dlq_retry_loop(self, interval: int = 60):
        """
        Основной цикл для повторной отправки сообщений из DLQ.

        Args:
            interval: Интервал между попытками (в секундах).
        """
        logger.info("DLQ retry loop started", interval=interval)
        while True:
            try:
                await self.dlq_handler.retry_messages()
            except Exception as e:
                logger.error("DLQ retry task failed", error=str(e))
            await asyncio.sleep(interval)

    async def send(self, message: Dict[str, Any]):
        """
        Добавляет сообщение в буфер для асинхронной отправки.

        Args:
            message: Сообщение для отправки.
        """
        logger.debug("Adding message to buffer", message_id=message.get("id"))
        try:
            await self.buffer.send(message)
        except Exception as e:
            logger.warning("Failed to add message to buffer", error=str(e))
            await self.dlq_handler.save_messages([message])

    async def send_immediately(self, message: Dict[str, Any]):
        """
        Отправляет сообщение в Kafka немедленно, минуя буфер.

        Args:
            message: Сообщение для отправки.
        """
        logger.debug("Sending message immediately", message_id=message.get("id"))
        try:
            await self.connection.producer.send(self.topic, value=message)
        except KafkaError as e:
            logger.error("Immediate Kafka send failed", error=str(e))
            await self.dlq_handler.save_messages([message])
        except Exception as e:
            logger.error("Unexpected error during immediate send", error=str(e))
            await self.dlq_handler.save_messages([message])