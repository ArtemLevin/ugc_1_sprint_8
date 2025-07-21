"""
Модуль для управления подключением к Kafka и отправкой сообщений.

Содержит класс `KafkaConnection`, реализующий подключение к Kafka и отправку сообщений.
"""

import json
from typing import List, Dict, Any, Optional
from aiokafka import AIOKafkaProducer
from app.core.config import settings
from app.core.logger import get_logger
from app.utils.retry import RetryHandler
from app.utils.cache import RedisService

logger = get_logger(__name__)


class KafkaConnection:
    """
    Класс для управления подключением к Kafka и отправкой сообщений.

    Attributes:
        bootstrap_server: Адрес Kafka-брокера.
        max_request_size: Максимальный размер запроса.
        producer: Активный продюсер Kafka.
        connected: Флаг, указывающий, установлено ли соединение.
    """

    def __init__(
            self,
            bootstrap_server: str | None = None,
            max_request_size: int | None = None
    ):
        """
        Инициализирует соединение с Kafka.

        Args:
            bootstrap_server: Адрес Kafka-брокера. Если не указан, используется значение из настроек.
            max_request_size: Максимальный размер запроса. Если не указан, используется значение из настроек.
        """
        self.bootstrap_server = bootstrap_server or settings.kafka.bootstrap_server
        self.max_request_size = max_request_size or settings.kafka.request_size
        self.producer: AIOKafkaProducer | None = None
        self.connected = False
        logger.info(
            "KafkaConnection initialized",
            bootstrap_server=self.bootstrap_server,
            max_request_size=self.max_request_size
        )

    @RetryHandler(max_retries=3, base_delay=1, max_delay=10)
    async def connect(self) -> None:
        """
        Подключается к Kafka и инициализирует продюсера.

        Raises:
            Exception: Если подключение к Kafka не удалось.
        """
        try:
            logger.info("Connecting to Kafka", bootstrap_server=self.bootstrap_server)
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_server,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                enable_idempotence=True,
                max_request_size=self.max_request_size
            )
            await self.producer.start()
            self.connected = True
            logger.info("Successfully connected to Kafka")
        except Exception as e:
            logger.error("Failed to connect to Kafka", error=str(e))
            self.connected = False
            raise

    async def disconnect(self) -> None:
        """
        Закрывает соединение с Kafka.
        """
        if self.producer:
            await self.producer.stop()
            self.connected = False
            logger.info("Disconnected from Kafka")

    class KafkaConnection:
        @RetryHandler(max_retries=3, base_delay=1, max_delay=10)
        async def send_batch(self, batch: List[Dict[str, Any]], topic: str) -> Any:
            if not self.connected:
                await self.connect()
            try:
                import json
                # records — список кортежей (key, value)
                records = [(None, json.dumps(msg).encode("utf-8")) for msg in batch]
                metadata = await self.producer.send_batch(topic, records)
                logger.info("Batch sent to Kafka", size=len(batch), topic=topic)
                return metadata
            except Exception as e:
                logger.error("Kafka send_batch error", error=str(e), topic=topic)
                raise

    async def send(self, message: Dict[str, Any], topic: str) -> None:
        """
        Отправляет одно сообщение в Kafka.

        Args:
            message: Сообщение для отправки.
            topic: Топик Kafka для отправки.

        Raises:
            Exception: Если отправка сообщения в Kafka не удалась.
        """
        if not self.connected:
            await self.connect()

        try:
            logger.debug("Sending message to Kafka", message_id=message.get("id"), topic=topic)
            future = await self.producer.send(
                topic,
                value=json.dumps(message).encode('utf-8')
            )
            metadata = await future
            logger.info(
                "Message sent to Kafka",
                message_id=message.get("id"),
                topic=topic,
                offset=metadata.offset
            )
        except Exception as e:
            logger.error(
                "Failed to send message to Kafka",
                error=str(e),
                message=message,
                topic=topic
            )
            raise