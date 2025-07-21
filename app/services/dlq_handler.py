"""
Модуль для работы с Dead Letter Queue (DLQ).

Содержит классы и функции для сохранения и повторной отправки сообщений,
которые не удалось обработать или отправить.
"""

import json
import asyncio
from typing import List, Dict, Any, Optional, Callable
from aiokafka import AIOKafkaProducer
from app.core.config import settings
from app.core.logger import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from app.utils.retry import RetryHandler
from app.utils.cache import RedisService

logger = get_logger(__name__)

DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY = 1  # в секундах
DEFAULT_RETRY_MAX_DELAY = 10  # в секундах
DEFAULT_QUEUE_KEY = settings.dlq.dlq_queue_key
DEFAULT_KAFKA_TOPIC = settings.kafka.topic
DEFAULT_KAFKA_BOOTSTRAP_SERVER = settings.kafka.bootstrap_server


class DLQMessage:
    """
    Класс для представления сообщения в DLQ.

    Attributes:
         Словарь с данными сообщения.
    """

    def __init__(self, data: Dict[str, Any]):
        self.data = data


    @property
    def retry_attempt(self) -> int:
        """Возвращает текущий номер попытки."""
        return self.data.get("retry_attempt", 0)


    @property
    def message_id(self) -> Optional[str]:
        """Возвращает ID сообщения, если он есть."""
        return self.data.get("id")


    def increment_retry(self) -> "DLQMessage":
        """Создаёт новую копию с увеличенным счётчиком попыток."""
        new_data = self.data.copy()
        new_data["retry_attempt"] = self.retry_attempt + 1
        return DLQMessage(new_data)


    def to_json(self) -> str:
        """Преобразует сообщение в JSON-строку."""
        return json.dumps(self.data)


class KafkaMessageSender:
    """
    Класс для отправки сообщений в Kafka.

    Attributes:
        bootstrap_server: Адрес Kafka-брокера.
        topic: Имя топика.
    """

    def __init__(self, bootstrap_server: str, topic: str):
        self.bootstrap_server = bootstrap_server
        self.topic = topic

    async def send(self, message: Dict[str, Any]) -> bool:
        """
        Отправляет сообщение в Kafka.

        Args:
            message: Сообщение для отправки.

        Returns:
            True, если сообщение успешно отправлено, иначе False.
        """
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_server,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            await producer.start()
            future = await producer.send(self.topic, value=message)
            result = await future
            logger.info("Message successfully sent to Kafka",
                        message_id=message.get("id"),
                        offset=result.offset)
            await producer.stop()
            return True
        except Exception as e:
            logger.error("Failed to send message to Kafka",
                         error=str(e),
                         message_id=message.get("id"))
            await producer.stop()
            return False


class DLQHandler:
    """
    Класс для работы с Dead Letter Queue (DLQ).

    Attributes:
        redis_service: Сервис для работы с Redis.
        queue_key: Ключ для хранения сообщений в Redis.
        max_retries: Максимальное количество попыток отправки сообщения.
        retry_delay: Начальная задержка между попытками (в секундах).
        max_delay: Максимальная задержка между попытками (в секундах).
        kafka_bootstrap_server: Адрес Kafka-брокера.
        kafka_topic: Имя топика.
    """

    def __init__(
            self,
            redis_service: RedisService,
            queue_key: Optional[str] = None,
            max_retries: int = DEFAULT_RETRY_ATTEMPTS,
            retry_delay: int = DEFAULT_RETRY_DELAY,
            max_delay: int = DEFAULT_RETRY_MAX_DELAY,
            kafka_bootstrap_server: Optional[str] = None,
            kafka_topic: Optional[str] = None
    ):
        self.redis_service = redis_service
        self.queue_key = queue_key or DEFAULT_QUEUE_KEY
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.max_delay = max_delay
        self.kafka_bootstrap_server = kafka_bootstrap_server or DEFAULT_KAFKA_BOOTSTRAP_SERVER
        self.kafka_topic = kafka_topic or DEFAULT_KAFKA_TOPIC
        self.message_sender = KafkaMessageSender(
            self.kafka_bootstrap_server,
            self.kafka_topic
        )
        logger.info("DLQHandler initialized",
                    queue_key=self.queue_key,
                    max_retries=self.max_retries,
                    retry_delay=self.retry_delay,
                    max_delay=self.max_delay,
                    kafka_bootstrap_server=self.kafka_bootstrap_server,
                    kafka_topic=self.kafka_topic)

    async def save_messages(self, messages: List[Dict[str, Any]]) -> None:
        """
        Сохраняет сообщения в DLQ.

        Args:
            messages: Список сообщений для сохранения.
        """
        dlq_messages = [DLQMessage(msg).increment_retry() for msg in messages]

        for dlq_msg in dlq_messages:
            await self.redis_service.rpush(self.queue_key, dlq_msg.to_json())

        queue_size = await self.redis_service.llen(self.queue_key)
        logger.warning(
            f"{len(dlq_messages)} messages saved to DLQ",
            queue_key=self.queue_key,
            queue_size=queue_size,
            retry_attempts=[msg.retry_attempt for msg in dlq_messages]
        )

    def _get_retry_handler(self) -> RetryHandler:
        """
        Возвращает настроенный обработчик повторных попыток.

        Returns:
            Обработчик повторных попыток.
        """
        return RetryHandler(
            max_retries=self.max_retries,
            base_delay=self.retry_delay,
            max_delay=self.max_delay
        )

    @property
    def retry_message(self) -> Callable:
        """
        Возвращает функцию с настроенными retry-попытками.

        Returns:
            Функцию с retry-логикой.
        """
        retry_handler = self._get_retry_handler()

        @retry_handler
        async def _retry_message(raw_msg: str) -> bool:
            """
            Повторяет попытку отправки сообщения в Kafka.

            Args:
                raw_msg: JSON-строка с сообщением.

            Returns:
                True, если сообщение успешно отправлено, иначе False.
            """
            dlq_message = DLQMessage(json.loads(raw_msg))
            logger.info("Retrying message from DLQ",
                        message_id=dlq_message.message_id,
                        attempt=dlq_message.retry_attempt)

            return await self.message_sender.send(dlq_message.data)

        return _retry_message

    async def retry_messages(self) -> None:
        """
        Повторяет отправку всех сообщений из DLQ.
        """
        retry_count = 0
        failed_messages = []

        while True:
            raw_msg = await self.redis_service.lpop(self.queue_key)
            if not raw_msg:
                break

            success = await self.retry_message(raw_msg)

            if success:
                retry_count += 1
            else:
                failed_messages.append(raw_msg)
                logger.error("DLQ message failed after retries",
                             message=raw_msg)

        if retry_count > 0:
            logger.info(f"Successfully retried {retry_count} messages from DLQ")

        if failed_messages:
            logger.warning(f"{len(failed_messages)} messages failed after all retries",
                           failed_messages_count=len(failed_messages))

    async def start_retry_loop(self, interval: int = 60) -> None:
        """
        Запускает цикл автоматического ретрая сообщений.

        Args:
            interval: Интервал между попытками (в секундах).
        """
        logger.info("Starting DLQ retry loop", interval=interval)
        while True:
            try:
                await self.retry_messages()
            except Exception as e:
                logger.error("DLQ retry loop failed", error=str(e))

            await asyncio.sleep(interval)