"""
Модуль для отправки событий в Kafka.

Содержит класс `KafkaEventSender`, реализующий отправку событий в Kafka.
"""

from typing import Any, Dict
from app_v_1.core.logger import get_logger
from app_v_1.services.kafka_producer import BufferedKafkaProducer

logger = get_logger(__name__)


class KafkaEventSender:
    """
    Сервис для отправки событий в Kafka.

    Attributes:
        kafka_producer: Буферизованный продюсер Kafka.
    """

    def __init__(self, kafka_producer: BufferedKafkaProducer):
        """
        Инициализирует KafkaEventSender.

        Args:
            kafka_producer: Буферизованный продюсер Kafka для отправки событий.
        """
        self.kafka_producer = kafka_producer
        logger.info(
            "KafkaEventSender initialized",
            producer_type=type(kafka_producer).__name__
        )

    async def send(self, message: Dict[str, Any]) -> None:
        """
        Отправляет событие в Kafka через буферизованный продюсер.

        Args:
            message: Событие в формате словаря.

        Raises:
            Exception: Если отправка сообщения в Kafka не удалась.
        """
        message_id = message.get("id")
        event_type = message.get("event_type")

        logger.debug(
            "Sending event to Kafka",
            event_type=event_type,
            message_id=message_id
        )

        try:
            await self.kafka_producer.send(message)
        except Exception as e:
            logger.error(
                "Failed to send event to Kafka",
                error=str(e),
                event_type=event_type,
                message_id=message_id,
                exc_info=True
            )
            raise