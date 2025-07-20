from app.core.logger import get_logger
from typing import Any

logger = get_logger(__name__)


class KafkaEventSender:
    def __init__(self, kafka_producer: "BufferedKafkaProducer"):
        self.kafka_producer = kafka_producer
        logger.info("KafkaEventSender initialized with buffered producer")

    async def send(self, message: dict[str, Any]) -> None:
        """
        Отправляет событие в Kafka через буферизованный продюсер.

        :param message: Событие в формате словаря
        :return: None
        """
        logger.debug("Sending event to Kafka", event_type=message.get("event_type"), message_id=message.get("id"))
        try:
            await self.kafka_producer.send(message)
        except Exception as e:
            logger.error("Failed to send event to Kafka", error=str(e), event=message)
            raise