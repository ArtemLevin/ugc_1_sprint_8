"""
Kafka продюсер для отправки пользовательских событий.

Асинхронно отправляет события в Kafka топик.
"""

from aiokafka import AIOKafkaProducer
import json
from typing import Dict, Any
from ..core.config import config
from ..core.logger import logger


class KafkaEventProducer:
    """
    Асинхронный Kafka продюсер для отправки событий.

    Обеспечивает подключение к Kafka и отправку сообщений.
    """

    def __init__(self):
        """Инициализирует Kafka продюсера."""
        self._producer: AIOKafkaProducer | None = None
        self.topic = config.kafka.topic
        self.dlq_topic = config.kafka.dlq_topic
        logger.info("KafkaEventProducer instance created")

    async def start(self) -> None:
        """Запускает Kafka продюсера."""
        if self._producer is None:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=config.kafka.bootstrap_servers,
                acks=1,  # Ждем подтверждения от лидера
                linger_ms=10  # Задержка для батчинга
            )

        try:
            await self._producer.start()
            logger.info("Kafka producer started")
        except Exception as e:
            logger.error("Failed to start Kafka producer", error=str(e))
            raise

    async def stop(self) -> None:
        """Останавливает Kafka продюсера."""
        if self._producer:
            try:
                await self._producer.stop()
                logger.info("Kafka producer stopped")
            except Exception as e:
                logger.error("Failed to stop Kafka producer", error=str(e))

    async def send_event(self, event: Dict[str, Any]) -> None:
        """
        Отправляет событие в Kafka.

        Args:
            event: Словарь с данными события

        Raises:
            Exception: При ошибке отправки сообщения
        """
        if not self._producer:
            raise RuntimeError("Kafka producer not started")
        
        try:
            message = json.dumps(event, ensure_ascii=False, default=str).encode('utf-8')

            # Отправляем сообщение в топик
            await self._producer.send_and_wait(self.topic, message)

            logger.info("Event sent to Kafka",
                        topic=self.topic,
                        event_type=event.get('event_type'),
                        user_id=event.get('user_id'))

        except Exception as e:
            logger.error("Failed to send event to Kafka",
                         topic=self.topic,
                         event=event,
                         error=str(e))
            raise

    async def send_to_dlq(self, event: Dict[str, Any]) -> None:
        """
        Отправляет событие в Dead Letter Queue.

        Args:
            event: Словарь с данными события
        """
        if not self._producer:
            raise RuntimeError("Kafka producer not started")
        
        try:
            message = json.dumps(event, ensure_ascii=False, default=str).encode('utf-8')
            await self._producer.send_and_wait(config.kafka.dlq_topic, message)
            logger.warning("Event sent to DLQ",
                           topic=config.kafka.dlq_topic,
                           event_type=event.get('event_type'))
        except Exception as e:
            logger.error("Failed to send event to DLQ",
                         topic=config.kafka.dlq_topic,
                         event=event,
                         error=str(e))
            raise
