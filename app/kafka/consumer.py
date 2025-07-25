"""
Kafka консьюмер для чтения пользовательских событий.

Асинхронно читает сообщения из Kafka топика для последующей обработки.
"""

from aiokafka import AIOKafkaConsumer
import json
from typing import AsyncGenerator, Dict, Any
from ..core.config import config
from ..core.logger import logger


class KafkaEventConsumer:
    """
    Асинхронный Kafka консьюмер для чтения событий.

    Обеспечивает подключение к Kafka и чтение сообщений из топика.
    """

    def __init__(self):
        """Инициализирует Kafka консьюмера."""
        self.consumer = AIOKafkaConsumer(
            config.kafka.topic,
            bootstrap_servers=config.kafka.bootstrap_servers,
            group_id=config.kafka.group_id,
            auto_offset_reset=config.kafka.auto_offset_reset,
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
        )
        self.topic = config.kafka.topic
        logger.info("Kafka consumer initialized",
                    bootstrap_servers=config.kafka.bootstrap_servers,
                    topic=self.topic,
                    group_id=config.kafka.group_id)

    async def start(self) -> None:
        """Запускает Kafka консьюмера."""
        try:
            await self.consumer.start()
            logger.info("Kafka consumer started")
        except Exception as e:
            logger.error("Failed to start Kafka consumer", error=str(e))
            raise

    async def stop(self) -> None:
        """Останавливает Kafka консьюмера."""
        try:
            await self.consumer.stop()
            logger.info("Kafka consumer stopped")
        except Exception as e:
            logger.error("Failed to stop Kafka consumer", error=str(e))

    async def consume(self) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Асинхронный генератор для чтения сообщений из Kafka.

        Yields:
            Dict[str, Any]: Десериализованное сообщение из Kafka

        Raises:
            Exception: При ошибке чтения сообщения
        """
        try:
            async for msg in self.consumer:
                try:
                    event = json.loads(msg.value.decode('utf-8'))
                    logger.debug("Message consumed from Kafka",
                                 topic=msg.topic,
                                 partition=msg.partition,
                                 offset=msg.offset)
                    yield event
                except json.JSONDecodeError as e:
                    logger.error("Failed to decode JSON message",
                                 topic=msg.topic,
                                 partition=msg.partition,
                                 offset=msg.offset,
                                 error=str(e))
                    # Пропускаем некорректные сообщения
                    continue
                except Exception as e:
                    logger.error("Failed to process Kafka message",
                                 topic=msg.topic,
                                 partition=msg.partition,
                                 offset=msg.offset,
                                 error=str(e))
                    # Пропускаем сообщения с ошибками обработки
                    continue

        except Exception as e:
            logger.error("Error in Kafka consumer loop", error=str(e))
            raise