"""
ETL процессор для обработки событий из Kafka.

Читает сообщения из Kafka, буферизует в Redis и записывает в ClickHouse.
"""

import asyncio
import json
from typing import List
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from ..kafka.consumer import KafkaEventConsumer
from ..redis.buffer import RedisBuffer
from ..redis.dlq import DLQHandler
from ..clickhouse.client import ClickHouseClient
from ..clickhouse.models import UserEventModel
from ..core.config import config
from ..core.logger import logger

class ETLProcessor:
    """
    Основной процессор ETL потока данных.

    Обеспечивает чтение из Kafka, буферизацию в Redis и запись в ClickHouse.
    """

    def __init__(self):
        """Инициализирует компоненты ETL процессора."""
        self.consumer = KafkaEventConsumer()
        self.buffer = RedisBuffer()
        self.dlq = DLQHandler()
        self.clickhouse = ClickHouseClient()
        self.batch_size = config.etl.batch_size
        self.running = True
        logger.info("ETL processor initialized")

    async def start(self) -> None:
        """Запускает компоненты ETL процессора."""
        try:
            await self.consumer.start()
            logger.info("ETL processor started")
        except Exception as e:
            logger.error("Failed to start ETL processor", error=str(e))
            raise

    async def stop(self) -> None:
        """Останавливает компоненты ETL процессора."""
        self.running = False
        try:
            await self.consumer.stop()
            logger.info("ETL processor stopped")
        except Exception as e:
            logger.error("Error during ETL processor shutdown", error=str(e))

    async def process(self) -> None:
        """
        Основной цикл обработки событий.

        Читает сообщения из Kafka, буферизует в Redis,
        периодически сливает в ClickHouse.
        """
        logger.info("Starting ETL processing loop")

        try:
            async for event in self.consumer.consume():
                if not self.running:
                    break

                # Добавляем событие в Redis буфер
                try:
                    event_json = json.dumps(event, ensure_ascii=False, default=str)
                    await self.buffer.push(config.redis.events_key, event_json)
                    logger.debug("Event buffered", event_type=event.get('event_type'))
                except Exception as e:
                    logger.error("Failed to buffer event", error=str(e))
                    continue

                # Периодически проверяем и сливаем батчи
                buffer_length = await self.buffer.get_length(config.redis.events_key)
                if buffer_length >= self.batch_size:
                    await self._process_batch()

                # Добавляем небольшую задержку для предотвращения busy loop
                await asyncio.sleep(config.etl.process_interval)

        except Exception as e:
            logger.error("Error in ETL processing loop", error=str(e))
            raise
        finally:
            # Обрабатываем оставшиеся события при остановке
            await self._process_remaining_events()

    async def _process_batch(self) -> None:
        """Обрабатывает пакет событий из буфера."""
        try:
            # Извлекаем пакет событий из Redis
            events_batch = await self.buffer.pop_batch(config.redis.events_key, self.batch_size)

            if not events_batch:
                return

            logger.info("Processing batch", batch_size=len(events_batch))

            # Преобразуем события в формат для ClickHouse
            clickhouse_events = []
            invalid_events = []

            for event_json in events_batch:
                try:
                    event_data = json.loads(event_json)
                    # Валидируем структуру события
                    user_event = UserEventModel.from_dict(event_data)
                    clickhouse_events.append(user_event.to_tuple())
                except (KeyError, ValueError, json.JSONDecodeError) as e:
                    logger.error("Invalid event format, sending to DLQ",
                               error=str(e),
                               event=event_json)
                    invalid_events.append(event_json)
                    continue

            # Отправляем невалидные события в DLQ
            for invalid_event in invalid_events:
                await self.dlq.send_to_dlq(invalid_event)

            if clickhouse_events:
                # Пытаемся записать в ClickHouse с повторными попытками
                await self._write_to_clickhouse_with_retry(clickhouse_events)

        except Exception as e:
            logger.error("Failed to process batch", error=str(e))

    @retry(
        stop=stop_after_attempt(config.etl.max_retries),
        wait=wait_fixed(config.etl.retry_delay),
        retry=retry_if_exception_type((ConnectionError, TimeoutError))
    )
    async def _write_to_clickhouse_with_retry(self, events: List[tuple]) -> None:
        """
        Записывает события в ClickHouse с повторными попытками.

        Args:
            events: Список кортежей для вставки в ClickHouse
        """
        try:
            self.clickhouse.insert_batch(events)
        except Exception as e:
            logger.error("Failed to write to ClickHouse, will retry",
                        error=str(e),
                        attempt=config.etl.max_retries)
            # Отправляем в DLQ если все попытки исчерпаны
            for event_tuple in events:
                # Преобразуем обратно в JSON для DLQ
                event_dict = {
                    'user_id': event_tuple[0],
                    'movie_id': event_tuple[1],
                    'event_type': event_tuple[2],
                    'timestamp': event_tuple[3].isoformat() if hasattr(event_tuple[3], 'isoformat') else str(event_tuple[3])
                }
                event_json = json.dumps(event_dict, ensure_ascii=False, default=str)
                await self.dlq.send_to_dlq(event_json)
            raise

    async def _process_remaining_events(self) -> None:
        """Обрабатывает оставшиеся события в буфере при остановке."""
        try:
            remaining_count = await self.buffer.get_length(config.redis.events_key)
            logger.info("Processing remaining events", count=remaining_count)

            while remaining_count > 0 and self.running:
                await self._process_batch()
                remaining_count = await self.buffer.get_length(config.redis.events_key)

        except Exception as e:
            logger.error("Error processing remaining events", error=str(e))