import pytest
from unittest.mock import AsyncMock, patch
from app.kafka.consumer import KafkaEventConsumer
from aiokafka.structs import ConsumerRecord


@pytest.fixture
def mock_consumer_instance():
    """Фикстура для создания экземпляра KafkaEventConsumer с замоканным AIOKafkaConsumer."""
    with patch('app.kafka.consumer.AIOKafkaConsumer') as MockAIOKafkaConsumer:
        mock_aio_consumer = MockAIOKafkaConsumer.return_value
        mock_aio_consumer.start = AsyncMock()
        mock_aio_consumer.stop = AsyncMock()
        # Мокаем асинхронный итератор для метода consume
        mock_aio_consumer.__aiter__.return_value = AsyncMock()

        consumer = KafkaEventConsumer()
        consumer.consumer = mock_aio_consumer
        yield consumer


@pytest.mark.asyncio
async def test_consumer_start_stop(mock_consumer_instance):
    """Тест корректного запуска и остановки консьюмера."""
    await mock_consumer_instance.start()
    mock_consumer_instance.consumer.start.assert_called_once()

    await mock_consumer_instance.stop()
    mock_consumer_instance.consumer.stop.assert_called_once()


@pytest.mark.asyncio
async def test_consume_valid_messages(mock_consumer_instance):
    """Тест потребления валидных JSON-сообщений."""
    # Симулируем сообщения Kafka
    mock_messages = [
        ConsumerRecord(topic='user_events', partition=0, offset=0, timestamp=0,
                       timestamp_type=0, key=None,
                       value=b'{"user_id": "u1", "movie_id": "m1", "event_type": "start", "timestamp": "2024-01-01T12:00:00"}',
                       checksum=0, serialized_key_size=0, serialized_value_size=0, headers=[]),
        ConsumerRecord(topic='user_events', partition=0, offset=1, timestamp=0,
                       timestamp_type=0, key=None,
                       value=b'{"user_id": "u2", "movie_id": "m2", "event_type": "stop", "timestamp": "2024-01-01T12:01:00"}',
                       checksum=0, serialized_key_size=0, serialized_value_size=0, headers=[]),
    ]

    # Конфигурируем асинхронный итератор мок-консьюмера
    mock_consumer_instance.consumer.__aiter__.return_value.__anext__.side_effect = mock_messages + [StopAsyncIteration]

    consumed_events = []
    async for event in mock_consumer_instance.consume():
        consumed_events.append(event)

    assert len(consumed_events) == 2
    assert consumed_events[0]["user_id"] == "u1"
    assert consumed_events[1]["movie_id"] == "m2"


@pytest.mark.asyncio
async def test_consume_invalid_json_message(mock_consumer_instance):
    """Тест потребления невалидного JSON-сообщения (должно быть пропущено)."""
    mock_messages = [
        ConsumerRecord(topic='user_events', partition=0, offset=0, timestamp=0,
                       timestamp_type=0, key=None,
                       value=b'{"user_id": "u1", "movie_id": "m1", "event_type": "start", "timestamp": "2024-01-01T12:00:00"}',
                       checksum=0, serialized_key_size=0, serialized_value_size=0, headers=[]),
        ConsumerRecord(topic='user_events', partition=0, offset=1, timestamp=0,
                       timestamp_type=0, key=None, value=b'{"invalid_json": "data',  # Невалидный JSON
                       checksum=0, serialized_key_size=0, serialized_value_size=0, headers=[]),
        ConsumerRecord(topic='user_events', partition=0, offset=2, timestamp=0,
                       timestamp_type=0, key=None,
                       value=b'{"user_id": "u3", "movie_id": "m3", "event_type": "pause", "timestamp": "2024-01-01T12:02:00"}',
                       checksum=0, serialized_key_size=0, serialized_value_size=0, headers=[]),
    ]

    mock_consumer_instance.consumer.__aiter__.return_value.__anext__.side_effect = mock_messages + [StopAsyncIteration]

    consumed_events = []
    async for event in mock_consumer_instance.consume():
        consumed_events.append(event)

    assert len(consumed_events) == 2  # Невалидное сообщение должно быть пропущено
    assert consumed_events[0]["user_id"] == "u1"
    assert consumed_events[1]["user_id"] == "u3"


@pytest.mark.asyncio
async def test_consume_general_error(mock_consumer_instance):
    """Тест общей ошибки во время обработки сообщения (должно быть пропущено)."""
    mock_messages = [
        ConsumerRecord(topic='user_events', partition=0, offset=0, timestamp=0,
                       timestamp_type=0, key=None,
                       value=b'{"user_id": "u1", "movie_id": "m1", "event_type": "start", "timestamp": "2024-01-01T12:00:00"}',
                       checksum=0, serialized_key_size=0, serialized_value_size=0, headers=[]),
    ]

    # Симулируем ошибку в цикле консьюмера после первого сообщения
    mock_consumer_instance.consumer.__aiter__.return_value.__anext__.side_effect = [
        mock_messages[0],
        Exception("Simulated consumer loop error")
    ]

    consumed_events = []
    with pytest.raises(Exception, match="Simulated consumer loop error"):
        async for event in mock_consumer_instance.consume():
            consumed_events.append(event)

    assert len(consumed_events) == 1
    assert consumed_events[0]["user_id"] == "u1"