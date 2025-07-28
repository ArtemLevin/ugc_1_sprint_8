import pytest
from unittest.mock import AsyncMock, patch
from app.kafka.producer import KafkaEventProducer
from app.core.config import config

@pytest.fixture
def mock_producer_instance():
    """Фикстура для создания экземпляра KafkaEventProducer с замоканным AIOKafkaProducer."""
    with patch('app.kafka.producer.AIOKafkaProducer') as MockAIOKafkaProducer:
        mock_aio_producer = MockAIOKafkaProducer.return_value
        mock_aio_producer.start = AsyncMock()
        mock_aio_producer.stop = AsyncMock()
        mock_aio_producer.send_and_wait = AsyncMock()
        producer = KafkaEventProducer()
        producer.producer = mock_aio_producer
        yield producer


@pytest.mark.asyncio
async def test_producer_start_stop(mock_producer_instance):
    """Тест корректного запуска и остановки продюсера."""
    await mock_producer_instance.start()
    mock_producer_instance.producer.start.assert_called_once()

    await mock_producer_instance.stop()
    mock_producer_instance.producer.stop.assert_called_once()


@pytest.mark.asyncio
async def test_send_event(mock_producer_instance):
    """Тест отправки обычного события."""
    event_data = {"user_id": "test_user", "movie_id": "test_movie", "event_type": "start",
                  "timestamp": "2024-01-01T12:00:00"}

    await mock_producer_instance.send_event(event_data)

    mock_producer_instance.producer.send_and_wait.assert_called_once()
    args, _ = mock_producer_instance.producer.send_and_wait.call_args

    assert args[0] == config.kafka.topic  # Проверяем топик

    sent_message = args[1].decode('utf-8')
    assert "test_user" in sent_message
    assert "test_movie" in sent_message
    assert "start" in sent_message


@pytest.mark.asyncio
async def test_send_to_dlq(mock_producer_instance):
    """Тест отправки события в Dead Letter Queue (DLQ)."""
    event_data = {"user_id": "bad_user", "error": "invalid_format"}

    await mock_producer_instance.send_to_dlq(event_data)

    mock_producer_instance.producer.send_and_wait.assert_called_once()
    args, _ = mock_producer_instance.producer.send_and_wait.call_args

    assert args[0] == config.kafka.dlq_topic  # Проверяем топик DLQ

    sent_message = args[1].decode('utf-8')
    assert "bad_user" in sent_message
    assert "invalid_format" in sent_message


@pytest.mark.asyncio
async def test_send_event_failure(mock_producer_instance):
    """Тест обработки ошибок при отправке события."""
    mock_producer_instance.producer.send_and_wait.side_effect = Exception("Kafka error")
    event_data = {"user_id": "fail_user", "movie_id": "fail_movie", "event_type": "stop",
                  "timestamp": "2024-01-01T12:00:00"}

    with pytest.raises(Exception, match="Kafka error"):
        await mock_producer_instance.send_event(event_data)

    mock_producer_instance.producer.send_and_wait.assert_called_once()