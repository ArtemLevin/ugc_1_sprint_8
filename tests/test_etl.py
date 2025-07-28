import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from app.etl.processor import ETLProcessor
from app.clickhouse.models import UserEventModel
from app.core.config import config


# Мокаем значения конфигурации для последовательного тестирования
@pytest.fixture(autouse=True)
def mock_config():
    with patch('app.core.config.config') as mock_cfg:
        mock_cfg.etl.batch_size = 2
        mock_cfg.etl.batch_timeout = 1
        mock_cfg.etl.max_retries = 1
        mock_cfg.etl.retry_delay = 0.01
        mock_cfg.etl.process_interval = 0.01
        mock_cfg.redis.events_key = "test_events_buffer"
        mock_cfg.redis.dlq_key = "test_dlq_events"
        mock_cfg.kafka.topic = "test_user_events"
        mock_cfg.kafka.dlq_topic = "test_dlq_user_events"
        mock_cfg.service_name = "test-service"
        yield mock_cfg


@pytest.fixture
def mock_etl_components():
    """Фикстура для мока всех внешних зависимостей ETLProcessor."""
    with patch('app.kafka.consumer.KafkaEventConsumer', AsyncMock) as MockKafkaConsumer, \
            patch('app.redis.buffer.RedisBuffer', AsyncMock) as MockRedisBuffer, \
            patch('app.redis.dlq.DLQHandler', AsyncMock) as MockDLQHandler, \
            patch('app.clickhouse.client.ClickHouseClient', MagicMock) as MockClickHouseClient, \
            patch('app.clickhouse.models.UserEventModel.from_dict') as MockUserEventModelFromDict:  # Мокаем этот метод

        # Конфигурируем моки
        mock_consumer = MockKafkaConsumer.return_value
        mock_consumer.start = AsyncMock()
        mock_consumer.stop = AsyncMock()
        mock_consumer.consume = AsyncMock()

        mock_buffer = MockRedisBuffer.return_value
        mock_buffer.push = AsyncMock()
        mock_buffer.pop_batch = AsyncMock()
        mock_buffer.get_length = AsyncMock(return_value=0)  # По умолчанию буфер пуст

        mock_dlq = MockDLQHandler.return_value
        mock_dlq.send_to_dlq = AsyncMock()

        mock_clickhouse = MockClickHouseClient.return_value
        mock_clickhouse.test_connection = MagicMock(return_value=True)
        mock_clickhouse.create_table = MagicMock()
        mock_clickhouse.insert_batch = MagicMock()

        # Конфигурируем мок UserEventModel.from_dict
        # Он должен возвращать мок-экземпляр UserEventModel с методом to_tuple
        mock_user_event_model_instance = MagicMock(spec=UserEventModel)
        mock_user_event_model_instance.to_tuple.return_value = ("mock_user", "mock_movie", "mock_event", datetime.now())
        MockUserEventModelFromDict.return_value = mock_user_event_model_instance

        processor = ETLProcessor()
        processor.consumer = mock_consumer
        processor.buffer = mock_buffer
        processor.dlq = mock_dlq
        processor.clickhouse = mock_clickhouse

        yield processor, mock_consumer, mock_buffer, mock_dlq, mock_clickhouse, MockUserEventModelFromDict


@pytest.mark.asyncio
async def test_etl_processor_start_stop(mock_etl_components):
    """Тест корректного запуска и остановки ETL-процессора."""
    processor, mock_consumer, _, _, _, _ = mock_etl_components

    await processor.start()
    mock_consumer.start.assert_called_once()

    await processor.stop()
    mock_consumer.stop.assert_called_once()
    assert not processor.running


@pytest.mark.asyncio
async def test_etl_processor_full_batch_processing(mock_etl_components, mock_config):
    """Тест обработки полного батча валидных событий."""
    processor, mock_consumer, mock_buffer, mock_dlq, mock_clickhouse, MockUserEventModelFromDict = mock_etl_components

    event1_data = {"user_id": "u1", "movie_id": "m1", "event_type": "start", "timestamp": datetime.now().isoformat()}
    event2_data = {"user_id": "u2", "movie_id": "m2", "event_type": "stop", "timestamp": datetime.now().isoformat()}

    # Конфигурируем консьюмер для выдачи событий
    mock_consumer.consume.return_value.__aiter__.return_value.__anext__.side_effect = [
        event1_data,
        event2_data,
        StopAsyncIteration  # Останавливаем после двух событий
    ]

    event1_json = json.dumps(event1_data, default=str)
    event2_json = json.dumps(event2_data, default=str)

    # Конфигурируем буфер для отчета о длине и выдачи батчей
    mock_buffer.get_length.side_effect = [
        0,  # Начальная длина до event1
        1,  # После push event1
        1,  # Начальная длина до event2
        2,  # После push event2 (запускает обработку батча)
        0,  # После извлечения батча
        0  # Для финальной проверки
    ]
    mock_buffer.pop_batch.return_value = [event1_json, event2_json]

    # Конфигурируем мок UserEventModel.from_dict для возврата конкретных кортежей
    MockUserEventModelFromDict.side_effect = [
        MagicMock(spec=UserEventModel, to_tuple=MagicMock(return_value=("u1", "m1", "start", datetime.now()))),
        MagicMock(spec=UserEventModel, to_tuple=MagicMock(return_value=("u2", "m2", "stop", datetime.now())))
    ]

    await processor.start()
    await processor.process()


    mock_consumer.start.assert_called_once()
    mock_buffer.push.assert_any_call(mock_config.redis.events_key, event1_json)
    mock_buffer.push.assert_any_call(mock_config.redis.events_key, event2_json)

    # Проверяем, что get_length был вызван несколько раз для проверки размера батча
    assert mock_buffer.get_length.call_count >= 2

    # Проверяем, что pop_batch был вызван один раз с корректным размером батча
    mock_buffer.pop_batch.assert_called_once_with(mock_config.redis.events_key, mock_config.etl.batch_size)

    # Проверяем, что ClickHouse insert_batch был вызван с корректными данными
    mock_clickhouse.insert_batch.assert_called_once()
    inserted_data = mock_clickhouse.insert_batch.call_args[0][0]
    assert len(inserted_data) == 2
    assert inserted_data[0][0] == "u1"  # user_id из event1
    assert inserted_data[1][0] == "u2"  # user_id из event2

    mock_dlq.send_to_dlq.assert_not_called()  # Не должно быть невалидных событий

    await processor.stop()
    mock_consumer.stop.assert_called_once()


@pytest.mark.asyncio
async def test_etl_processor_invalid_event_to_dlq(mock_etl_components, mock_config):
    """Тест, что невалидные события отправляются в DLQ."""
    processor, mock_consumer, mock_buffer, mock_dlq, mock_clickhouse, MockUserEventModelFromDict = mock_etl_components

    valid_event_data = {"user_id": "u1", "movie_id": "m1", "event_type": "start",
                        "timestamp": datetime.now().isoformat()}
    invalid_event_json_str = '{"user_id": "u2", "movie_id": "m2", "event_type": "bad_type", "timestamp": "invalid_date"}'  # Невалидная временная метка и тип события
    invalid_event_data = json.loads(invalid_event_json_str)  # Консьюмер выдает dict

    mock_consumer.consume.return_value.__aiter__.return_value.__anext__.side_effect = [
        valid_event_data,
        invalid_event_data,
        StopAsyncIteration
    ]

    valid_event_json_str = json.dumps(valid_event_data, default=str)

    mock_buffer.get_length.side_effect = [
        0, 1, 2, 0, 0  # Симулируем заполнение и опустошение буфера
    ]
    mock_buffer.pop_batch.return_value = [
        valid_event_json_str,
        invalid_event_json_str
    ]

    # Конфигурируем UserEventModel.from_dict: валидное событие работает, невалидное вызывает ошибку
    MockUserEventModelFromDict.side_effect = [
        MagicMock(spec=UserEventModel, to_tuple=MagicMock(return_value=("u1", "m1", "start", datetime.now()))),
        ValueError("Invalid event data")
    ]

    await processor.start()
    await processor.process()

    mock_buffer.push.assert_any_call(mock_config.redis.events_key, valid_event_json_str)
    mock_buffer.push.assert_any_call(mock_config.redis.events_key, invalid_event_json_str)

    mock_clickhouse.insert_batch.assert_called_once()  # Только валидное событие должно быть вставлено
    inserted_data = mock_clickhouse.insert_batch.call_args[0][0]
    assert len(inserted_data) == 1
    assert inserted_data[0][0] == "u1"

    mock_dlq.send_to_dlq.assert_called_once_with(invalid_event_json_str)  # Невалидное событие отправлено в DLQ

    await processor.stop()


@pytest.mark.asyncio
async def test_etl_processor_clickhouse_insert_failure_to_dlq(mock_etl_components, mock_config):
    """Тест, что события отправляются в DLQ, если вставка в ClickHouse не удалась после повторных попыток."""
    processor, mock_consumer, mock_buffer, mock_dlq, mock_clickhouse, MockUserEventModelFromDict = mock_etl_components

    event1_data = {"user_id": "u1", "movie_id": "m1", "event_type": "start", "timestamp": datetime.now().isoformat()}
    event2_data = {"user_id": "u2", "movie_id": "m2", "event_type": "stop", "timestamp": datetime.now().isoformat()}

    mock_consumer.consume.return_value.__aiter__.return_value.__anext__.side_effect = [
        event1_data,
        event2_data,
        StopAsyncIteration
    ]

    event1_json = json.dumps(event1_data, default=str)
    event2_json = json.dumps(event2_data, default=str)
    mock_buffer.get_length.side_effect = [
        0, 1, 2, 0, 0
    ]
    mock_buffer.pop_batch.return_value = [event1_json, event2_json]

    # Конфигурируем UserEventModel.from_dict для возврата валидных экземпляров
    MockUserEventModelFromDict.side_effect = [
                                                 MagicMock(spec=UserEventModel, to_tuple=MagicMock(
                                                     return_value=("u1", "m1", "start", datetime.now()))),
                                                 MagicMock(spec=UserEventModel, to_tuple=MagicMock(
                                                     return_value=("u2", "m2", "stop", datetime.now())))
                                             ] * (
                                                         mock_config.etl.max_retries + 1)  # Обеспечиваем достаточное количество валидных моков для повторных попыток

    # Симулируем сбой вставки в ClickHouse
    mock_clickhouse.insert_batch.side_effect = Exception("ClickHouse connection error")

    await processor.start()
    await processor.process()

    # insert_batch ClickHouse должен быть вызван (max_retries + 1) раз (начальный + повторные попытки)
    # Поскольку max_retries = 1, он должен быть вызван дважды.
    assert mock_clickhouse.insert_batch.call_count == (mock_config.etl.max_retries + 1)

    # Оба события должны быть отправлены в DLQ
    assert mock_dlq.send_to_dlq.call_count == 2
    mock_dlq.send_to_dlq.assert_any_call(event1_json)
    mock_dlq.send_to_dlq.assert_any_call(event2_json)

    await processor.stop()


@pytest.mark.asyncio
async def test_etl_processor_remaining_events_on_shutdown(mock_etl_components, mock_config):
    """Тест, что оставшиеся события в буфере обрабатываются при завершении работы."""
    processor, mock_consumer, mock_buffer, mock_dlq, mock_clickhouse, MockUserEventModelFromDict = mock_etl_components

    event1_data = {"user_id": "u1", "movie_id": "m1", "event_type": "start", "timestamp": datetime.now().isoformat()}

    # Консьюмер выдает одно событие, но этого недостаточно для полного батча
    mock_consumer.consume.return_value.__aiter__.return_value.__anext__.side_effect = [
        event1_data,
        StopAsyncIteration  # Консьюмер останавливается, но событие остается в буфере
    ]

    event1_json = json.dumps(event1_data, default=str)

    # Симулируем наличие 1 события в буфере, когда цикл обработки завершается
    mock_buffer.get_length.side_effect = [
        0,  # Начальная
        1,  # После push event1
        1,  # Когда цикл обработки завершается и проверяет оставшиеся
        0  # После извлечения и обработки оставшегося события
    ]
    mock_buffer.pop_batch.side_effect = [
        [event1_json],  # Извлекаем оставшееся событие
        []  # Больше событий нет
    ]

    MockUserEventModelFromDict.return_value = MagicMock(spec=UserEventModel, to_tuple=MagicMock(
        return_value=("u1", "m1", "start", datetime.now())))

    await processor.start()
    await processor.process()  # Это будет выполняться до StopAsyncIteration от консьюмера

    # После завершения process(), должен быть вызван _process_remaining_events,
    # который снова вызовет pop_batch и insert_batch.

    # Проверяем, что одно событие было отправлено в буфер
    mock_buffer.push.assert_called_once_with(mock_config.redis.events_key, event1_json)

    # Проверяем, что pop_batch был вызван дважды: один раз в цикле (не удалось получить batch_size)
    # и один раз во время _process_remaining_events
    assert mock_buffer.pop_batch.call_count == 2

    # Проверяем, что insert_batch был вызван один раз с одним событием
    mock_clickhouse.insert_batch.assert_called_once()
    inserted_data = mock_clickhouse.insert_batch.call_args[0][0]
    assert len(inserted_data) == 1
    assert inserted_data[0][0] == "u1"

    await processor.stop()
    mock_consumer.stop.assert_called_once()