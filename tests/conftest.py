import pytest
from unittest.mock import patch, MagicMock
from app.core.config import config


@pytest.fixture(autouse=True)
def mock_config():
    """
    Фикстура для мокирования глобального объекта конфигурации.
    Устанавливает тестовые значения для параметров ETL, Redis и Kafka.
    `autouse=True` означает, что эта фикстура будет автоматически применяться ко всем тестам.
    """
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

        mock_cfg.api.host = "127.0.0.1"
        mock_cfg.api.port = 8000
        mock_cfg.api.debug = False
        mock_cfg.clickhouse.host = "localhost"
        mock_cfg.clickhouse.port = 9000
        mock_cfg.clickhouse.database = "default"
        mock_cfg.clickhouse.user = "default"
        mock_cfg.clickhouse.password = ""
        mock_cfg.clickhouse.table = "user_events"
        mock_cfg.jaeger.agent_host = "localhost"
        mock_cfg.jaeger.agent_port = 6831
        mock_cfg.log_level = "DEBUG"
        mock_cfg.environment = "test"

        yield mock_cfg


# Дополнительные фикстуры для мокирования логгера и трассировки,
# чтобы они не мешали выводу тестов и не пытались подключиться к реальным сервисам.
@pytest.fixture(autouse=True)
def mock_logger_and_tracer():
    with patch('app.core.logger.logger') as mock_logger, \
            patch('app.core.tracer.TracerConfig'):  # Мокаем инициализацию трассировки
        # Можно настроить поведение мок-логгера, если нужно
        mock_logger.info = MagicMock()
        mock_logger.error = MagicMock()
        mock_logger.debug = MagicMock()
        mock_logger.warning = MagicMock()
        yield