"""
Модуль конфигурации приложения.

Содержит настройки для всех компонентов приложения, включая Kafka, Redis, DLQ, Rate Limiting,
трассировку и общие параметры приложения.
"""

from pydantic_settings import BaseSettings
from pydantic import Field, BaseModel, ConfigDict, field_validator
import logging

logger = logging.getLogger(__name__)

class KafkaSettings(BaseModel):
    """
    Настройки для Kafka.

    Attributes:
        bootstrap_server: Адрес Kafka-брокера.
        topic: Имя топика по умолчанию.
        send_timeout: Таймаут для отправки сообщения (в секундах).
        max_batch_size: Максимальный размер батча для буферизации.
        request_size: Максимальный размер запроса (в байтах).
        flush_interval: Интервал отправки батча (в секундах).
    """
    bootstrap_server: str = Field(default="message-broker:9092", validation_alias="KAFKA_BOOTSTRAP_SERVER")
    topic: str = Field(default="user_events", validation_alias="KAFKA_TOPIC")
    send_timeout: int = Field(default=5, validation_alias="KAFKA_SEND_TIMEOUT")
    max_batch_size: int = Field(default=16384, validation_alias="KAFKA_MAX_BATCH_SIZE")
    request_size: int = Field(default=1048576, validation_alias="KAFKA_REQUEST_SIZE")
    flush_interval: int = Field(default=5, validation_alias="KAFKA_FLUSH_INTERVAL")

    @field_validator("request_size")
    @classmethod
    def validate_request_size(cls, v: int) -> int:
        """Проверяет, что размер запроса не превышает 10MB."""
        if v > 10 * 1024 * 1024:  # 10MB
            logger.warning("Kafka request size exceeds 10MB", size=v)
            raise ValueError("Request size must not exceed 10MB")
        return v


class RedisSettings(BaseModel):
    """
    Настройки для Redis.

    Attributes:
        url: URL подключения к Redis.
        max_connections: Максимальное количество подключений.
        retry_attempts: Количество попыток при неудачном подключении.
        default_ttl: Время жизни ключей по умолчанию (в секундах).
    """
    url: str = Field(default="redis://cache:6379", validation_alias="REDIS_URL")
    max_connections: int = Field(default=10, validation_alias="REDIS_MAX_CONNECTIONS")
    retry_attempts: int = Field(default=3, validation_alias="REDIS_RETRY_ATTEMPTS")
    default_ttl: int = 3600  # 1 hour


class DLQSettings(BaseModel):
    """
    Настройки для Dead Letter Queue (DLQ) на основе Redis.

    Attributes:
        redis_url: URL подключения к Redis для DLQ.
        queue_key: Ключ для хранения сообщений в Redis.
    """
    redis_url: str = Field(default="redis://dlq-cache:6379", validation_alias="DLQ_REDIS_URL")
    dlq_queue_key: str = Field(default="dlq_kafka", validation_alias="DLQ_QUEUE_KEY")


class RateLimitSettings(BaseModel):
    """
    Настройки рейт-лимита.

    Attributes:
        rate_limit: Максимальное количество запросов в интервале.
        window_seconds: Длительность окна ограничения (в секундах).
    """
    rate_limit: int = Field(default=100, validation_alias="RATE_LIMIT")
    window_seconds: int = Field(default=60, validation_alias="RATE_LIMIT_WINDOW")


class TracingSettings(BaseModel):
    """
    Настройки трассировки с использованием OpenTelemetry.

    Attributes:
        otlp_endpoint: URL для отправки трассировок.
    """
    otel_exporter_otlp_endpoint: str = Field(default="http://tracing:4318/v1/traces", env="OTEL_EXPORTER_OTLP_ENDPOINT")


class AppSettings(BaseModel):
    """
    Общие настройки приложения.

    Attributes:
        environment: Текущая среда (development, staging, production).
        debug: Включен ли режим отладки.
        name: Имя приложения.
    """
    environment: str = Field(default="development", validation_alias="APP_ENV")
    debug: bool = Field(default=False, validation_alias="APP_DEBUG")
    name: str = Field(default="UserActionCollector", validation_alias="APP_NAME")


class Settings(BaseSettings):
    """
    Главная конфигурация приложения.

    Attributes:
        kafka: Настройки Kafka.
        redis: Настройки Redis.
        dlq: Настройки DLQ.
        rate_limit: Настройки рейт-лимита.
        tracing: Настройки трассировки.
        app: Общие настройки приложения.
    """
    model_config = ConfigDict(env_nested_delimiter="__", extra="ignore")

    kafka: KafkaSettings = KafkaSettings()
    redis: RedisSettings = RedisSettings()
    dlq: DLQSettings = DLQSettings()
    rate_limit: RateLimitSettings = RateLimitSettings()
    tracing: TracingSettings = TracingSettings()
    app: AppSettings = AppSettings()



settings = Settings()
logger.info("Application settings loaded", app_name=settings.app.name, environment=settings.app.environment)