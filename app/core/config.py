"""
Конфигурация приложения.

Содержит все настройки для подключения к внешним сервисам
и параметры работы приложения в виде классов Pydantic.
"""

from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class KafkaConfig(BaseSettings):
    """Конфигурация Kafka."""

    bootstrap_servers: str = Field(default="localhost:9092", env="KAFKA_BOOTSTRAP_SERVERS")
    topic: str = Field(default="user_events", env="KAFKA_TOPIC")
    dlq_topic: str = Field(default="dlq_user_events", env="KAFKA_DLQ_TOPIC")
    group_id: str = Field(default="etl-consumer-group", env="KAFKA_GROUP_ID")
    auto_offset_reset: str = Field(default="earliest", env="KAFKA_AUTO_OFFSET_RESET")

    class Config:
        env_prefix = "KAFKA_"


class RedisConfig(BaseSettings):
    """Конфигурация Redis."""

    host: str = Field(default="localhost", env="REDIS_HOST")
    port: int = Field(default=6379, env="REDIS_PORT")
    db: int = Field(default=0, env="REDIS_DB")
    events_key: str = Field(default="events_buffer", env="REDIS_EVENTS_KEY")
    dlq_key: str = Field(default="dlq_events", env="REDIS_DLQ_KEY")

    class Config:
        env_prefix = "REDIS_"


class ClickHouseConfig(BaseSettings):
    """Конфигурация ClickHouse."""

    host: str = Field(default="localhost", env="CLICKHOUSE_HOST")
    port: int = Field(default=9000, env="CLICKHOUSE_PORT")
    database: str = Field(default="default", env="CLICKHOUSE_DATABASE")
    table: str = Field(default="user_events", env="CLICKHOUSE_TABLE")
    user: str = Field(default="default", env="CLICKHOUSE_USER")
    password: str = Field(default="", env="CLICKHOUSE_PASSWORD")

    class Config:
        env_prefix = "CLICKHOUSE_"


class JaegerConfig(BaseSettings):
    """Конфигурация Jaeger."""

    agent_host: str = Field(default="localhost", env="JAEGER_AGENT_HOST")
    agent_port: int = Field(default=6831, env="JAEGER_AGENT_PORT")
    collector_endpoint: Optional[str] = Field(default=None, env="JAEGER_COLLECTOR_ENDPOINT")

    class Config:
        env_prefix = "JAEGER_"


class ETLConfig(BaseSettings):
    """Конфигурация ETL процесса."""

    batch_size: int = Field(default=100, env="BATCH_SIZE")
    batch_timeout: int = Field(default=5, env="BATCH_TIMEOUT")  # секунды
    max_retries: int = Field(default=3, env="MAX_RETRIES")
    retry_delay: int = Field(default=2, env="RETRY_DELAY")  # секунды
    process_interval: int = Field(default=1, env="PROCESS_INTERVAL")  # секунды

    class Config:
        env_prefix = "ETL_"


class APIConfig(BaseSettings):
    """Конфигурация API сервиса."""

    host: str = Field(default="0.0.0.0", env="API_HOST")
    port: int = Field(default=8000, env="API_PORT")
    debug: bool = Field(default=False, env="API_DEBUG")

    class Config:
        env_prefix = "API_"


class AppConfig(BaseSettings):
    """Основная конфигурация приложения."""

    kafka: KafkaConfig = KafkaConfig()
    redis: RedisConfig = RedisConfig()
    clickhouse: ClickHouseConfig = ClickHouseConfig()
    jaeger: JaegerConfig = JaegerConfig()
    etl: ETLConfig = ETLConfig()
    api: APIConfig = APIConfig()

    service_name: str = Field(default="cinema-analytics", env="SERVICE_NAME")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    environment: str = Field(default="development", env="ENVIRONMENT")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


config = AppConfig()