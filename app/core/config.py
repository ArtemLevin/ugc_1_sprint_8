from pydantic_settings import BaseSettings
from pydantic import Field, BaseModel

class KafkaSettings(BaseModel):
    bootstrap_server: str = Field(default="message-broker:9092", validation_alias="KAFKA_BOOTSTRAP_SERVER")
    topic: str = Field(default="user_events", validation_alias="KAFKA_TOPIC")
    send_timeout: int = Field(default=5, validation_alias="KAFKA_SEND_TIMEOUT")
    max_batch_size: int = Field(default=16384, validation_alias="KAFKA_MAX_BATCH_SIZE")
    request_size: int = Field(default=1048576, validation_alias="KAFKA_REQUEST_SIZE")
    flush_interval: int = Field(default=5, validation_alias="KAFKA_FLUSH_INTERVAL")

class RedisSettings(BaseModel):
    url: str = Field(default="redis://cache:6379", validation_alias="REDIS_URL")
    max_connections: int = Field(default=10, validation_alias="REDIS_MAX_CONNECTIONS")
    retry_attempts: int = Field(default=3, validation_alias="REDIS_RETRY_ATTEMPTS")

class DLQSettings(BaseModel):
    redis_url: str = Field(default="redis://dlq-cache:6379", validation_alias="DLQ_REDIS_URL")
    queue_key: str = Field(default="dlq_kafka", validation_alias="DLQ_QUEUE_KEY")

class RateLimitSettings(BaseModel):
    rate_limit: int = Field(default=100, validation_alias="RATE_LIMIT")
    window_seconds: int = Field(default=60, validation_alias="RATE_LIMIT_WINDOW")

class TracingSettings(BaseModel):
    otlp_endpoint: str = Field(default="http://tracing:4318/v1/traces", validation_alias="OTEL_EXPORTER_OTLP_ENDPOINT")

class AppSettings(BaseModel):
    environment: str = Field(default="development", validation_alias="APP_ENV")
    debug: bool = Field(default=False, validation_alias="APP_DEBUG")
    name: str = Field(default="UserActionCollector", validation_alias="APP_NAME")

class Settings(BaseSettings):
    model_config = ConfigDict(env_nested_delimiter="__", extra="ignore")

    kafka: KafkaSettings = KafkaSettings()
    redis: RedisSettings = RedisSettings()
    dlq: DLQSettings = DLQSettings()
    rate_limit: RateLimitSettings = RateLimitSettings()
    tracing: TracingSettings = TracingSettings()
    app: AppSettings = AppSettings()

settings = Settings()