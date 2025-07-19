from pydantic_settings import BaseSettings
from pydantic import Field

class KafkaSettings(BaseSettings):
    kafka_bootstrap_server: str = Field(default="message-broker:9092", env="KAFKA_BOOTSTRAP_SERVER")
    kafka_topic: str = Field(default="user_events", env="KAFKA_TOPIC")
    kafka_send_timeout: int = Field(default=5, env="KAFKA_SEND_TIMEOUT")
    kafka_max_batch_size: int = Field(default=16384, env="KAFKA_MAX_BATCH_SIZE")
    kafka_request_size: int = Field(default=1048576, env="KAFKA_REQUEST_SIZE")
    kafka_flush_interval: int = Field(default=5, env="KAFKA_FLUSH_INTERVAL")

class RedisSettings(BaseSettings):
    redis_url: str = Field(default="redis://cache:6379", env="REDIS_URL")
    redis_max_connections: int = Field(default=10, env="REDIS_MAX_CONNECTIONS")
    redis_retry_attempts: int = Field(default=3, env="REDIS_RETRY_ATTEMPTS")

class DLQSettings(BaseSettings):
    dlq_redis_url: str = Field(default="redis://dlq-cache:6379", env="DLQ_REDIS_URL")
    dlq_queue_key: str = Field(default="dlq_kafka", env="DLQ_QUEUE_KEY")

class RateLimitSettings(BaseSettings):
    rate_limit: int = Field(default=100, env="RATE_LIMIT")
    rate_limit_window: int = Field(default=60, env="RATE_LIMIT_WINDOW")

class TracingSettings(BaseSettings):
    otel_exporter_otlp_endpoint: str = Field(
        default="http://tracing:4318/v1/traces",
        env="OTEL_EXPORTER_OTLP_ENDPOINT"
    )

class AppSettings(BaseSettings):
    app_env: str = Field(default="development", env="APP_ENV")
    app_debug: bool = Field(default=False, env="APP_DEBUG")
    app_name: str = Field(default="UserActionCollector", env="APP_NAME")

class Settings:
    def __init__(self):
        self.kafka = KafkaSettings()
        self.redis = RedisSettings()
        self.dlq = DLQSettings()
        self.rate_limit = RateLimitSettings()
        self.tracing = TracingSettings()
        self.app = AppSettings()

settings = Settings()