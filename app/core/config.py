from pydantic_settings import BaseSettings
from pydantic import Field, field_validator


class Settings(BaseSettings):
    environment: str = "development"
    app_name: str = "User Action Collector"
    debug: bool = False

    kafka_bootstrap_server: str = Field(..., env="KAFKA_BOOTSTRAP_SERVER")
    kafka_topic: str = Field("user_events", env="KAFKA_TOPIC")
    kafka_send_timeout: int = Field(5, env="KAFKA_SEND_TIMEOUT")
    kafka_max_batch_size: int = Field(16384, env="KAFKA_MAX_BATCH_SIZE")
    kafka_request_size: int = Field(1048576, env="KAFKA_REQUEST_SIZE")

    redis_url: str = Field("redis://redis:6379", env="REDIS_URL")
    redis_max_connections: int = Field(10, env="REDIS_MAX_CONNECTIONS")
    dlq_redis_url: str = Field("redis://dlq_redis:6379", env="DLQ_REDIS_URL")
    dlq_queue_key: str = Field("dlq_kafka", env="DLQ_QUEUE_KEY")

    rate_limit: int = Field(100, env="RATE_LIMIT")
    rate_limit_window: int = Field(60, env="RATE_LIMIT_WINDOW")  # seconds

    otel_exporter_otlp_endpoint: str = Field("http://jaeger:4318/v1/traces", env="OTEL_EXPORTER_OTLP_ENDPOINT")

    @field_validator("redis_url", mode="after")
    def validate_redis_url(cls, v):
        if not v.startswith("redis://"):
            raise ValueError("redis_url должен начинаться с redis://")
        return v

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()