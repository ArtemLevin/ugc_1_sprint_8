from pydantic_settings import BaseSettings
from pydantic import Field

class KafkaSettings(BaseSettings):
    kafka_bootstrap_server: str = Field(..., env="KAFKA_BOOTSTRAP_SERVER")
    kafka_topic: str = Field("user_events", env="KAFKA_TOPIC")
    kafka_send_timeout: int = Field(5, env="KAFKA_SEND_TIMEOUT")
    kafka_max_batch_size: int = Field(16384, env="KAFKA_MAX_BATCH_SIZE")

class RedisSettings(BaseSettings):
    redis_url: str = Field("redis://redis:6379", env="REDIS_URL")
    redis_max_connections: int = Field(10, env="REDIS_MAX_CONNECTIONS")

class RateLimitSettings(BaseSettings):
    rate_limit: int = Field(100, env="RATE_LIMIT")
    rate_limit_window: int = Field(60, env="RATE_LIMIT_WINDOW")

class Settings(BaseSettings):
    environment: str = "development"
    app_name: str = "User Action Collector"
    debug: bool = False

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()