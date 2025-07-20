import json
from aiokafka import AIOKafkaProducer
from app.core.config import settings
from app.core.logger import get_logger
from app.utils.retry import RetryHandler
from typing import List, Dict, Any
logger = get_logger(__name__)
retry_handler = RetryHandler(max_retries=3, base_delay=1, max_delay=10)

class KafkaConnection:
    def __init__(
        self,
        bootstrap_server: str | None= None,
        max_request_size: int |None = None
    ):
        self.bootstrap_server = bootstrap_server or settings.kafka.kafka_bootstrap_server
        self.max_request_size = max_request_size or settings.kafka.kafka_request_size
        self.producer = None
        self.connected = False

    @retry_handler
    async def connect(self) -> None:
        try:
            logger.info("Connecting to Kafka", bootstrap_server=self.bootstrap_server)
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_server,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                enable_idempotence=True,
                max_request_size=self.max_request_size
            )
            await self.producer.start()
            self.connected = True
            logger.info("Successfully connected to Kafka")
        except Exception as e:
            logger.error("Failed to connect to Kafka", error=str(e))
            self.connected = False
            raise

    async def disconnect(self) -> None:
        if self.producer:
            await self.producer.stop()
            self.connected = False
            logger.info("Disconnected from Kafka")

    @retry_handler
    async def send_batch(self, batch: List[Dict[str, Any]], topic: str) -> Any | None:
        if not self.connected:
            await self.connect()
        try:
            logger.debug("Sending batch to Kafka", size=len(batch), topic=topic)
            future = await self.producer.send_batch(
                [(json.dumps(msg).encode('utf-8'), None) for msg in batch],
                topic=topic
            )
            metadata = await future
            logger.info("Batch sent to Kafka", size=len(batch), topic=topic, offset=metadata.base_offset)
            return metadata
        except Exception as e:
            logger.error("Kafka send error", error=str(e), topic=topic, batch_size=len(batch))
            raise