import aiokafka
from aiokafka import AIOKafkaProducer
from app.core.logger import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential

logger = get_logger(__name__)


class KafkaConnection:
    def __init__(self, bootstrap_server=None):
        self.bootstrap_server = bootstrap_server or settings.kafka.kafka_bootstrap_server
        self.producer = None
        self.connected = False

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    async def connect(self):
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_server,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                enable_idempotence=True,
                max_request_size=settings.kafka.kafka_request_size
            )
            await self.producer.start()
            self.connected = True
            logger.info("Connected to Kafka")
        except Exception as e:
            logger.error("Failed to connect to Kafka", error=str(e))
            self.connected = False
            raise

    async def disconnect(self):
        if self.producer:
            await self.producer.stop()
            self.connected = False
            logger.info("Disconnected from Kafka")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    async def send_batch(self, batch, topic):
        if not self.connected:
            await self.connect()

        try:
            future = await self.producer.send_batch(
                [(msg, None) for msg in batch],
                topic=topic
            )
            metadata = await future
            logger.debug(f"Sent {len(batch)} events to Kafka",
                         topic=topic,
                         partition=metadata.partition,
                         offset=metadata.offset)
            return True
        except Exception as e:
            logger.error("Kafka send error", error=str(e))
            return False