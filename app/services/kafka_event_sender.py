from app.core.logger import get_logger

logger = get_logger(__name__)

class KafkaEventSender:
    def __init__(self, kafka_producer):
        self.kafka_producer = kafka_producer

    async def send(self, message):
        await self.kafka_producer.send(message)