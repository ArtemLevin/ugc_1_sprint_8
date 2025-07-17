import aiokafka
from aiokafka import AIOKafkaProducer
import json
import asyncio
from app.core.config import settings
from app.core.logger import get_logger

logger = get_logger(__name__)

class KafkaProducerPool:
    _producer = None

    async def get_producer(self):
        if not self._producer:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_server,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                enable_idempotence=True,
                max_request_size=settings.kafka_request_size
            )
            try:
                await self._producer.start()
                logger.info("Kafka producer started")
            except Exception as e:
                logger.error("Failed to start Kafka producer", error=str(e))
                raise
        return self._producer

    async def stop_producer(self):
        if self._producer:
            await self._producer.stop()
            logger.info("Kafka producer stopped")

kafka_pool = KafkaProducerPool()

class BufferedKafkaProducer:
    def __init__(self, buffer_size=100, flush_interval=5):
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        self.queue = asyncio.Queue()
        self.task = None

    async def start(self):
        self.task = asyncio.create_task(self._flush_loop())

    async def stop(self):
        if self.task:
            self.task.cancel()
        await kafka_pool.stop_producer()

    async def _flush_loop(self):
        while True:
            batch = []
            while len(batch) < self.buffer_size and not self.queue.empty():
                batch.append(await self.queue.get())
            if batch:
                try:
                    producer = await kafka_pool.get_producer()
                    await asyncio.wait_for(
                        producer.send_batch(
                            messages=[json.dumps(msg).encode("utf-8") for msg in batch],
                            topic=settings.kafka_topic
                        ),
                        timeout=settings.kafka_send_timeout
                    )
                    logger.debug(f"Sent {len(batch)} events to Kafka")
                except asyncio.TimeoutError:
                    logger.error("Kafka send timeout")
                    await self._save_to_dlq(batch)
                except Exception as e:
                    logger.error("Kafka send error", error=str(e))
                    await self._save_to_dlq(batch)
            await asyncio.sleep(self.flush_interval)

    async def _save_to_dlq(self, messages):
        from app.services.dlq_handler import DLQHandler
        handler = DLQHandler()
        await handler.save_messages(messages)

    async def send(self, message):
        await self.queue.put(message)