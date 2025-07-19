import asyncio
from app.core.logger import get_logger
from app.services.dlq_handler import DLQHandler

logger = get_logger(__name__)


class KafkaBuffer:
    def __init__(self, max_size=None, flush_interval=None):
        self.max_size = max_size or settings.kafka.kafka_max_batch_size
        self.flush_interval = flush_interval or 5
        self.queue = asyncio.Queue()
        self.task = None
        self.dlq_handler = None

    async def start(self, connection, topic):
        self.connection = connection
        self.topic = topic
        self.dlq_handler = DLQHandler(await self._get_redis_client())
        self.task = asyncio.create_task(self._flush_loop())

    async def _get_redis_client(self):
        redis_pool = RedisPool()
        return await redis_pool.get_client()

    async def stop(self):
        if self.task:
            self.task.cancel()
        await self._flush_queue()

    async def _flush_loop(self):
        while True:
            try:
                batch = []
                while not self.queue.empty() and len(batch) < self.max_size:
                    batch.append(await self.queue.get())

                if batch:
                    success = await self.connection.send_batch(batch, self.topic)
                    if not success:
                        logger.warning("Saving messages to DLQ", count=len(batch))
                        await self.dlq_handler.save_messages(batch)

                await asyncio.sleep(self.flush_interval)
            except asyncio.CancelledError:
                logger.info("Flushing queue on shutdown")
                await self._flush_queue()
                break
            except Exception as e:
                logger.error("Error in flush loop", error=str(e))
                await asyncio.sleep(5)

    async def _flush_queue(self):
        if self.queue.empty():
            return

        batch = []
        while not self.queue.empty():
            batch.append(await self.queue.get())

        success = await self.connection.send_batch(batch, self.topic)
        if not success:
            logger.warning("Saving remaining messages to DLQ", count=len(batch))
            await self.dlq_handler.save_messages(batch)

    async def send(self, message):
        await self.queue.put(message)