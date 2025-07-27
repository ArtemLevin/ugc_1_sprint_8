"""
Точка входа для API сервиса.

Запускает Quart приложение с API маршрутами.
"""

import asyncio
import signal
import sys
from quart import Quart
from app.api.routes import bp
from app.kafka.producer import KafkaEventProducer
from app.core.config import config
from app.core.logger import logger
from app.core.tracer import TracerConfig

app = Quart(__name__)
app.register_blueprint(bp)

# Глобальные переменные для graceful shutdown
running: bool = True
producer: KafkaEventProducer | None = None


@app.before_serving
async def startup():
    """Выполняется перед запуском сервера."""
    global producer
    logger.info("Starting API service")
    try:
        producer = KafkaEventProducer()
        await producer.start()
        app.extensions["kafka_producer"] = producer
        logger.info("Kafka producer initialized and started")
        logger.info("API service started successfully")
    except Exception as e:
        logger.error("Failed to start API service", error=str(e))
        raise


def signal_handler(signum, frame):
    """Обработчик сигналов для graceful shutdown."""
    global running
    logger.info("Received shutdown signal", signal=signum)
    running = False
    # Запускаем остановку в отдельной таске
    asyncio.create_task(shutdown())


async def shutdown():
    """Выполняется после остановки сервера."""
    logger.info("Shutting down API service")
    try:
        if producer:
            await producer.stop()
            logger.info("Kafka producer stopped")

        logger.info("API service shut down successfully")
        sys.exit(0)
    except Exception as e:
        logger.error("Error during API service shutdown", error=str(e))
        sys.exit(1)


@app.after_serving
async def cleanup():
    """Выполняется после остановки сервера."""
    await shutdown()


def main():
    """Основная точка входа для API сервиса."""
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    tracer_config_instance = TracerConfig(config.service_name)
    logger.info("Initializing API service",
                host=config.api.host,
                port=config.api.port,
                debug=config.api.debug)

    try:
        # Запускаем Quart приложение
        app.run(
            host=config.api.host,
            port=config.api.port,
            debug=config.api.debug,
            use_reloader=True  # Отключаем reloader для корректной работы asyncio
        )
    except KeyboardInterrupt:
        logger.info("API service interrupted by user")
    except Exception as e:
        logger.error("API service crashed", error=str(e))
        raise


if __name__ == "__main__":
    main()