"""
Точка входа для API сервиса.

Запускает Quart приложение с API маршрутами.
"""

import asyncio
import signal
import sys
from quart import Quart
from .api.routes import bp
from .kafka.producer import KafkaEventProducer
from .core.config import config
from .core.logger import logger

app = Quart(__name__)
app.register_blueprint(bp)

producer = KafkaEventProducer()

# Глобальные переменные для graceful shutdown
running = True


def signal_handler(signum, frame):
    """Обработчик сигналов для graceful shutdown."""
    global running
    logger.info("Received shutdown signal", signal=signum)
    running = False
    # Запускаем остановку в отдельном таске
    asyncio.create_task(shutdown())


async def shutdown():
    """Выполняется после остановки сервера."""
    logger.info("Shutting down API service")
    try:
        await producer.stop()
        logger.info("API service shut down successfully")
        sys.exit(0)
    except Exception as e:
        logger.error("Error during API service shutdown", error=str(e))
        sys.exit(1)


@app.before_serving
async def startup():
    """Выполняется перед запуском сервера."""
    logger.info("Starting API service")
    try:
        await producer.start()
        logger.info("API service started successfully")
    except Exception as e:
        logger.error("Failed to start API service", error=str(e))
        raise


@app.after_serving
async def cleanup():
    """Выполняется после остановки сервера."""
    await shutdown()


def main():
    """Основная точка входа для API сервиса."""
    global producer

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("Initializing API service",
                host=config.api.host,
                port=config.api.port,
                debug=config.api.debug)

    # Делаем продюсера доступным в routes
    from .api.routes import bp
    bp.producer = producer

    try:
        # Запускаем Quart приложение
        app.run(
            host=config.api.host,
            port=config.api.port,
            debug=config.api.debug,
            use_reloader=False  # Отключаем reloader для корректной работы asyncio
        )
    except KeyboardInterrupt:
        logger.info("API service interrupted by user")
    except Exception as e:
        logger.error("API service crashed", error=str(e))
        raise


if __name__ == "__main__":
    main()