"""
Точка входа для ETL процесса.

Запускает процесс чтения из Kafka и записи в ClickHouse.
"""

import asyncio
import signal
import sys
from .etl.runner import ETLRunner
from .core.logger import logger
from .clickhouse.client import ClickHouseClient

# Глобальные переменные для graceful shutdown
running = True
etl_runner: ETLRunner = None


def signal_handler(signum, frame):
    """Обработчик сигналов для graceful shutdown."""
    global running
    logger.info("Received shutdown signal", signal=signum)
    running = False
    if etl_runner:
        etl_runner.stop()


def main():
    """Основная точка входа для ETL процесса."""
    global etl_runner

    # Регистрируем обработчики сигналов
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("Initializing ETL service")

    try:
        # Проверяем соединение с ClickHouse
        clickhouse_client = ClickHouseClient()
        if not clickhouse_client.test_connection():
            logger.error("Failed to connect to ClickHouse")
            sys.exit(1)

        clickhouse_client.create_table()

        # Инициализируем и запускаем ETL процесс
        etl_runner = ETLRunner()
        logger.info("Starting ETL process")

        # Запускаем ETL процесс
        asyncio.run(etl_runner.run())

    except KeyboardInterrupt:
        logger.info("ETL service interrupted by user")
    except Exception as e:
        logger.error("ETL service crashed", error=str(e), exc_info=True)
        sys.exit(1)
    finally:
        logger.info("ETL service shutdown complete")


if __name__ == "__main__":
    main()