"""
Runner для запуска ETL процесса.

Управляет жизненным циклом ETL процессора.
"""

import asyncio
from .processor import ETLProcessor
from ..core.logger import logger


class ETLRunner:
    """
    Runner для управления ETL процессом.

    Обеспечивает запуск, остановку и мониторинг ETL процессора.
    """

    def __init__(self):
        """Инициализирует ETL runner."""
        self.processor = ETLProcessor()
        self.running = False
        logger.info("ETL runner initialized")

    async def run(self) -> None:
        """
        Запускает ETL процесс.

        Выполняет основной цикл обработки событий.
        """
        self.running = True
        logger.info("Starting ETL runner")

        try:
            await self.processor.start()

            # Запускаем основной цикл обработки
            await self.processor.process()

        except Exception as e:
            logger.error("ETL runner failed", error=str(e))
            raise
        finally:
            await self.stop()

    def stop(self) -> None:
        """
        Останавливает ETL процесс.

        Выполняет корректную остановку всех компонентов.
        """
        if not self.running:
            return

        self.running = False
        logger.info("Stopping ETL runner")

        try:
            # Создаем таск для асинхронной остановки
            asyncio.create_task(self._async_stop())
        except Exception as e:
            logger.error("Error initiating ETL runner shutdown", error=str(e))

    async def _async_stop(self) -> None:
        """Асинхронная остановка ETL процессора."""
        try:
            await self.processor.stop()
            logger.info("ETL runner stopped successfully")
        except Exception as e:
            logger.error("Error during ETL runner shutdown", error=str(e))