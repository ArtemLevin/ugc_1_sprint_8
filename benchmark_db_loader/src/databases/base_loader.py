from abc import ABC
from ..interfaces.db_loader_interface import DBLoaderInterface
from ..interfaces.logger_interface import LoggerInterface


class BaseLoader(DBLoaderInterface, ABC):
    """
    Базовый абстрактный класс для загрузчиков данных.

    """

    def __init__(self, db_name: str, logger: LoggerInterface) -> None:
        """
        Инициализирует базовый загрузчик.

        Args:
            db_name: Имя базы данных для логирования
            logger: Экземпляр логгера, реализующий LoggerInterface
        """
        self.db_name = db_name
        self.logger = logger

    def log_start(self) -> None:
        """Логирует начало бенчмарка для конкретной БД."""
        self.logger.info(f"Starting benchmark for {self.db_name}")

    def log_completion(self, elapsed: float) -> None:
        """Логирует завершение бенчмарка для конкретной БД."""
        self.logger.info(f"{self.db_name} benchmark completed in {elapsed:.2f} seconds")