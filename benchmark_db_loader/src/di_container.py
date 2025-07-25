from typing import Dict, Callable
from .interfaces.db_loader_interface import DBLoaderInterface
from .interfaces.logger_interface import LoggerInterface
from .databases.clickhouse_loader import ClickhouseLoader
from .databases.postgres_loader import PostgresLoader
from .databases.vertica_loader import VerticaLoader
from .logging_config import setup_logger
from .utils.data_generator import generate_dataframe_batch


class DIContainer:
    """
    Контейнер зависимостей (Dependency Injection Container).

    Реализует паттерн Service Locator для управления
    зависимостями в приложении.
    """

    def __init__(self) -> None:
        """Инициализирует контейнер зависимостей."""
        self._logger_instances: Dict[str, LoggerInterface] = {}
        self._loader_factories: Dict[str, Callable[[LoggerInterface], DBLoaderInterface]] = {
            "ClickHouse": lambda logger: ClickhouseLoader(logger),
            "PostgreSQL": lambda logger: PostgresLoader(logger),
            "Vertica": lambda logger: VerticaLoader(logger),
        }

    def get_logger(self, name: str) -> LoggerInterface:
        """
        Получает экземпляр логгера по имени.

        Реализует singleton паттерн для логгеров.

        Args:
            name: Имя логгера

        Returns:
            Экземпляр логгера
        """
        if name not in self._logger_instances:
            self._logger_instances[name] = setup_logger(name)
        return self._logger_instances[name]

    def get_loader(self, db_name: str) -> DBLoaderInterface:
        """
        Получает загрузчик для указанной базы данных.

        Args:
            db_name: Имя базы данных

        Returns:
            Экземпляр загрузчика

        Raises:
            ValueError: Если фабрика для указанной БД не найдена
        """
        logger = self.get_logger(f"src.databases.{db_name.lower()}_loader.{db_name}")
        factory = self._loader_factories.get(db_name)
        if not factory:
            raise ValueError(f"No factory found for database: {db_name}")
        return factory(logger)

    def get_data_generator(self, rows: int):
        """
        Получает генератор данных.

        Args:
            rows: Количество строк для генерации

        Returns:
            Итератор с батчами данных
        """
        logger = self.get_logger("src.utils.data_generator")
        return generate_dataframe_batch(rows, logger)