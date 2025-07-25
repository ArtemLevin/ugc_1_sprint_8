import time
import vertica_python
import pandas as pd
from typing import Iterator
from .base_loader import BaseLoader
from ..interfaces.logger_interface import LoggerInterface
from ..config import VERTICA_CONFIG, BENCHMARK_TABLE


class VerticaLoader(BaseLoader):
    """
    Загрузчик данных для Vertica.

    """

    def __init__(self, logger: LoggerInterface) -> None:
        """
        Инициализирует загрузчик Vertica.

        Args:
            logger: Экземпляр логгера
        """
        super().__init__("Vertica", logger)
        self.conn = None

    def connect(self) -> None:
        """Устанавливает соединение с Vertica."""
        self.logger.info("Connecting to Vertica...")
        conn_info = {
            "host": VERTICA_CONFIG.host,
            "port": VERTICA_CONFIG.port,
            "user": VERTICA_CONFIG.user,
            "password": VERTICA_CONFIG.password,
            "database": VERTICA_CONFIG.database,
            "autocommit": True,
        }
        self.conn = vertica_python.connect(**conn_info)
        self.logger.info("Connected to Vertica")

    def create_table(self) -> None:
        """Создает таблицу для бенчмарка в Vertica."""
        self.logger.info(f"Creating table {BENCHMARK_TABLE} in Vertica")
        with self.conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {BENCHMARK_TABLE}")
            cur.execute(
                f"""
                CREATE TABLE {BENCHMARK_TABLE} (
                    id INT,
                    name VARCHAR(255),
                    value FLOAT
                )
                """
            )
        self.logger.info(f"Table {BENCHMARK_TABLE} created successfully")

    def load_data(self, data_batches: Iterator[pd.DataFrame]) -> float:
        """
        Загружает данные в Vertica с измерением времени.


        Args:
            data_batches: Итератор с батчами данных

        Returns:
            Время загрузки в секундах
        """
        self.logger.info("Starting data load to Vertica")
        start_time = time.time()
        with self.conn.cursor() as cur:
            for i, df in enumerate(data_batches, start=1):
                # Преобразуем DataFrame в список кортежей
                tuples = [tuple(x) for x in df.to_numpy()]
                cols = ",".join(df.columns)
                placeholders = ",".join(["%s"] * len(df.columns))
                query = f"INSERT INTO {BENCHMARK_TABLE} ({cols}) VALUES ({placeholders})"
                cur.executemany(query, tuples)
                self.logger.debug(f"Loaded batch {i} to Vertica")
        elapsed = time.time() - start_time
        self.logger.info("Data load to Vertica completed")
        return elapsed

    def close(self) -> None:
        """Закрывает соединение с Vertica."""
        if self.conn:
            self.conn.close()
            self.logger.info("Vertica connection closed")