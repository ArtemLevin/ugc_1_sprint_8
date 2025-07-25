import time
import psycopg2
import pandas as pd
from typing import Iterator
from .base_loader import BaseLoader
from ..interfaces.logger_interface import LoggerInterface
from ..config import POSTGRES_CONFIG, BENCHMARK_TABLE


class PostgresLoader(BaseLoader):
    """
    Загрузчик данных для PostgreSQL.

    Реализует загрузку больших объемов данных с использованием
    batch-обработки и executemany для оптимизации производительности.
    """

    def __init__(self, logger: LoggerInterface) -> None:
        """
        Инициализирует загрузчик PostgreSQL.

        Args:
            logger: Экземпляр логгера
        """
        super().__init__("PostgreSQL", logger)
        self.conn = None

    def connect(self) -> None:
        """Устанавливает соединение с PostgreSQL."""
        self.logger.info("Connecting to PostgreSQL...")
        self.conn = psycopg2.connect(
            host=POSTGRES_CONFIG.host,
            port=POSTGRES_CONFIG.port,
            user=POSTGRES_CONFIG.user,
            password=POSTGRES_CONFIG.password,
            dbname=POSTGRES_CONFIG.database,
        )
        self.conn.autocommit = True
        self.logger.info("Connected to PostgreSQL")

    def create_table(self) -> None:
        """Создает таблицу для бенчмарка в PostgreSQL."""
        self.logger.info(f"Creating table {BENCHMARK_TABLE} in PostgreSQL")
        with self.conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {BENCHMARK_TABLE}")
            cur.execute(
                f"""
                CREATE TABLE {BENCHMARK_TABLE} (
                    id INTEGER,
                    name TEXT,
                    value FLOAT
                )
                """
            )
        self.logger.info(f"Table {BENCHMARK_TABLE} created successfully")

    def load_data(self, data_batches: Iterator[pd.DataFrame]) -> float:
        """
        Загружает данные в PostgreSQL с измерением времени.

        Использует executemany для batch-загрузки, что оптимизирует
        производительность по сравнению с построчной вставкой.

        Args:
            data_batches: Итератор с батчами данных

        Returns:
            Время загрузки в секундах
        """
        self.logger.info("Starting data load to PostgreSQL")
        start_time = time.time()
        with self.conn.cursor() as cur:
            for i, df in enumerate(data_batches, start=1):
                # Преобразуем DataFrame в список кортежей для executemany
                tuples = [tuple(x) for x in df.to_numpy()]
                cols = ",".join(df.columns)
                placeholders = ",".join(["%s"] * len(df.columns))
                query = f"INSERT INTO {BENCHMARK_TABLE} ({cols}) VALUES ({placeholders})"
                cur.executemany(query, tuples)
                self.logger.debug(f"Loaded batch {i} to PostgreSQL")
        elapsed = time.time() - start_time
        self.logger.info("Data load to PostgreSQL completed")
        return elapsed

    def close(self) -> None:
        """Закрывает соединение с PostgreSQL."""
        if self.conn:
            self.conn.close()
            self.logger.info("PostgreSQL connection closed")