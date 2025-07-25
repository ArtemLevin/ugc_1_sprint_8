import time
from clickhouse_driver import Client
import pandas as pd
from typing import Iterator
from .base_loader import BaseLoader
from ..interfaces.logger_interface import LoggerInterface
from ..config import CLICKHOUSE_CONFIG, BENCHMARK_TABLE


class ClickhouseLoader(BaseLoader):
    """
    Загрузчик данных для ClickHouse.

    Использует оптимизированный метод insert_dataframe для
    максимальной производительности загрузки больших объемов данных.
    """

    def __init__(self, logger: LoggerInterface) -> None:
        """
        Инициализирует загрузчик ClickHouse.

        Args:
            logger: Экземпляр логгера
        """
        super().__init__("ClickHouse", logger)
        self.client = None

    def connect(self) -> None:
        """Устанавливает соединение с ClickHouse."""
        self.logger.info("Connecting to ClickHouse...")
        self.client = Client(
            host=CLICKHOUSE_CONFIG.host,
            port=CLICKHOUSE_CONFIG.port,
            user=CLICKHOUSE_CONFIG.user,
            password=CLICKHOUSE_CONFIG.password,
            database=CLICKHOUSE_CONFIG.database,
        )
        self.logger.info("Connected to ClickHouse")

    def create_table(self) -> None:
        """Создает таблицу для бенчмарка в ClickHouse."""
        self.logger.info(f"Creating table {BENCHMARK_TABLE} in ClickHouse")
        self.client.execute(f"DROP TABLE IF EXISTS {BENCHMARK_TABLE}")
        self.client.execute(
            f"""
            CREATE TABLE {BENCHMARK_TABLE} (
                id UInt32,
                name String,
                value Float64
            ) ENGINE = MergeTree()
            ORDER BY id
            """
        )
        self.logger.info(f"Table {BENCHMARK_TABLE} created successfully")

    def load_data(self, data_batches: Iterator[pd.DataFrame]) -> float:
        """
        Загружает данные в ClickHouse с использованием insert_dataframe.

        ClickHouse driver предоставляет оптимизированный метод
        insert_dataframe, который значительно быстрее executemany.

        Args:
            data_batches: Итератор с батчами данных

        Returns:
            Время загрузки в секундах
        """
        self.logger.info("Starting data load to ClickHouse")
        start_time = time.time()
        for i, df in enumerate(data_batches, start=1):
            # Используем оптимизированный метод ClickHouse
            self.client.insert_dataframe(
                f"INSERT INTO {BENCHMARK_TABLE} VALUES", df
            )
            self.logger.debug(f"Loaded batch {i} to ClickHouse")
        elapsed = time.time() - start_time
        self.logger.info("Data load to ClickHouse completed")
        return elapsed

    def close(self) -> None:
        """Закрывает соединение с ClickHouse."""
        self.logger.info("ClickHouse client closed")