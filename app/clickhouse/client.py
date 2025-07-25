"""
ClickHouse клиент для работы с аналитической базой данных.

Обеспечивает создание таблиц, вставку данных и обработку ошибок.
"""

from clickhouse_driver import Client
from typing import List, Tuple, Any, Optional
from ..core.config import config
from ..core.logger import logger
from .models import TableInfo, QueryResult


class ClickHouseClient:
    """
    Клиент для работы с ClickHouse.

    Обеспечивает подключение, создание таблиц и вставку данных.
    """

    def __init__(self):
        """Инициализирует клиент ClickHouse."""
        self.client = Client(
            host=config.clickhouse.host,
            port=config.clickhouse.port,
            database=config.clickhouse.database,
            user=config.clickhouse.user,
            password=config.clickhouse.password
        )
        self.table_name = config.clickhouse.table
        logger.info("ClickHouse client initialized",
                    host=config.clickhouse.host,
                    port=config.clickhouse.port,
                    database=config.clickhouse.database)

    def create_table(self) -> None:
        """
        Создает таблицу для хранения пользовательских событий.

        Использует MergeTree движок для оптимизации аналитических запросов.
        """
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            user_id String,
            movie_id String,
            event_type String,
            timestamp DateTime,
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, user_id)
        SETTINGS index_granularity = 8192
        """

        try:
            self.client.execute(create_table_query)
            logger.info("ClickHouse table created or already exists", table=self.table_name)
        except Exception as e:
            logger.error("Failed to create ClickHouse table",
                         table=self.table_name,
                         error=str(e))
            raise

    def insert_batch(self, events: List[Tuple[Any, ...]]) -> None:
        """
        Вставляет пакет событий в таблицу.

        Args:
            events: Список кортежей с данными событий
                   (user_id, movie_id, event_type, timestamp)

        Raises:
            Exception: При ошибке вставки данных
        """
        if not events:
            logger.warning("Attempted to insert empty batch")
            return

        insert_query = f"""
        INSERT INTO {self.table_name} 
        (user_id, movie_id, event_type, timestamp) 
        VALUES
        """

        try:
            # Выполняем вставку пакета данных
            result = self.client.execute(insert_query, events)
            logger.info("Batch inserted to ClickHouse",
                        count=len(events),
                        result=str(result))
        except Exception as e:
            logger.error("Failed to insert batch to ClickHouse",
                         count=len(events),
                         error=str(e))
            raise

    def test_connection(self) -> bool:
        """
        Проверяет соединение с ClickHouse.

        Returns:
            bool: True если соединение успешно, False в противном случае
        """
        try:
            self.client.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error("ClickHouse connection failed", error=str(e))
            return False

    def get_table_info(self) -> TableInfo:
        """
        Получает информацию о таблице.

        Returns:
            TableInfo: Информация о таблице
        """
        try:
            result = self.client.execute(f"""
                SELECT 
                    count() as row_count,
                    min(timestamp) as first_event,
                    max(timestamp) as last_event
                FROM {self.table_name}
            """)
            if result:
                return TableInfo(
                    row_count=int(result[0][0]),
                    first_event=str(result[0][1]) if result[0][1] else None,
                    last_event=str(result[0][2]) if result[0][2] else None
                )
            return TableInfo(row_count=0, first_event="", last_event="")
        except Exception as e:
            logger.error("Failed to get table info", error=str(e))
            return TableInfo(row_count=0, first_event="", last_event="")

    def execute_query(self, query: str, params: Optional[List] = None) -> QueryResult:
        """
        Выполняет произвольный SQL запрос.

        Args:
            query: SQL запрос
            params: Параметры запроса

        Returns:
            QueryResult: Результат запроса
        """
        try:
            if params:
                result = self.client.execute(query, params)
            else:
                result = self.client.execute(query)

            # Получаем информацию о колонках
            columns = [column[0] for column in self.client.execute("DESCRIBE TABLE " + self.table_name)]

            return QueryResult(rows=result, columns=columns)
        except Exception as e:
            logger.error("Failed to execute query", query=query, error=str(e))
            raise