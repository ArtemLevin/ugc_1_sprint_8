from clickhouse_driver import Client
from app.core.config import config
from app.core.logger import logger


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
        

print(config.clickhouse)
client = ClickHouseClient()
print(client.client.execute('SHOW DATABASES'))