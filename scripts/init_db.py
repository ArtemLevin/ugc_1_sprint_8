#!/usr/bin/env python3
"""
Скрипт инициализации базы данных ClickHouse.

Создает необходимые таблицы и проверяет соединение.
"""

import sys
import os

# Добавляем корневую директорию в путь для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.clickhouse.client import ClickHouseClient
from app.core.logger import logger


def main():
    """Основная функция инициализации базы данных."""
    print("Initializing ClickHouse database...")

    try:
        client = ClickHouseClient()

        if not client.test_connection():
            print("Error: Failed to connect to ClickHouse")
            sys.exit(1)

        client.create_table()

        table_info = client.get_table_info()
        print("Database initialization completed successfully!")
        print(
            f"Table info: row_count={table_info.row_count}, first_event={table_info.first_event}, last_event={table_info.last_event}")

    except Exception as e:
        logger.error("Database initialization failed", error=str(e))
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()