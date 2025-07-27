from clickhouse_driver import Client

from ..core.config import config
from ..core.logger import logger

EVENT_TABLES_SQL = {
    "click_events": f"""
        CREATE TABLE IF NOT EXISTS events.click_events ON CLUSTER {config.clickhouse.database}
            user_id String,
            event_id String,
            timestamp DateTime64,
            target String
        ) 
        ENGINE = MergeTree()
        ORDER BY (timestamp, user_id)
    """,
    "page_view_events": """
        CREATE TABLE IF NOT EXISTS page_view_events (
            user_id String,
            event_id String,
            timestamp DateTime64,
            page String,
            duration_seconds UInt32
        ) ENGINE = MergeTree()
        ORDER BY timestamp
    """,
    "quality_change_events": """
        CREATE TABLE IF NOT EXISTS quality_change_events (
            user_id String,
            event_id String,
            timestamp DateTime64,
            from_quality String,
            to_quality String,
            movie_id String
        ) ENGINE = MergeTree()
        ORDER BY timestamp
    """,
    "watched_to_end_events": """
        CREATE TABLE IF NOT EXISTS watched_to_end_events (
            user_id String,
            event_id String,
            timestamp DateTime64,
            movie_id String
        ) ENGINE = MergeTree()
        ORDER BY timestamp
    """,
    "filter_used_events": """
        CREATE TABLE IF NOT EXISTS filter_used_events (
            user_id String,
            event_id String,
            timestamp DateTime64,
            filter_name String,
            filter_value String
        ) ENGINE = MergeTree()
        ORDER BY timestamp
    """
}


def create_clickhouse_tables():
    client = Client(
        host=config.clickhouse.host,
        port=config.clickhouse.port,
        database=config.clickhouse.database,
        user=config.clickhouse.user,
        password=config.clickhouse.password
    )
    logger.info("ClickHouse client initialized",
                    host=config.clickhouse.host,
                    port=config.clickhouse.port,
                    database=config.clickhouse.database)
    
    client.execute(
        f'CREATE DATABASE IF NOT EXISTS example ON CLUSTER {config.clickhouse.database}'
    )
    logger.info("ClickHouse database created", database=config.clickhouse.database)

    for table_name, sql in EVENT_TABLES_SQL.items():
        client.command(sql)
        print(f"[+] Created or verified: {table_name}")


if __name__ == "__main__":
    create_clickhouse_tables()
