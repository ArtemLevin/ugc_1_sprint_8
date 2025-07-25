import time
import socket
import psycopg2
import vertica_python
from clickhouse_driver import Client
from ..config import (
    CLICKHOUSE_CONFIG, POSTGRES_CONFIG, VERTICA_CONFIG
)
from ..logging_config import setup_logger

logger = setup_logger(__name__)


def check_port(host: str, port: int, timeout: int = 5) -> bool:
    """
    Проверяет доступность порта на указанном хосте.

    Args:
        host: Хост для проверки
        port: Порт для проверки
        timeout: Таймаут в секундах

    Returns:
        True если порт доступен, False в противном случае
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            return result == 0
    except Exception as e:
        logger.debug(f"Port check failed for {host}:{port} - {e}")
        return False


def check_clickhouse() -> bool:
    """
    Проверяет функциональную готовность ClickHouse.

    Returns:
        True если ClickHouse готов принимать запросы, False в противном случае
    """
    try:
        client = Client(
            host=CLICKHOUSE_CONFIG.host,
            port=CLICKHOUSE_CONFIG.port,
            user=CLICKHOUSE_CONFIG.user,
            password=CLICKHOUSE_CONFIG.password,
            database=CLICKHOUSE_CONFIG.database,
        )
        client.execute("SELECT 1")
        return True
    except Exception as e:
        logger.debug(f"ClickHouse check failed: {e}")
        return False


def check_postgres() -> bool:
    """
    Проверяет функциональную готовность PostgreSQL.

    Returns:
        True если PostgreSQL готов принимать запросы, False в противном случае
    """
    try:
        conn = psycopg2.connect(
            host=POSTGRES_CONFIG.host,
            port=POSTGRES_CONFIG.port,
            user=POSTGRES_CONFIG.user,
            password=POSTGRES_CONFIG.password,
            dbname=POSTGRES_CONFIG.database,
            connect_timeout=5
        )
        conn.close()
        return True
    except Exception as e:
        logger.debug(f"PostgreSQL check failed: {e}")
        return False


def check_vertica() -> bool:
    """
    Проверяет функциональную готовность Vertica.

    Returns:
        True если Vertica готова принимать запросы, False в противном случае
    """
    try:
        conn_info = {
            'host': VERTICA_CONFIG.host,
            'port': VERTICA_CONFIG.port,
            'user': VERTICA_CONFIG.user,
            'password': VERTICA_CONFIG.password,
            'database': VERTICA_CONFIG.database,
            'connection_timeout': 5
        }
        with vertica_python.connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
        return True
    except Exception as e:
        logger.debug(f"Vertica check failed: {e}")
        return False


def wait_for_database(
        name: str,
        host: str,
        port: int,
        check_function,
        max_attempts: int = 30,
        delay: int = 2
) -> bool:
    """
    Ждет готовности конкретной базы данных.

    Args:
        name: Имя базы данных для логирования
        host: Хост базы данных
        port: Порт базы данных
        check_function: Функция для проверки готовности
        max_attempts: Максимальное количество попыток
        delay: Задержка между попытками в секундах

    Returns:
        True если база данных готова, False в противном случае
    """
    logger.info(f"Waiting for {name} to be ready at {host}:{port}...")

    for attempt in range(1, max_attempts + 1):
        if check_port(host, port):
            logger.debug(f"Port {port} on {host} is open")
            if check_function():
                logger.info(f"{name} is ready!")
                return True
            else:
                logger.debug(f"{name} port is open but service is not ready yet")
        else:
            logger.debug(f"Port {port} on {host} is not open yet")

        if attempt < max_attempts:
            logger.debug(f"Attempt {attempt}/{max_attempts}. Retrying in {delay} seconds...")
            time.sleep(delay)

    logger.error(f"{name} failed to become ready after {max_attempts} attempts")
    return False


def wait_for_all_databases(max_attempts: int = 30, delay: int = 2) -> bool:
    """
    Ждет готовности всех баз данных.

    Args:
        max_attempts: Максимальное количество попыток для каждой БД
        delay: Задержка между попытками в секундах

    Returns:
        True если все базы данных готовы, False в противном случае
    """
    logger.info("Starting database readiness checks...")

    databases = [
        ("ClickHouse", CLICKHOUSE_CONFIG.host, CLICKHOUSE_CONFIG.port, check_clickhouse),
        ("PostgreSQL", POSTGRES_CONFIG.host, POSTGRES_CONFIG.port, check_postgres),
        ("Vertica", VERTICA_CONFIG.host, VERTICA_CONFIG.port, check_vertica),
    ]

    all_ready = True

    for name, host, port, check_func in databases:
        if not wait_for_database(name, host, port, check_func, max_attempts, delay):
            all_ready = False

    if all_ready:
        logger.info("All databases are ready!")
    else:
        logger.error("Some databases failed to become ready")

    return all_ready