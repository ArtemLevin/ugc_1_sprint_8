import os
from dataclasses import dataclass
from dotenv import load_dotenv
from .logging_config import setup_logger

load_dotenv()

logger = setup_logger(__name__)


@dataclass
class DBConfig:
    """
    Конфигурация подключения к базе данных.

    Attributes:
        host: Хост базы данных
        port: Порт базы данных
        user: Пользователь базы данных
        password: Пароль пользователя
        database: Имя базы данных
    """
    host: str
    port: int
    user: str
    password: str
    database: str

    def __post_init__(self) -> None:
        """Логирует инициализацию конфигурации."""
        logger.debug(f"Database config initialized: {self}")


def get_env_var(name: str, default: str = None) -> str:
    """
    Получает переменную окружения с проверкой наличия.

    Args:
        name: Имя переменной окружения
        default: Значение по умолчанию

    Returns:
        Значение переменной окружения

    Raises:
        ValueError: Если переменная не найдена и нет дефолтного значения
    """
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Environment variable {name} is not set")
    return value


def get_env_int(name: str, default: int = None) -> int:
    """
    Получает целочисленную переменную окружения.

    Args:
        name: Имя переменной окружения
        default: Значение по умолчанию

    Returns:
        Целочисленное значение переменной окружения

    Raises:
        ValueError: Если значение не может быть преобразовано в int
    """
    value = get_env_var(name, str(default) if default is not None else None)
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"Environment variable {name} must be an integer")


CLICKHOUSE_CONFIG = DBConfig(
    host=get_env_var("CLICKHOUSE_HOST"),
    port=get_env_int("CLICKHOUSE_PORT"),
    user=get_env_var("CLICKHOUSE_USER"),
    password=get_env_var("CLICKHOUSE_PASSWORD"),
    database=get_env_var("CLICKHOUSE_DATABASE"),
)

POSTGRES_CONFIG = DBConfig(
    host=get_env_var("POSTGRES_HOST"),
    port=get_env_int("POSTGRES_PORT"),
    user=get_env_var("POSTGRES_USER"),
    password=get_env_var("POSTGRES_PASSWORD"),
    database=get_env_var("POSTGRES_DATABASE"),
)

VERTICA_CONFIG = DBConfig(
    host=get_env_var("VERTICA_HOST"),
    port=get_env_int("VERTICA_PORT"),
    user=get_env_var("VERTICA_USER"),
    password=get_env_var("VERTICA_PASSWORD"),
    database=get_env_var("VERTICA_DATABASE"),
)

BENCHMARK_TABLE = get_env_var("BENCHMARK_TABLE")
DATA_ROWS = get_env_int("DATA_ROWS")
BATCH_SIZE = get_env_int("BATCH_SIZE")
LOG_LEVEL = get_env_var("LOG_LEVEL", "INFO")

logger.info("Configuration loaded successfully from environment variables")