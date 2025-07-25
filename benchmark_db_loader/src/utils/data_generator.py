import pandas as pd
import numpy as np
from typing import Iterator, Tuple
from ..config import BATCH_SIZE
from ..interfaces.logger_interface import LoggerInterface


def generate_data(rows: int, logger: LoggerInterface) -> Iterator[Tuple[int, str, float]]:
    """
    Генерирует тестовые данные для бенчмарка.

    Args:
        rows: Количество строк для генерации
        logger: Экземпляр логгера

    Yields:
        Кортежи (id, name, value) для каждой строки
    """
    logger.info(f"Generating {rows} rows of test data...")
    for i in range(rows):
        yield (
            i,
            f"user_{i}",
            np.random.rand(),
        )
    logger.info("Data generation completed.")


def generate_dataframe_batch(rows: int, logger: LoggerInterface) -> Iterator[pd.DataFrame]:
    """
    Генерирует батчи данных в формате DataFrame.

    Использует ленивую генерацию для эффективного использования памяти
    при работе с большими объемами данных.

    Args:
        rows: Общее количество строк
        logger: Экземпляр логгера

    Yields:
        DataFrame с батчами данных
    """
    logger.info(f"Generating DataFrame batches of size {BATCH_SIZE}...")
    batch_count = 0
    for start in range(0, rows, BATCH_SIZE):
        end = min(start + BATCH_SIZE, rows)
        data = list(generate_data(end - start, logger))
        df = pd.DataFrame(data, columns=["id", "name", "value"])
        batch_count += 1
        logger.debug(f"Generated batch {batch_count} with {len(df)} rows")
        yield df
    logger.info(f"Total batches generated: {batch_count}")