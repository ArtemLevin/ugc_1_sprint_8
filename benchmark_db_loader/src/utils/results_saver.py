import csv
import os
from datetime import datetime
from typing import Dict, Any
from ..config import DATA_ROWS, BATCH_SIZE
from ..logging_config import setup_logger

logger = setup_logger(__name__)


class ResultsSaver:
    """
    Сохраняет результаты бенчмарка в CSV файл.

    Предоставляет функциональность для сохранения,
    анализа и получения последних результатов.
    """

    def __init__(self, results_dir: str = "results") -> None:
        """
        Инициализирует сохранялку результатов.

        Args:
            results_dir: Директория для сохранения результатов
        """
        self.results_dir = results_dir
        self.ensure_results_directory()

    def ensure_results_directory(self) -> None:
        """Создает директорию для результатов, если она не существует."""
        if not os.path.exists(self.results_dir):
            os.makedirs(self.results_dir)
            logger.info(f"Created results directory: {self.results_dir}")

    def save_results(self, results: Dict[str, float]) -> str:
        """
        Сохраняет результаты в CSV файл.

        Args:
            results: Словарь с результатами бенчмарка

        Returns:
            Путь к созданному CSV файлу
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_results_{timestamp}.csv"
        filepath = os.path.join(self.results_dir, filename)

        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)

                # Записываем заголовок
                writer.writerow([
                    "Database",
                    "Load_Time_Seconds",
                    "Data_Rows",
                    "Batch_Size",
                    "Timestamp",
                    "Status"
                ])

                # Записываем результаты для каждой БД
                for db_name, load_time in results.items():
                    status = "SUCCESS" if load_time != float('inf') else "FAILED"
                    writer.writerow([
                        db_name,
                        f"{load_time:.2f}" if load_time != float('inf') else "FAILED",
                        DATA_ROWS,
                        BATCH_SIZE,
                        timestamp,
                        status
                    ])

                # Записываем сводную статистику
                writer.writerow([])  # Пустая строка
                writer.writerow(["SUMMARY STATISTICS"])
                writer.writerow(["Metric", "Value"])

                successful_results = {
                    db: time for db, time in results.items()
                    if time != float('inf')
                }

                if successful_results:
                    avg_time = sum(successful_results.values()) / len(successful_results)
                    fastest_db = min(successful_results.items(), key=lambda x: x[1])
                    slowest_db = max(successful_results.items(), key=lambda x: x[1])

                    writer.writerow(["Average_Load_Time", f"{avg_time:.2f}"])
                    writer.writerow(["Fastest_Database", f"{fastest_db[0]} ({fastest_db[1]:.2f}s)"])
                    writer.writerow(["Slowest_Database", f"{slowest_db[0]} ({slowest_db[1]:.2f}s)"])
                    writer.writerow(["Successful_Databases", len(successful_results)])
                    writer.writerow(["Total_Databases_Tested", len(results)])
                else:
                    writer.writerow(["All_Databases_Failed", "TRUE"])

            logger.info(f"Results saved to: {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"Failed to save results to CSV: {e}")
            raise

    def get_latest_results_file(self) -> str:
        """
        Возвращает путь к последнему файлу результатов.

        Returns:
            Путь к последнему CSV файлу или None если файлов нет
        """
        try:
            files = [f for f in os.listdir(self.results_dir) if f.endswith('.csv')]
            if not files:
                return None

            files.sort(reverse=True)  # Сортировка по имени (timestamp)
            return os.path.join(self.results_dir, files[0])
        except Exception as e:
            logger.error(f"Failed to get latest results file: {e}")
            return None