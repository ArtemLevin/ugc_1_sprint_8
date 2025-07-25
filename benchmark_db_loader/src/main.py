from .benchmark_runner import run_benchmark
from .di_container import DIContainer
from .utils.wait_for_db import wait_for_all_databases
from .utils.results_saver import ResultsSaver
from .utils.plot_generator import PlotGenerator
from .logging_config import setup_logger

logger = setup_logger(__name__)


def main() -> None:
    """
    Основная точка входа в приложение.

    Координирует весь процесс бенчмарка:
    1. Ожидание готовности БД
    2. Запуск бенчмарка
    3. Отображение результатов
    4. Показ путей к файлам результатов
    """
    logger.info("Starting database load benchmark...")

    # Ждем готовности всех БД
    if not wait_for_all_databases(max_attempts=30, delay=2):
        logger.error("Not all databases are ready. Exiting...")
        return

    container = DIContainer()
    results = run_benchmark(container, save_results=True, generate_plots=True)

    logger.info("📊 Benchmark Results:")
    for db, time_taken in results.items():
        status = "SUCCESS" if time_taken != float('inf') else "FAILED"
        logger.info(f"  {db}: {time_taken:.2f} seconds ({status})")

    # Показываем путь к последним файлам результатов
    try:
        saver = ResultsSaver()
        latest_file = saver.get_latest_results_file()
        if latest_file:
            logger.info(f"Latest results saved to: {latest_file}")

        plot_generator = PlotGenerator()
        # Можно добавить логику для отображения последних графиков
        logger.info("Performance plots have been generated in the 'plots' directory")

    except Exception as e:
        logger.error(f"Could not retrieve latest files: {e}")


if __name__ == "__main__":
    main()