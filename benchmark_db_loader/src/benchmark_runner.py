from typing import Dict
from .interfaces.db_loader_interface import DBLoaderInterface
from .di_container import DIContainer
from .config import DATA_ROWS, BATCH_SIZE
from .utils.results_saver import ResultsSaver
from .utils.plot_generator import PlotGenerator
from .logging_config import setup_logger

logger = setup_logger(__name__)


def run_benchmark(container: DIContainer, save_results: bool = True,
                  generate_plots: bool = True) -> Dict[str, float]:
    """
    Запускает бенчмарк и опционально сохраняет результаты и генерирует графики.

    Args:
        container: DI контейнер для получения зависимостей
        save_results: Флаг сохранения результатов в CSV
        generate_plots: Флаг генерации графиков

    Returns:
        Словарь с результатами бенчмарка
    """
    db_names = ["ClickHouse", "PostgreSQL", "Vertica"]
    results = {}

    logger.info("Starting benchmark for databases: %s", ", ".join(db_names))

    for db_name in db_names:
        loader: DBLoaderInterface = container.get_loader(db_name)
        try:
            loader.log_start()
            loader.connect()
            loader.create_table()

            data_gen = container.get_data_generator(DATA_ROWS)
            elapsed = loader.load_data(data_gen)
            results[db_name] = elapsed
            loader.log_completion(elapsed)

        except Exception as e:
            logger.error(f"Failed to benchmark {db_name}: {e}", exc_info=True)
            results[db_name] = float('inf')
        finally:
            try:
                loader.close()
            except Exception as e:
                logger.warning(f"Error closing connection for {db_name}: {e}")

    # Сохраняем результаты, если требуется
    if save_results:
        try:
            saver = ResultsSaver()
            filepath = saver.save_results(results)
            logger.info(f"Benchmark results saved to: {filepath}")
        except Exception as e:
            logger.error(f"Failed to save benchmark results: {e}")

    # Генерируем графики, если требуется
    if generate_plots:
        try:
            plot_generator = PlotGenerator()

            # Основной график сравнения
            perf_plot = plot_generator.generate_performance_comparison_plot(results)
            if perf_plot:
                logger.info(f"Performance comparison plot saved to: {perf_plot}")

            # Детальный график
            detailed_plot = plot_generator.generate_detailed_performance_plot(
                results, DATA_ROWS, BATCH_SIZE
            )
            if detailed_plot:
                logger.info(f"Detailed performance plot saved to: {detailed_plot}")

            # Dashboard
            dashboard_plot = plot_generator.generate_summary_dashboard(
                results, DATA_ROWS, BATCH_SIZE
            )
            if dashboard_plot:
                logger.info(f"Performance dashboard saved to: {dashboard_plot}")

        except Exception as e:
            logger.error(f"Failed to generate plots: {e}")

    return results