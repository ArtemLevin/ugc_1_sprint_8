import os
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from datetime import datetime
from typing import Dict, Any
from ..logging_config import setup_logger

logger = setup_logger(__name__)


class PlotGenerator:
    """
    Генерирует графики на основе результатов бенчмарка.

    Использует matplotlib и seaborn для создания
    профессиональных визуализаций результатов.
    """

    def __init__(self, plots_dir: str = "plots") -> None:
        """
        Инициализирует генератор графиков.

        Args:
            plots_dir: Директория для сохранения графиков
        """
        self.plots_dir = plots_dir
        self.ensure_plots_directory()
        self.setup_plotting_style()

    def ensure_plots_directory(self) -> None:
        """Создает директорию для графиков, если она не существует."""
        if not os.path.exists(self.plots_dir):
            os.makedirs(self.plots_dir)
            logger.info(f"Created plots directory: {self.plots_dir}")

    def setup_plotting_style(self) -> None:
        """Настраивает стиль графиков."""
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")

    def generate_performance_comparison_plot(self, results: Dict[str, float]) -> str:
        """
        Генерирует график сравнения производительности БД.

        Args:
            results: Словарь с результатами бенчмарка

        Returns:
            Путь к созданному PNG файлу или None при ошибке
        """
        try:
            # Фильтруем успешные результаты
            successful_results = {
                db: time for db, time in results.items()
                if time != float('inf')
            }

            if not successful_results:
                logger.warning("No successful results to plot")
                return None

            # Создаем DataFrame
            df = pd.DataFrame(list(successful_results.items()),
                              columns=['Database', 'Load_Time_Seconds'])
            df = df.sort_values('Load_Time_Seconds', ascending=False)

            # Создаем график
            plt.figure(figsize=(10, 6))
            bars = plt.bar(df['Database'], df['Load_Time_Seconds'],
                           color=sns.color_palette("husl", len(df)))

            # Настройки графика
            plt.title('Database Load Performance Comparison', fontsize=16, pad=20)
            plt.xlabel('Database', fontsize=12)
            plt.ylabel('Load Time (seconds)', fontsize=12)
            plt.xticks(rotation=45, ha='right')
            plt.grid(axis='y', alpha=0.3)

            # Добавляем значения на столбцы
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width() / 2., height,
                         f'{height:.2f}s',
                         ha='center', va='bottom', fontsize=10)

            plt.tight_layout()

            # Сохраняем график
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"performance_comparison_{timestamp}.png"
            filepath = os.path.join(self.plots_dir, filename)

            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            plt.close()

            logger.info(f"Performance comparison plot saved to: {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"Failed to generate performance comparison plot: {e}")
            return None

    def generate_detailed_performance_plot(self, results: Dict[str, float],
                                           data_rows: int, batch_size: int) -> str:
        """
        Генерирует детальный график производительности.

        Args:
            results: Словарь с результатами бенчмарка
            data_rows: Количество строк данных
            batch_size: Размер батча

        Returns:
            Путь к созданному PNG файлу или None при ошибке
        """
        try:
            # Фильтруем успешные результаты
            successful_results = {
                db: time for db, time in results.items()
                if time != float('inf')
            }

            if not successful_results:
                return None

            # Вычисляем дополнительные метрики
            metrics_data = []
            for db, time in successful_results.items():
                throughput = data_rows / time if time > 0 else 0
                metrics_data.append({
                    'Database': db,
                    'Load_Time': time,
                    'Throughput_RowsPerSec': throughput,
                    'Batch_Size': batch_size
                })

            df = pd.DataFrame(metrics_data)

            # Создаем subplot с двумя графиками
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

            # График 1: Время загрузки
            bars1 = ax1.bar(df['Database'], df['Load_Time'],
                            color=sns.color_palette("Blues_d", len(df)))
            ax1.set_title('Load Time Comparison', fontsize=14)
            ax1.set_ylabel('Time (seconds)', fontsize=12)
            ax1.set_xticklabels(df['Database'], rotation=45, ha='right')
            ax1.grid(axis='y', alpha=0.3)

            # Добавляем значения
            for bar in bars1:
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width() / 2., height,
                         f'{height:.2f}s',
                         ha='center', va='bottom', fontsize=10)

            # График 2: Пропускная способность
            bars2 = ax2.bar(df['Database'], df['Throughput_RowsPerSec'],
                            color=sns.color_palette("Greens_d", len(df)))
            ax2.set_title('Throughput Comparison', fontsize=14)
            ax2.set_ylabel('Rows per Second', fontsize=12)
            ax2.set_xticklabels(df['Database'], rotation=45, ha='right')
            ax2.grid(axis='y', alpha=0.3)

            # Добавляем значения
            for bar in bars2:
                height = bar.get_height()
                ax2.text(bar.get_x() + bar.get_width() / 2., height,
                         f'{height:,.0f}',
                         ha='center', va='bottom', fontsize=10)

            plt.suptitle(f'Detailed Performance Analysis\n(Data: {data_rows:,} rows, Batch: {batch_size:,} rows)',
                         fontsize=16)
            plt.tight_layout()

            # Сохраняем график
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"detailed_performance_{timestamp}.png"
            filepath = os.path.join(self.plots_dir, filename)

            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            plt.close()

            logger.info(f"Detailed performance plot saved to: {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"Failed to generate detailed performance plot: {e}")
            return None

    def generate_summary_dashboard(self, results: Dict[str, float],
                                   data_rows: int, batch_size: int) -> str:
        """
        Генерирует сводную панель с несколькими графиками.

        Args:
            results: Словарь с результатами бенчмарка
            data_rows: Количество строк данных
            batch_size: Размер батча

        Returns:
            Путь к созданному PNG файлу или None при ошибке
        """
        try:
            # Фильтруем успешные результаты
            successful_results = {
                db: time for db, time in results.items()
                if time != float('inf')
            }

            if not successful_results:
                return None

            # Подготовка данных
            metrics_data = []
            for db, time in successful_results.items():
                throughput = data_rows / time if time > 0 else 0
                metrics_data.append({
                    'Database': db,
                    'Load_Time': time,
                    'Throughput': throughput
                })

            df = pd.DataFrame(metrics_data)

            # Создаем dashboard
            fig = plt.figure(figsize=(16, 12))

            # График 1: Время загрузки (горизонтальный bar chart)
            ax1 = plt.subplot(2, 2, 1)
            df_sorted_time = df.sort_values('Load_Time', ascending=True)
            bars1 = ax1.barh(df_sorted_time['Database'], df_sorted_time['Load_Time'],
                             color=sns.color_palette("Reds_d", len(df_sorted_time)))
            ax1.set_title('Load Time (Lower is Better)', fontsize=14, pad=20)
            ax1.set_xlabel('Time (seconds)', fontsize=12)
            ax1.grid(axis='x', alpha=0.3)

            # Добавляем значения
            for i, bar in enumerate(bars1):
                width = bar.get_width()
                ax1.text(width + max(df_sorted_time['Load_Time']) * 0.01, bar.get_y() + bar.get_height() / 2,
                         f'{width:.2f}s',
                         ha='left', va='center', fontsize=10)

            # График 2: Пропускная способность (горизонтальный bar chart)
            ax2 = plt.subplot(2, 2, 2)
            df_sorted_throughput = df.sort_values('Throughput', ascending=False)
            bars2 = ax2.barh(df_sorted_throughput['Database'], df_sorted_throughput['Throughput'],
                             color=sns.color_palette("Greens_d", len(df_sorted_throughput)))
            ax2.set_title('Throughput (Higher is Better)', fontsize=14, pad=20)
            ax2.set_xlabel('Rows per Second', fontsize=12)
            ax2.grid(axis='x', alpha=0.3)

            # Добавляем значения
            for i, bar in enumerate(bars2):
                width = bar.get_width()
                ax2.text(width + max(df_sorted_throughput['Throughput']) * 0.01, bar.get_y() + bar.get_height() / 2,
                         f'{width:,.0f}',
                         ha='left', va='center', fontsize=10)

            # График 3: Сравнение (вертикальный)
            ax3 = plt.subplot(2, 2, 3)
            x_pos = range(len(df))
            bars3_1 = ax3.bar([x - 0.2 for x in x_pos], df['Load_Time'],
                              width=0.4, label='Load Time (s)',
                              color=sns.color_palette("Blues_d", 1)[0])
            ax3_twin = ax3.twinx()
            bars3_2 = ax3_twin.bar([x + 0.2 for x in x_pos], df['Throughput'],
                                   width=0.4, label='Throughput (rows/sec)',
                                   color=sns.color_palette("Greens_d", 1)[0], alpha=0.7)

            ax3.set_xlabel('Database', fontsize=12)
            ax3.set_ylabel('Load Time (seconds)', fontsize=12, color=sns.color_palette("Blues_d", 1)[0])
            ax3_twin.set_ylabel('Throughput (rows/sec)', fontsize=12, color=sns.color_palette("Greens_d", 1)[0])
            ax3.set_xticks(x_pos)
            ax3.set_xticklabels(df['Database'], rotation=45, ha='right')
            ax3.set_title('Performance Metrics Comparison', fontsize=14, pad=20)
            ax3.grid(axis='y', alpha=0.3)

            # Легенда
            bars3 = [bars3_1[0], bars3_2[0]]
            labels3 = [bar.get_label() for bar in bars3]
            ax3.legend(bars3, labels3, loc='upper right')

            # График 4: Рейтинг
            ax4 = plt.subplot(2, 2, 4)
            # Рассчитываем рейтинг (комбинированный показатель)
            df['Rating_Score'] = (df['Throughput'] / df['Throughput'].max()) * 0.7 + \
                                 (1 - df['Load_Time'] / df['Load_Time'].max()) * 0.3
            df_rating = df.sort_values('Rating_Score', ascending=False)

            colors = sns.color_palette("viridis", len(df_rating))
            bars4 = ax4.bar(df_rating['Database'], df_rating['Rating_Score'],
                            color=colors)
            ax4.set_title('Performance Rating (Combined Score)', fontsize=14, pad=20)
            ax4.set_ylabel('Rating Score (0-1)', fontsize=12)
            ax4.set_xticklabels(df_rating['Database'], rotation=45, ha='right')
            ax4.grid(axis='y', alpha=0.3)

            # Добавляем значения
            for bar in bars4:
                height = bar.get_height()
                ax4.text(bar.get_x() + bar.get_width() / 2., height,
                         f'{height:.3f}',
                         ha='center', va='bottom', fontsize=10)

            # Общий заголовок
            fig.suptitle(f'Database Performance Dashboard\nTotal Rows: {data_rows:,} | Batch Size: {batch_size:,}',
                         fontsize=18, y=0.95)

            plt.tight_layout()

            # Сохраняем dashboard
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"performance_dashboard_{timestamp}.png"
            filepath = os.path.join(self.plots_dir, filename)

            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            plt.close()

            logger.info(f"Performance dashboard saved to: {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"Failed to generate performance dashboard: {e}")
            return None