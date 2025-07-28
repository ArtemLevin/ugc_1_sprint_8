from clickhouse_driver import Client

from clickhouse.create_tables import create_tables
import time
import uuid
import random
from datetime import datetime, timedelta
from clickhouse_driver import Client
from multiprocessing import Pool
from faker import Faker
import statistics
import matplotlib.pyplot as plt

client = Client(host='localhost')

create_tables(client)



# Инициализация
fake = Faker()
CH_HOST = 'localhost'
CH_PORT = 9000
CH_USER = 'default'
CH_PASSWORD = ''
CH_DB = 'events'


# Подключение к ClickHouse
def get_client():
    return Client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB
    )


# Генерация тестовых данных
def generate_event_data(num_records):
    events = []
    for _ in range(num_records):
        event_id = uuid.uuid4()
        user_id = uuid.uuid4()
        event_type = random.choice([1, 2, 3, 4, 5])
        timestamp = datetime.now() - timedelta(days=random.randint(0, 365))
        session_id = uuid.uuid4() if random.random() > 0.2 else None

        base_event = {
            'event_id': event_id,
            'user_id': user_id,
            'event_type': event_type,
            'timestamp': timestamp,
            'session_id': session_id
        }

        # Генерация специфичных данных для каждого типа события
        if event_type == 1:  # VIDEO_QUALITY_CHANGE
            detail = {
                'event_id': event_id,
                'from_quality': random.choice(['240p', '360p', '480p', '720p', '1080p']),
                'to_quality': random.choice(['240p', '360p', '480p', '720p', '1080p'])
            }
        elif event_type == 2:  # VIDEO_WATCHED_TO_END
            detail = {
                'event_id': event_id,
                'movie_id': uuid.uuid4(),
                'watch_time_seconds': random.randint(60, 7200)
            }
        elif event_type == 3:  # SEARCH_FILTER_USED
            detail = {
                'event_id': event_id,
                'filter_type': random.choice(['genre', 'year', 'rating', 'duration']),
                'value': random.randint(1, 100)
            }
        elif event_type == 4:  # CLICK
            detail = {
                'event_id': event_id,
                'element_type': random.choice(['button', 'link', 'image', 'video']),
                'element_id': str(uuid.uuid4())
            }
        else:  # PAGE_VIEW
            detail = {
                'event_id': event_id,
                'page': random.choice(['home', 'search', 'movie', 'profile', 'settings']),
                'duration_seconds': random.randint(1, 600)
            }

        events.append((base_event, event_type, detail))

    return events


# Вставка данных в ClickHouse
def insert_data(batch):
    client = get_client()
    base_events = []
    video_quality = []
    video_watched = []
    search_filter = []
    click = []
    page_view = []

    for base_event, event_type, detail in batch:
        base_events.append(base_event)

        if event_type == 1:
            video_quality.append(detail)
        elif event_type == 2:
            video_watched.append(detail)
        elif event_type == 3:
            search_filter.append(detail)
        elif event_type == 4:
            click.append(detail)
        elif event_type == 5:
            page_view.append(detail)

    # Вставка в основную таблицу
    if base_events:
        client.execute(
            "INSERT INTO event_base (event_id, user_id, event_type, timestamp, session_id) VALUES",
            [(
                str(e['event_id']),
                str(e['user_id']),
                e['event_type'],
                e['timestamp'],
                str(e['session_id']) if e['session_id'] else None
            ) for e in base_events]
        )

    # Вставка в специфичные таблицы
    if video_quality:
        client.execute(
            "INSERT INTO events_video_quality (event_id, from_quality, to_quality) VALUES",
            [(str(e['event_id']), e['from_quality'], e['to_quality']) for e in
             video_quality]
        )

    if video_watched:
        client.execute(
            "INSERT INTO video_watched_to_end (event_id, movie_id, watch_time_seconds) VALUES",
            [(str(e['event_id']), str(e['movie_id']), e['watch_time_seconds']) for e in
             video_watched]
        )

    if search_filter:
        client.execute(
            "INSERT INTO search_filter_used (event_id, filter_type, value) VALUES",
            [(str(e['event_id']), e['filter_type'], e['value']) for e in search_filter]
        )

    if click:
        client.execute(
            "INSERT INTO click (event_id, element_type, element_id) VALUES",
            [(str(e['event_id']), e['element_type'], e['element_id']) for e in click]
        )

    if page_view:
        client.execute(
            "INSERT INTO page_view (event_id, page, duration_seconds) VALUES",
            [(str(e['event_id']), e['page'], e['duration_seconds']) for e in page_view]
        )


# Функция для многопоточной вставки
def batch_insert_worker(batch_size, num_batches):
    for _ in range(num_batches):
        data = generate_event_data(batch_size)
        insert_data(data)


# Тестирование вставки данных
def test_insert_performance(total_records=10_000_000, batch_size=1000, num_threads=30):
    print(f"Testing insert performance for {total_records} records...")

    batches_per_thread = (total_records // batch_size) // num_threads
    remaining_batches = (total_records // batch_size) % num_threads

    args = []
    for i in range(num_threads):
        batches = batches_per_thread + (1 if i < remaining_batches else 0)
        if batches > 0:
            args.append((batch_size, batches))

    start_time = time.time()

    with Pool(num_threads) as pool:
        pool.starmap(batch_insert_worker, args)

    elapsed = time.time() - start_time
    records_per_second = total_records / elapsed

    print(f"Inserted {total_records} records in {elapsed:.2f} seconds")
    print(f"Insert performance: {records_per_second:,.2f} records/second")

    return elapsed, records_per_second


# Тестирование чтения данных
def test_read_performance():
    print("Testing read performance...")
    client = get_client()

    # Тест 1: Простой SELECT
    start_time = time.time()
    result = client.execute("SELECT count() FROM event_base")
    elapsed_simple = time.time() - start_time
    print(f"Simple SELECT count(): {elapsed_simple:.4f} seconds, result: {result[0][0]}")

    # Тест 2: Агрегация по типам событий
    start_time = time.time()
    result = client.execute("""
        SELECT event_type, count() as cnt 
        FROM event_base 
        GROUP BY event_type 
        ORDER BY cnt DESC
    """)
    elapsed_agg = time.time() - start_time
    print(f"Aggregation query: {elapsed_agg:.4f} seconds, results: {result}")

    # Тест 3: JOIN с детализированными таблицами
    start_time = time.time()
    result = client.execute("""
        SELECT 
            b.event_type,
            b.timestamp,
            v.movie_id,
            v.watch_time_seconds
        FROM event_base b
        JOIN video_watched_to_end v ON b.event_id = v.event_id
        WHERE b.timestamp > now() - INTERVAL 30 DAY
        LIMIT 1000
    """)
    elapsed_join = time.time() - start_time
    print(f"JOIN query (1000 records): {elapsed_join:.4f} seconds")

    # Тест 4: Сложная аналитика
    start_time = time.time()
    result = client.execute("""
        SELECT 
            toStartOfDay(timestamp) as day,
            event_type,
            count() as events,
            uniq(user_id) as unique_users,
            avgIf(
                CASE 
                    WHEN event_type = 2 THEN v.watch_time_seconds 
                    WHEN event_type = 5 THEN p.duration_seconds 
                    ELSE NULL 
                END,
                event_type IN (2, 5)
            ) as avg_duration
        FROM event_base b
        LEFT JOIN video_watched_to_end v ON b.event_id = v.event_id AND b.event_type = 2
        LEFT JOIN page_view p ON b.event_id = p.event_id AND b.event_type = 5
        WHERE timestamp > now() - INTERVAL 30 DAY
        GROUP BY day, event_type
        ORDER BY day DESC, events DESC
    """)
    elapsed_complex = time.time() - start_time
    print(f"Complex analytics query: {elapsed_complex:.4f} seconds")

    return {
        'simple_select': elapsed_simple,
        'aggregation': elapsed_agg,
        'join': elapsed_join,
        'complex_query': elapsed_complex
    }


# Тестирование под нагрузкой
def test_under_load(duration_seconds=180, insert_batch_size=500, insert_threads=30):
    print(f"Testing under load for {duration_seconds} seconds...")
    client = get_client()

    # Запускаем фоновые процессы вставки
    from threading import Thread, Event
    stop_event = Event()

    def insert_worker():
        while not stop_event.is_set():
            data = generate_event_data(insert_batch_size)
            insert_data(data)

    threads = []
    for _ in range(insert_threads):
        t = Thread(target=insert_worker)
        t.start()
        threads.append(t)

    # Запускаем тесты чтения во время нагрузки
    read_results = []
    query_times = []

    start_time = time.time()
    while time.time() - start_time < duration_seconds:
        query_start = time.time()

        # Выполняем тестовый запрос
        result = client.execute("""
            SELECT 
                event_type,
                count() as cnt,
                uniq(user_id) as unique_users
            FROM event_base
            WHERE timestamp > now() - INTERVAL 1 HOUR
            GROUP BY event_type
            ORDER BY cnt DESC
        """)

        query_elapsed = time.time() - query_start
        query_times.append(query_elapsed)

        # Сохраняем результаты для статистики
        read_results.append({
            'time': time.time() - start_time,
            'elapsed': query_elapsed,
            'result': result
        })

        # Пауза между запросами
        time.sleep(5)

    # Останавливаем вставку
    stop_event.set()
    for t in threads:
        t.join()

    # Анализ результатов
    avg_query_time = statistics.mean(query_times)
    max_query_time = max(query_times)
    min_query_time = min(query_times)

    print(f"Query performance under load (avg/min/max): "
          f"{avg_query_time:.4f}/{min_query_time:.4f}/{max_query_time:.4f} seconds")

    # Визуализация
    times = [r['time'] for r in read_results]
    elapsed = [r['elapsed'] for r in read_results]

    plt.figure(figsize=(4, 5))
    plt.plot(times, elapsed, marker='o')
    plt.axhline(y=4, color='r', linestyle='--', label='10s threshold')
    plt.title('Query performance under load')
    plt.xlabel('Test time (seconds)')
    plt.ylabel('Query time (seconds)')
    plt.legend()
    plt.grid()
    plt.savefig('query_performance_under_load.png')
    plt.close()

    return {
        'avg_query_time': avg_query_time,
        'max_query_time': max_query_time,
        'min_query_time': min_query_time,
        'query_times': query_times
    }


# Основная функция тестирования
def run_performance_tests():
    print("Starting ClickHouse performance tests...")

    # 1. Тестирование вставки
    insert_time, insert_rate = test_insert_performance()

    # 2. Тестирование чтения
    read_results = test_read_performance()

    # 3. Тестирование под нагрузкой
    load_results = test_under_load()

    # Сохранение результатов
    results = {
        'insert_performance': {
            'total_time_seconds': insert_time,
            'records_per_second': insert_rate
        },
        'read_performance': read_results,
        'load_test': load_results
    }

    # Проверка требований
    requirements_met = True
    if read_results['complex_query'] > 10:
        print("WARNING: Complex query exceeds 10 second limit")
        requirements_met = False

    if load_results['max_query_time'] > 10:
        print("WARNING: Some queries under load exceed 10 second limit")
        requirements_met = False

    results['requirements_met'] = requirements_met

    # Вывод сводки
    print("\n=== Performance Test Summary ===")
    print(f"Insert Performance: {insert_rate:,.2f} records/second")
    print(f"Simple SELECT: {read_results['simple_select']:.4f} seconds")
    print(f"Aggregation Query: {read_results['aggregation']:.4f} seconds")
    print(f"JOIN Query: {read_results['join']:.4f} seconds")
    print(f"Complex Analytics Query: {read_results['complex_query']:.4f} seconds")
    print(f"Average Query Time Under Load: {load_results['avg_query_time']:.4f} seconds")
    print(f"Maximum Query Time Under Load: {load_results['max_query_time']:.4f} seconds")
    print(f"Requirements Met: {'Yes' if requirements_met else 'No'}")

    return results


if __name__ == "__main__":
    run_performance_tests()
