import time
import uuid
import random
from datetime import datetime, timedelta
import vertica_python
from multiprocessing import Pool
from faker import Faker
import statistics
import matplotlib.pyplot as plt

from vertica.create_tables import create_schema

# Инициализация
fake = Faker()
VERTICA_HOST = 'localhost'
VERTICA_PORT = 5433
VERTICA_USER = 'dbadmin'
VERTICA_PASSWORD = ''
VERTICA_DB = 'docker'


# Подключение к Vertica
def get_connection():
    return vertica_python.connect(
        host=VERTICA_HOST,
        port=VERTICA_PORT,
        user=VERTICA_USER,
        password=VERTICA_PASSWORD,
        database=VERTICA_DB,
        tlsmode='disable',  # Явное отключение TLS
        connection_load_balance=True,
        connection_timeout=30,
        backup_server_node=['vertica-node2', 'vertica-node3', 'vertica-node4'],
        # Отключаем предупреждения о TLS
        use_prepared_statements=False
    )


# Создание схемы (аналог вашей схемы ClickHouse)


# Генерация тестовых данных (аналогично ClickHouse)
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


# Вставка данных в Vertica
def insert_data(batch):
    conn = get_connection()
    cur = conn.cursor()

    try:
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
            cur.executemany(
                """
                INSERT INTO events.event_base 
                (event_id, user_id, event_type, timestamp, session_id) 
                VALUES (%s, %s, %s, %s, %s)
                """,
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
            cur.executemany(
                """
                INSERT INTO events.events_video_quality 
                (event_id, from_quality, to_quality) 
                VALUES (%s, %s, %s)
                """,
                [(str(e['event_id']), e['from_quality'], e['to_quality']) for e in
                 video_quality]
            )

        if video_watched:
            cur.executemany(
                """
                INSERT INTO events.video_watched_to_end 
                (event_id, movie_id, watch_time_seconds) 
                VALUES (%s, %s, %s)
                """,
                [(str(e['event_id']), str(e['movie_id']), e['watch_time_seconds']) for e
                 in video_watched]
            )

        if search_filter:
            cur.executemany(
                """
                INSERT INTO events.search_filter_used 
                (event_id, filter_type, value) 
                VALUES (%s, %s, %s)
                """,
                [(str(e['event_id']), e['filter_type'], e['value']) for e in
                 search_filter]
            )

        if click:
            cur.executemany(
                """
                INSERT INTO events.click 
                (event_id, element_type, element_id) 
                VALUES (%s, %s, %s)
                """,
                [(str(e['event_id']), e['element_type'], e['element_id']) for e in click]
            )

        if page_view:
            cur.executemany(
                """
                INSERT INTO events.page_view 
                (event_id, page, duration_seconds) 
                VALUES (%s, %s, %s)
                """,
                [(str(e['event_id']), e['page'], e['duration_seconds']) for e in
                 page_view]
            )

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data: {e}")
    finally:
        cur.close()
        conn.close()


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
    conn = get_connection()
    cur = conn.cursor()

    results = {}

    try:
        # Тест 1: Простой SELECT
        start_time = time.time()
        cur.execute("SELECT count(*) FROM events.event_base")
        result = cur.fetchone()
        elapsed_simple = time.time() - start_time
        print(f"Simple SELECT count(): {elapsed_simple:.4f} seconds, result: {result[0]}")
        results['simple_select'] = elapsed_simple

        # Тест 2: Агрегация по типам событий
        start_time = time.time()
        cur.execute("""
            SELECT event_type, count(*) as cnt 
            FROM events.event_base 
            GROUP BY event_type 
            ORDER BY cnt DESC
        """)
        result = cur.fetchall()
        elapsed_agg = time.time() - start_time
        print(f"Aggregation query: {elapsed_agg:.4f} seconds, results: {result}")
        results['aggregation'] = elapsed_agg

        # Тест 3: JOIN с детализированными таблицами
        start_time = time.time()
        cur.execute("""
            SELECT 
                b.event_type,
                b.timestamp,
                v.movie_id,
                v.watch_time_seconds
            FROM events.event_base b
            JOIN events.video_watched_to_end v ON b.event_id = v.event_id
            WHERE b.timestamp > CURRENT_TIMESTAMP - INTERVAL '30 days'
            LIMIT 1000
        """)
        result = cur.fetchall()
        elapsed_join = time.time() - start_time
        print(f"JOIN query (1000 records): {elapsed_join:.4f} seconds")
        results['join'] = elapsed_join

        # Тест 4: Сложная аналитика
        start_time = time.time()
        cur.execute("""
            SELECT 
                DATE_TRUNC('day', timestamp) as day,
                b.event_type,
                count(*) as events,
                COUNT(DISTINCT user_id) as unique_users,
                AVG(CASE 
                    WHEN b.event_type = 2 THEN v.watch_time_seconds 
                    WHEN b.event_type = 5 THEN p.duration_seconds 
                    ELSE NULL 
                END) as avg_duration
            FROM events.event_base b
            LEFT JOIN events.video_watched_to_end v ON b.event_id = v.event_id AND b.event_type = 2
            LEFT JOIN events.page_view p ON b.event_id = p.event_id AND b.event_type = 5
            WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '30 days'
            GROUP BY day, b.event_type
            ORDER BY day DESC, events DESC
        """)
        result = cur.fetchall()
        elapsed_complex = time.time() - start_time
        print(f"Complex analytics query: {elapsed_complex:.4f} seconds")
        results['complex_query'] = elapsed_complex

    finally:
        cur.close()
        conn.close()

    return results


# Тестирование под нагрузкой
def test_under_load(duration_seconds=300, insert_batch_size=500, insert_threads=30):
    print(f"Testing under load for {duration_seconds} seconds...")

    # Запускаем фоновые процессы вставки
    from threading import Thread, Event
    stop_event = Event()

    def insert_worker():
        conn = get_connection()
        while not stop_event.is_set():
            data = generate_event_data(insert_batch_size)

            try:
                cur = conn.cursor()

                # Вставляем только в основную таблицу для упрощения нагрузки
                cur.executemany(
                    """
                    INSERT INTO events.event_base 
                    (event_id, user_id, event_type, timestamp, session_id) 
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    [(
                        str(e['event_id']),
                        str(e['user_id']),
                        e['event_type'],
                        e['timestamp'],
                        str(e['session_id']) if e['session_id'] else None
                    ) for e, _, _ in data]
                )
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"Error in insert worker: {e}")
            finally:
                if 'cur' in locals():
                    cur.close()

        conn.close()

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

        try:
            conn = get_connection()
            cur = conn.cursor()

            # Выполняем тестовый запрос
            cur.execute("""
                SELECT 
                    event_type,
                    count(*) as cnt,
                    COUNT(DISTINCT user_id) as unique_users
                FROM events.event_base
                WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '1 hour'
                GROUP BY event_type
                ORDER BY cnt DESC
            """)
            result = cur.fetchall()

            query_elapsed = time.time() - query_start
            query_times.append(query_elapsed)

            # Сохраняем результаты для статистики
            read_results.append({
                'time': time.time() - start_time,
                'elapsed': query_elapsed,
                'result': result
            })

        except Exception as e:
            print(f"Query error: {e}")
        finally:
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()

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
    plt.title('Query performance under load (Vertica)')
    plt.xlabel('Test time (seconds)')
    plt.ylabel('Query time (seconds)')
    plt.legend()
    plt.grid()
    plt.savefig('vertica_query_performance_under_load.png')
    plt.close()

    return {
        'avg_query_time': avg_query_time,
        'max_query_time': max_query_time,
        'min_query_time': min_query_time,
        'query_times': query_times
    }


# Основная функция тестирования
def run_performance_tests():
    print("Starting Vertica performance tests...")

    # 0. Создание схемы
    create_schema(get_connection())

    # 1. Тестирование вставки
    insert_time, insert_rate = test_insert_performance(
        total_records=3_000_000)  # Уменьшил кол-во записей для Vertica

    # 2. Тестирование чтения
    read_results = test_read_performance()

    # 3. Тестирование под нагрузкой
    load_results = test_under_load(duration_seconds=180,
                                   insert_threads=15)  # Уменьшил нагрузку для Vertica

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