def create_schema(conn):
    cur = conn.cursor()
    print(123123)
    try:
        # Создание схемы
        cur.execute("CREATE SCHEMA IF NOT EXISTS events")

        # Создание таблицы event_base
        cur.execute("""
        CREATE TABLE IF NOT EXISTS events.event_base
        (
            event_id UUID,
            user_id UUID,
            event_type INT,
            timestamp TIMESTAMP,
            session_id UUID
        )
        ORDER BY timestamp, user_id, event_type
        SEGMENTED BY HASH(event_id) ALL NODES
        """)

        # Создание таблицы events_video_quality
        cur.execute("""
        CREATE TABLE IF NOT EXISTS events.events_video_quality
        (
            event_id UUID,
            from_quality VARCHAR(20),
            to_quality VARCHAR(20)
        )
        ORDER BY event_id
        SEGMENTED BY HASH(event_id) ALL NODES
        """)

        # Создание таблицы video_watched_to_end
        cur.execute("""
        CREATE TABLE IF NOT EXISTS events.video_watched_to_end
        (
            event_id UUID,
            movie_id UUID,
            watch_time_seconds INT
        )
        ORDER BY event_id
        SEGMENTED BY HASH(event_id) ALL NODES
        """)

        # Создание таблицы search_filter_used
        cur.execute("""
        CREATE TABLE IF NOT EXISTS events.search_filter_used
        (
            event_id UUID,
            filter_type VARCHAR(20),
            value INT
        )
        ORDER BY event_id
        SEGMENTED BY HASH(event_id) ALL NODES
        """)

        # Создание таблицы click
        cur.execute("""
        CREATE TABLE IF NOT EXISTS events.click
        (
            event_id UUID,
            element_type VARCHAR(20),
            element_id VARCHAR(50)
        )
        ORDER BY event_id
        SEGMENTED BY HASH(event_id) ALL NODES
        """)

        # Создание таблицы page_view
        cur.execute("""
        CREATE TABLE IF NOT EXISTS events.page_view
        (
            event_id UUID,
            page VARCHAR(20),
            duration_seconds INT
        )
        ORDER BY event_id
        SEGMENTED BY HASH(event_id) ALL NODES
        """)

        conn.commit()
        print("Schema created successfully")
    except Exception as e:
        conn.rollback()
        print(f"Error creating schema: {e}")
    finally:
        cur.close()
        conn.close()
