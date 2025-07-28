from clickhouse_driver import Client


def create_tables(client: Client):
    client.execute('CREATE DATABASE IF NOT EXISTS events ON CLUSTER company_cluster')

    client.execute("""
    CREATE TABLE IF NOT EXISTS events.event_base ON CLUSTER company_cluster
    (
        event_id UUID,
        user_id UUID,
        event_type Enum8(
            'VIDEO_QUALITY_CHANGE' = 1,
            'VIDEO_WATCHED_TO_END' = 2,
            'SEARCH_FILTER_USED' = 3,
            'CLICK' = 4,
            'PAGE_VIEW' = 5
            ),
        timestamp DateTime,
        session_id Nullable(UUID)
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events.event_base', '{replica}')
    ORDER BY (timestamp, user_id, event_type)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS events.events_video_quality ON CLUSTER company_cluster
    (
        event_id UUID,
        from_quality LowCardinality(String),
        to_quality LowCardinality(String)
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events.events_video_quality', '{replica}')
    ORDER BY event_id;
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS events.video_watched_to_end ON CLUSTER company_cluster
    (
        event_id UUID,
        movie_id UUID,
        watch_time_seconds UInt16
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events.video_watched_to_end', '{replica}')
    ORDER BY event_id;
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS events.search_filter_used ON CLUSTER company_cluster
    (
        event_id UUID,
        filter_type String,
        value UInt16
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events.search_filter_used', '{replica}')
    ORDER BY event_id;
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS events.click ON CLUSTER company_cluster
    (
        event_id UUID,
        element_type LowCardinality(String),
        element_id String
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events.click', '{replica}')
    ORDER BY event_id;
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS events.page_view ON CLUSTER company_cluster
    (
        event_id UUID,
        page LowCardinality(String),
        duration_seconds UInt16
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events.page_view', '{replica}')
    ORDER BY event_id;
    """)
