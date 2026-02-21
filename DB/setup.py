from src.SideFunctions import get_connection

def setup_database():
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")

    #all clear data
    cur.execute("""
        CREATE TABLE IF NOT EXISTS traffic_data (
            time        TIMESTAMPTZ NOT NULL,
            link_id     INT NOT NULL,
            speed       FLOAT,
            travel_time INT,
            link_name   TEXT,
            borough     TEXT,
            hour_of_day INT,
            day_period  TEXT,
            is_weekend BOOLEAN
        );
    """)
    
    #table partitioning by time
    cur.execute("""
        SELECT create_hypertable('traffic_data', 'time', if_not_exists => TRUE);
    """)

    #statistics data
    cur.execute("""
        CREATE TABLE IF NOT EXISTS traffic_stats (
            link_id      INT NOT NULL,
            hour_of_day  INT NOT NULL,
            day_period   TEXT NOT NULL,
            is_weekend   BOOLEAN NOT NULL DEFAULT FALSE,
            avg_speed    FLOAT,
            p10_speed    FLOAT,
            p25_speed    FLOAT,
            p75_speed    FLOAT,
            sample_count INT,
            updated_at   TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (link_id, hour_of_day, is_weekend)
        );
    """)

    #warnings
    cur.execute("""
        CREATE TABLE IF NOT EXISTS traffic_warnings (
            time         TIMESTAMPTZ NOT NULL,
            link_id      INT NOT NULL,
            speed        FLOAT,
            avg_speed    FLOAT,
            p10_speed    FLOAT,
            hour_of_day  INT,
            day_period   TEXT,
            warning_type TEXT,
            source       TEXT
        );
    """)
    cur.execute("""
        SELECT create_hypertable('traffic_warnings', 'time', if_not_exists => TRUE);
    """)


    cur.close()
    conn.close()
