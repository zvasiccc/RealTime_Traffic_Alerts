import psycopg2

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="traffic_monitoring",
        user="zeljko",
        password="lozinka123"
    )

def setup_database():
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")

    #svi cisti podaci
    cur.execute("""
        CREATE TABLE IF NOT EXISTS traffic_data (
            time        TIMESTAMPTZ NOT NULL,
            link_id     INT NOT NULL,
            speed       FLOAT,
            travel_time INT,
            status      INT,
            link_name   TEXT,
            borough     TEXT,
            hour_of_day INT,
            day_period  TEXT,
            is_weekend BOOLEAN
        );
    """)
    cur.execute("""
        SELECT create_hypertable('traffic_data', 'time', if_not_exists => TRUE);
    """)

    #statistika
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

    # WARNINGS LOG
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
    print("[4/4] Warnings tabela kreirana.")

    # CONTINUOUS AGGREGATE
    cur.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS traffic_stats_hourly
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('1 hour', time) AS bucket,
            link_id,
            avg(speed)                  AS avg_speed,
            min(speed)                  AS min_speed,
            max(speed)                  AS max_speed,
            count(*)                    AS sample_count
        FROM traffic_data
        GROUP BY bucket, link_id;
    """)

    try:
        cur.execute("""
            SELECT add_continuous_aggregate_policy('traffic_stats_hourly',
                start_offset      => INTERVAL '1 month',
                end_offset        => INTERVAL '1 minute',
                schedule_interval => INTERVAL '5 minutes');
        """)
    except:
        print("Politika vec postoji, preskacem.")

    try:
        cur.execute("""
            SELECT add_retention_policy('traffic_data', INTERVAL '6 months');
        """)
    except:
        print("Retention policy vec postoji.")

    cur.close()
    conn.close()
    print("Baza je spremna.")
