from src.SideFunctions import get_connection

def setup_database():
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis CASCADE;")  
    #all clear data
    cur.execute("""
        CREATE TABLE IF NOT EXISTS traffic_data (
            time TIMESTAMPTZ NOT NULL,
            link_id INT NOT NULL,
            link_name TEXT,
            speed FLOAT,
            travel_time INT,
            borough TEXT,
            hour_of_day INT,
            day_period TEXT,
            is_weekend BOOLEAN
            );
        """)
    
    #table partitioning by time
    cur.execute("""
        SELECT create_hypertable('traffic_data', 'time', if_not_exists => TRUE);
    """)
    print(1)
    #statistics data
    cur.execute("""
        CREATE TABLE IF NOT EXISTS traffic_stats (
            link_id INT NOT NULL,
            hour_of_day INT NOT NULL,
            day_period TEXT NOT NULL,
            is_weekend BOOLEAN NOT NULL DEFAULT FALSE,
            avg_speed FLOAT,
            p10_speed FLOAT,
            p25_speed FLOAT,
            sample_count INT,
            updated_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (link_id, hour_of_day, is_weekend)
        );
    """)

    #warnings
    cur.execute("""
        CREATE TABLE IF NOT EXISTS traffic_warnings (
            time TIMESTAMPTZ NOT NULL,
            link_id INT NOT NULL,
            speed FLOAT,
            avg_speed FLOAT,
            p10_speed FLOAT,
            p25_speed FLOAT,
            hour_of_day INT,
            day_period TEXT,
            warning_type TEXT
        );
    """)
    cur.execute("""
        SELECT create_hypertable('traffic_warnings', 'time', if_not_exists => TRUE);
    """)
    print(1)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS road_geometries(
                link_id INT PRIMARY KEY,
                link_name TEXT,
                borough TEXT,
                geom GEOMETRY(LineString, 4326)
                )              
                """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_road_geometries_geom 
        ON road_geometries USING GIST (geom);
    """)
    print(2)
    cur.close()
    conn.close()

def load_existing_geometries(cursor):
    cursor.execute("SELECT link_id FROM road_geometries")
    return set(row[0] for row in cursor.fetchall())