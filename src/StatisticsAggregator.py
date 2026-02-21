from kafka import KafkaConsumer
import json

from datetime import datetime, timezone
from src.SideFunctions import day_period, get_connection



def start_aggregating_statistics():
    consumer = KafkaConsumer(
        'clean_traffic',
        bootstrap_servers='localhost:29092',
        group_id='aggregatorsGroup',
        auto_offset_reset='earliest',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    )

    connection = get_connection()
    cursor = connection.cursor()

    for message in consumer:
        data = message.value

        try:
            link_id = int(data.get('LINK_ID', 0))
            speed = float(data.get('SPEED', 0))
            travel_time = int(data.get('TRAVEL_TIME', 0))
            link_name= data.get('LINK_NAME', '')
            borough = data.get('BOROUGH', '')

            timestamp = data.get('DATA_AS_OF') 
            if timestamp:
                time = datetime.strptime(timestamp, '%m/%d/%Y %I:%M:%S %p')
                time = time.replace(tzinfo=timezone.utc)
            else:
                time = datetime.now(timezone.utc)
            hour = time.hour if time.minute<30 else time.hour+1
            if hour == 24:
                hour = 0
            period = day_period(hour)
            is_weekend = time.weekday()>=5 #saturday=5, sunday=6
            
            cursor.execute("""
                INSERT INTO traffic_data
                    (time, link_id, link_name, speed, travel_time,  borough, hour_of_day, day_period,is_weekend)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (time, link_id,link_name, speed, travel_time, borough, hour, period,is_weekend))
            
            cursor.execute("""
            INSERT INTO traffic_stats 
                (link_id, hour_of_day, day_period, is_weekend, avg_speed, p10_speed, p25_speed,  sample_count, updated_at)
            SELECT
                %s, %s, %s, %s,
                avg(speed),
                percentile_cont(0.10) WITHIN GROUP (ORDER BY speed),
                percentile_cont(0.25) WITHIN GROUP (ORDER BY speed),
                count(*),
                now()
            FROM traffic_data
            WHERE link_id = %s AND hour_of_day = %s AND is_weekend = %s
            ON CONFLICT (link_id, hour_of_day, is_weekend)
            DO UPDATE SET
                avg_speed = EXCLUDED.avg_speed,
                p10_speed = EXCLUDED.p10_speed,
                p25_speed = EXCLUDED.p25_speed,
                sample_count = EXCLUDED.sample_count,
                updated_at = EXCLUDED.updated_at;
        """, (link_id, hour, period, is_weekend, link_id, hour, is_weekend))

            connection.commit()

        except Exception as e:
            connection.rollback()
            print(f"Error in aggregating statistics: {e}")