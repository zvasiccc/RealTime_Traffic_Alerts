from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime, timezone
from DB.setup import get_connection
from src.SideFunctions import day_period

def start_detecting_congestion():
    consumer = KafkaConsumer(
            'clean_traffic',
            bootstrap_servers='localhost:29092',
            group_id='detectorsGroup',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')), 
        )
    
    producer = KafkaProducer(
        bootstrap_servers='localhost:29092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    
    connection = get_connection()
    cursor = connection.cursor()
    
    for message in consumer:
        data=message.value
        
        link_id=int(data.get('LINK_ID',0))
        speed= float(data.get('SPEED',0))
        link_name = data.get('LINK_NAME', '')
        borough   = data.get('BOROUGH', '')
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
        is_weekend = time.weekday() >= 5
        
        #historical stats 
        cursor.execute(""" 
            SELECT avg_speed, p10_speed, p25_speed, sample_count
            FROM traffic_stats
            WHERE link_id=%s 
                AND hour_of_day=%s
                AND is_weekend = %s
            """,(link_id,hour,is_weekend))
        
        stats = cursor.fetchone()
        
        if not stats:
            continue

        avg_speed, p10_speed, p25_speed, sample_count = stats
        
        if sample_count<7:
            continue
        
        #stats last 15 min
        cursor.execute("""
            SELECT avg(speed) 
            FROM traffic_data
            WHERE link_id=%s
                AND time >= %s - INTERVAL '15 minutes'
                AND is_weekend = %s
        """,(link_id,time,is_weekend))
        
        result = cursor.fetchone()
        if not result or result[0]is None:
            continue
        
        speed_average_last_15min = float(result[0])   
        
        warning_type = None
        if speed_average_last_15min < p10_speed:
            warning_type = 'VERY_SLOW_TRAFFIC'
        elif (speed_average_last_15min >= p10_speed and speed_average_last_15min < p25_speed):
            warning_type = 'SLOW_TRAFFIC'

        if warning_type:
            print(f"Alert! {warning_type}: {link_name}")
        
        if warning_type:
            warning = {
                'timestamp': time.isoformat(),
                'link_id': link_id,
                'link_name': link_name,
                'borough': borough,
                'speed': speed,
                'avg_speed': avg_speed,
                'p10_speed': p10_speed,
                'p25_speed': p25_speed,
                'hour_of_day': hour,
                'day_period': period,
                'warning_type': warning_type
            }
            producer.send('traffic_alerts', warning)
            producer.flush()
            cursor.execute("""
                INSERT INTO traffic_warnings
                    (time, link_id, speed, avg_speed, p10_speed, p25_speed, hour_of_day, day_period, warning_type)
                VALUES
                    (%s, %s, %s, %s, %s, %s,  %s, %s, %s)
            """, (time, link_id, speed, avg_speed, p10_speed, p25_speed, hour, period, warning_type))
            connection.commit()
            

            
            
        
