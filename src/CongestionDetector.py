from kafka import KafkaConsumer
import json

def start_detecting_congestion():
    consumer = KafkaConsumer(
            'clean_traffic',
            bootstrap_servers='localhost:29092',
            group_id='detectorsGroup',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')), 
    )
    
    for message in consumer:
        data=message.value
        # print("detector cita poruku", data)
        # print("-------------------------------------------------------------")