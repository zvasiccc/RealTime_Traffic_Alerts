from kafka import KafkaConsumer
import json

def start_aggregating_statistics():    
    consumer = KafkaConsumer(
            'clean_traffic',
            bootstrap_servers='localhost:29092',
            group_id='aggregatorsGroup',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')), 
    )
    
    for message in consumer:
        data=message.value
        # print("aggregator cita poruku:",data)
        # print("-------------------------------------------------------------")
    

