import pandas as pd
from kafka import KafkaProducer
import json
import time
from src.LoadData import load_data
NUM_OF_ROWS = 100

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)


def simulate_traffic():


    traffic_chunks = load_data(5000)

    for chunk in traffic_chunks:

        for i in range(0, len(chunk), NUM_OF_ROWS):
            batch = chunk.iloc[i : i + NUM_OF_ROWS]
            
            for index, row in batch.iterrows():
                payload = row.to_dict()
                
                producer.send(
                    'raw_traffic', 
                    key=str(payload.get('ID', 'unknown')), 
                    value=payload)
            
            producer.flush() 
            time.sleep(0.1)
