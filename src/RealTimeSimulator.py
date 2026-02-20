import pandas as pd
from kafka import KafkaProducer
import json
import time
from datetime import datetime
from src.LoadData import load_data

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)


def simulate_traffic():
    print("simulate trafic")
    BROJ_REDOVA_PO_GRUPI = 3
    PAUZA_IZMEDJU_GRUPA = 3 

    traffic_chunks = load_data(500)

    for chunk in traffic_chunks:

        for i in range(0, len(chunk), BROJ_REDOVA_PO_GRUPI):
            batch = chunk.iloc[i : i + BROJ_REDOVA_PO_GRUPI]
            
            for index, row in batch.iterrows():
                payload = row.to_dict()
                payload['ingestion_timestamp'] = datetime.utcnow().isoformat()
                
                producer.send(
                    'raw_traffic', 
                    key=str(payload.get('ID', 'unknown')), 
                    value=payload
                )
            
            producer.flush() 
            time.sleep(PAUZA_IZMEDJU_GRUPA)
