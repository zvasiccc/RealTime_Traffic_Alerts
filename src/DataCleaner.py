from kafka import KafkaConsumer, KafkaProducer
import json
import multiprocessing
import time
import os

def data_cleaner(worker_id):
    consumer = KafkaConsumer(
        'raw_traffic',
        bootstrap_servers='localhost:29092',
        auto_offset_reset='earliest',
        group_id='dataCleaners',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),     
    )
    
    producer = KafkaProducer(
        bootstrap_servers='localhost:29092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for message in consumer:
        raw_data=message.value
        structured_data = clean_garbage_data(raw_data)
        print(structured_data)
        print(f"LINK_ID je : {structured_data['LINK_ID']} Brzina je: {structured_data['SPEED']}")
        print("----------------------")
        
        #TODO ciscenje podataka
        producer.send('clean_traffic',value=structured_data)
        producer.flush() 


def start_cleaning_data():
    workers =[]
    
    for i in range(5):
        p=multiprocessing.Process(target=data_cleaner,args=(i,))
        workers.append(p)
        p.start()
        
    try:
        for p in workers:
            p.join()
    except KeyboardInterrupt:
        for p in workers:
            p.terminate()
            

def clean_garbage_data(raw_message):
    big_message_key = next((k for k in raw_message.keys() if 'ID' in k and 'SPEED' in k), None)
    
    if not big_message_key:
        return None 
    
    raw_values = raw_message[big_message_key]
    
    column_names = [
        "ID", "SPEED", "TRAVEL_TIME", "STATUS", "DATA_AS_OF", 
        "LINK_ID", "LINK_POINTS", "ENCODED_POLY_LINE", 
        "ENCODED_POLY_LINE_LVLS", "OWNER", "TRANSCOM_ID", 
        "BOROUGH", "LINK_NAME"
    ]
    
    import csv
    import io
    
    f = io.StringIO(raw_values)
    reader = csv.reader(f, delimiter=',')
    actual_values = next(reader)

    clean_dict = dict(zip(column_names, actual_values))
    
    clean_dict['ingestion_timestamp'] = raw_message.get('ingestion_timestamp')
    
    return clean_dict
