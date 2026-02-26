from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json
import multiprocessing
import csv
import io

def data_cleaner():
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
        clean_data = clean_garbage_data(raw_data)
        
        if clean_data is None:
            continue
        
        producer.send('clean_traffic',value=clean_data)
        producer.flush() 


def start_cleaning_data():
    workers =[]
    
    for i in range(5):
        p=multiprocessing.Process(target=data_cleaner)
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
    
    column_names = ["ID", "SPEED", "TRAVEL_TIME", "STATUS", "DATA_AS_OF", "LINK_ID", "LINK_POINTS", "ENCODED_POLY_LINE", 
        "ENCODED_POLY_LINE_LVLS", "OWNER", "TRANSCOM_ID", "BOROUGH", "LINK_NAME"]  
    
    f = io.StringIO(raw_values)
    reader = csv.reader(f, delimiter=',')
    actual_values = next(reader)

    data_dictionary = dict(zip(column_names, actual_values))
    
    try:
        speed=float(data_dictionary['SPEED'])
        link_id=int(data_dictionary['LINK_ID'])
        travel_time=int(data_dictionary['TRAVEL_TIME'])
        datetime.strptime(data_dictionary['DATA_AS_OF'], '%m/%d/%Y %I:%M:%S %p')
    except (ValueError, TypeError):
        return None

    if speed <= 0 or speed > 200:
        return None
    if link_id <= 0:
        return None
    if travel_time <= 0 or travel_time>7200:
        return None
    if not data_dictionary['LINK_NAME'].strip():
        return None

    data_dictionary['LINK_NAME'] = data_dictionary.get('LINK_NAME', '').strip()
    data_dictionary['BOROUGH'] = data_dictionary.get('BOROUGH', '').strip()
    
    new_dictionary = {
    'DATA_AS_OF':data_dictionary['DATA_AS_OF'],
    'LINK_ID':data_dictionary['LINK_ID'],
    'LINK_NAME':data_dictionary['LINK_NAME'],
    'SPEED':data_dictionary['SPEED'],
    'TRAVEL_TIME':data_dictionary['TRAVEL_TIME'],
    'BOROUGH': data_dictionary['BOROUGH'],
    'LINK_POINTS': data_dictionary['LINK_POINTS']
    }
    
    return new_dictionary
