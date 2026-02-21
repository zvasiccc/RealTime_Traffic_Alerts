import psycopg2

def day_period(hour: int) -> str:
    if 7 <= hour <= 9:
        return 'morning_peak'
    elif 10 <= hour <= 15:
        return 'day_time'
    elif 16 <= hour <= 18:
        return 'evening_peak'
    else:
        return 'night'
    
def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="traffic_monitoring",
        user="zeljko",
        password="lozinka123"
    )