import psycopg2

from src.GlobalVariables import LAT_MIN, LAT_MAX, LON_MIN, LON_MAX

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

def format_to_wkt(link_points_raw):

    points = link_points_raw.strip().split()
        
    formatted_pairs = []
    for pair in points:
        if ',' not in pair:
            continue
        
        coords = pair.split(',')
        if len(coords) >= 2:
            try:
                lat = coords[0]
                lon = coords[1]
                if not lat or not lon:
                    continue

                if not (LAT_MIN < float(lat) < LAT_MAX): 
                    continue
                if not (LON_MIN < float(lon) < LON_MAX):
                    continue
                

                formatted_pairs.append(f"{lon} {lat}")
            except ValueError:
                continue
    
    if len(formatted_pairs) < 2:
        return None
            
    return f"LINESTRING({', '.join(formatted_pairs)})"
