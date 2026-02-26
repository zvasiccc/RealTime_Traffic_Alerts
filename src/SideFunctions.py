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

def format_to_wkt(link_points_raw):
    if not link_points_raw:
        return None
    
    try:

        points = link_points_raw.strip().split()
        
        formatted_pairs = []
        for p in points:
            if ',' not in p:
                continue
            
            coords = p.split(',')
            if len(coords) >= 2:
                lat = coords[0].strip()
                lon = coords[1].strip()
                if lat and lon:
                    formatted_pairs.append(f"{lon} {lat}")
        
        if len(formatted_pairs) < 2:
            return None
            
        return f"LINESTRING({', '.join(formatted_pairs)})"
    except Exception:
        return None