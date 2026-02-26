from fastapi import FastAPI
import psycopg2
import psycopg2.extras
from DB.DB_functions import get_connection
import json
app = FastAPI()


@app.get("/api/traffic")
async def get_traffic():
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute("""
        SELECT 
            r.link_id,
            r.link_name,
            r.borough,
            ST_AsGeoJSON(r.geom) as geojson,
            w.warning_type,
            w.speed as current_speed,
            w.avg_speed
        FROM road_geometries r
        LEFT JOIN (
            SELECT DISTINCT ON (link_id)
                link_id, warning_type, speed, avg_speed
            FROM traffic_warnings
            WHERE time >= (SELECT max(time) FROM traffic_warnings) - INTERVAL '15 minutes'
            ORDER BY link_id, time DESC
        ) w ON r.link_id = w.link_id
    """)

    rows=cursor.fetchall()
    cursor.close()
    connection.close()

    features=[]
    for row in rows:
        if not row['geojson']:
            continue
        
        color = '#00ff00'
        if row['warning_type']=='VERY_SLOW_TRAFFIC':
            color = '#ff0000'
        if row['warning_type']=='SLOW_TRAFFIC':
            color='#ffff00'

        features.append({
            "type": "Feature",
            "geometry": json.loads(row['geojson']),
            "properties": {
                "link_id": row['link_id'],
                "link_name": row['link_name'],
                "borough": row['borough'],
                "warning_type": row['warning_type'] or 'NORMAL_TRAFFIC',
                "current_speed": row['current_speed'],
                "avg_speed": row['avg_speed'],
                "line_color": color
            }
        })

    return {"type": "FeatureCollection", "features": features}
    