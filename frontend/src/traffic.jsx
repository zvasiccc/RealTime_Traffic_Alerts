import { useEffect, useState, useCallback, useRef } from 'react'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'


const API_URL = 'http://127.0.0.1:8000/api/traffic'
const REFRESH_MS = 1 * 1000

function useTraffic() {
    const [data, setData] = useState(null)

    const fetch_data = useCallback(async () => {
        const res = await fetch(API_URL)
        const json = await res.json()
        setData(json)        
    }, [])

    useEffect(() => {
        fetch_data()
        const interval = setInterval(fetch_data, REFRESH_MS)
        return () => clearInterval(interval)
    }, [fetch_data])

    return { data }
}

const Traffic = () => {
    const mapContainer = useRef(null)
    const map = useRef(null)
    const { data } = useTraffic()

    useEffect(() => {
        if (map.current) return

        map.current = new maplibregl.Map({
            container: mapContainer.current,
            style: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
            center: [-74.006, 40.7128],
            zoom: 11
        })

        map.current.addControl(new maplibregl.NavigationControl(), 'top-right')

        map.current.on('load', () => {
            map.current.addSource('traffic', {
                type: 'geojson',
                data: { type: 'FeatureCollection', features: [] }
            })

            map.current.addLayer({
                id: 'traffic-lines',
                type: 'line',
                source: 'traffic',
                layout: {
                    'line-join': 'round',
                    'line-cap': 'round'
                },
                paint: {
                    'line-color': ['get','line_color'],
                    'line-width': [
                        'match',
                        ['get', 'warning_type'],
                        'VERY_SLOW_TRAFFIC', 5,
                        'SLOW_TRAFFIC', 4,
                        'NORMAL_TRAFFIC', 2,
                        10
                    ],
                }
            })

            const popup = new maplibregl.Popup({ closeButton: false, closeOnClick: false })
            map.current.on('mouseenter', 'traffic-lines', (e) => {
                map.current.getCanvas().style.cursor = 'pointer'
                const { current_speed, warning_type } = e.features[0].properties
                popup
                    .setLngLat(e.lngLat)
                    .setHTML(`<div style="font-family:monospace;font-size:12px">
                        <b> ${warning_type}</b> 
                        ${warning_type !== 'NORMAL_TRAFFIC' ? `<br/>${current_speed} km/h` : ''}
                    </div>`)
                    .addTo(map.current)
            })

            map.current.on('mouseleave', 'traffic-lines', () => {
                map.current.getCanvas().style.cursor = ''
                popup.remove()
            })
        })
    }, [])

    useEffect(() => {
        if (!data || !map.current) return
        const source = map.current.getSource('traffic')
        if (source) source.setData(data)
    }, [data])

    return (
        <div style={{ display: 'flex', flexDirection: 'column', height: '100vh' }}>
            
            <div ref={mapContainer} style={{ flex: 1 }} />
        </div>
    )
}

export default Traffic