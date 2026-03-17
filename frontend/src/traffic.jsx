import { useEffect, useState, useRef } from 'react'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'

const START_TIME = new Date("2017-09-10T02:00:00Z")
const STEP_MINUTES = 5

function formatTime(date) {
    const pad = (n) => String(n).padStart(2, '0')
    return `${date.getUTCFullYear()}-${pad(date.getUTCMonth()+1)}-${pad(date.getUTCDate())} ${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}+00`
}

function useTraffic(time, playing) {
    const [data, setData] = useState(null)
     const initialFetched = useRef(false)

    useEffect(() => {
        if (!time ) return
        if (!playing && initialFetched.current) return 

        initialFetched.current = true
        const url = `http://127.0.0.1:8000/api/traffic?time=${encodeURIComponent(formatTime(time))}`
        fetch(url)
            .then(res => res.json())
            .then(setData)
    }, [time,playing])

    return { data }
}

const Traffic = () => {
    const mapContainer = useRef(null)
    const map = useRef(null)
    const mapLoaded = useRef(false)
    const [currentTime, setCurrentTime] = useState(START_TIME)
    const [playing, setPlaying] = useState(false) 
    const { data } = useTraffic(currentTime, playing)

    useEffect(() => {
        if (!playing) return
        const interval = setInterval(() => {
            setCurrentTime(prev => {
                const next = new Date(prev.getTime() + STEP_MINUTES * 60 * 1000)
                return next
            })
        }, 700)
        return () => clearInterval(interval)
    }, [playing])

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
            mapLoaded.current = true

            map.current.addSource('traffic', {
                type: 'geojson',
                data: { type: 'FeatureCollection', features: [] }
            })

            map.current.addLayer({
                id: 'traffic-lines',
                type: 'line',
                source: 'traffic',
                layout: { 'line-join': 'round', 'line-cap': 'round' },
                paint: {
                    'line-color': ['get', 'line_color'],
                    'line-width': 2
                }
            })

            const popup = new maplibregl.Popup({ closeButton: false, closeOnClick: false })
            map.current.on('mouseenter', 'traffic-lines', (e) => {
                map.current.getCanvas().style.cursor = 'pointer'
                const { current_speed, warning_type } = e.features[0].properties
                popup
                    .setLngLat(e.lngLat)
                    .setHTML(`<div style="font-family:monospace;font-size:12px">
                        <b>${warning_type}</b>
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
        if (!data || !mapLoaded.current) return
        const source = map.current.getSource('traffic')
        if (source) source.setData(data)
    }, [data])

    return (
        <div style={{ display: 'flex', flexDirection: 'column', height: '100vh' }}>
            <div style={{
                position: 'absolute', top: 10, left: 10, zIndex: 10,
                background: 'rgba(0,0,0,0.7)', color: 'white',
                padding: '6px 12px', borderRadius: 8, fontFamily: 'monospace', fontSize: 13
            }}>
                time: {formatTime(currentTime).slice(0, 16)}
            </div>
            <button
    onClick={() => setPlaying(p => !p)}
    style={{
        position: 'absolute', bottom: 30, left: '50%', transform: 'translateX(-50%)',
        zIndex: 10, background: 'rgba(0,0,0,0.75)', color: 'white',
        border: '1px solid white', padding: '8px 20px', borderRadius: 8,
        cursor: 'pointer', fontFamily: 'monospace', fontSize: 14
    }}>
    {playing ? 'Stop' : 'Start'}
    </button>
            <div ref={mapContainer} style={{ flex: 1 }} />
        </div>
    )
}

export default Traffic