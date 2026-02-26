import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import Traffic from './traffic.jsx'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <Traffic />
  </StrictMode>,
)
