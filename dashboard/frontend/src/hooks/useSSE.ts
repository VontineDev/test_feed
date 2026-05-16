import { useEffect, useRef, useState } from 'react'

export function useSSE<T>(url: string, maxItems = 50): T[] {
  const [items, setItems] = useState<T[]>([])
  const esRef = useRef<EventSource | null>(null)

  useEffect(() => {
    const es = new EventSource(url)
    esRef.current = es

    es.onmessage = (e: MessageEvent) => {
      try {
        const incoming: T[] = JSON.parse(e.data)
        setItems(prev => [...incoming, ...prev].slice(0, maxItems))
      } catch {
        // ignore malformed frames
      }
    }

    es.onerror = () => {
      // EventSource auto-reconnects; log only for debugging
      console.warn('[SSE] connection error, will retry')
    }

    return () => {
      es.close()
      esRef.current = null
    }
  }, [url, maxItems])

  return items
}
