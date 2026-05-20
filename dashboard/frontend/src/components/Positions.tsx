import { useEffect, useState } from 'react'

interface Position {
  id: number
  ticker: string
  name: string
  model: string
  signal_date: string
  entry_actual: number | null
  qty: number
  status: string
  tp1_pct: number | null
  trail_pct: number | null
  current_price: number | null
  unrealized_pct: number | null
}

const pctColor = (v: number | null) => {
  if (v === null) return '#94a3b8'
  if (v > 0) return '#4ade80'
  if (v < 0) return '#f87171'
  return '#94a3b8'
}

interface Props {
  onSelect?: (ticker: string, name: string) => void
  selectedTicker?: string | null
}

export default function Positions({ onSelect, selectedTicker }: Props) {
  const [rows, setRows] = useState<Position[]>([])
  const [loading, setLoading] = useState(true)

  const load = () => {
    fetch('/api/positions')
      .then(r => r.json())
      .then(j => { setRows(j.data); setLoading(false) })
      .catch(() => setLoading(false))
  }

  useEffect(() => {
    load()
    const id = setInterval(load, 60_000)
    return () => clearInterval(id)
  }, [])

  if (loading) return <div style={styles.empty}>포지션 로딩 중…</div>
  if (!rows.length) return <div style={styles.empty}>오픈 포지션 없음</div>

  return (
    <div style={styles.wrap}>
      <div style={styles.hdr}>모의투자 포지션 ({rows.length})</div>
      <table style={styles.table}>
        <thead>
          <tr style={styles.theadRow}>
            {['종목', '모델', '진입일', '진입가', '현재가', '수익률', '상태'].map(h => (
              <th key={h} style={styles.th}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map(r => {
            const isSelected = r.ticker === selectedTicker
            return (
              <tr
                key={r.id}
                style={{
                  ...styles.tr,
                  background: isSelected ? '#0f2849' : undefined,
                  cursor: onSelect ? 'pointer' : 'default',
                }}
                onClick={() => onSelect?.(r.ticker, r.name)}
              >
                <td style={styles.td}>
                  <div style={{ fontWeight: 600, color: isSelected ? '#93c5fd' : '#e2e8f0' }}>{r.name}</div>
                  <div style={{ fontSize: 10, color: '#475569' }}>{r.ticker}</div>
                </td>
                <td style={styles.td}><span style={styles.badge}>{r.model}</span></td>
                <td style={styles.td}>{r.signal_date?.slice(0, 10)}</td>
                <td style={styles.td}>{r.entry_actual?.toLocaleString() ?? '—'}</td>
                <td style={styles.td}>{r.current_price?.toLocaleString() ?? '—'}</td>
                <td style={{ ...styles.td, color: pctColor(r.unrealized_pct), fontWeight: 600 }}>
                  {r.unrealized_pct !== null ? `${r.unrealized_pct > 0 ? '+' : ''}${r.unrealized_pct.toFixed(2)}%` : '—'}
                </td>
                <td style={styles.td}>{r.status}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  wrap: { padding: 12, overflowX: 'auto' },
  hdr: { fontWeight: 700, fontSize: 13, marginBottom: 8, color: '#94a3b8' },
  table: { width: '100%', borderCollapse: 'collapse', fontSize: 12 },
  theadRow: { background: '#1e293b' },
  th: { padding: '6px 10px', textAlign: 'left', color: '#64748b', fontWeight: 600, borderBottom: '1px solid #334155' },
  tr: { borderBottom: '1px solid #1e293b' },
  td: { padding: '6px 10px', color: '#e2e8f0' },
  badge: { background: '#1e3a5f', color: '#93c5fd', padding: '2px 6px', borderRadius: 4, fontSize: 11 },
  empty: { padding: 24, color: '#64748b', textAlign: 'center' },
}
