import { useState, useEffect, useCallback } from 'react'

interface TopItem {
  rank: number
  ticker: string
  name: string
  price: number
  change_pct: number
  amount: number
}

const th: React.CSSProperties = { padding: '4px 8px', textAlign: 'left', fontWeight: 600 }
const td: React.CSSProperties = { padding: '4px 8px' }

const fmtAmt = (v: number) =>
  v >= 1e12 ? `${(v / 1e12).toFixed(1)}조`
  : v >= 1e8 ? `${(v / 1e8).toFixed(0)}억`
  : `${v.toLocaleString()}원`

export default function Top() {
  const [items, setItems] = useState<TopItem[]>([])
  const [fetchedAt, setFetchedAt] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const load = useCallback(async (refresh = false) => {
    setLoading(true)
    setError('')
    try {
      const r = await fetch(`/api/top?n=20${refresh ? '&refresh=true' : ''}`)
      const d = await r.json()
      setItems(d.items ?? [])
      setFetchedAt(d.fetched_at ?? '')
      if (d.error) setError(d.error)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  useEffect(() => {
    const t = setInterval(() => load(), 5 * 60 * 1000)
    return () => clearInterval(t)
  }, [load])

  return (
    <div style={{ padding: 12, overflow: 'auto', height: '100%' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
        <span style={{ fontSize: 13, fontWeight: 700 }}>거래대금 상위 20</span>
        <span style={{ fontSize: 11, color: '#64748b' }}>
          {fetchedAt && `갱신: ${fetchedAt}`}
          <button
            onClick={() => load(true)}
            disabled={loading}
            style={{
              marginLeft: 8, padding: '2px 8px', fontSize: 11,
              background: '#1e293b', border: '1px solid #334155',
              color: '#94a3b8', cursor: 'pointer', borderRadius: 4,
            }}
          >
            {loading ? '…' : '↻'}
          </button>
        </span>
      </div>

      {error && (
        <div style={{ color: '#f87171', fontSize: 11, marginBottom: 8 }}>
          {error.includes('ka10052') || error.includes('API') ?
            `API 미검증: Kiwoom OpenAPI 포털에서 거래대금상위 API ID 확인 필요 (${error})` :
            error}
        </div>
      )}

      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
        <thead>
          <tr style={{ color: '#64748b', borderBottom: '1px solid #1e293b' }}>
            <th style={th}>#</th>
            <th style={th}>종목명</th>
            <th style={{ ...th, textAlign: 'right' }}>현재가</th>
            <th style={{ ...th, textAlign: 'right' }}>등락률</th>
            <th style={{ ...th, textAlign: 'right' }}>거래대금</th>
          </tr>
        </thead>
        <tbody>
          {items.map(it => (
            <tr key={it.ticker} style={{ borderBottom: '1px solid #0f172a' }}>
              <td style={td}>{it.rank}</td>
              <td style={td}>
                {it.name}
                <span style={{ color: '#475569', marginLeft: 4, fontSize: 10 }}>
                  {it.ticker.replace(/_[A-Z]+$/, '')}
                </span>
              </td>
              <td style={{ ...td, textAlign: 'right' }}>
                {it.price.toLocaleString()}
              </td>
              <td style={{
                ...td, textAlign: 'right',
                color: it.change_pct > 0 ? '#f87171' : it.change_pct < 0 ? '#60a5fa' : '#64748b',
              }}>
                {it.change_pct > 0 ? '+' : ''}{it.change_pct.toFixed(2)}%
              </td>
              <td style={{ ...td, textAlign: 'right' }}>{fmtAmt(it.amount)}</td>
            </tr>
          ))}
        </tbody>
      </table>

      {items.length === 0 && !loading && !error && (
        <div style={{ textAlign: 'center', color: '#64748b', marginTop: 40, fontSize: 12 }}>
          데이터 없음 (장 종료 후 또는 API 미검증)
        </div>
      )}
    </div>
  )
}
