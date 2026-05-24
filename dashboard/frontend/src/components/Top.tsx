import { useState, useEffect, useCallback } from 'react'
import { tokens, pctTextColor } from '../tokens'

interface TopItem {
  rank: number
  ticker: string
  name: string
  price: number
  change_pct: number
  amount: number
  eps?: number | null
}

const th: React.CSSProperties = { padding: '4px 8px', textAlign: 'left', fontWeight: 600, whiteSpace: 'nowrap' as const }
const td: React.CSSProperties = { padding: '4px 8px' }
const tdNum: React.CSSProperties = { padding: '4px 8px', textAlign: 'right' as const, fontVariantNumeric: 'tabular-nums' }

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
      const r = await fetch(`/api/top?n=50${refresh ? '&refresh=true' : ''}`)
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
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
        <span style={{ fontSize: 13, fontWeight: 700 }}>거래대금 상위 {items.length || 50}</span>
        <span style={{ fontSize: 11, color: tokens.tx.muted }}>
          {fetchedAt && `갱신: ${fetchedAt}`}
          <button
            onClick={() => load(true)}
            disabled={loading}
            className="app-refresh-btn"
            style={{
              marginLeft: 8, padding: '4px 10px', fontSize: 13,
              background: tokens.bg.raised, border: `1px solid ${tokens.bd.emphasis}`,
              color: tokens.tx.secondary, cursor: 'pointer', borderRadius: tokens.radius.sm,
              minWidth: 36, minHeight: 36,
            }}
          >
            {loading ? '…' : '↻'}
          </button>
        </span>
      </div>

      {error && (
        <div style={{ color: tokens.semantic.up, fontSize: 11, marginBottom: 8 }}>
          {error}
        </div>
      )}

      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
          <thead>
            <tr style={{ color: tokens.tx.muted, borderBottom: `1px solid ${tokens.bd.default}` }}>
              <th style={th}>#</th>
              <th style={th}>종목명</th>
              <th style={{ ...th, textAlign: 'right' }}>현재가</th>
              <th style={{ ...th, textAlign: 'right' }}>등락률</th>
              <th style={{ ...th, textAlign: 'right' }}>거래대금</th>
              <th style={{ ...th, textAlign: 'right' }}>EPS</th>
            </tr>
          </thead>
          <tbody>
            {items.map(it => (
              <tr key={it.ticker} style={{ borderBottom: `1px solid ${tokens.bg.row}` }}>
                <td style={td}>{it.rank}</td>
                <td style={td}>
                  {it.name}
                  <span style={{ color: tokens.tx.subtle, marginLeft: 4, fontSize: 10 }}>
                    {it.ticker.replace(/_[A-Z]+$/, '')}
                  </span>
                </td>
                <td style={tdNum}>
                  {it.price.toLocaleString()}
                </td>
                {/* 한국식 등락 — pctTextColor 헬퍼 사용 */}
                <td style={{ ...tdNum, color: pctTextColor(it.change_pct) }}>
                  {it.change_pct > 0 ? '+' : ''}{it.change_pct.toFixed(2)}%
                </td>
                <td style={tdNum}>{fmtAmt(it.amount)}</td>
                <td style={{ ...tdNum, color: it.eps != null && it.eps < 0 ? tokens.semantic.up : undefined }}>
                  {it.eps != null ? it.eps.toLocaleString() : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {items.length === 0 && !loading && !error && (
        <div style={{ textAlign: 'center', color: tokens.tx.muted, marginTop: 40, fontSize: 12 }}>
          <div>개장 시간(09:00~15:30)에 데이터가 갱신됩니다</div>
          <button
            onClick={() => load(true)}
            style={{
              marginTop: 12, padding: '8px 16px', fontSize: 12,
              background: tokens.bg.raised, border: `1px solid ${tokens.bd.emphasis}`,
              color: tokens.tx.secondary, cursor: 'pointer', borderRadius: tokens.radius.sm,
            }}
          >
            ↻ 새로고침
          </button>
        </div>
      )}
    </div>
  )
}
