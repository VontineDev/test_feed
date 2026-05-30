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
  per?: number | null
  forward_per?: number | null
}

interface TopResponse {
  items: TopItem[]
  fetched_at: string
  is_aftermarket?: boolean
  error?: string
  stale?: boolean
}

const th: React.CSSProperties = { padding: '3px 5px', textAlign: 'left', fontWeight: 600, whiteSpace: 'nowrap' as const }
const td: React.CSSProperties = { padding: '3px 5px' }
const tdNum: React.CSSProperties = { padding: '3px 5px', textAlign: 'right' as const, fontVariantNumeric: 'tabular-nums' }

const fmtAmt = (v: number) =>
  v >= 1e12 ? `${(v / 1e12).toFixed(1)}조`
  : v >= 1e8 ? `${(v / 1e8).toFixed(0)}억`
  : `${v.toLocaleString()}원`

const fmtPer = (v: number | null | undefined): string => {
  if (v == null) return '—'
  return `${v.toFixed(1)}x`
}

export default function Top() {
  const [items, setItems] = useState<TopItem[]>([])
  const [fetchedAt, setFetchedAt] = useState('')
  const [isAftermarket, setIsAftermarket] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const load = useCallback(async (refresh = false) => {
    setLoading(true)
    setError('')
    try {
      const r = await fetch(`/api/top?n=50${refresh ? '&refresh=true' : ''}`)
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      const d: TopResponse = await r.json()
      setItems(d.items ?? [])
      setFetchedAt(d.fetched_at ?? '')
      setIsAftermarket(d.is_aftermarket ?? false)
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
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8, alignItems: 'center', flexWrap: 'wrap', gap: 6 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: 13, fontWeight: 700 }}>거래대금 상위 {items.length || 50}</span>
          {isAftermarket && (
            <span style={{
              fontSize: 10, padding: '2px 6px', borderRadius: 10,
              background: tokens.bg.raised, border: `1px solid ${tokens.bd.emphasis}`,
              color: tokens.tx.muted, fontWeight: 600,
            }}>
              전일 합산
            </span>
          )}
        </div>
        <span style={{ fontSize: 11, color: tokens.tx.muted, display: 'flex', alignItems: 'center', gap: 6 }}>
          {fetchedAt && `갱신: ${fetchedAt}`}
          <button
            onClick={() => load(true)}
            disabled={loading}
            className="app-refresh-btn"
            style={{
              padding: '4px 10px', fontSize: 13,
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
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
          <thead>
            <tr style={{ color: tokens.tx.muted, borderBottom: `1px solid ${tokens.bd.default}` }}>
              <th style={{ ...th, width: 24 }}>#</th>
              <th style={{ ...th, minWidth: 90 }}>종목</th>
              <th style={{ ...th, textAlign: 'right', width: 72 }}>현재가</th>
              <th style={{ ...th, textAlign: 'right', width: 56 }}>등락률</th>
              <th style={{ ...th, textAlign: 'right', width: 64 }}>거래대금</th>
              <th style={{ ...th, textAlign: 'right', width: 52 }}>EPS</th>
              <th style={{ ...th, textAlign: 'right', width: 44 }}>PER</th>
              <th style={{ ...th, textAlign: 'right', width: 52 }}>Fwd PER</th>
            </tr>
          </thead>
          <tbody>
            {items.filter(it => it.amount > 0).map(it => {
              const pureCode = it.ticker.replace(/_[A-Z]+$/, '').replace(/\.(KS|KQ)$/, '')
              return (
                <tr key={it.ticker} style={{ borderBottom: `1px solid ${tokens.bg.row}` }}>
                  <td style={{ ...td, color: tokens.tx.muted }}>{it.rank}</td>
                  <td style={{ ...td, lineHeight: 1.3 }}>
                    <div style={{ fontWeight: 500, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 110 }}>
                      {it.name}
                    </div>
                    <div style={{ color: tokens.tx.subtle, fontSize: 9.5 }}>
                      {pureCode}
                    </div>
                  </td>
                  <td style={tdNum}>
                    {it.price.toLocaleString()}
                  </td>
                  <td style={{ ...tdNum, color: pctTextColor(it.change_pct) }}>
                    {it.change_pct > 0 ? '+' : ''}{it.change_pct.toFixed(2)}%
                  </td>
                  <td style={tdNum}>{fmtAmt(it.amount)}</td>
                  <td style={{ ...tdNum, color: it.eps != null && it.eps < 0 ? tokens.semantic.up : undefined }}>
                    {it.eps != null ? it.eps.toLocaleString() : '—'}
                  </td>
                  <td style={tdNum}>{fmtPer(it.per)}</td>
                  <td style={tdNum}>{fmtPer(it.forward_per)}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {items.length === 0 && !loading && !error && (
        <div style={{ textAlign: 'center', color: tokens.tx.muted, marginTop: 40, fontSize: 12 }}>
          <div>데이터를 불러오는 중이거나 아직 없습니다</div>
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
