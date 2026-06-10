import { useState, useEffect, useCallback } from 'react'
import { tokens } from '../tokens'

interface NarrativeItem {
  ticker:          string
  name:            string
  attention_score: number
  attention_q:     number
  stage:           number | null
  is_enhanced:     boolean | null
  has_gapjum:      boolean | null
  sector:          string | null
  close:           number | null
}

interface NarrativeData {
  total:        number
  stage2_plus:  number
  in_screener:  number
  narrative_q:  number
  triple_combo: number
  items:        NarrativeItem[]
}

const ATTENTION_META: Record<number, { label: string; color: string; bg: string }> = {
  1: { label: 'Q1', color: '#6b7280', bg: 'rgba(107,114,128,0.12)' },
  2: { label: 'Q2', color: '#60a5fa', bg: 'rgba(96,165,250,0.12)'  },
  3: { label: 'Q3', color: '#34d399', bg: 'rgba(52,211,153,0.15)'  },
  4: { label: 'Q4', color: '#fbbf24', bg: 'rgba(251,191,36,0.13)'  },
  5: { label: 'Q5', color: '#f87171', bg: 'rgba(248,113,113,0.13)' },
}

const STAGE_META: Record<number, { label: string; color: string; bg: string }> = {
  1: { label: 'S1', color: '#6b7280', bg: 'rgba(107,114,128,0.12)' },
  2: { label: 'S2', color: '#60a5fa', bg: 'rgba(96,165,250,0.15)'  },
  3: { label: 'S3', color: '#f97316', bg: 'rgba(249,115,22,0.13)'  },
  4: { label: 'S4', color: '#ef4444', bg: 'rgba(239,68,68,0.13)'   },
}

function Badge({ label, color, bg }: { label: string; color: string; bg: string }) {
  return (
    <span style={{
      display: 'inline-block', padding: '1px 6px', borderRadius: 4,
      fontSize: 10, fontWeight: 700, color, background: bg,
    }}>
      {label}
    </span>
  )
}

function AttentionBadge({ q, score }: { q: number; score: number }) {
  const m = ATTENTION_META[q] ?? ATTENTION_META[1]
  return (
    <span title={`attention_score: ${score.toFixed(3)}`}>
      <Badge label={m.label} color={m.color} bg={m.bg} />
    </span>
  )
}

function StageBadge({ stage }: { stage: number | null }) {
  if (stage === null) return <span style={{ color: tokens.tx.subtle }}>—</span>
  const m = STAGE_META[stage] ?? STAGE_META[1]
  return <Badge label={m.label} color={m.color} bg={m.bg} />
}

function ScreenerBadges({ isEnhanced, hasGapjum }: { isEnhanced: boolean | null; hasGapjum: boolean | null }) {
  if (!isEnhanced && !hasGapjum) return <span style={{ color: tokens.tx.subtle }}>—</span>
  return (
    <span style={{ display: 'flex', gap: 3, flexWrap: 'wrap' }}>
      {isEnhanced  && <Badge label="상승강화" color="#a78bfa" bg="rgba(167,139,250,0.13)" />}
      {hasGapjum   && <Badge label="갭점프"   color="#34d399" bg="rgba(52,211,153,0.13)"  />}
    </span>
  )
}

function Chip({ label, value, color }: { label: string; value: number; color?: string }) {
  return (
    <div style={{
      padding: '6px 12px', borderRadius: 8,
      background: tokens.bg.raised,
      border: `1px solid ${tokens.bd.default}`,
      display: 'flex', flexDirection: 'column', alignItems: 'center', minWidth: 72,
    }}>
      <span style={{ fontSize: 18, fontWeight: 700, color: color ?? tokens.tx.primary }}>{value}</span>
      <span style={{ fontSize: 10, color: tokens.tx.muted, marginTop: 1 }}>{label}</span>
    </div>
  )
}

const th: React.CSSProperties = { padding: '4px 6px', textAlign: 'left', fontWeight: 600, whiteSpace: 'nowrap', borderBottom: `1px solid ${tokens.bd.default}`, fontSize: 11 }
const td: React.CSSProperties = { padding: '4px 6px', verticalAlign: 'middle' }
const tdNum: React.CSSProperties = { padding: '4px 6px', textAlign: 'right', fontVariantNumeric: 'tabular-nums', verticalAlign: 'middle' }

type FilterKey = 'all' | 'triple' | 'stage2' | 'screener'

export default function Narrative() {
  const [data, setData]       = useState<NarrativeData | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError]     = useState('')
  const [filter, setFilter]   = useState<FilterKey>('all')

  const load = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      const r = await fetch('/api/youtube/screener')
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      const d = await r.json()
      setData(d.data ?? null)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  const items = data?.items ?? []
  const visible = items.filter(i => {
    if (filter === 'triple')  return (i.stage ?? 0) >= 2 && (i.is_enhanced || i.has_gapjum) && [2,3,4].includes(i.attention_q)
    if (filter === 'stage2')  return (i.stage ?? 0) >= 2
    if (filter === 'screener') return i.is_enhanced || i.has_gapjum
    return true
  })

  const filterBtn = (key: FilterKey, label: string, cnt: number, color?: string) => {
    const active = filter === key
    return (
      <button
        onClick={() => setFilter(key)}
        style={{
          padding: '4px 10px', borderRadius: 14, fontSize: 11, fontWeight: 600, cursor: 'pointer',
          border: `1px solid ${active ? (color ?? tokens.accent.blue) : tokens.bd.default}`,
          background: active ? (color ? `${color}20` : `${tokens.accent.blue}20`) : tokens.bg.root,
          color: active ? (color ?? tokens.accent.blue) : tokens.tx.muted,
        }}
      >
        {label} {cnt}
      </button>
    )
  }

  return (
    <div style={{ padding: 12, overflow: 'auto', height: '100%' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 10, alignItems: 'center', flexWrap: 'wrap', gap: 8 }}>
        <span style={{ fontSize: 13, fontWeight: 700 }}>유튜브 내러티브</span>
        <button
          onClick={load}
          disabled={loading}
          style={{ padding: '3px 10px', borderRadius: 6, fontSize: 11, cursor: 'pointer',
            border: `1px solid ${tokens.bd.default}`, background: tokens.bg.raised, color: tokens.tx.muted }}
        >
          {loading ? '로딩…' : '새로고침'}
        </button>
      </div>

      {error && (
        <div style={{ color: tokens.semantic.up, fontSize: 11, marginBottom: 8 }}>{error}</div>
      )}

      {data && (
        <>
          <div style={{ display: 'flex', gap: 8, marginBottom: 10, flexWrap: 'wrap' }}>
            <Chip label="전체"      value={data.total}        />
            <Chip label="스테이지2+" value={data.stage2_plus}  color={tokens.accent.blue} />
            <Chip label="스크리너"   value={data.in_screener}  color="#a78bfa" />
            <Chip label="내러티브Q"  value={data.narrative_q}  color="#34d399" />
            <Chip label="트리플"     value={data.triple_combo} color="#fbbf24" />
          </div>

          <div style={{ display: 'flex', gap: 6, marginBottom: 10, flexWrap: 'wrap' }}>
            {filterBtn('all',      '전체',       data.total)}
            {filterBtn('triple',   '트리플콤보',  data.triple_combo, '#fbbf24')}
            {filterBtn('stage2',   'S2+',         data.stage2_plus,  tokens.accent.blue)}
            {filterBtn('screener', '스크리너',    data.in_screener,  '#a78bfa')}
          </div>

          <div style={{ fontSize: 11, color: tokens.tx.subtle, marginBottom: 6 }}>
            {visible.length}개 종목 표시
          </div>

          <div style={{ overflowX: 'auto' }}>
            <table style={{ borderCollapse: 'collapse', fontSize: 11, width: '100%', minWidth: 560 }}>
              <thead>
                <tr style={{ background: tokens.bg.raised }}>
                  <th style={th}>티커</th>
                  <th style={th}>종목명</th>
                  <th style={{ ...th, textAlign: 'right' }}>관심도</th>
                  <th style={th}>내러티브Q</th>
                  <th style={th}>스테이지</th>
                  <th style={th}>스크리너</th>
                  <th style={th}>섹터</th>
                  <th style={{ ...th, textAlign: 'right' }}>현재가</th>
                </tr>
              </thead>
              <tbody>
                {visible.map(item => (
                  <tr key={item.ticker} style={{ borderBottom: `1px solid ${tokens.bd.default}` }}>
                    <td style={{ ...td, fontWeight: 600, fontVariantNumeric: 'tabular-nums', color: tokens.accent.blue }}>
                      {item.ticker}
                    </td>
                    <td style={{ ...td, maxWidth: 120, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {item.name}
                    </td>
                    <td style={{ ...tdNum, color: tokens.tx.muted }}>
                      {item.attention_score.toFixed(3)}
                    </td>
                    <td style={td}>
                      <AttentionBadge q={item.attention_q} score={item.attention_score} />
                    </td>
                    <td style={td}>
                      <StageBadge stage={item.stage} />
                    </td>
                    <td style={td}>
                      <ScreenerBadges isEnhanced={item.is_enhanced} hasGapjum={item.has_gapjum} />
                    </td>
                    <td style={{ ...td, color: tokens.tx.subtle, maxWidth: 100, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {item.sector ?? '—'}
                    </td>
                    <td style={{ ...tdNum }}>
                      {item.close != null ? item.close.toLocaleString() + '원' : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div style={{ marginTop: 10, padding: '6px 10px', borderRadius: 6, background: tokens.bg.raised, fontSize: 10, color: tokens.tx.subtle, lineHeight: 1.5 }}>
            IC 분석 기준: Q3 hit rate 62.4% (T+20d), ICIR 0.42 (90일). Q1·Q5 제외 Q2-Q4가 유효 신호.
            트리플콤보 = S2+ + 스크리너 통과 + 내러티브 Q2-4.
          </div>
        </>
      )}

      {loading && !data && (
        <div style={{ color: tokens.tx.subtle, fontSize: 12, marginTop: 20, textAlign: 'center' }}>
          데이터 로딩 중…
        </div>
      )}
    </div>
  )
}
