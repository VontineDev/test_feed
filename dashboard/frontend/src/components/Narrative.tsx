import { useState, useEffect, useCallback } from 'react'
import { tokens } from '../tokens'

interface NarrativeItem {
  ticker:          string
  name:            string
  attention_score: number | null
  attention_q:     number | null
  stage:           number | null
  s1_high:         number | null
  s1_volume:       number | null
  peakout_flag:    boolean
  is_enhanced:     boolean
  has_gapjum:      boolean
  sector:          string | null
  close:           number | null
  ma_20w:          number | null
  cloud_top:       number | null
}

interface AsOf {
  stage:     string | null
  screener:  string | null
  narrative: string | null
}

interface NarrativeData {
  total:        number
  stage1:       number
  stage2:       number
  in_screener:  number
  narrative_q:  number
  triple_combo: number
  items:        NarrativeItem[]
  as_of:        AsOf | null
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

function AttentionBadge({ q, score }: { q: number | null; score: number | null }) {
  if (q === null) return <span style={{ color: tokens.tx.subtle }}>—</span>
  const m = ATTENTION_META[q] ?? ATTENTION_META[1]
  return (
    <span title={score != null ? `attention_score: ${score.toFixed(3)}` : undefined}>
      <Badge label={m.label} color={m.color} bg={m.bg} />
    </span>
  )
}

function StageBadge({ stage, peakout }: { stage: number | null; peakout?: boolean }) {
  if (stage === null) return <span style={{ color: tokens.tx.subtle }}>—</span>
  const m = STAGE_META[stage] ?? STAGE_META[1]
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 3 }}>
      <Badge label={m.label} color={m.color} bg={m.bg} />
      {peakout && <span title="고점 이탈" style={{ fontSize: 10 }}>⚠</span>}
    </span>
  )
}

function ScreenerBadges({ isEnhanced, hasGapjum }: { isEnhanced: boolean; hasGapjum: boolean }) {
  if (!isEnhanced && !hasGapjum) return <span style={{ color: tokens.tx.subtle }}>—</span>
  return (
    <span style={{ display: 'flex', gap: 3, flexWrap: 'wrap' }}>
      {isEnhanced && <Badge label="강화" color="#a78bfa" bg="rgba(167,139,250,0.13)" />}
      {hasGapjum  && <Badge label="갭"   color="#34d399" bg="rgba(52,211,153,0.13)"  />}
    </span>
  )
}

function Chip({ label, value, color, active, onClick }: {
  label: string; value: number; color?: string; active?: boolean; onClick?: () => void
}) {
  return (
    <div
      onClick={onClick}
      style={{
        padding: '6px 12px', borderRadius: 8,
        background: active ? (color ? `${color}18` : `${tokens.accent.blue}18`) : tokens.bg.raised,
        border: `1px solid ${active ? (color ?? tokens.accent.blue) : tokens.bd.default}`,
        display: 'flex', flexDirection: 'column', alignItems: 'center', minWidth: 64,
        cursor: onClick ? 'pointer' : 'default',
        transition: 'border-color 0.12s, background 0.12s',
      }}
    >
      <span style={{ fontSize: 18, fontWeight: 700, color: active ? (color ?? tokens.accent.blue) : (color ?? tokens.tx.primary) }}>{value}</span>
      <span style={{ fontSize: 10, color: active ? (color ?? tokens.accent.blue) : tokens.tx.muted, marginTop: 1 }}>{label}</span>
    </div>
  )
}

const th: React.CSSProperties = {
  padding: '4px 6px', textAlign: 'left', fontWeight: 600, whiteSpace: 'nowrap',
  borderBottom: `1px solid ${tokens.bd.default}`, fontSize: 11,
  position: 'sticky', top: 0, background: tokens.bg.raised,
}
const td: React.CSSProperties = { padding: '4px 6px', verticalAlign: 'middle' }
const tdNum: React.CSSProperties = { padding: '4px 6px', textAlign: 'right', fontVariantNumeric: 'tabular-nums', verticalAlign: 'middle' }

type FilterKey = 'all' | 'triple' | 'stage12' | 'screener' | 'narrative'

interface Props {
  onSelect?:       (ticker: string, name: string) => void
  selectedTicker?: string | null
  onLoad?:         (at: Date, asOf: AsOf | null) => void
  refreshKey?:     number
  start?:          string   // YYYY-MM-DD, 없으면 최신 스냅샷
  end?:            string
}

export default function Narrative({ onSelect, selectedTicker, onLoad, refreshKey, start, end }: Props) {
  const [data, setData]       = useState<NarrativeData | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError]     = useState('')
  const [filter, setFilter]   = useState<FilterKey>('all')

  const load = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      const params = start && end ? `?start=${start}&end=${end}` : ''
      const r = await fetch(`/api/report/unified${params}`)
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      const d = await r.json()
      setData(d.data ?? null)
      onLoad?.(new Date(), d.data?.as_of ?? null)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }, [onLoad, start, end])

  useEffect(() => { load() }, [load])
  useEffect(() => { if (refreshKey !== undefined && refreshKey > 0) load() }, [refreshKey]) // eslint-disable-line react-hooks/exhaustive-deps

  const items = data?.items ?? []
  const visible = items.filter(i => {
    if (filter === 'triple')    return (i.stage === 1 || i.stage === 2) && (i.is_enhanced || i.has_gapjum) && i.attention_q !== null && [2,3,4].includes(i.attention_q)
    if (filter === 'stage12')   return i.stage === 1 || i.stage === 2
    if (filter === 'screener')  return i.is_enhanced || i.has_gapjum
    if (filter === 'narrative') return i.attention_q !== null && [2,3,4].includes(i.attention_q)
    return true
  })

  return (
    <div style={{ padding: 12, overflow: 'auto' }}>
      {error && (
        <div style={{ color: tokens.semantic.up, fontSize: 11, marginBottom: 8 }}>{error}</div>
      )}

      {data && (
        <>
          <div style={{ display: 'flex', gap: 8, marginBottom: 10, flexWrap: 'wrap' }}>
            <Chip label="전체"    value={data.total}                    active={filter === 'all'}       onClick={() => setFilter('all')}       />
            <Chip label="S1·S2"   value={data.stage1 + data.stage2}    color={tokens.accent.blue} active={filter === 'stage12'}  onClick={() => setFilter('stage12')}  />
            <Chip label="스크리너" value={data.in_screener}              color="#a78bfa"            active={filter === 'screener'}  onClick={() => setFilter('screener')}  />
            <Chip label="내러티브" value={data.narrative_q}              color="#34d399"            active={filter === 'narrative'} onClick={() => setFilter('narrative')} />
            <Chip label="트리플"   value={data.triple_combo}             color="#fbbf24"            active={filter === 'triple'}    onClick={() => setFilter('triple')}    />
          </div>

          <div style={{ fontSize: 11, color: tokens.tx.subtle, marginBottom: 6 }}>
            {visible.length}개 종목 표시
          </div>

          <div style={{ overflowX: 'auto' }}>
            <table style={{ borderCollapse: 'collapse', fontSize: 11, width: '100%', minWidth: 620 }}>
              <thead>
                <tr style={{ background: tokens.bg.raised }}>
                  <th style={th}>종목</th>
                  <th style={{ ...th, textAlign: 'right' }}>관심도</th>
                  <th style={th}>내러티브</th>
                  <th style={th}>스테이지</th>
                  <th style={th}>스크리너</th>
                  <th style={th}>섹터</th>
                  <th style={{ ...th, textAlign: 'right' }}>현재가</th>
                </tr>
              </thead>
              <tbody>
                {visible.map(item => (
                  <tr
                    key={item.ticker}
                    onClick={() => onSelect?.(item.ticker, item.name)}
                    style={{
                      borderBottom: `1px solid ${tokens.bd.default}`,
                      background: item.ticker === selectedTicker
                        ? tokens.bg.active
                        : item.peakout_flag ? '#1a0f0f' : 'transparent',
                      cursor: onSelect ? 'pointer' : 'default',
                    }}
                  >
                    <td style={td}>
                      <div style={{ fontWeight: 600, color: tokens.tx.secondary }}>{item.name}</div>
                      <div style={{ fontSize: 10, color: tokens.tx.subtle, fontVariantNumeric: 'tabular-nums' }}>
                        {item.ticker}
                      </div>
                    </td>
                    <td style={{ ...tdNum, color: tokens.tx.muted }}>
                      {item.attention_score != null ? item.attention_score.toFixed(3) : '—'}
                    </td>
                    <td style={td}>
                      <AttentionBadge q={item.attention_q} score={item.attention_score} />
                    </td>
                    <td style={td}>
                      <StageBadge stage={item.stage} peakout={item.peakout_flag} />
                    </td>
                    <td style={td}>
                      <ScreenerBadges isEnhanced={item.is_enhanced} hasGapjum={item.has_gapjum} />
                    </td>
                    <td style={{ ...td, color: tokens.tx.subtle, maxWidth: 100, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {item.sector ?? '—'}
                    </td>
                    <td style={tdNum}>
                      {item.close != null ? item.close.toLocaleString() + '원' : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div style={{ marginTop: 10, padding: '6px 10px', borderRadius: 6, background: tokens.bg.raised, fontSize: 10, color: tokens.tx.subtle, lineHeight: 1.5 }}>
            정렬: 유튜브 관심도 ↓ → 스테이지 ↑ → 스크리너 우선.
            트리플콤보 = S1·S2 + 스크리너 통과 + 내러티브 Q2-4.
            IC 분석 기준: Q3 hit rate 62.4% (T+20d), ICIR 0.42 (90일).
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
