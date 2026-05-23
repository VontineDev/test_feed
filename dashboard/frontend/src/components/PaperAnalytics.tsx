import { useState, useEffect } from 'react'
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
} from 'recharts'

// ── 타입 ──────────────────────────────────────────────────────

interface ModelStats {
  n_trades: number
  n_wins: number
  win_rate: number
  avg_win: number | null
  avg_loss: number | null
  total_realized: number
  total_unrealized: number
}

interface OpenPosition {
  ticker: string
  name: string
  model: string
  unrealized_pct: number | null
}

interface CurveData {
  series: Record<string, { date: string; cumulative: number }[]>
  model_stats: Record<string, ModelStats>
  ticker_name_map: Record<string, string>
  open_positions: OpenPosition[]
}

interface PaperAnalyticsProps {
  onSelect: (ticker: string, name: string) => void
  collapsed?: boolean
  onToggle?: () => void
}

// ── 상수 ─────────────────────────────────────────────────────

const MODEL_COLOR: Record<string, string> = {
  stage: '#3b82f6',
  kosdaq: '#a78bfa',
  cross: '#f97316',
  ichimoku: '#4ade80',
}

const MODEL_LABEL: Record<string, string> = {
  stage: 'Stage',
  kosdaq: 'KOSDAQ',
  cross: 'Cross',
  ichimoku: 'Ichimoku',
}

// ── pivotSeries (순수 함수 — 단위 테스트용으로 export) ────────

export function pivotSeries(
  series: Record<string, { date: string; cumulative: number }[]>
): Array<Record<string, string | number | null>> {
  const dateSet = new Set<string>()
  for (const rows of Object.values(series)) {
    for (const r of rows) dateSet.add(r.date)
  }
  const dates = [...dateSet].sort()

  // 모델별 마지막 누적값 추적 (null이면 아직 거래 없음)
  const lastVal: Record<string, number | null> = {}
  for (const m of Object.keys(series)) lastVal[m] = null

  return dates.map((date) => {
    const row: Record<string, string | number | null> = { date }
    for (const [m, rows] of Object.entries(series)) {
      const match = rows.find((r) => r.date === date)
      if (match !== undefined) lastVal[m] = match.cumulative
      row[m] = lastVal[m]  // null: 아직 거래 없음 → connectNulls로 연결
    }
    return row
  })
}

// ── pctColor ─────────────────────────────────────────────────

function pctColor(v: number | null | undefined): string {
  if (v == null) return '#64748b'
  return v > 0 ? '#4ade80' : v < 0 ? '#f87171' : '#94a3b8'
}

function fmt(v: number | null, digits = 1): string {
  if (v == null) return '—'
  const s = (v * 100).toFixed(digits)
  return (v > 0 ? '+' : '') + s + '%'
}

// ── PaperAnalytics ────────────────────────────────────────────

export default function PaperAnalytics({ onSelect, collapsed = false, onToggle }: PaperAnalyticsProps) {
  const [data, setData] = useState<CurveData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(false)

  const load = () => {
    setLoading(true)
    setError(false)
    fetch('/api/paper/curve')
      .then((r) => {
        if (!r.ok) throw new Error(r.statusText)
        return r.json()
      })
      .then((j) => { setData(j.data); setLoading(false) })
      .catch(() => { setError(true); setLoading(false) })
  }

  useEffect(() => { load() }, [])

  const header = (
    <div
      style={{ ...S.header, cursor: onToggle ? 'pointer' : undefined }}
      onClick={onToggle}
    >
      <span style={S.title}>성과 분석</span>
      <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
        {!collapsed && (
          <button style={S.btn} onClick={(e) => { e.stopPropagation(); load() }}>갱신</button>
        )}
        {onToggle && <span style={S.chevron}>{collapsed ? '▼' : '▲'}</span>}
      </div>
    </div>
  )

  if (collapsed) return <div style={S.wrap}>{header}</div>

  if (loading) return (
    <div style={S.wrap}>
      {header}
      <div style={S.empty}>성과분석 불러오는 중…</div>
    </div>
  )
  if (error) return (
    <div style={S.wrap}>
      {header}
      <div style={S.empty}>데이터 조회 실패 <button style={S.btn} onClick={(e) => { e.stopPropagation(); load() }}>재시도</button></div>
    </div>
  )
  if (!data) return null

  const models = Object.keys(data.series)
  const pivot = pivotSeries(data.series)

  // 미실현 연장점: 마지막 실현 누적 + total_unrealized
  const unrealizedPoint: Record<string, string | number | null> = { date: '현재' }
  for (const m of models) {
    const stat = data.model_stats[m]
    const series = data.series[m]
    if (!stat || !series?.length) {
      unrealizedPoint[m] = null
      unrealizedPoint[m + '_u'] = null
      continue
    }
    const lastRealized = series[series.length - 1].cumulative
    unrealizedPoint[m] = null
    unrealizedPoint[m + '_u'] = round4(lastRealized + (stat.total_unrealized ?? 0))
  }
  const chartData = [...pivot, unrealizedPoint]

  // 리더보드: open 포지션 (unrealized_pct 기준)
  type LeaderRow = {
    ticker: string; name: string; model: string
    pct: number | null; isOpen: boolean
  }
  const openRows: LeaderRow[] = data.open_positions.map((p) => ({
    ticker: p.ticker,
    name: p.name,
    model: p.model,
    pct: p.unrealized_pct,
    isOpen: true,
  })).sort((a, b) => (b.pct ?? -Infinity) - (a.pct ?? -Infinity))

  // CSV 다운로드
  const handleExport = async () => {
    try {
      const res = await fetch('/api/paper/export')
      if (!res.ok) throw new Error(res.statusText)
      const blob = await res.blob()
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `paper_positions_${new Date().toISOString().slice(0, 10).replace(/-/g, '')}.csv`
      a.click()
      setTimeout(() => URL.revokeObjectURL(url), 100)
    } catch {
      alert('CSV 다운로드 실패')
    }
  }

  return (
    <div style={S.wrap}>
      {header}

      {/* 섹션 1: 모델 통계 테이블 */}
      {Object.keys(data.model_stats).length > 0 && (
        <div style={S.section}>
          <table style={S.table}>
            <thead>
              <tr>
                {['모델', '거래', '승/패', '승률', '평균승', '평균패', '실현누적', '미실현'].map((h) => (
                  <th key={h} style={S.th}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {Object.entries(data.model_stats).map(([m, st]) => (
                <tr key={m}>
                  <td style={{ ...S.td, color: MODEL_COLOR[m] ?? '#e2e8f0', fontWeight: 700 }}>
                    {MODEL_LABEL[m] ?? m}
                  </td>
                  <td style={S.td}>{st.n_trades}</td>
                  <td style={S.td}>{st.n_wins} / {st.n_trades - st.n_wins}</td>
                  <td style={{ ...S.td, color: pctColor(st.win_rate - 0.5) }}>
                    {(st.win_rate * 100).toFixed(0)}%
                  </td>
                  <td style={{ ...S.td, color: pctColor(st.avg_win) }}>
                    {fmt(st.avg_win)}
                  </td>
                  <td style={{ ...S.td, color: pctColor(st.avg_loss) }}>
                    {fmt(st.avg_loss)}
                  </td>
                  <td style={{ ...S.td, color: pctColor(st.total_realized), fontWeight: 600 }}>
                    {fmt(st.total_realized)}
                  </td>
                  <td style={{ ...S.td, color: pctColor(st.total_unrealized) }}>
                    {st.total_unrealized !== 0 ? fmt(st.total_unrealized) : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* 섹션 2: 누적 P&L 커브 */}
      {models.length > 0 && (
        <div style={{ ...S.section, paddingTop: 8 }}>
          <ResponsiveContainer width="100%" height={180}>
            <LineChart data={chartData}>
              <XAxis dataKey="date" tick={{ fontSize: 9, fill: '#475569' }} interval="preserveStartEnd" />
              <YAxis
                tickFormatter={(v) => `${(v * 100).toFixed(0)}%`}
                tick={{ fontSize: 9, fill: '#475569' }}
                width={42}
              />
              <Tooltip
                formatter={(v: number) => [`${(v * 100).toFixed(1)}%`]}
                contentStyle={{ background: '#1e293b', border: '1px solid #334155', fontSize: 11 }}
              />
              <Legend wrapperStyle={{ fontSize: 11 }} formatter={(v) => MODEL_LABEL[v] ?? v} />
              {models.map((m) => (
                <Line
                  key={m}
                  type="monotone"
                  dataKey={m}
                  stroke={MODEL_COLOR[m] ?? '#94a3b8'}
                  dot={false}
                  strokeWidth={1.5}
                  connectNulls
                />
              ))}
              {/* 미실현 연장선 (점선) */}
              {models.map((m) => (
                <Line
                  key={m + '_u'}
                  type="monotone"
                  dataKey={m + '_u'}
                  stroke={MODEL_COLOR[m] ?? '#94a3b8'}
                  dot={{ r: 3 }}
                  strokeDasharray="4 2"
                  strokeWidth={1}
                  connectNulls={false}
                  legendType="none"
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* 섹션 3: 종목 손익 리더보드 */}
      {openRows.length > 0 && (
        <div style={S.section}>
          <div style={S.sectionTitle}>미실현 포지션</div>
          <table style={S.table}>
            <thead>
              <tr>
                {['종목', '모델', '미실현'].map((h) => <th key={h} style={S.th}>{h}</th>)}
              </tr>
            </thead>
            <tbody>
              {openRows.map((r, i) => (
                <tr
                  key={i}
                  style={{ cursor: 'pointer' }}
                  onClick={() => onSelect(r.ticker, r.name)}
                >
                  <td style={S.td}>
                    <div style={{ fontWeight: 600 }}>{r.name}</div>
                    <div style={{ fontSize: 10, color: '#475569' }}>{r.ticker}</div>
                  </td>
                  <td style={{ ...S.td, color: MODEL_COLOR[r.model] ?? '#94a3b8' }}>
                    {MODEL_LABEL[r.model] ?? r.model}
                    <span style={{ fontSize: 10, color: '#64748b', marginLeft: 4 }}>(미)</span>
                  </td>
                  <td style={{ ...S.td, color: pctColor(r.pct), fontWeight: 600 }}>
                    {fmt(r.pct)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* 섹션 4: CSV 다운로드 */}
      <div style={{ ...S.section, display: 'flex', justifyContent: 'flex-end' }}>
        <button style={{ ...S.btn, padding: '4px 12px' }} onClick={handleExport}>
          CSV 다운로드
        </button>
      </div>
    </div>
  )
}

function round4(v: number): number {
  return Math.round(v * 10000) / 10000
}

// ── 스타일 ────────────────────────────────────────────────────

const S: Record<string, React.CSSProperties> = {
  wrap: {
    borderTop: '1px solid #1e293b',
    fontSize: 11,
    color: '#e2e8f0',
  },
  header: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    padding: '7px 12px 0',
  },
  title: {
    fontWeight: 700,
    fontSize: 11,
    color: '#475569',
  },
  section: {
    padding: '4px 12px 8px',
    overflowX: 'auto',
  },
  sectionTitle: {
    fontWeight: 700,
    fontSize: 11,
    color: '#475569',
    marginBottom: 4,
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
    fontSize: 11,
  },
  th: {
    position: 'sticky' as const,
    top: 0,
    background: '#0f172a',
    color: '#475569',
    padding: '4px 8px',
    textAlign: 'left' as const,
    fontWeight: 600,
    whiteSpace: 'nowrap' as const,
    borderRight: '1px solid #1e293b',
    borderBottom: '1px solid #1e293b',
  },
  td: {
    padding: '4px 8px',
    borderBottom: '1px solid #1e293b',
    borderRight: '1px solid #1e293b',
    verticalAlign: 'middle' as const,
  },
  empty: {
    padding: 16,
    color: '#64748b',
    textAlign: 'center' as const,
    fontSize: 11,
  },
  btn: {
    background: '#1e293b',
    border: '1px solid #334155',
    borderRadius: 4,
    color: '#94a3b8',
    cursor: 'pointer',
    fontSize: 11,
    padding: '2px 8px',
  },
  chevron: { fontSize: 10, color: '#475569' },
}
