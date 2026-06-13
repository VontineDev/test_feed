import { useEffect, useState } from 'react'
import { tokens } from '../tokens'
import InfoTip from './InfoTip'

interface StageRow {
  classified_date: string
  stage: number
  peakout_flag: boolean
  s1_high: number | null
  s1_txamt: number | null
}

interface ScreenerRow {
  week_of: string
  is_enhanced: boolean
  has_gapjum: boolean
  close: number | null
}

interface RevenueJson {
  periods?: string[]
  unit?: string
  segments?: { name: string; revenues: (number | null)[]; yoy_growth?: (number | null)[] }[]
  consolidated?: { revenue?: (number | null)[]; op_profit?: (number | null)[] }
}

interface SegmentItem {
  segment_name: string
  products?: string[]
  revenue_share_pct?: number | null
  note?: string | null
}

interface DartData {
  corp_name: string
  period: string | null
  report_type: string | null
  extracted_at: string | null
  revenue: RevenueJson | null
  segments: SegmentItem[] | null
}

interface Props {
  ticker: string
  name: string
  start: string
  end: string
  onClose: () => void
  mode?: 'modal' | 'panel'
}

const STAGE_COLOR: Record<number, string> = {
  1: tokens.stage[1],
  2: tokens.stage[2],
  3: tokens.stage[3],
}

const fmt = (v: number | null | undefined, digits = 0) =>
  v == null ? '—' : v.toLocaleString('ko-KR', { maximumFractionDigits: digits })

// ── 금액 단위 변환 (억원 기준) ────────────────────────────────
const UNIT_TO_EOKWON: Record<string, number> = {
  '원': 1e-8, '천원': 1e-5, '백만원': 0.01, '억원': 1, '조원': 10000,
}

function fmtRevenue(raw: number | null | undefined, unit: string | undefined): string {
  if (raw == null) return '—'
  const mult = UNIT_TO_EOKWON[unit ?? '억원'] ?? 1
  const eok = raw * mult
  if (eok >= 10000) return `${(eok / 10000).toFixed(1)}조원`
  if (eok >= 1)     return `${Math.round(eok).toLocaleString('ko-KR')}억원`
  return `${(eok * 100).toFixed(0)}백만원`
}

function fmtYoy(growth: number | null | undefined): { text: string; color: string } {
  if (growth == null) return { text: '—', color: tokens.tx.muted }
  const pct = growth * 100
  const color = pct > 0 ? tokens.semantic.up : pct < 0 ? tokens.semantic.down : tokens.tx.muted
  return { text: `${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%`, color }
}

export default function StageHistoryPopup({ ticker, name, start, end, onClose, mode = 'modal' }: Props) {
  const [stageHistory, setStageHistory] = useState<StageRow[]>([])
  const [screenerHistory, setScreenerHistory] = useState<ScreenerRow[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(false)

  const [dartData, setDartData]     = useState<DartData | null>(null)
  const [dartLoading, setDartLoading] = useState(false)

  useEffect(() => {
    setLoading(true)
    setError(false)
    const url = `/api/history/ticker/${encodeURIComponent(ticker)}?start=${start}&end=${end}`
    fetch(url)
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then(j => {
        setStageHistory(j.data?.stage_history ?? [])
        setScreenerHistory(j.data?.screener_history ?? [])
        setLoading(false)
      })
      .catch(() => {
        setError(true)
        setLoading(false)
      })
  }, [ticker, start, end])

  useEffect(() => {
    setDartData(null)
    setDartLoading(true)
    fetch(`/api/dart/summary/${encodeURIComponent(ticker)}`)
      .then(r => r.ok ? r.json() : Promise.reject())
      .then(j => { setDartData(j.data ?? null) })
      .catch(() => { setDartData(null) })
      .finally(() => setDartLoading(false))
  }, [ticker])

  const content = (
    <div style={mode === 'panel' ? s.panelInline : s.panel}>
        {/* 헤더 */}
        <div style={s.header}>
          <div style={{ flex: 1, minWidth: 0 }}>
            <span style={s.name}>{name}</span>
            <span style={s.tickerCode}>{ticker}</span>
          </div>
          <span style={s.dateRange}>{start} ~ {end}</span>
          <button style={s.closeBtn} onClick={onClose} title="닫기">✕</button>
        </div>

        {/* 본문 */}
        <div style={s.body}>
          {loading && (
            <div style={s.center}>이력 로딩 중…</div>
          )}
          {error && !loading && (
            <div style={{ ...s.center, color: tokens.semantic.up }}>
              이력을 불러오지 못했습니다
            </div>
          )}
          {!loading && !error && (
            <>
              {/* Stage 이력 */}
              <div style={s.sectionTitle}>Stage 이력</div>
              {stageHistory.length === 0 ? (
                <div style={s.empty}>Stage 이력 없음</div>
              ) : (
                <div style={s.tableWrap}>
                  <table style={s.table}>
                    <thead>
                      <tr>
                        {([
                          '날짜', 'Stage',
                          { label: 'S1 고가', tip: 'Stage 1으로 분류된 날의 당일 고가. 진입 시점 가격 압박을 가늠하는 기준점.' },
                          { label: '고점 이탈', tip: '외국인·기관 동시 순매도 또는 윗꼬리+거래량 급증 감지. 단기 고점에서 매물 압력이 집중된 신호.' },
                        ] as const).map(h => {
                          const label = typeof h === 'string' ? h : h.label
                          const tip   = typeof h === 'object' ? h.tip : undefined
                          return (
                            <th key={label} style={s.th}>
                              {label}{tip && <InfoTip text={tip} width={220} zIndex={200} />}
                            </th>
                          )
                        })}
                      </tr>
                    </thead>
                    <tbody>
                      {stageHistory.map(r => (
                        <tr key={r.classified_date}
                            style={{ background: r.peakout_flag ? '#1a0f0f' : 'transparent' }}>
                          <td style={s.td}>{r.classified_date}</td>
                          <td style={{ ...s.td, color: STAGE_COLOR[r.stage] ?? tokens.tx.secondary, fontWeight: 700 }}>
                            S{r.stage}
                          </td>
                          <td style={{ ...s.td, textAlign: 'right' as const }}>
                            {fmt(r.s1_high)}
                          </td>
                          <td style={{ ...s.td, textAlign: 'center' as const }}>
                            {r.peakout_flag ? <span style={{ color: tokens.semantic.up }}>⚠</span> : ''}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {/* 스크리너 이력 */}
              <div style={{ ...s.sectionTitle, marginTop: 14 }}>강세 후보 이력</div>
              {screenerHistory.length === 0 ? (
                <div style={s.empty}>강세 후보 이력 없음</div>
              ) : (
                <div style={s.tableWrap}>
                  <table style={s.table}>
                    <thead>
                      <tr>
                        {['주차', '종가', '강화', '갭점프'].map(h => (
                          <th key={h} style={s.th}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {screenerHistory.map(r => (
                        <tr key={r.week_of}>
                          <td style={s.td}>{r.week_of}</td>
                          <td style={{ ...s.td, textAlign: 'right' as const }}>{fmt(r.close)}</td>
                          <td style={{ ...s.td, textAlign: 'center' as const }}>
                            {r.is_enhanced && <span style={{ color: tokens.stage[2] }}>강화</span>}
                          </td>
                          <td style={{ ...s.td, textAlign: 'center' as const }}>
                            {r.has_gapjum && <span style={{ color: tokens.chart.cat.ichimoku }}>갭</span>}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {/* DART 재무 현황 */}
              <DartFinancials data={dartData} loading={dartLoading} />
            </>
          )}
        </div>
      </div>
  )

  if (mode === 'panel') return content
  return (
    <>
      <div style={s.overlay} onClick={onClose} />
      {content}
    </>
  )
}

// ── DartFinancials ────────────────────────────────────────────

function DartFinancials({ data, loading }: { data: DartData | null; loading: boolean }) {
  if (loading) {
    return (
      <div style={{ marginTop: 16, borderTop: `1px solid ${tokens.bd.default}`, paddingTop: 10 }}>
        <div style={ds.sectionTitle}>재무 현황 <span style={{ color: tokens.tx.subtle, fontWeight: 400 }}>(DART)</span></div>
        <div style={ds.dim}>조회 중…</div>
      </div>
    )
  }

  if (!data) {
    return (
      <div style={{ marginTop: 16, borderTop: `1px solid ${tokens.bd.default}`, paddingTop: 10 }}>
        <div style={ds.sectionTitle}>재무 현황 <span style={{ color: tokens.tx.subtle, fontWeight: 400 }}>(DART)</span></div>
        <div style={ds.dim}>DART 재무 데이터 없음</div>
      </div>
    )
  }

  const rev   = data.revenue
  const segs  = data.segments
  const unit  = rev?.unit
  const periods = rev?.periods ?? []
  const cons  = rev?.consolidated

  // 연결 매출/영업이익 최근 2개 기간만 표시
  const showPeriods = periods.slice(-2)
  const revVals  = (cons?.revenue  ?? []).slice(-2)
  const opVals   = (cons?.op_profit ?? []).slice(-2)

  const opMargins = revVals.map((rv, i) => {
    const op = opVals[i]
    if (rv == null || op == null || rv === 0) return null
    return (op / rv) * 100
  })

  const revYoy = revVals.length >= 2 && revVals[0] && revVals[1]
    ? (revVals[0]! / revVals[1]! - 1) : null
  const opYoy = opVals.length >= 2 && opVals[0] && opVals[1]
    ? (opVals[0]! / opVals[1]! - 1) : null

  const badgeLabel = [data.period, data.report_type].filter(Boolean).join(' ')

  return (
    <div style={{ marginTop: 16, borderTop: `1px solid ${tokens.bd.default}`, paddingTop: 10 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
        <span style={ds.sectionTitle}>재무 현황 <span style={{ color: tokens.tx.subtle, fontWeight: 400 }}>(DART)</span></span>
        {badgeLabel && <span style={ds.badge}>{badgeLabel}</span>}
      </div>

      {/* 연결 재무 테이블 */}
      {showPeriods.length > 0 && (cons?.revenue?.length || cons?.op_profit?.length) ? (
        <div style={{ overflowX: 'auto', marginBottom: 10 }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${tokens.bd.default}` }}>
                <th style={ds.th}></th>
                {showPeriods.map(p => <th key={p} style={{ ...ds.th, textAlign: 'right' }}>{p}</th>)}
                <th style={{ ...ds.th, textAlign: 'right' }}>YoY</th>
              </tr>
            </thead>
            <tbody>
              {revVals.some(v => v != null) && (
                <tr>
                  <td style={ds.td}>매출</td>
                  {revVals.map((v, i) => (
                    <td key={i} style={{ ...ds.td, textAlign: 'right' }}>{fmtRevenue(v, unit)}</td>
                  ))}
                  <td style={{ ...ds.td, textAlign: 'right', ...fmtYoy(revYoy) }}>
                    {fmtYoy(revYoy).text}
                  </td>
                </tr>
              )}
              {opVals.some(v => v != null) && (
                <tr>
                  <td style={ds.td}>영업이익</td>
                  {opVals.map((v, i) => (
                    <td key={i} style={{ ...ds.td, textAlign: 'right' }}>{fmtRevenue(v, unit)}</td>
                  ))}
                  <td style={{ ...ds.td, textAlign: 'right', ...fmtYoy(opYoy) }}>
                    {fmtYoy(opYoy).text}
                  </td>
                </tr>
              )}
              {opMargins.some(v => v != null) && (
                <tr style={{ borderTop: `1px solid ${tokens.bd.default}` }}>
                  <td style={{ ...ds.td, color: tokens.tx.muted }}>이익률</td>
                  {opMargins.map((m, i) => (
                    <td key={i} style={{ ...ds.td, textAlign: 'right', color: tokens.tx.muted }}>
                      {m != null ? `${m.toFixed(1)}%` : '—'}
                    </td>
                  ))}
                  <td style={ds.td}></td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      ) : null}

      {/* 사업부문 구성 */}
      {segs && segs.length > 0 && (
        <>
          <div style={{ ...ds.subTitle, marginTop: 6 }}>사업부문 구성</div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
            {segs
              .sort((a, b) => (b.revenue_share_pct ?? 0) - (a.revenue_share_pct ?? 0))
              .map((seg, i) => (
                <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                  <div style={{
                    width: `${Math.max(4, (seg.revenue_share_pct ?? 0) * 0.9)}%`,
                    minWidth: 4, height: 6, borderRadius: 3,
                    background: tokens.accent.blueSoft, flexShrink: 0,
                  }} />
                  <span style={{ fontSize: 11, color: tokens.tx.secondary, flex: 1, minWidth: 0,
                    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {seg.segment_name}
                  </span>
                  {seg.revenue_share_pct != null && (
                    <span style={{ fontSize: 11, color: tokens.tx.muted, flexShrink: 0 }}>
                      {seg.revenue_share_pct.toFixed(1)}%
                    </span>
                  )}
                </div>
              ))}
          </div>
        </>
      )}
    </div>
  )
}

const ds: Record<string, React.CSSProperties> = {
  sectionTitle: { fontSize: 11, fontWeight: 700, color: tokens.tx.secondary },
  subTitle:     { fontSize: 10, fontWeight: 700, color: tokens.tx.muted, marginBottom: 4 },
  badge:  {
    fontSize: 9, color: tokens.tx.subtle,
    background: tokens.bg.raised, border: `1px solid ${tokens.bd.default}`,
    borderRadius: 3, padding: '1px 5px',
  },
  dim: { fontSize: 11, color: tokens.tx.subtle, padding: '4px 0' },
  th:  {
    color: tokens.tx.subtle, padding: '3px 6px', fontWeight: 600,
    textAlign: 'left' as const, whiteSpace: 'nowrap' as const, fontSize: 10,
    borderBottom: `1px solid ${tokens.bd.default}`,
  },
  td:  {
    padding: '4px 6px', color: tokens.tx.secondary,
    borderBottom: `1px solid ${tokens.bd.default}`, whiteSpace: 'nowrap' as const, fontSize: 11,
  },
}

const s: Record<string, React.CSSProperties> = {
  overlay: {
    position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)', zIndex: 100,
  },
  panel: {
    position: 'fixed', zIndex: 101,
    background: tokens.bg.row, border: `1px solid ${tokens.bd.default}`, borderRadius: 10,
    display: 'flex', flexDirection: 'column',
    top: '50%', left: '50%', transform: 'translate(-50%,-50%)',
    width: 'min(680px, 95vw)', maxHeight: '80vh',
    overflow: 'hidden',
  },
  panelInline: {
    display: 'flex', flexDirection: 'column',
    width: '100%', height: '100%',
    background: tokens.bg.row,
    overflow: 'hidden',
  },
  header: {
    display: 'flex', alignItems: 'center', gap: 8,
    padding: '10px 14px', borderBottom: `1px solid ${tokens.bd.default}`,
    background: tokens.bg.panel, flexShrink: 0,
  },
  name: { fontWeight: 700, color: tokens.tx.secondary, fontSize: 13 },
  tickerCode: { color: tokens.tx.subtle, fontSize: 10, marginLeft: 6 },
  dateRange: { fontSize: 10, color: tokens.tx.subtle, flexShrink: 0 },
  closeBtn: {
    background: 'none', border: 'none', color: tokens.tx.muted,
    cursor: 'pointer', fontSize: 15, padding: '2px 4px', flexShrink: 0,
  },
  body: { flex: 1, overflowY: 'auto', padding: '10px 12px' },
  center: { color: tokens.tx.muted, textAlign: 'center', padding: '24px 0', fontSize: 12 },
  sectionTitle: { fontSize: 11, fontWeight: 700, color: tokens.tx.secondary, marginBottom: 6 },
  empty: { color: tokens.tx.subtle, fontSize: 11, padding: '8px 0' },
  tableWrap: { overflowX: 'auto' },
  table: { width: '100%', borderCollapse: 'collapse', fontSize: 11 },
  th: {
    position: 'sticky', top: 0, background: tokens.bg.row,
    color: tokens.tx.subtle, padding: '4px 8px', textAlign: 'left',
    fontWeight: 600, whiteSpace: 'nowrap',
    borderRight: `1px solid ${tokens.bd.default}`, borderBottom: `1px solid ${tokens.bd.default}`,
  },
  td: { padding: '4px 8px', borderBottom: `1px solid ${tokens.bd.default}`, borderRight: `1px solid ${tokens.bd.default}`, color: tokens.tx.secondary },
}
