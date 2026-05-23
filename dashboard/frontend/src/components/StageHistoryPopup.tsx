import { useEffect, useState } from 'react'
import { tokens } from '../tokens'

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

interface Props {
  ticker: string
  name: string
  start: string
  end: string
  onClose: () => void
}

const STAGE_COLOR: Record<number, string> = {
  1: tokens.stage[1],
  2: tokens.stage[2],
  3: tokens.stage[3],
}

const fmt = (v: number | null | undefined, digits = 0) =>
  v == null ? '—' : v.toLocaleString('ko-KR', { maximumFractionDigits: digits })

export default function StageHistoryPopup({ ticker, name, start, end, onClose }: Props) {
  const [stageHistory, setStageHistory] = useState<StageRow[]>([])
  const [screenerHistory, setScreenerHistory] = useState<ScreenerRow[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(false)

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

  return (
    <>
      {/* 배경 오버레이 */}
      <div style={s.overlay} onClick={onClose} />

      {/* 팝업 패널 */}
      <div style={s.panel}>
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
                        {['날짜', 'Stage', '진입고가', '피크아웃'].map(h => (
                          <th key={h} style={s.th}>{h}</th>
                        ))}
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
              <div style={{ ...s.sectionTitle, marginTop: 14 }}>차트 스크리너 이력</div>
              {screenerHistory.length === 0 ? (
                <div style={s.empty}>스크리너 이력 없음</div>
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
            </>
          )}
        </div>
      </div>
    </>
  )
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
    width: 'min(480px, 95vw)', maxHeight: '80vh',
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
