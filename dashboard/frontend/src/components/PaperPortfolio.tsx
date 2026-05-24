import { useState, useEffect } from 'react'
import { tokens, pctTextColor } from '../tokens'
import Positions from './Positions'
import Scheduler from './Scheduler'
import TickerHistory from './TickerHistory'
import PaperAnalytics from './PaperAnalytics'

interface PaperData {
  model_summary: Record<string, Record<string, { count: number; avg_return: number | null }>>
  open: { ticker: string; name: string; model: string; signal_date: string; entry_actual: number | null; current_price: number | null; unrealized_pct: number | null; status: string; qty: number | null }[]
  closed: { ticker: string; name: string; model: string; signal_date: string; exit_date: string | null; exit_type: string | null; blended_return: number | null; tp1_date: string | null }[]
}

const MODEL_LABEL: Record<string, string> = {
  stage: 'Stage', kosdaq: 'KOSDAQ', cross: 'Cross', ichimoku: 'Ichimoku',
}

const EXIT_LABEL: Record<string, string> = {
  hard_stop: '손절', trail: '트레일', period_end: '기간종료', manual: '수동',
}


export default function PaperPortfolio() {
  const [data, setData] = useState<PaperData | null>(null)
  const [selectedTicker, setSelectedTicker] = useState<string | null>(null)
  const [selectedName, setSelectedName] = useState<string>('')
  const [filterModel, setFilterModel] = useState<string | null>(null)
  const [posOpen, setPosOpen] = useState(true)
  const [analyticsOpen, setAnalyticsOpen] = useState(true)

  useEffect(() => {
    fetch('/api/report/paper')
      .then(r => r.json())
      .then(j => setData(j.data))
      .catch(() => {})
  }, [])

  const handleSelect = (ticker: string, name: string) => {
    if (selectedTicker === ticker) {
      setSelectedTicker(null)
      setSelectedName('')
    } else {
      setSelectedTicker(ticker)
      setSelectedName(name)
    }
  }

  const models = data ? Object.keys(data.model_summary) : []

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', fontSize: 12, color: tokens.tx.secondary }}>

      {/* 모델별 요약 */}
      {data && models.length > 0 && (
        <div style={{ padding: '10px 12px', borderBottom: `1px solid ${tokens.bd.default}`, flexShrink: 0 }}>
          <div style={{ fontWeight: 700, fontSize: 11, color: tokens.tx.subtle, marginBottom: 8 }}>모델별 요약</div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(110px, 1fr))', gap: 8 }}>
            {models.map(m => {
              const info = data.model_summary[m]
              const open    = info['open']    ?? { count: 0, avg_return: null }
              const pending = info['pending'] ?? { count: 0, avg_return: null }
              const closed  = info['closed']  ?? { count: 0, avg_return: null }
              return (
                <div
                key={m}
                onClick={() => setFilterModel(prev => prev === m ? null : m)}
                style={{
                  background: filterModel === m ? tokens.bg.active : tokens.bg.raised,
                  borderRadius: 8,
                  padding: '8px 12px',
                  cursor: 'pointer',
                  outline: filterModel === m ? `1px solid ${tokens.accent.blue}` : undefined,
                }}
              >
                  <div style={{ fontWeight: 700, fontSize: 12, color: tokens.accent.blueLight, marginBottom: 6 }}>{MODEL_LABEL[m] ?? m}</div>
                  <div style={s.statRow}>
                    <div style={s.stat}>
                      <span style={s.statVal}>{open.count}</span>
                      <span style={s.statLbl}>오픈</span>
                    </div>
                    <div style={s.stat}>
                      <span style={s.statVal}>{pending.count}</span>
                      <span style={s.statLbl}>대기</span>
                    </div>
                    {closed.count > 0 && (
                      <div style={s.stat}>
                        <span style={s.statVal}>{closed.count}</span>
                        <span style={s.statLbl}>청산</span>
                      </div>
                    )}
                  </div>
                  {closed.avg_return != null && (
                    <div style={{ marginTop: 5, fontSize: 11, color: tokens.tx.subtle }}>
                      평균 <span style={{ color: pctTextColor(closed.avg_return), fontWeight: 700 }}>
                        {closed.avg_return > 0 ? '+' : ''}{closed.avg_return.toFixed(1)}%
                      </span>
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* 포지션 섹션 헤더 */}
      <button style={s.collapseHdr} onClick={() => setPosOpen(o => !o)}>
        <span style={s.collapseTitle}>포지션</span>
        {data && (
          <span style={s.collapseBadge}>{data.open.length}건</span>
        )}
        <span style={s.chevron}>{posOpen ? '▲' : '▼'}</span>
      </button>

      {/* 포지션 + 이력 패널 */}
      {posOpen && (
        <>
          <div style={{ flex: 1, minHeight: 0, display: 'flex', overflow: 'hidden' }}>
            <div style={{
              flex: selectedTicker ? '0 0 55%' : 1,
              minWidth: 0,
              overflowY: 'auto',
              borderRight: selectedTicker ? `1px solid ${tokens.bd.default}` : undefined,
              transition: 'flex 0.15s',
            }}>
              <Positions onSelect={handleSelect} selectedTicker={selectedTicker} filterModel={filterModel} />
            </div>
            {selectedTicker && (
              <div style={{ flex: '0 0 45%', minWidth: 0, overflowY: 'auto' }}>
                <TickerHistory
                  ticker={selectedTicker}
                  name={selectedName}
                  onClose={() => { setSelectedTicker(null); setSelectedName('') }}
                />
              </div>
            )}
          </div>

          {/* 최근 청산 이력 */}
          {data && data.closed.length > 0 && (
            <div style={{ borderTop: `1px solid ${tokens.bd.default}`, flexShrink: 0, maxHeight: 220, overflowY: 'auto' }}>
              <div style={{ padding: '7px 12px 0', fontWeight: 700, fontSize: 12, color: tokens.tx.secondary, position: 'sticky', top: 0, background: tokens.bg.row }}>
                최근 청산 이력
              </div>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
                <thead>
                  <tr>
                    {['종목', '모델', '청산일', '청산유형', '실현수익'].map(h => (
                      <th key={h} style={s.th}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {data.closed.map((r, i) => {
                    const isSelected = r.ticker === selectedTicker
                    return (
                      <tr
                        key={i}
                        style={{ background: isSelected ? tokens.bg.active : undefined, cursor: 'pointer' }}
                        onClick={() => handleSelect(r.ticker, r.name)}
                      >
                        <td style={s.td}>
                          <div style={{ color: isSelected ? tokens.accent.blueLight : tokens.tx.secondary, fontWeight: 600 }}>{r.name}</div>
                          <div style={{ color: tokens.tx.subtle, fontSize: 10 }}>{r.signal_date}</div>
                        </td>
                        <td style={{ ...s.td, color: tokens.tx.secondary }}>{MODEL_LABEL[r.model] ?? r.model}</td>
                        <td style={{ ...s.td, color: tokens.tx.muted }}>{r.exit_date ?? '—'}</td>
                        <td style={{ ...s.td, color: tokens.tx.secondary }}>
                          {r.exit_type ? (EXIT_LABEL[r.exit_type] ?? r.exit_type) : '—'}
                          {r.tp1_date && <span style={{ color: tokens.stage[2], marginLeft: 4 }}>TP1</span>}
                        </td>
                        <td style={{ ...s.td, textAlign: 'right', color: pctTextColor(r.blended_return != null ? r.blended_return * 100 : null), fontWeight: 600 }}>
                          {r.blended_return != null
                            ? `${r.blended_return > 0 ? '+' : ''}${(r.blended_return * 100).toFixed(1)}%`
                            : '—'}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}

      {/* 성과 분석 */}
      <div style={analyticsOpen ? { flexShrink: 0, maxHeight: 420, overflowY: 'auto' } : { flexShrink: 0 }}>
        <PaperAnalytics
          onSelect={handleSelect}
          collapsed={!analyticsOpen}
          onToggle={() => setAnalyticsOpen(o => !o)}
        />
      </div>

      {/* 스케줄러 */}
      <div style={{ flexShrink: 0 }}>
        <Scheduler />
      </div>
    </div>
  )
}

const s: Record<string, React.CSSProperties> = {
  modelRow: { display: 'flex', justifyContent: 'space-between', fontSize: 11, color: tokens.tx.secondary, marginBottom: 2 },
  lbl: { color: tokens.tx.subtle },
  statRow: { display: 'flex', gap: 10 },
  stat: { display: 'flex', flexDirection: 'column', alignItems: 'center', minWidth: 28 },
  statVal: { fontSize: 16, fontWeight: 700, color: tokens.tx.primary, lineHeight: '1.1' },
  statLbl: { fontSize: 9, color: tokens.tx.subtle, marginTop: 1, letterSpacing: '0.03em' },
  collapseHdr: {
    width: '100%', display: 'flex', alignItems: 'center', gap: 8,
    padding: '7px 12px', background: 'none', border: 'none',
    borderTop: `1px solid ${tokens.bd.default}`, borderBottom: `1px solid ${tokens.bd.default}`,
    color: tokens.tx.secondary, cursor: 'pointer', textAlign: 'left', flexShrink: 0,
  },
  collapseTitle: { fontWeight: 700, fontSize: 11, flex: 1 },
  collapseBadge: { background: tokens.bg.raised, color: tokens.tx.muted, fontSize: 10, padding: '2px 7px', borderRadius: 10 },
  chevron: { fontSize: 10, color: tokens.tx.subtle },
  th: { position: 'sticky', top: 0, background: tokens.bg.row, color: tokens.tx.subtle, padding: '5px 8px', textAlign: 'left', fontWeight: 600, whiteSpace: 'nowrap', borderRight: `1px solid ${tokens.bd.default}`, borderBottom: `1px solid ${tokens.bd.default}` },
  td: { padding: '5px 8px', borderBottom: `1px solid ${tokens.bd.default}`, borderRight: `1px solid ${tokens.bd.default}`, verticalAlign: 'middle' },
}
