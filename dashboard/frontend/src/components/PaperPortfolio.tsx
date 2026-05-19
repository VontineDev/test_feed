import { useState, useEffect } from 'react'
import Positions from './Positions'
import Scheduler from './Scheduler'

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

const pctColor = (v: number | null | undefined) => {
  if (v == null) return '#64748b'
  return v > 0 ? '#4ade80' : v < 0 ? '#f87171' : '#94a3b8'
}

export default function PaperPortfolio() {
  const [data, setData] = useState<PaperData | null>(null)

  useEffect(() => {
    fetch('/api/report/paper')
      .then(r => r.json())
      .then(j => setData(j.data))
      .catch(() => {})
  }, [])

  const models = data ? Object.keys(data.model_summary) : []

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', overflow: 'auto', fontSize: 12, color: '#e2e8f0' }}>

      {/* 모델별 요약 */}
      {data && models.length > 0 && (
        <div style={{ padding: '10px 12px', borderBottom: '1px solid #1e293b', flexShrink: 0 }}>
          <div style={{ fontWeight: 700, fontSize: 11, color: '#475569', marginBottom: 8 }}>모델별 요약</div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(110px, 1fr))', gap: 8 }}>
            {models.map(m => {
              const info = data.model_summary[m]
              const open    = info['open']    ?? { count: 0, avg_return: null }
              const pending = info['pending'] ?? { count: 0, avg_return: null }
              const closed  = info['closed']  ?? { count: 0, avg_return: null }
              return (
                <div key={m} style={{ background: '#1e293b', borderRadius: 8, padding: '10px 12px' }}>
                  <div style={{ fontWeight: 700, color: '#93c5fd', marginBottom: 6 }}>{MODEL_LABEL[m] ?? m}</div>
                  <div style={s.modelRow}><span style={s.lbl}>오픈</span><span>{open.count}</span></div>
                  <div style={s.modelRow}><span style={s.lbl}>대기</span><span>{pending.count}</span></div>
                  {closed.count > 0 && (
                    <div style={s.modelRow}><span style={s.lbl}>청산</span><span>{closed.count}건</span></div>
                  )}
                  {closed.avg_return != null && (
                    <div style={{ ...s.modelRow, marginTop: 4 }}>
                      <span style={s.lbl}>평균수익</span>
                      <span style={{ color: pctColor(closed.avg_return), fontWeight: 700 }}>
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

      {/* 실시간 포지션 */}
      <div style={{ flex: 1, minHeight: 200, borderBottom: '1px solid #1e293b', overflowY: 'auto' }}>
        <Positions />
      </div>

      {/* 최근 청산 이력 */}
      {data && data.closed.length > 0 && (
        <div style={{ borderBottom: '1px solid #1e293b', padding: '10px 12px', flexShrink: 0 }}>
          <div style={{ fontWeight: 700, fontSize: 11, color: '#475569', marginBottom: 8 }}>최근 청산 이력</div>
          <div style={{ overflowX: 'auto', maxHeight: 220, overflowY: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
              <thead>
                <tr>
                  {['종목', '모델', '청산일', '청산유형', 'blended수익'].map(h => (
                    <th key={h} style={s.th}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.closed.map((r, i) => (
                  <tr key={i}>
                    <td style={s.td}>
                      <div style={{ color: '#cbd5e1', fontWeight: 600 }}>{r.name}</div>
                      <div style={{ color: '#475569', fontSize: 10 }}>{r.signal_date}</div>
                    </td>
                    <td style={{ ...s.td, color: '#94a3b8' }}>{MODEL_LABEL[r.model] ?? r.model}</td>
                    <td style={{ ...s.td, color: '#64748b' }}>{r.exit_date ?? '—'}</td>
                    <td style={{ ...s.td, color: '#94a3b8' }}>
                      {r.exit_type ? (EXIT_LABEL[r.exit_type] ?? r.exit_type) : '—'}
                      {r.tp1_date && <span style={{ color: '#a78bfa', marginLeft: 4 }}>TP1</span>}
                    </td>
                    <td style={{ ...s.td, textAlign: 'right', color: pctColor(r.blended_return ? r.blended_return * 100 : null), fontWeight: 600 }}>
                      {r.blended_return != null
                        ? `${r.blended_return > 0 ? '+' : ''}${(r.blended_return * 100).toFixed(1)}%`
                        : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* 스케줄러 */}
      <div style={{ flexShrink: 0 }}>
        <Scheduler />
      </div>
    </div>
  )
}

const s: Record<string, React.CSSProperties> = {
  modelRow: { display: 'flex', justifyContent: 'space-between', fontSize: 11, color: '#94a3b8', marginBottom: 2 },
  lbl: { color: '#475569' },
  th: { position: 'sticky', top: 0, background: '#0f172a', color: '#475569', padding: '5px 8px', textAlign: 'left', fontWeight: 600, whiteSpace: 'nowrap' },
  td: { padding: '5px 8px', borderBottom: '1px solid #0f172a', verticalAlign: 'middle' },
}
