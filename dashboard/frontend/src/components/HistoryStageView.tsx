import { useState } from 'react'
import StageHistoryPopup from './StageHistoryPopup'

interface StageItem {
  ticker: string
  name: string
  appearance_count: number
  first_seen: string | null
  last_seen: string | null
  any_peakout: boolean
  stage_queried: number
  latest_stage: number | null
}

interface Props {
  items: StageItem[]
  start: string
  end: string
}

const STAGE_COLOR: Record<number, string> = {
  1: '#60a5fa',
  2: '#a78bfa',
  3: '#f59e0b',
}

export default function HistoryStageView({ items, start, end }: Props) {
  const [activeStage, setActiveStage] = useState<1 | 2 | 3>(1)
  const [popup, setPopup] = useState<{ ticker: string; name: string } | null>(null)

  const stageCounts: Record<number, number> = { 1: 0, 2: 0, 3: 0 }
  items.forEach(it => { stageCounts[it.stage_queried] = (stageCounts[it.stage_queried] ?? 0) + 1 })

  const filtered = items.filter(it => it.stage_queried === activeStage)

  return (
    <>
      {/* 스테이지 칩 */}
      <div style={s.chips}>
        {([1, 2, 3] as const).map(sg => (
          <button
            key={sg}
            style={{
              ...s.chip,
              border: activeStage === sg ? `1px solid ${STAGE_COLOR[sg]}` : '1px solid transparent',
              background: activeStage === sg ? '#1e3a5f' : '#1e293b',
            }}
            onClick={() => setActiveStage(sg)}
          >
            <span style={s.chipLabel}>Stage {sg}</span>
            <span style={{ ...s.chipVal, color: STAGE_COLOR[sg] }}>
              {stageCounts[sg] ?? 0}
            </span>
          </button>
        ))}
      </div>

      {/* 테이블 */}
      {filtered.length === 0 ? (
        <div style={s.empty}>해당 기간에 Stage {activeStage} 등장 종목 없음</div>
      ) : (
        <div style={s.tableWrap}>
          <table style={s.table}>
            <thead>
              <tr>
                {['종목', '업종', '등장횟수', '최초등장', '최근등장', '현재단계', ''].map(h => (
                  <th key={h} style={s.th}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filtered.map(it => {
                const progressed = it.latest_stage != null && it.latest_stage !== it.stage_queried
                return (
                  <tr
                    key={`${it.ticker}-${it.stage_queried}`}
                    style={{ background: it.any_peakout ? '#1a0f0f' : 'transparent', cursor: 'pointer' }}
                    onClick={() => setPopup({ ticker: it.ticker, name: it.name })}
                  >
                    <td style={s.td}>
                      <div style={s.tickerName}>{it.name}</div>
                      <div style={s.tickerCode}>{it.ticker}</div>
                    </td>
                    <td style={{ ...s.td, color: '#64748b', fontSize: 10 }}>—</td>
                    <td style={{ ...s.td, textAlign: 'right', fontWeight: 700, color: '#60a5fa' }}>
                      {it.appearance_count}회
                    </td>
                    <td style={{ ...s.td, color: '#64748b' }}>{it.first_seen ?? '—'}</td>
                    <td style={{ ...s.td, color: '#64748b' }}>{it.last_seen ?? '—'}</td>
                    <td style={s.td}>
                      {it.latest_stage != null ? (
                        <span style={{
                          color: STAGE_COLOR[it.latest_stage] ?? '#94a3b8',
                          fontWeight: progressed ? 700 : 400,
                        }}>
                          S{it.latest_stage}
                          {progressed && ' ↑'}
                        </span>
                      ) : '—'}
                    </td>
                    <td style={{ ...s.td, textAlign: 'center' }}>
                      {it.any_peakout && <span style={{ color: '#f87171' }}>⚠</span>}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* 히스토리 팝업 */}
      {popup && (
        <StageHistoryPopup
          ticker={popup.ticker}
          name={popup.name}
          start={start}
          end={end}
          onClose={() => setPopup(null)}
        />
      )}
    </>
  )
}

const s: Record<string, React.CSSProperties> = {
  chips: { display: 'flex', gap: 8, marginBottom: 10, flexWrap: 'wrap' },
  chip: {
    background: '#1e293b', borderRadius: 6, padding: '6px 12px',
    display: 'flex', flexDirection: 'column', alignItems: 'center',
    gap: 2, outline: 'none', cursor: 'pointer',
  },
  chipLabel: { fontSize: 10, color: '#64748b' },
  chipVal: { fontSize: 18, fontWeight: 700 },
  empty: { color: '#475569', textAlign: 'center', padding: '24px 0', fontSize: 12 },
  tableWrap: { overflowX: 'auto', maxHeight: 320, overflowY: 'auto' },
  table: { width: '100%', borderCollapse: 'collapse', fontSize: 11 },
  th: {
    position: 'sticky', top: 0, background: '#0f172a',
    color: '#475569', padding: '5px 8px', textAlign: 'left',
    fontWeight: 600, whiteSpace: 'nowrap',
  },
  td: { padding: '5px 8px', borderBottom: '1px solid #0f172a', verticalAlign: 'middle' },
  tickerName: { color: '#cbd5e1', fontWeight: 600 },
  tickerCode: { color: '#475569', fontSize: 10 },
}
