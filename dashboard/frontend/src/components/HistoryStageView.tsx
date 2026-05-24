import { useState, useMemo } from 'react'
import { tokens } from '../tokens'

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
  selectedTicker?: string | null
  onSelect?: (ticker: string, name: string) => void
}

const STAGE_COLOR: Record<number, string> = {
  1: tokens.stage[1],
  2: tokens.stage[2],
  3: tokens.stage[3],
}

export default function HistoryStageView({ items, start: _start, end: _end, selectedTicker, onSelect }: Props) {
  const [activeStage, setActiveStage] = useState<1 | 2 | 3>(1)

  const stageCounts = useMemo(() => {
    const counts: Record<number, number> = { 1: 0, 2: 0, 3: 0 }
    items.forEach(it => { counts[it.stage_queried] = (counts[it.stage_queried] ?? 0) + 1 })
    return counts
  }, [items])

  const filtered = useMemo(
    () => items.filter(it => it.stage_queried === activeStage),
    [items, activeStage],
  )

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
              background: activeStage === sg ? tokens.bg.active : tokens.bg.raised,
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
                {(['종목', '업종', '등장횟수', '최초등장', '최근등장', '현재단계'] as const).map(h => (
                  <th key={h} style={s.th}>{h}</th>
                ))}
                <th style={s.th} title="고점 이탈 — 외국인·기관 동시 순매도 또는 윗꼬리+거래량 급증 감지">⚠</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map(it => {
                const progressed = it.latest_stage != null && it.latest_stage !== it.stage_queried
                return (
                  <tr
                    key={`${it.ticker}-${it.stage_queried}`}
                    style={{
                      background: it.ticker === selectedTicker ? tokens.bg.active : it.any_peakout ? '#1a0f0f' : 'transparent',
                      cursor: 'pointer',
                    }}
                    onClick={() => onSelect?.(it.ticker, it.name)}
                  >
                    <td style={s.td}>
                      <div style={s.tickerName}>{it.name}</div>
                      <div style={s.tickerCode}>{it.ticker}</div>
                    </td>
                    <td style={{ ...s.td, color: tokens.tx.muted, fontSize: 10 }}>—</td>
                    <td style={{ ...s.td, textAlign: 'right', fontWeight: 700, color: tokens.stage[1] }}>
                      {it.appearance_count}회
                    </td>
                    <td style={{ ...s.td, color: tokens.tx.muted }}>{it.first_seen ?? '—'}</td>
                    <td style={{ ...s.td, color: tokens.tx.muted }}>{it.last_seen ?? '—'}</td>
                    <td style={s.td}>
                      {it.latest_stage != null ? (
                        <span style={{
                          color: STAGE_COLOR[it.latest_stage] ?? tokens.tx.secondary,
                          fontWeight: progressed ? 700 : 400,
                        }}>
                          S{it.latest_stage}
                          {progressed && ' ↑'}
                        </span>
                      ) : '—'}
                    </td>
                    <td style={{ ...s.td, textAlign: 'center' }}>
                      {it.any_peakout && <span style={{ color: tokens.semantic.up }}>⚠</span>}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

    </>
  )
}

const s: Record<string, React.CSSProperties> = {
  chips: { display: 'flex', gap: 8, marginBottom: 10, flexWrap: 'wrap' },
  chip: {
    background: tokens.bg.raised, borderRadius: 6, padding: '6px 12px',
    display: 'flex', flexDirection: 'column', alignItems: 'center',
    gap: 2, outline: 'none', cursor: 'pointer',
  },
  chipLabel: { fontSize: 10, color: tokens.tx.muted },
  chipVal: { fontSize: 18, fontWeight: 700 },
  empty: { color: tokens.tx.subtle, textAlign: 'center', padding: '24px 0', fontSize: 12 },
  tableWrap: { overflowX: 'auto', maxHeight: 320, overflowY: 'auto' },
  table: { width: '100%', borderCollapse: 'collapse', fontSize: 11 },
  th: {
    position: 'sticky', top: 0, background: tokens.bg.row,
    color: tokens.tx.subtle, padding: '5px 8px', textAlign: 'left',
    fontWeight: 600, whiteSpace: 'nowrap',
    borderRight: `1px solid ${tokens.bd.default}`, borderBottom: `1px solid ${tokens.bd.default}`,
  },
  td: { padding: '5px 8px', borderBottom: `1px solid ${tokens.bd.default}`, borderRight: `1px solid ${tokens.bd.default}`, verticalAlign: 'middle' },
  tickerName: { color: tokens.tx.secondary, fontWeight: 600 },
  tickerCode: { color: tokens.tx.subtle, fontSize: 10 },
}
