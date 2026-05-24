import { useState, useMemo } from 'react'
import { tokens } from '../tokens'

interface ScreenerItem {
  ticker: string
  name: string
  week_count: number
  first_week: string
  last_week: string
  any_enhanced: boolean
  any_gapjum: boolean
}

interface Props {
  items: ScreenerItem[]
  start: string
  end: string
  selectedTicker?: string | null
  onSelect?: (ticker: string, name: string) => void
}

const CHIP_COLOR = { all: tokens.accent.blueSoft, enhanced: tokens.stage[2], gapjum: tokens.chart.cat.ichimoku } as const

export default function HistoryScreenerView({ items, start: _start, end: _end, selectedTicker, onSelect }: Props) {
  const [filter, setFilter] = useState<'all' | 'enhanced' | 'gapjum'>('all')

  const counts = useMemo(() => ({
    all: items.length,
    enhanced: items.filter(i => i.any_enhanced).length,
    gapjum: items.filter(i => i.any_gapjum).length,
  }), [items])

  const filtered = useMemo(() =>
    filter === 'all' ? items
    : filter === 'enhanced' ? items.filter(i => i.any_enhanced)
    : items.filter(i => i.any_gapjum),
  [items, filter])

  if (items.length === 0) {
    return <div style={s.empty}>해당 기간에 스크리닝된 종목 없음</div>
  }

  return (
    <>
      <div style={s.chips}>
        {(['all', 'enhanced', 'gapjum'] as const).map(f => (
          <button
            key={f}
            style={{
              ...s.chip,
              border: filter === f ? `1px solid ${CHIP_COLOR[f]}` : '1px solid transparent',
              background: filter === f ? tokens.bg.active : tokens.bg.raised,
            }}
            onClick={() => setFilter(f)}
          >
            <span style={s.chipLabel}>{f === 'all' ? '통과' : f === 'enhanced' ? '강화' : '갭점프'}</span>
            <span style={{ ...s.chipVal, color: CHIP_COLOR[f] }}>{counts[f]}</span>
          </button>
        ))}
      </div>

      <div style={s.tableWrap}>
        <table style={s.table}>
          <thead>
            <tr>
              {['종목', '등장횟수(주)', '최초등장주', '최근등장주', '강화', '갭점프'].map(h => (
                <th key={h} style={s.th}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filtered.map(it => (
              <tr
                key={it.ticker}
                style={{ background: it.ticker === selectedTicker ? tokens.bg.active : 'transparent', cursor: 'pointer' }}
                onClick={() => onSelect?.(it.ticker, it.name)}
              >
                <td style={s.td}>
                  <div style={s.tickerName}>{it.name}</div>
                  <div style={s.tickerCode}>{it.ticker}</div>
                </td>
                <td style={{ ...s.td, textAlign: 'right', fontWeight: 700, color: tokens.accent.blueSoft }}>
                  {it.week_count}주
                </td>
                <td style={{ ...s.td, color: tokens.tx.muted }}>{it.first_week}</td>
                <td style={{ ...s.td, color: tokens.tx.muted }}>{it.last_week}</td>
                <td style={{ ...s.td, textAlign: 'center' }}>
                  {it.any_enhanced && <span style={{ color: tokens.stage[2] }}>강화</span>}
                </td>
                <td style={{ ...s.td, textAlign: 'center' }}>
                  {it.any_gapjum && <span style={{ color: tokens.chart.cat.ichimoku }}>갭</span>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

    </>
  )
}

const s: Record<string, React.CSSProperties> = {
  empty: { color: tokens.tx.subtle, textAlign: 'center', padding: '24px 0', fontSize: 12 },
  chips: { display: 'flex', gap: 8, marginBottom: 10, flexWrap: 'wrap' },
  chip: { background: tokens.bg.raised, borderRadius: 6, padding: '6px 12px', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 2, outline: 'none', cursor: 'pointer' },
  chipLabel: { fontSize: 10, color: tokens.tx.muted },
  chipVal: { fontSize: 18, fontWeight: 700 },
  tableWrap: { overflowX: 'auto', maxHeight: 280, overflowY: 'auto' },
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
