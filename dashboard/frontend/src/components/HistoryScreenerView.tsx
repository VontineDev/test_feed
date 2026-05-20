import { useState } from 'react'
import StageHistoryPopup from './StageHistoryPopup'

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
}

export default function HistoryScreenerView({ items, start, end }: Props) {
  const [popup, setPopup] = useState<{ ticker: string; name: string } | null>(null)

  if (items.length === 0) {
    return <div style={s.empty}>해당 기간에 스크리닝된 종목 없음</div>
  }

  return (
    <>
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
            {items.map(it => (
              <tr
                key={it.ticker}
                style={{ cursor: 'pointer' }}
                onClick={() => setPopup({ ticker: it.ticker, name: it.name })}
              >
                <td style={s.td}>
                  <div style={s.tickerName}>{it.name}</div>
                  <div style={s.tickerCode}>{it.ticker}</div>
                </td>
                <td style={{ ...s.td, textAlign: 'right', fontWeight: 700, color: '#60a5fa' }}>
                  {it.week_count}주
                </td>
                <td style={{ ...s.td, color: '#64748b' }}>{it.first_week}</td>
                <td style={{ ...s.td, color: '#64748b' }}>{it.last_week}</td>
                <td style={{ ...s.td, textAlign: 'center' }}>
                  {it.any_enhanced && <span style={{ color: '#a78bfa' }}>강화</span>}
                </td>
                <td style={{ ...s.td, textAlign: 'center' }}>
                  {it.any_gapjum && <span style={{ color: '#34d399' }}>갭</span>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

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
  empty: { color: '#475569', textAlign: 'center', padding: '24px 0', fontSize: 12 },
  tableWrap: { overflowX: 'auto', maxHeight: 280, overflowY: 'auto' },
  table: { width: '100%', borderCollapse: 'collapse', fontSize: 11 },
  th: {
    position: 'sticky', top: 0, background: '#0f172a',
    color: '#475569', padding: '5px 8px', textAlign: 'left',
    fontWeight: 600, whiteSpace: 'nowrap',
    borderRight: '1px solid #1e293b', borderBottom: '1px solid #1e293b',
  },
  td: { padding: '5px 8px', borderBottom: '1px solid #1e293b', borderRight: '1px solid #1e293b', verticalAlign: 'middle' },
  tickerName: { color: '#cbd5e1', fontWeight: 600 },
  tickerCode: { color: '#475569', fontSize: 10 },
}
