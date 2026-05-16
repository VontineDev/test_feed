import { useEffect, useState } from 'react'
import { ResponsiveTreeMap } from '@nivo/treemap'

interface HeatmapItem {
  ticker: string
  name: string
  stage: number | null
  amount: number
  change_pct: number
  market: string
}

const stageColor = (stage: number | null): string => {
  switch (stage) {
    case 1: return '#16a34a'  // green
    case 2: return '#ca8a04'  // yellow
    case 3: return '#ea580c'  // orange
    default: return '#374151' // gray
  }
}

const formatAmount = (amount: number): string => {
  if (amount >= 1e12) return `${(amount / 1e12).toFixed(1)}조`
  if (amount >= 1e8)  return `${(amount / 1e8).toFixed(0)}억`
  return `${amount}`
}

export default function Heatmap() {
  const [items, setItems] = useState<HeatmapItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selected, setSelected] = useState<HeatmapItem | null>(null)

  useEffect(() => {
    fetch('/api/heatmap')
      .then(r => r.json())
      .then(j => { setItems(j.data); setLoading(false) })
      .catch(e => { setError(String(e)); setLoading(false) })
  }, [])

  if (loading) return <div style={styles.placeholder}>히트맵 로딩 중…</div>
  if (error)   return <div style={styles.placeholder}>오류: {error}</div>
  if (!items.length) return <div style={styles.placeholder}>오늘 분류 데이터 없음</div>

  // Stage 1/2/3 종목만 표시 (null=미분류 제외), 거래금액 0 제외
  const filtered = items.filter(i => i.stage && i.amount > 0)

  const nivoData = {
    id: 'root',
    children: filtered.map(i => ({
      id: i.ticker,
      value: i.amount,
      label: i.name,
      stage: i.stage,
      item: i,
    })),
  }

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <span style={styles.title}>시장 히트맵</span>
        <span style={styles.legend}>
          <span style={{ color: '#16a34a' }}>● Stage 1</span>{' '}
          <span style={{ color: '#ca8a04' }}>● Stage 2</span>{' '}
          <span style={{ color: '#ea580c' }}>● Stage 3</span>
        </span>
        <span style={styles.count}>{filtered.length}종목</span>
      </div>
      <div style={styles.mapArea}>
        <ResponsiveTreeMap
          data={nivoData}
          identity="id"
          value="value"
          colors={(node) => stageColor((node.data as unknown as { stage: number | null }).stage)}
          label={(node) => (node.data as unknown as { label: string }).label}
          labelSkipSize={24}
          labelTextColor="#fff"
          borderWidth={1}
          borderColor="#0f1117"
          tooltip={({ node }) => {
            const d = node.data as unknown as { item: HeatmapItem }
            const i = d.item
            return (
              <div style={styles.tooltip}>
                <strong>{i.name}</strong> ({i.ticker})<br />
                Stage {i.stage} | {i.change_pct > 0 ? '+' : ''}{i.change_pct.toFixed(2)}%<br />
                거래금액: {formatAmount(i.amount)}
              </div>
            )
          }}
          onClick={(node) => setSelected((node.data as unknown as { item: HeatmapItem }).item)}
        />
      </div>
      {selected && (
        <div style={styles.infoBar}>
          <strong>{selected.name}</strong> ({selected.ticker}) —
          Stage {selected.stage} |
          {selected.change_pct > 0 ? ' +' : ' '}{selected.change_pct.toFixed(2)}% |
          거래금액 {formatAmount(selected.amount)}
          <button style={styles.closeBtn} onClick={() => setSelected(null)}>✕</button>
        </div>
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  container: { display: 'flex', flexDirection: 'column', height: '100%' },
  header: { display: 'flex', alignItems: 'center', gap: 16, padding: '8px 12px', background: '#1a1d2e' },
  title: { fontWeight: 700, fontSize: 14 },
  legend: { fontSize: 12, display: 'flex', gap: 8 },
  count: { marginLeft: 'auto', fontSize: 12, color: '#94a3b8' },
  mapArea: { flex: 1, minHeight: 0 },
  placeholder: { display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: '#64748b' },
  tooltip: { background: '#1e293b', padding: '8px 12px', borderRadius: 6, fontSize: 12, lineHeight: 1.6, border: '1px solid #334155' },
  infoBar: { padding: '6px 12px', background: '#1e293b', fontSize: 13, borderTop: '1px solid #334155', position: 'relative' },
  closeBtn: { position: 'absolute', right: 12, top: '50%', transform: 'translateY(-50%)', background: 'none', border: 'none', color: '#94a3b8', cursor: 'pointer', fontSize: 14 },
}
