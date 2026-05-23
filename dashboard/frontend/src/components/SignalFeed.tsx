import { useSSE } from '../hooks/useSSE'
import { tokens } from '../tokens'

interface Signal {
  id: number
  direction: 'BUY' | 'SELL' | 'HOLD'
  strength: number
  tickers: string[]
  detected_at: string
  article_type: string
  title_en: string
  summary_ko: string | null
}

const directionStyle = (d: string): React.CSSProperties => ({
  color: d === 'BUY' ? tokens.chart.cat.ichimoku : d === 'SELL' ? tokens.semantic.up : tokens.tx.secondary,
  fontWeight: 700,
  fontSize: 11,
})

const strengthDots = (s: number) =>
  Array.from({ length: 5 }, (_, i) => (
    <span key={i} style={{ color: i < s ? tokens.chart.significance : tokens.tx.separator, fontSize: 10 }}>●</span>
  ))

export default function SignalFeed() {
  const signals = useSSE<Signal[]>('/api/signals/stream', 50).flat()

  if (!signals.length) {
    return <div style={styles.empty}>신호 대기 중…</div>
  }

  return (
    <div style={styles.wrap}>
      <div style={styles.hdr}>라이브 신호 피드</div>
      {signals.map(s => (
        <div key={s.id} style={styles.card}>
          <div style={styles.cardTop}>
            <span style={directionStyle(s.direction)}>{s.direction}</span>
            <span style={styles.strength}>{strengthDots(s.strength)}</span>
            <span style={styles.type}>{s.article_type}</span>
            <span style={styles.time}>{new Date(s.detected_at).toLocaleTimeString('ko-KR')}</span>
          </div>
          <div style={styles.title}>{s.title_en}</div>
          {s.summary_ko && <div style={styles.summary}>{s.summary_ko}</div>}
          {s.tickers?.length > 0 && (
            <div style={styles.tickers}>
              {s.tickers.slice(0, 5).map(t => (
                <span key={t} style={styles.ticker}>{t}</span>
              ))}
            </div>
          )}
        </div>
      ))}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  wrap: { padding: '8px 12px', overflowY: 'auto', height: '100%' },
  hdr: { fontWeight: 700, fontSize: 13, color: tokens.tx.secondary, marginBottom: 8 },
  card: { background: tokens.bg.panel, border: `1px solid ${tokens.bd.default}`, borderRadius: 6, padding: '8px 10px', marginBottom: 8, boxShadow: tokens.shadow.tooltip },
  cardTop: { display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 },
  strength: { display: 'flex', gap: 1 },
  type: { fontSize: 10, color: tokens.tx.muted, marginLeft: 'auto' },
  time: { fontSize: 10, color: tokens.tx.subtle },
  title: { fontSize: 12, color: tokens.tx.secondary, marginBottom: 4, lineHeight: 1.4 },
  summary: { fontSize: 11, color: tokens.tx.muted, marginBottom: 4, lineHeight: 1.4 },
  tickers: { display: 'flex', gap: 4, flexWrap: 'wrap' },
  ticker: { background: tokens.bg.row, color: tokens.accent.blueLight, padding: '1px 6px', borderRadius: 3, fontSize: 10 },
  empty: { padding: 24, color: tokens.tx.muted, textAlign: 'center' },
}
