import { useEffect, useState, useCallback } from 'react'
import { ResponsiveTreeMap } from '@nivo/treemap'
import { tokens, heatCellColor, stageColor, pctTextColor } from '../tokens'
import MarketSummaryBanner from './MarketSummaryBanner'

interface HeatmapItem {
  ticker: string
  name: string
  stage: number | null
  amount: number
  change_pct: number
  market: string
}

const REFRESH_MS = 5 * 60 * 1000  // 5분

const formatAmount = (amount: number): string => {
  if (amount >= 1e12) return `${(amount / 1e12).toFixed(1)}조`
  if (amount >= 1e8)  return `${(amount / 1e8).toFixed(0)}억`
  return `${amount}`
}

const formatCountdown = (ms: number): string => {
  const s = Math.ceil(ms / 1000)
  return s >= 60 ? `${Math.floor(s / 60)}분 ${s % 60}초` : `${s}초`
}

export default function Heatmap() {
  const [items, setItems]           = useState<HeatmapItem[]>([])
  const [loading, setLoading]       = useState(true)
  const [error, setError]           = useState<string | null>(null)
  const [selected, setSelected]     = useState<HeatmapItem | null>(null)
  const [updatedAt, setUpdatedAt]   = useState<Date | null>(null)
  const [nextRefresh, setNextRefresh] = useState<number>(REFRESH_MS)

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const j = await fetch('/api/heatmap').then(r => r.json())
      setItems(j.data ?? [])
      setUpdatedAt(new Date())
      setNextRefresh(REFRESH_MS)
      setError(null)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  // 초기 로드 + 5분 자동갱신
  useEffect(() => {
    load()
    const refreshId = setInterval(load, REFRESH_MS)
    return () => clearInterval(refreshId)
  }, [load])

  // 카운트다운 타이머 (1초 tick)
  useEffect(() => {
    const id = setInterval(() => {
      setNextRefresh(prev => (prev <= 1000 ? REFRESH_MS : prev - 1000))
    }, 1000)
    return () => clearInterval(id)
  }, [])

  const filtered = items.filter(i => i.amount > 0).slice(0, 20)

  const nivoData = {
    id: 'root',
    children: filtered.map(i => ({
      id: i.ticker,
      value: i.amount,
      label: i.name,
      stage: i.stage,
      change_pct: i.change_pct,
      item: i,
    })),
  }

  return (
    <div style={styles.container} className="heatmap-root">
      <MarketSummaryBanner />
      {/* 헤더 */}
      <div style={styles.header}>
        <span style={styles.title}>시장 히트맵</span>
        <span style={styles.legend}>
          <span style={{ color: tokens.stage[1] }}>▮S1</span>{' '}
          <span style={{ color: tokens.stage[2] }}>▮S2</span>{' '}
          <span style={{ color: tokens.stage[3] }}>▮S3</span>
          <span style={styles.legendSep}>|</span>
          {/* 한국식 등락 — 트리맵 셀과 별개로 텍스트는 KR 컨벤션 */}
          <span style={{ color: tokens.semantic.up }}>▲상승</span>{' '}
          <span style={{ color: tokens.semantic.down }}>▼하락</span>
        </span>
        <span style={styles.meta}>
          {updatedAt && (
            <span style={styles.updatedAt}>
              {updatedAt.toLocaleTimeString('ko-KR')} 기준
            </span>
          )}
          {!loading && (
            <span style={styles.countdown}>{formatCountdown(nextRefresh)} 후 갱신</span>
          )}
          <button style={styles.refreshBtn} onClick={load} disabled={loading}>
            {loading ? '…' : '↻'}
          </button>
        </span>
        <span style={styles.count}>{filtered.length}종목 (Top 거래대금)</span>
      </div>

      {/* 맵 영역 */}
      <div style={styles.mapArea}>
        {loading && !items.length
          ? <div style={styles.placeholder}>히트맵 로딩 중…</div>
          : error
          ? <div style={styles.placeholder}>오류: {error}</div>
          : !filtered.length
          ? (
            <div style={styles.placeholder}>
              <svg width="40" height="40" viewBox="0 0 40 40" fill="none" style={{ marginBottom: 12, opacity: 0.35 }}>
                <rect x="4" y="22" width="7" height="14" rx="2" fill={tokens.accent.blueSoft}/>
                <rect x="16" y="14" width="7" height="22" rx="2" fill={tokens.accent.blueSoft}/>
                <rect x="28" y="6" width="7" height="30" rx="2" fill={tokens.accent.blueSoft}/>
                <path d="M7 20 L19 12 L31 4" stroke={tokens.semantic.up} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
              </svg>
              <div style={{ fontSize: 14, fontWeight: 600, color: tokens.tx.muted, marginBottom: 4 }}>거래 데이터 없음</div>
              <div style={{ fontSize: 12, color: tokens.tx.separator }}>Kiwoom 연결을 확인하거나 새로고침하세요</div>
            </div>
          )
          : (
          <ResponsiveTreeMap
            data={nivoData}
            identity="id"
            value="value"
            leavesOnly={true}
            colors={(node) => {
              // 셀 배경 — 트리맵 전용 그린/레드 그라데이션 (heat scale)
              const d = node.data as unknown as { change_pct?: number }
              return heatCellColor(d.change_pct ?? 0)
            }}
            borderWidth={2}
            borderColor={(node) => {
              // 셀 보더 — Stage 색
              const d = node.data as unknown as { stage?: number | null }
              return stageColor(d.stage ?? null)
            }}
            label={(node) => {
              const d = node.data as unknown as { label?: string; change_pct?: number; stage?: number | null }
              if (!d.label) return ''
              const { width } = node
              if (width < 28) return ''
              const pct = d.change_pct ?? 0
              const sign = pct > 0 ? '+' : ''
              const pctStr = `${sign}${pct.toFixed(1)}%`
              const stageStr = d.stage != null ? `S${d.stage} ` : ''
              const maxChars = Math.max(1, Math.floor(width / 11) - 1)
              const full = `${stageStr}${d.label}  ${pctStr}`
              if (full.length <= maxChars) return full
              const nameMax = maxChars - pctStr.length - 2
              if (nameMax >= 2) {
                const name = d.label.length > nameMax ? d.label.slice(0, nameMax - 1) + '…' : d.label
                return `${name}  ${pctStr}`
              }
              return maxChars >= 2 ? d.label.slice(0, maxChars - 1) + '…' : d.label.slice(0, 1)
            }}
            orientLabel={false}
            labelSkipSize={0}
            labelTextColor={tokens.tx.primary}
            tooltip={({ node }) => {
              const d = node.data as unknown as { item?: HeatmapItem }
              if (!d.item) return <div />
              const i = d.item
              const sign = i.change_pct > 0 ? '+' : ''
              return (
                <div style={styles.tooltip}>
                  <strong>{i.name}</strong>
                  <span style={{ color: tokens.tx.muted, marginLeft: 6, fontSize: 10 }}>{i.ticker}</span>
                  <br />
                  <span style={{ color: tokens.tx.secondary }}>Stage {i.stage}</span>
                  {' · '}
                  {/* 텍스트 등락률 — 한국식 (셀 색과 별개) */}
                  <span style={{ color: pctTextColor(i.change_pct), fontWeight: 700 }}>
                    {sign}{i.change_pct.toFixed(2)}%
                  </span>
                  <br />
                  <span style={{ color: tokens.tx.muted }}>거래대금 {formatAmount(i.amount)}</span>
                </div>
              )
            }}
            onClick={(node) => {
              const d = node.data as unknown as { item?: HeatmapItem }
              if (d.item) setSelected(d.item)
            }}
          />
        )}
      </div>

      {/* 선택 종목 정보바 */}
      {selected && (
        <div style={styles.infoBar}>
          <strong>{selected.name}</strong>
          <span style={styles.infoTicker}>{selected.ticker}</span>
          <span style={styles.infoSep}>|</span>
          Stage {selected.stage}
          <span style={styles.infoSep}>|</span>
          {/* 텍스트 등락률 — 한국식 */}
          <span style={{ color: pctTextColor(selected.change_pct), fontWeight: 700 }}>
            {selected.change_pct > 0 ? '+' : ''}{selected.change_pct.toFixed(2)}%
          </span>
          <span style={styles.infoSep}>|</span>
          거래대금 {formatAmount(selected.amount)}
          <button style={styles.closeBtn} onClick={() => setSelected(null)}>✕</button>
        </div>
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  container: { display: 'flex', flexDirection: 'column', height: '100%' },

  header: {
    display: 'flex', alignItems: 'center', gap: 10, padding: '7px 12px',
    background: tokens.bg.panel, flexShrink: 0, flexWrap: 'wrap' as const,
  },
  title:     { fontWeight: 700, fontSize: 13 },
  legend:    { display: 'flex', gap: 5, fontSize: 11, alignItems: 'center' },
  legendSep: { color: tokens.tx.separator, margin: '0 2px' },
  meta:      { display: 'flex', alignItems: 'center', gap: 6, marginLeft: 'auto' },
  updatedAt: { fontSize: 10, color: tokens.tx.subtle },
  countdown: { fontSize: 10, color: tokens.tx.separator },
  refreshBtn: {
    background: tokens.bg.raised, color: tokens.tx.muted, border: `1px solid ${tokens.bd.emphasis}`,
    borderRadius: tokens.radius.sm, padding: '6px 10px', cursor: 'pointer', fontSize: 13,
    minWidth: 36, minHeight: 36,
  },
  count: { fontSize: 11, color: tokens.tx.muted },

  mapArea: { flex: 1, minHeight: 0 },
  placeholder: {
    display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center',
    height: '100%', color: tokens.tx.muted, textAlign: 'center' as const, padding: 24,
  },

  tooltip: {
    background: tokens.bg.raised, padding: '8px 12px', borderRadius: tokens.radius.md,
    fontSize: 12, lineHeight: 1.7, border: `1px solid ${tokens.bd.emphasis}`,
    boxShadow: tokens.shadow.tooltip,
  },

  infoBar: {
    padding: '6px 14px', background: tokens.bg.raised, fontSize: 12,
    borderTop: `1px solid ${tokens.bd.emphasis}`, position: 'relative',
    display: 'flex', alignItems: 'center', gap: 6,
  },
  infoTicker: { color: tokens.tx.subtle, fontSize: 10 },
  infoSep:    { color: tokens.tx.separator },
  closeBtn: {
    position: 'absolute', right: 12, top: '50%', transform: 'translateY(-50%)',
    background: 'none', border: 'none', color: tokens.tx.muted, cursor: 'pointer', fontSize: 14,
  },
}
