import { useCallback, useEffect, useState } from 'react'

interface HistoryRow {
  id: number
  model: string
  ticker: string
  name: string
  signal_date: string
  entry_theory: number | null
  entry_actual: number | null
  slippage_pct: number | null
  qty: number | null
  status: string
  tp1_pct: number | null
  tp1_ratio: number | null
  tp1_date: string | null
  tp1_price: number | null
  trail_pct: number | null
  hard_stop_pct: number | null
  watermark: number | null
  exit_date: string | null
  exit_price: number | null
  exit_type: string | null
  blended_return: number | null
  current_price: number | null
  unrealized_pct: number | null
}

const MODEL_COLORS: Record<string, string> = {
  stage: '#3b82f6',
  kosdaq: '#f59e0b',
  cross: '#8b5cf6',
  ichimoku: '#10b981',
}

const EXIT_LABEL: Record<string, string> = {
  hard_stop: '손절',
  trail: '트레일링',
  period_end: '기간종료',
  manual: '수동',
}

const pctColor = (v: number | null) => {
  if (v == null) return '#94a3b8'
  return v > 0 ? '#4ade80' : v < 0 ? '#f87171' : '#94a3b8'
}

const fmtPct = (v: number | null) =>
  v == null ? '—' : `${v > 0 ? '+' : ''}${v.toFixed(1)}%`

interface Props {
  ticker: string
  name: string
  onClose: () => void
}

export default function TickerHistory({ ticker, name, onClose }: Props) {
  const [rows, setRows] = useState<HistoryRow[]>([])
  const [loading, setLoading] = useState(true)
  const [lastUpdated, setLastUpdated] = useState<string>('')

  const load = useCallback(() => {
    fetch(`/api/paper/history?ticker=${encodeURIComponent(ticker)}`)
      .then(r => r.json())
      .then(j => {
        setRows(j.data ?? [])
        setLoading(false)
        setLastUpdated(new Date().toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit', second: '2-digit' }))
      })
      .catch(() => setLoading(false))
  }, [ticker])

  useEffect(() => {
    setLoading(true)
    setRows([])
    load()
    const id = setInterval(load, 30_000)
    return () => clearInterval(id)
  }, [load])

  const hasActive = rows.some(r => r.status === 'open' || r.status === 'pending')
  const totalPnl = rows
    .filter(r => r.blended_return != null)
    .reduce((s, r) => s + (r.blended_return! * 100), 0)
  const closedCount = rows.filter(r => r.status === 'closed').length

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', fontSize: 12, color: '#e2e8f0' }}>

      {/* 헤더 */}
      <div style={{
        display: 'flex', alignItems: 'center', gap: 8,
        padding: '8px 12px', borderBottom: '1px solid #1e293b',
        background: '#1a1d2e', flexShrink: 0,
      }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <span style={{ fontWeight: 700, color: '#e2e8f0', fontSize: 13 }}>{name}</span>
          <span style={{ color: '#475569', fontSize: 10, marginLeft: 6 }}>{ticker}</span>
          {hasActive && (
            <span style={{
              marginLeft: 6, fontSize: 9, background: '#052e16',
              color: '#4ade80', padding: '1px 5px', borderRadius: 4,
            }}>● LIVE</span>
          )}
        </div>
        {closedCount > 0 && (
          <div style={{ color: pctColor(totalPnl), fontSize: 11, fontWeight: 700, flexShrink: 0 }}>
            누적 {fmtPct(totalPnl)}
          </div>
        )}
        <button
          onClick={onClose}
          style={{
            background: 'none', border: 'none', color: '#64748b',
            cursor: 'pointer', fontSize: 15, padding: '2px 4px', flexShrink: 0,
          }}
          title="닫기"
        >✕</button>
      </div>

      {/* 업데이트 시각 */}
      {lastUpdated && (
        <div style={{ padding: '3px 12px', color: '#334155', fontSize: 10, flexShrink: 0, borderBottom: '1px solid #1e293b' }}>
          갱신 {lastUpdated} · 30초 자동갱신
        </div>
      )}

      {/* 포지션 목록 */}
      <div style={{ flex: 1, overflowY: 'auto', padding: '8px 10px' }}>
        {loading && (
          <div style={{ color: '#64748b', padding: 16, textAlign: 'center' }}>이력 로딩 중…</div>
        )}
        {!loading && rows.length === 0 && (
          <div style={{ color: '#64748b', padding: 16, textAlign: 'center' }}>이력 없음</div>
        )}
        {rows.map(r => (
          <PositionCard key={r.id} row={r} />
        ))}
      </div>
    </div>
  )
}

function PositionCard({ row: r }: { row: HistoryRow }) {
  const modelColor = MODEL_COLORS[r.model] ?? '#64748b'
  const isActive = r.status === 'open' || r.status === 'pending'

  return (
    <div style={{
      border: `1px solid ${isActive ? '#1e3a5f' : '#1e293b'}`,
      borderRadius: 8,
      marginBottom: 10,
      overflow: 'hidden',
      background: isActive ? '#060d1a' : '#0a0f1a',
    }}>

      {/* 카드 헤더 */}
      <div style={{
        display: 'flex', alignItems: 'center', gap: 7,
        padding: '6px 10px',
        background: isActive ? '#0d1e35' : '#111827',
        borderBottom: '1px solid #1e293b',
      }}>
        <span style={{
          background: modelColor + '25', color: modelColor,
          padding: '1px 7px', borderRadius: 4,
          fontSize: 10, fontWeight: 700, letterSpacing: 0.5,
        }}>
          {r.model.toUpperCase()}
        </span>
        <span style={{ color: '#64748b', fontSize: 11 }}>신호 {r.signal_date}</span>
        <span style={{
          marginLeft: 'auto', fontSize: 10, flexShrink: 0,
          color: isActive ? '#4ade80' : '#475569',
          fontWeight: isActive ? 700 : 400,
        }}>
          {r.status === 'pending' ? '⏳ 매수대기' : r.status === 'open' ? '● 보유중' : '✓ 완료'}
        </span>
      </div>

      {/* 타임라인 */}
      <div style={{ padding: '8px 10px', display: 'flex', flexDirection: 'column', gap: 6 }}>

        {/* 진입 / 매수대기 */}
        {r.entry_actual != null ? (
          <TimelineRow icon="▶" iconColor="#3b82f6">
            <span>
              진입{' '}
              <strong style={{ color: '#cbd5e1' }}>₩{r.entry_actual.toLocaleString()}</strong>
              {r.qty != null && (
                <> × <strong style={{ color: '#cbd5e1' }}>{r.qty}주</strong></>
              )}
            </span>
            {r.slippage_pct != null && (
              <span style={{ color: '#64748b', marginLeft: 8 }}>
                슬리피지{' '}
                <span style={{ color: pctColor(r.slippage_pct * 100) }}>
                  {fmtPct(r.slippage_pct * 100)}
                </span>
              </span>
            )}
          </TimelineRow>
        ) : (
          <TimelineRow icon="⏳" iconColor="#f59e0b">
            <span style={{ color: '#64748b' }}>
              T+1 시가 매수 대기
              {r.entry_theory != null && (
                <> (이론가 ₩{r.entry_theory.toLocaleString()})</>
              )}
            </span>
          </TimelineRow>
        )}

        {/* TP1 */}
        {r.tp1_date && r.tp1_price != null && r.entry_actual != null && (
          <TimelineRow icon="★" iconColor="#a78bfa">
            <span>
              1차익절{' '}
              <strong style={{ color: '#a78bfa' }}>₩{r.tp1_price.toLocaleString()}</strong>
              <span style={{ color: '#4ade80', marginLeft: 4 }}>
                {fmtPct((r.tp1_price / r.entry_actual - 1) * 100)}
              </span>
            </span>
            <span style={{ color: '#64748b', marginLeft: 8 }}>{r.tp1_date}</span>
            {r.tp1_ratio != null && (
              <span style={{ color: '#64748b', marginLeft: 4 }}>
                ({(r.tp1_ratio * 100).toFixed(0)}% 익절)
              </span>
            )}
          </TimelineRow>
        )}

        {/* 고점 (watermark) — 보유중일 때만 */}
        {isActive && r.watermark != null && r.entry_actual != null && r.watermark > r.entry_actual && (
          <TimelineRow icon="▲" iconColor="#334155">
            <span style={{ color: '#64748b' }}>
              고점{' '}
              <strong style={{ color: '#94a3b8' }}>₩{r.watermark.toLocaleString()}</strong>
              <span style={{ color: '#4ade80', marginLeft: 4 }}>
                {fmtPct((r.watermark / r.entry_actual - 1) * 100)}
              </span>
            </span>
            {r.trail_pct != null && (
              <span style={{ color: '#334155', marginLeft: 8 }}>
                트레일링 스탑 ₩{Math.round(r.watermark * (1 - r.trail_pct)).toLocaleString()}
              </span>
            )}
          </TimelineRow>
        )}

        {/* 현재가 (보유중) */}
        {isActive && r.current_price != null && (
          <TimelineRow icon="◉" iconColor={pctColor(r.unrealized_pct)}>
            <span>
              현재{' '}
              <strong style={{ color: '#e2e8f0' }}>₩{r.current_price.toLocaleString()}</strong>
              {r.unrealized_pct != null && (
                <span style={{ color: pctColor(r.unrealized_pct), fontWeight: 700, marginLeft: 6 }}>
                  {fmtPct(r.unrealized_pct)}
                </span>
              )}
            </span>
          </TimelineRow>
        )}

        {/* 청산 */}
        {r.exit_date && r.exit_price != null && (
          <TimelineRow
            icon={r.blended_return != null && r.blended_return > 0 ? '✓' : '✗'}
            iconColor={r.blended_return != null && r.blended_return > 0 ? '#4ade80' : '#f87171'}
          >
            <span>
              청산{' '}
              <strong style={{ color: '#cbd5e1' }}>₩{r.exit_price.toLocaleString()}</strong>
              <span style={{ color: '#64748b', marginLeft: 4 }}>{r.exit_date}</span>
              {r.exit_type && (
                <span style={{
                  marginLeft: 4,
                  color: r.exit_type === 'hard_stop' ? '#f87171'
                    : r.exit_type === 'trail' ? '#a78bfa'
                    : '#94a3b8',
                }}>
                  {EXIT_LABEL[r.exit_type] ?? r.exit_type}
                </span>
              )}
            </span>
            {r.blended_return != null && (
              <span style={{
                color: pctColor(r.blended_return * 100),
                fontWeight: 700, marginLeft: 8,
              }}>
                {fmtPct(r.blended_return * 100)}
              </span>
            )}
          </TimelineRow>
        )}
      </div>
    </div>
  )
}

function TimelineRow({
  icon, iconColor, children,
}: {
  icon: string
  iconColor: string
  children: React.ReactNode
}) {
  return (
    <div style={{ display: 'flex', alignItems: 'baseline', gap: 6, fontSize: 11, color: '#94a3b8' }}>
      <span style={{ color: iconColor, fontSize: 12, flexShrink: 0, width: 14, textAlign: 'center' }}>
        {icon}
      </span>
      <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'baseline', gap: 0 }}>
        {children}
      </div>
    </div>
  )
}
