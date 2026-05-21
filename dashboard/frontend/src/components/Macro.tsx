import { useState, useEffect, useCallback } from 'react'

// ── 타입 ──────────────────────────────────────────────────────

interface FactorSnap {
  name: string
  current: number
  change_1d: number
  change_5d: number
  change_20d: number
  change_60d: number
  z_score_60d: number
}

interface StockResult {
  ticker: string
  name: string
  n_obs: number
  r_squared: number
  adj_r_squared: number
  residual_std: number
  macro_score: number
  macro_score_5d: number
  macro_score_20d: number
  significant_factors: string[]
  betas: Record<string, number>
  alpha: number
  t_stats: Record<string, number>
  p_values: Record<string, number>
  factor_contribs_5d: Record<string, number>
}

interface MacroData {
  snapshot: Record<string, FactorSnap>
  stocks: StockResult[]
  fetched_at: string
  cached: boolean
  stale?: boolean
  error?: string
}

// ── 상수 ──────────────────────────────────────────────────────

const FACTOR_KEYS = ['rate', 'fx', 'oil', 'vix', 'dxy', 'export']
const FACTOR_LABELS: Record<string, string> = {
  rate:   '미국10년금리',
  fx:     'USD/KRW',
  oil:    '브렌트유',
  vix:    'VIX',
  dxy:    '달러인덱스',
  export: '수출EWY',
}
const FACTOR_UNITS: Record<string, string> = {
  rate: '%p', fx: '%', oil: '%', vix: '%', dxy: '%', export: '%',
}

// ── 유틸 ──────────────────────────────────────────────────────

const clr = (v: number, inv = false) => {
  const pos = inv ? v < 0 : v > 0
  if (v === 0) return '#64748b'
  return pos ? '#f87171' : '#60a5fa'
}

const fmt = (v: number, d = 3) =>
  (v >= 0 ? '+' : '') + v.toFixed(d)

const ScoreBar = ({ score }: { score: number }) => {
  const w = Math.min(Math.abs(score), 100) / 100
  const isPos = score >= 0
  return (
    <div style={{
      width: 100, height: 8, background: '#1e293b', borderRadius: 4,
      position: 'relative', overflow: 'hidden',
    }}>
      <div style={{
        position: 'absolute',
        width: `${w * 50}%`,
        height: '100%',
        background: score >= 20 ? '#22c55e' : score >= -20 ? '#f59e0b' : '#ef4444',
        borderRadius: 2,
        left: isPos ? '50%' : `${50 - w * 50}%`,
      }} />
      <div style={{
        position: 'absolute', left: '50%', top: 0, bottom: 0,
        width: 1, background: '#334155',
      }} />
    </div>
  )
}

// ── 메인 컴포넌트 ───────────────────────────────────────────────

export default function Macro() {
  const [data, setData]       = useState<MacroData | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError]     = useState('')
  const [expanded, setExpanded] = useState<string | null>(null)

  const load = useCallback(async (refresh = false) => {
    setLoading(true)
    setError('')
    try {
      const r = await fetch(`/api/macro${refresh ? '?refresh=true' : ''}`)
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      const d: MacroData = await r.json()
      setData(d)
      if (d.error) setError(d.error)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  const snap = data?.snapshot ?? {}
  const stocks = data?.stocks ?? []

  return (
    <div style={{ padding: 12, overflowY: 'auto', height: '100%', boxSizing: 'border-box' }}>

      {/* 헤더 */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
        <span style={{ fontSize: 13, fontWeight: 700 }}>
          📊 매크로 팩터 영향 분석
        </span>
        <span style={{ fontSize: 11, color: '#64748b', display: 'flex', alignItems: 'center', gap: 8 }}>
          {data && `갱신: ${data.fetched_at}${data.cached ? ' (캐시)' : ''}`}
          {data?.stale && <span style={{ color: '#f59e0b' }}>⚠ 구데이터</span>}
          <button
            onClick={() => load(true)}
            disabled={loading}
            style={{
              padding: '4px 10px', fontSize: 12, background: '#1e293b',
              border: '1px solid #334155', color: '#94a3b8',
              cursor: 'pointer', borderRadius: 4,
            }}
          >
            {loading ? '분석 중…' : '↻'}
          </button>
        </span>
      </div>

      {error && (
        <div style={{ color: '#f87171', fontSize: 11, marginBottom: 8 }}>
          {error}
        </div>
      )}

      {loading && !data && (
        <div style={{ color: '#64748b', fontSize: 12, textAlign: 'center', marginTop: 40 }}>
          <div>yfinance 데이터 다운로드 중…</div>
          <div style={{ marginTop: 4 }}>최초 로딩 30~60초 소요</div>
        </div>
      )}

      {/* ── 매크로 스냅샷 ─────────────────────────────────────── */}
      {Object.keys(snap).length > 0 && (
        <div style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: '#94a3b8', marginBottom: 6 }}>
            현재 매크로 환경
          </div>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
            <thead>
              <tr style={{ color: '#475569', borderBottom: '1px solid #1e293b' }}>
                {['팩터', '현재값', '1일Δ', '5일Δ', '20일Δ', 'z점수(60d)'].map(h => (
                  <th key={h} style={{ padding: '3px 6px', textAlign: h === '팩터' ? 'left' : 'right', fontWeight: 600 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {FACTOR_KEYS.map(k => {
                const s = snap[k]
                if (!s) return null
                const z = s.z_score_60d
                const zMark = z > 2 ? '▲▲' : z > 1 ? '▲' : z < -2 ? '▼▼' : z < -1 ? '▼' : ''
                return (
                  <tr key={k} style={{ borderBottom: '1px solid #0f172a' }}>
                    <td style={{ padding: '3px 6px', color: '#94a3b8' }}>
                      {FACTOR_LABELS[k]}
                    </td>
                    <td style={{ padding: '3px 6px', textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                      {s.current.toFixed(k === 'fx' ? 1 : 3)}{k === 'rate' ? '%' : ''}
                    </td>
                    {([s.change_1d, s.change_5d, s.change_20d] as number[]).map((v, i) => (
                      <td key={i} style={{
                        padding: '3px 6px', textAlign: 'right',
                        color: clr(v), fontVariantNumeric: 'tabular-nums',
                      }}>
                        {fmt(v, 3)}{FACTOR_UNITS[k]}
                      </td>
                    ))}
                    <td style={{
                      padding: '3px 6px', textAlign: 'right',
                      color: Math.abs(z) > 2 ? '#f59e0b' : Math.abs(z) > 1 ? '#94a3b8' : '#475569',
                      fontVariantNumeric: 'tabular-nums',
                    }}>
                      {fmt(z, 1)}{zMark}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
          <div style={{ fontSize: 10, color: '#475569', marginTop: 4 }}>
            z점수: 60일 평균 대비 현재 수준. |z|&gt;2 = 역사적 극단값
          </div>
        </div>
      )}

      {/* ── 종목별 매크로 영향 점수 순위 ──────────────────────── */}
      {stocks.length > 0 && (
        <div>
          <div style={{ fontSize: 11, fontWeight: 700, color: '#94a3b8', marginBottom: 6 }}>
            종목별 매크로 영향 점수 (현재 매크로 기준, 최근 5일)
          </div>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
            <thead>
              <tr style={{ color: '#475569', borderBottom: '1px solid #1e293b' }}>
                <th style={{ padding: '3px 6px', textAlign: 'left', fontWeight: 600 }}>#</th>
                <th style={{ padding: '3px 6px', textAlign: 'left', fontWeight: 600 }}>종목</th>
                <th style={{ padding: '3px 6px', textAlign: 'right', fontWeight: 600 }}>점수</th>
                <th style={{ padding: '3px 6px' }}>바</th>
                <th style={{ padding: '3px 6px', textAlign: 'right', fontWeight: 600 }}>5일기여</th>
                <th style={{ padding: '3px 6px', textAlign: 'right', fontWeight: 600 }}>20일기여</th>
                <th style={{ padding: '3px 6px', textAlign: 'right', fontWeight: 600 }}>R²</th>
                <th style={{ padding: '3px 6px', textAlign: 'left', fontWeight: 600 }}>유의팩터</th>
              </tr>
            </thead>
            <tbody>
              {stocks.map((s, i) => {
                const isExpanded = expanded === s.ticker
                const scoreColor = s.macro_score >= 20 ? '#22c55e'
                  : s.macro_score >= -20 ? '#f59e0b' : '#ef4444'
                return (
                  <>
                    <tr
                      key={s.ticker}
                      style={{
                        borderBottom: isExpanded ? 'none' : '1px solid #0f172a',
                        cursor: 'pointer',
                        background: isExpanded ? '#1a1d2e' : 'transparent',
                      }}
                      onClick={() => setExpanded(isExpanded ? null : s.ticker)}
                    >
                      <td style={{ padding: '4px 6px', color: '#475569' }}>{i + 1}</td>
                      <td style={{ padding: '4px 6px' }}>
                        <span style={{ fontWeight: 600 }}>{s.name}</span>
                        <span style={{ color: '#475569', marginLeft: 4, fontSize: 10 }}>
                          {s.ticker.replace(/\.KS$|\.KQ$/, '')}
                        </span>
                      </td>
                      <td style={{ padding: '4px 6px', textAlign: 'right', color: scoreColor, fontWeight: 700 }}>
                        {s.macro_score > 0 ? '+' : ''}{s.macro_score.toFixed(0)}
                      </td>
                      <td style={{ padding: '4px 6px' }}>
                        <ScoreBar score={s.macro_score} />
                      </td>
                      <td style={{ padding: '4px 6px', textAlign: 'right', color: clr(s.macro_score_5d), fontVariantNumeric: 'tabular-nums' }}>
                        {fmt(s.macro_score_5d, 2)}%
                      </td>
                      <td style={{ padding: '4px 6px', textAlign: 'right', color: clr(s.macro_score_20d), fontVariantNumeric: 'tabular-nums' }}>
                        {fmt(s.macro_score_20d, 2)}%
                      </td>
                      <td style={{ padding: '4px 6px', textAlign: 'right', color: '#64748b' }}>
                        {s.r_squared.toFixed(3)}
                      </td>
                      <td style={{ padding: '4px 6px', color: '#94a3b8', fontSize: 10 }}>
                        {s.significant_factors.join(', ') || '—'}
                      </td>
                    </tr>

                    {/* 펼쳐진 팩터 상세 */}
                    {isExpanded && (
                      <tr key={`${s.ticker}-detail`}>
                        <td colSpan={8} style={{
                          padding: '8px 12px 12px',
                          background: '#1a1d2e',
                          borderBottom: '1px solid #1e293b',
                        }}>
                          <div style={{ marginBottom: 6, fontSize: 11, color: '#64748b' }}>
                            관측수: {s.n_obs}일 &nbsp;|&nbsp; R²: {s.r_squared.toFixed(3)} &nbsp;|&nbsp;
                            잔차σ: {s.residual_std.toFixed(3)}%/일 &nbsp;|&nbsp;
                            알파α: {s.alpha >= 0 ? '+' : ''}{(s.alpha * 252).toFixed(1)}%/년
                          </div>
                          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 10 }}>
                            <thead>
                              <tr style={{ color: '#475569' }}>
                                {['팩터', '베타β', 't값', 'p값', '유의', '5일Δ', '5일기여'].map(h => (
                                  <th key={h} style={{ padding: '2px 6px', textAlign: h === '팩터' || h === '유의' ? 'left' : 'right', fontWeight: 600 }}>{h}</th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {FACTOR_KEYS.map(f => {
                                const beta   = s.betas[f] ?? 0
                                const tstat  = s.t_stats[f] ?? 0
                                const pval   = s.p_values[f] ?? 1
                                const contrib = s.factor_contribs_5d[f] ?? 0
                                const delta5  = snap[f]?.change_5d ?? 0
                                const sig = pval < 0.01 ? '***' : pval < 0.05 ? '** ' : pval < 0.1 ? '*  ' : '   '
                                return (
                                  <tr key={f} style={{ borderBottom: '1px solid #0f172a' }}>
                                    <td style={{ padding: '2px 6px', color: FACTOR_LABELS[f] ? '#94a3b8' : '#475569' }}>
                                      {FACTOR_LABELS[f] || f}
                                    </td>
                                    <td style={{ padding: '2px 6px', textAlign: 'right', color: clr(beta), fontVariantNumeric: 'tabular-nums' }}>
                                      {fmt(beta, 4)}
                                    </td>
                                    <td style={{ padding: '2px 6px', textAlign: 'right', color: '#64748b', fontVariantNumeric: 'tabular-nums' }}>
                                      {fmt(tstat, 2)}
                                    </td>
                                    <td style={{ padding: '2px 6px', textAlign: 'right', color: pval < 0.1 ? '#fbbf24' : '#475569', fontVariantNumeric: 'tabular-nums' }}>
                                      {pval.toFixed(4)}
                                    </td>
                                    <td style={{ padding: '2px 6px', color: '#fbbf24', fontFamily: 'monospace' }}>
                                      {sig}
                                    </td>
                                    <td style={{ padding: '2px 6px', textAlign: 'right', color: clr(delta5), fontVariantNumeric: 'tabular-nums' }}>
                                      {fmt(delta5, 3)}{FACTOR_UNITS[f]}
                                    </td>
                                    <td style={{ padding: '2px 6px', textAlign: 'right', color: clr(contrib), fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>
                                      {fmt(contrib, 3)}%
                                    </td>
                                  </tr>
                                )
                              })}
                            </tbody>
                          </table>
                          <div style={{ fontSize: 10, color: '#475569', marginTop: 6 }}>
                            * p&lt;0.1 &nbsp; ** p&lt;0.05 &nbsp; *** p&lt;0.01 &nbsp;|&nbsp;
                            5일기여 = β × 최근5일팩터변화 (예상 수익률 기여분)
                          </div>
                        </td>
                      </tr>
                    )}
                  </>
                )
              })}
            </tbody>
          </table>

          <div style={{ fontSize: 10, color: '#475569', marginTop: 8, lineHeight: 1.6 }}>
            <b>점수 해석:</b> 최근 5일 팩터 변화 × 종목 베타 합산 → tanh 정규화 (-100~+100).
            양수=현재 매크로가 해당 종목에 유리, 음수=불리.
            <br/>
            <b>R²:</b> 해당 종목 주가 변동성 중 매크로로 설명되는 비율.
            R²가 높을수록 매크로 민감주.
          </div>
        </div>
      )}
    </div>
  )
}
