import { useState, useEffect, useCallback, Fragment } from 'react'
import {
  FACTOR_KEYS, FACTOR_LABELS, FACTOR_UNITS, FACTOR_CARD_INTERPRET,
  getFactorState, getSensitivity, generateVerdict, generateBanner,
  type FactorSnap, type StockResult,
} from './Macro.utils'
import { tokens, pctTextColor, scoreColor } from '../tokens'

// ── 타입 (MacroData는 API 전용이라 여기 유지) ──────────────────

interface MacroData {
  snapshot: Record<string, FactorSnap>
  stocks: StockResult[]
  fetched_at: string
  cached: boolean
  stale?: boolean
  error?: string
}

// ── 유틸 ──────────────────────────────────────────────────────

const fmt = (v: number, d = 3) =>
  (v >= 0 ? '+' : '') + v.toFixed(d)

// ── ScoreBar ────────────────────────────────────────────────────

const ScoreBar = ({ score }: { score: number }) => {
  const w = Math.min(Math.abs(score), 100) / 100
  const isPos = score >= 0
  return (
    <div style={{
      width: 80, height: 8, background: tokens.bg.raised, borderRadius: 4,
      position: 'relative', overflow: 'hidden', flexShrink: 0,
    }}>
      <div style={{
        position: 'absolute',
        width: `${w * 50}%`,
        height: '100%',
        background: scoreColor(score),
        borderRadius: 2,
        left: isPos ? '50%' : `${50 - w * 50}%`,
      }} />
      <div style={{
        position: 'absolute', left: '50%', top: 0, bottom: 0,
        width: 1, background: tokens.bd.emphasis,
      }} />
    </div>
  )
}

// ── FactorCard ──────────────────────────────────────────────────

const FactorCard = ({ factorKey: k, snap }: { factorKey: string; snap: FactorSnap | undefined }) => {
  if (!snap) {
    return (
      <div className="macro-skeleton" style={{
        background: tokens.bg.raised, border: `1px solid ${tokens.bd.emphasis}`,
        borderRadius: 6, padding: '10px', height: 86,
      }} />
    )
  }

  const state = getFactorState(snap.z_score_60d)
  const dir = snap.change_5d > 0 ? '▲' : snap.change_5d < 0 ? '▼' : '→'
  const interpretText = snap.change_5d >= 0
    ? FACTOR_CARD_INTERPRET[k]?.up ?? ''
    : FACTOR_CARD_INTERPRET[k]?.down ?? ''

  return (
    <div style={{
      background: tokens.bg.raised,
      border: `1px solid ${state.borderColor}`,
      borderRadius: 6,
      padding: '8px 10px',
      cursor: 'default',
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
        <span style={{ fontSize: 11, fontWeight: 700, color: tokens.tx.secondary }}>
          {FACTOR_LABELS[k]}
        </span>
        <span style={{ fontSize: 9, color: state.borderColor }}>
          {state.text}
        </span>
      </div>
      <div style={{
        fontSize: 15, fontWeight: 700, fontVariantNumeric: 'tabular-nums',
        color: pctTextColor(snap.change_5d),
        marginBottom: 4,
      }}>
        {dir} {Math.abs(snap.change_5d).toFixed(k === 'rate' ? 2 : 1)}{FACTOR_UNITS[k]}
      </div>
      <div style={{ fontSize: 10, color: tokens.tx.muted, lineHeight: 1.4, marginBottom: 4 }}>
        {interpretText}
      </div>
      <div style={{ fontSize: 9, color: tokens.tx.separator, fontVariantNumeric: 'tabular-nums' }}>
        현재 {snap.current.toFixed(k === 'fx' ? 1 : 3)}{k === 'rate' ? '%' : ''}
        &nbsp;|&nbsp;
        1일 {fmt(snap.change_1d, k === 'rate' ? 3 : 2)}{FACTOR_UNITS[k]}
      </div>
    </div>
  )
}

// ── CSS ─────────────────────────────────────────────────────────

const GLOBAL_CSS = `
@keyframes macro-shimmer {
  0%   { opacity: 0.4; }
  50%  { opacity: 0.75; }
  100% { opacity: 0.4; }
}
.macro-skeleton { animation: macro-shimmer 1.5s ease-in-out infinite; }
.macro-factor-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 8px;
  margin-bottom: 16px;
}
@media (max-width: 768px) {
  .macro-factor-grid { grid-template-columns: 1fr; }
}
`

// ── 메인 컴포넌트 ───────────────────────────────────────────────

export default function Macro() {
  const [data, setData]         = useState<MacroData | null>(null)
  const [loading, setLoading]   = useState(false)
  const [error, setError]       = useState('')
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

  const snap   = data?.snapshot ?? {}
  const stocks = data?.stocks ?? []
  const banner = Object.keys(snap).length > 0 ? generateBanner(snap) : null

  return (
    <div style={{ padding: 12, overflowY: 'auto', height: '100%', boxSizing: 'border-box' }}>
      <style>{GLOBAL_CSS}</style>

      {/* ── 헤더 ──────────────────────────────────────────────── */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
        <span style={{ fontSize: 13, fontWeight: 700 }}>
          📊 매크로 팩터 영향 분석
        </span>
        <span style={{ fontSize: 11, color: tokens.tx.muted, display: 'flex', alignItems: 'center', gap: 8 }}>
          {data && `갱신: ${data.fetched_at}${data.cached ? ' (캐시)' : ''}`}
          {data?.stale && <span style={{ color: tokens.chart.score.neutral }}>⚠ 구데이터</span>}
          <button
            onClick={() => load(true)}
            disabled={loading}
            style={{
              padding: '4px 10px', fontSize: 12, background: tokens.bg.raised,
              border: `1px solid ${tokens.bd.emphasis}`, color: tokens.tx.secondary,
              cursor: 'pointer', borderRadius: 4,
            }}
          >
            {loading ? '분석 중…' : '↻'}
          </button>
        </span>
      </div>

      {error && (
        <div style={{ color: tokens.semantic.up, fontSize: 11, marginBottom: 8 }}>
          {error}
        </div>
      )}

      {loading && !data && (
        <div style={{ color: tokens.tx.muted, fontSize: 12, textAlign: 'center', marginTop: 40 }}>
          <div>yfinance 데이터 다운로드 중…</div>
          <div style={{ marginTop: 4 }}>최초 로딩 30~60초 소요</div>
        </div>
      )}

      {/* ── 상단 배너 ─────────────────────────────────────────── */}
      {banner && (
        <div style={{
          background: tokens.bg.panel,
          border: `1px solid ${tokens.chart.significance}`,
          borderRadius: 4,
          padding: '6px 10px',
          marginBottom: 12,
          fontSize: 11,
          color: tokens.tx.secondary,
          lineHeight: 1.6,
          whiteSpace: 'pre-line',
        }}>
          {banner}
        </div>
      )}

      {/* ── 팩터 카드 그리드 ──────────────────────────────────── */}
      <div className="macro-factor-grid">
        {FACTOR_KEYS.map(k => (
          <FactorCard key={k} factorKey={k} snap={snap[k]} />
        ))}
      </div>

      {/* ── 종목별 매크로 영향 점수 순위 ──────────────────────── */}
      {stocks.length > 0 && (
        <div>
          <div style={{ fontSize: 11, fontWeight: 700, color: tokens.tx.secondary, marginBottom: 6 }}>
            종목별 매크로 영향 점수 (현재 매크로 기준, 최근 5일)
          </div>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
            <thead>
              <tr style={{ color: tokens.tx.subtle, borderBottom: `1px solid ${tokens.bd.default}` }}>
                <th style={{ padding: '3px 6px', textAlign: 'left',  fontWeight: 600 }}>#</th>
                <th style={{ padding: '3px 6px', textAlign: 'left',  fontWeight: 600 }}>종목</th>
                <th style={{ padding: '3px 6px', textAlign: 'right', fontWeight: 600 }}>점수</th>
                <th style={{ padding: '3px 6px' }}>바</th>
                <th style={{ padding: '3px 6px', textAlign: 'left',  fontWeight: 600 }}>판결</th>
                <th style={{ padding: '3px 6px', textAlign: 'right', fontWeight: 600 }}>민감도</th>
              </tr>
            </thead>
            <tbody>
              {stocks.map((s, i) => {
                const isExpanded  = expanded === s.ticker
                const verdict     = generateVerdict(s)
                const sensitivity = getSensitivity(s.r_squared)
                return (
                  <Fragment key={s.ticker}>
                    <tr
                      style={{
                        borderBottom: isExpanded ? 'none' : `1px solid ${tokens.bg.row}`,
                        cursor: 'pointer',
                        background: isExpanded ? tokens.bg.panel : 'transparent',
                      }}
                      onClick={() => setExpanded(isExpanded ? null : s.ticker)}
                      role="button"
                      tabIndex={0}
                      onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') setExpanded(isExpanded ? null : s.ticker) }}
                    >
                      <td style={{ padding: '4px 6px', color: tokens.tx.subtle }}>{i + 1}</td>
                      <td style={{ padding: '4px 6px' }}>
                        <span style={{ fontWeight: 600 }}>{s.name}</span>
                        <span style={{ color: tokens.tx.subtle, marginLeft: 4, fontSize: 10 }}>
                          {s.ticker.replace(/\.KS$|\.KQ$/, '')}
                        </span>
                      </td>
                      <td style={{ padding: '4px 6px', textAlign: 'right', color: scoreColor(s.macro_score), fontWeight: 700, fontVariantNumeric: 'tabular-nums' }}>
                        {s.macro_score > 0 ? '+' : ''}{s.macro_score.toFixed(0)}
                      </td>
                      <td style={{ padding: '4px 6px' }}>
                        <ScoreBar score={s.macro_score} />
                      </td>
                      <td style={{ padding: '4px 6px', color: tokens.tx.secondary, fontSize: 10 }}>
                        {verdict}
                      </td>
                      <td style={{ padding: '4px 6px', textAlign: 'right' }}>
                        <span style={{
                          fontSize: 9, color: sensitivity.color,
                          border: `1px solid ${sensitivity.color}`,
                          borderRadius: 3, padding: '1px 4px',
                        }}>
                          {sensitivity.label}
                        </span>
                      </td>
                    </tr>

                    {/* 펼쳐진 팩터 상세 */}
                    {isExpanded && (
                      <tr>
                        <td colSpan={6} style={{
                          padding: '8px 12px 12px',
                          background: tokens.bg.panel,
                          borderBottom: `1px solid ${tokens.bd.default}`,
                        }}>
                          <div style={{ marginBottom: 6, fontSize: 11, color: tokens.tx.muted }}>
                            관측수: {s.n_obs}일 &nbsp;|&nbsp; R²: {s.r_squared.toFixed(3)} &nbsp;|&nbsp;
                            잔차σ: {s.residual_std.toFixed(3)}%/일 &nbsp;|&nbsp;
                            알파α: {s.alpha >= 0 ? '+' : ''}{(s.alpha * 252).toFixed(1)}%/년 &nbsp;|&nbsp;
                            5일기여: <span style={{ color: pctTextColor(s.macro_score_5d), fontVariantNumeric: 'tabular-nums' }}>{fmt(s.macro_score_5d, 2)}%</span> &nbsp;|&nbsp;
                            20일기여: <span style={{ color: pctTextColor(s.macro_score_20d), fontVariantNumeric: 'tabular-nums' }}>{fmt(s.macro_score_20d, 2)}%</span>
                          </div>
                          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 10 }}>
                            <thead>
                              <tr style={{ color: tokens.tx.subtle }}>
                                {['팩터', '베타β', 't값', 'p값', '유의', '5일Δ', '5일기여'].map(h => (
                                  <th key={h} style={{ padding: '2px 6px', textAlign: h === '팩터' || h === '유의' ? 'left' : 'right', fontWeight: 600 }}>{h}</th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {FACTOR_KEYS.map(f => {
                                const beta    = s.betas[f] ?? 0
                                const tstat   = s.t_stats[f] ?? 0
                                const pval    = s.p_values[f] ?? 1
                                const contrib = s.factor_contribs_5d[f] ?? 0
                                const delta5  = snap[f]?.change_5d ?? 0
                                const sig = pval < 0.01 ? '***' : pval < 0.05 ? '** ' : pval < 0.1 ? '*  ' : '   '
                                return (
                                  <tr key={f} style={{ borderBottom: `1px solid ${tokens.bg.row}` }}>
                                    <td style={{ padding: '2px 6px', color: tokens.tx.secondary }}>
                                      {FACTOR_LABELS[f] || f}
                                    </td>
                                    <td style={{ padding: '2px 6px', textAlign: 'right', color: pctTextColor(beta), fontVariantNumeric: 'tabular-nums' }}>
                                      {fmt(beta, 4)}
                                    </td>
                                    <td style={{ padding: '2px 6px', textAlign: 'right', color: tokens.tx.muted, fontVariantNumeric: 'tabular-nums' }}>
                                      {fmt(tstat, 2)}
                                    </td>
                                    <td style={{ padding: '2px 6px', textAlign: 'right', color: pval < 0.1 ? tokens.chart.significance : tokens.tx.subtle, fontVariantNumeric: 'tabular-nums' }}>
                                      {pval.toFixed(4)}
                                    </td>
                                    <td style={{ padding: '2px 6px', color: tokens.chart.significance, fontFamily: 'monospace' }}>
                                      {sig}
                                    </td>
                                    <td style={{ padding: '2px 6px', textAlign: 'right', color: pctTextColor(delta5), fontVariantNumeric: 'tabular-nums' }}>
                                      {fmt(delta5, 3)}{FACTOR_UNITS[f]}
                                    </td>
                                    <td style={{ padding: '2px 6px', textAlign: 'right', color: pctTextColor(contrib), fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>
                                      {fmt(contrib, 3)}%
                                    </td>
                                  </tr>
                                )
                              })}
                            </tbody>
                          </table>
                          <div style={{ fontSize: 10, color: tokens.tx.subtle, marginTop: 6 }}>
                            * p&lt;0.1 &nbsp; ** p&lt;0.05 &nbsp; *** p&lt;0.01 &nbsp;|&nbsp;
                            5일기여 = β × 최근5일팩터변화
                          </div>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                )
              })}
            </tbody>
          </table>

          <div style={{ fontSize: 10, color: tokens.tx.subtle, marginTop: 8, lineHeight: 1.6 }}>
            <b>점수 해석:</b> 최근 5일 팩터 변화 × 종목 베타 합산 → tanh 정규화 (-100~+100).
            양수=현재 매크로가 해당 종목에 유리, 음수=불리.
          </div>
        </div>
      )}
    </div>
  )
}
