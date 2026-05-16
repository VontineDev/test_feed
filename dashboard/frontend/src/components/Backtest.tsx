import { useState, useEffect } from 'react'

interface VerdictRow {
  verdict: string
  checkpoint: string
  count: number
  hit_rate: number | null
  avg_return: number
  median_return: number
  avg_score: number
}

interface ScoreBucket {
  count: number
  avg_return: number
}

interface BacktestData {
  rows: number
  by_verdict_checkpoint: VerdictRow[]
  score_correlation: Record<string, ScoreBucket>
  market_baseline: Record<string, number> | null
}

const VERDICTS = ['CONFIRM', 'CAUTION', 'FILTER', 'NEUTRAL'] as const
const CHECKPOINTS = ['1h', '4h', '1d', '3d'] as const

const VERDICT_ICON: Record<string, string> = {
  CONFIRM: '✅', CAUTION: '⚠️', FILTER: '🚫', NEUTRAL: '➖',
}

function retColor(v: number): string {
  if (v > 0) return '#4ade80'
  if (v < 0) return '#f87171'
  return '#94a3b8'
}

function hitColor(v: number | null): string {
  if (v === null) return '#475569'
  if (v >= 60) return '#4ade80'
  if (v >= 45) return '#facc15'
  return '#f87171'
}

function fmt(v: number | null, suffix = '%'): string {
  if (v === null || v === undefined) return '—'
  const sign = v > 0 ? '+' : ''
  return `${sign}${v.toFixed(1)}${suffix}`
}

export default function Backtest() {
  const [data, setData] = useState<BacktestData | null>(null)
  const [message, setMessage] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [trackBusy, setTrackBusy] = useState(false)
  const [lastFetched, setLastFetched] = useState<Date | null>(null)

  const fetchResults = async () => {
    setLoading(true)
    try {
      const r = await fetch('/api/backtest')
      const j = await r.json()
      if (j.message) {
        setMessage(j.message)
        setData(null)
      } else {
        setData(j.data)
        setMessage(null)
      }
      setLastFetched(new Date())
    } catch {
      setMessage('백테스트 데이터 로드 실패')
    } finally {
      setLoading(false)
    }
  }

  const triggerTracking = async () => {
    setTrackBusy(true)
    try {
      const r = await fetch('/api/scheduler/trigger', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ job: 'backtest_track' }),
      })
      const j = await r.json()
      if (j.status === 'already_queued') {
        alert('이미 트래킹 실행 중입니다')
      } else {
        alert('트래킹 잡이 큐에 등록되었습니다.\n스케줄러가 30초 내로 실행합니다.')
      }
    } catch {
      alert('트리거 요청 실패')
    } finally {
      setTrackBusy(false)
    }
  }

  useEffect(() => { fetchResults() }, [])

  // verdict × checkpoint 셀 값 조회
  const getCell = (verdict: string, cp: string): VerdictRow | undefined =>
    data?.by_verdict_checkpoint.find(r => r.verdict === verdict && r.checkpoint === cp)

  return (
    <div style={s.wrap}>
      {/* 헤더 */}
      <div style={s.hdr}>
        <span style={s.title}>교차분석 백테스팅</span>
        <div style={s.btnRow}>
          <button style={s.btn} onClick={fetchResults} disabled={loading}>
            {loading ? '조회 중…' : '결과 새로고침'}
          </button>
          <button style={{ ...s.btn, ...s.trackBtn }} onClick={triggerTracking} disabled={trackBusy}>
            {trackBusy ? '요청 중…' : '가격 트래킹 실행'}
          </button>
        </div>
      </div>

      {lastFetched && (
        <div style={s.meta}>
          마지막 조회: {lastFetched.toLocaleTimeString('ko-KR')}
          {data && <span style={s.rowCount}> · 총 {data.rows}건</span>}
        </div>
      )}

      {/* 시장 기준선 */}
      {data?.market_baseline && (
        <div style={s.baseline}>
          <span style={s.baselineLabel}>랜덤 기준선</span>
          {data.market_baseline.BUY !== undefined && (
            <span style={s.baselineChip}>
              BUY <span style={{ color: hitColor(data.market_baseline.BUY) }}>
                {data.market_baseline.BUY.toFixed(1)}%
              </span>
            </span>
          )}
          {data.market_baseline.SELL !== undefined && (
            <span style={s.baselineChip}>
              SELL <span style={{ color: hitColor(data.market_baseline.SELL) }}>
                {data.market_baseline.SELL.toFixed(1)}%
              </span>
            </span>
          )}
        </div>
      )}

      {/* 메시지 (데이터 없음) */}
      {message && <div style={s.empty}>{message}</div>}

      {/* 판정×체크포인트 테이블 */}
      {data && (
        <>
          <div style={s.sectionTitle}>판정별 적중률 / 평균수익</div>
          <div style={s.tableWrap}>
            <table style={s.table}>
              <thead>
                <tr>
                  <th style={s.th}>판정</th>
                  {CHECKPOINTS.map(cp => (
                    <th key={cp} style={s.th}>{cp}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {VERDICTS.map(verdict => (
                  <tr key={verdict}>
                    <td style={s.tdLabel}>
                      {VERDICT_ICON[verdict]} {verdict}
                    </td>
                    {CHECKPOINTS.map(cp => {
                      const cell = getCell(verdict, cp)
                      if (!cell) return <td key={cp} style={s.tdEmpty}>—</td>
                      return (
                        <td key={cp} style={s.td}>
                          {cell.hit_rate !== null && (
                            <div style={{ color: hitColor(cell.hit_rate), fontWeight: 700, fontSize: 13 }}>
                              {cell.hit_rate.toFixed(1)}%
                            </div>
                          )}
                          <div style={{ color: retColor(cell.avg_return), fontSize: 11 }}>
                            avg {fmt(cell.avg_return)}
                          </div>
                          <div style={s.cellCount}>{cell.count}건</div>
                        </td>
                      )
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* 점수 구간별 수익률 */}
          {Object.keys(data.score_correlation).length > 0 && (
            <>
              <div style={s.sectionTitle}>점수 구간별 평균 수익률</div>
              <div style={s.scoreWrap}>
                {(['0-3', '4-6', '7-10'] as const).map(bucket => {
                  const b = data.score_correlation[bucket]
                  if (!b) return null
                  return (
                    <div key={bucket} style={s.scoreCard}>
                      <div style={s.scoreLabel}>점수 {bucket}</div>
                      <div style={{ color: retColor(b.avg_return), fontSize: 18, fontWeight: 700 }}>
                        {fmt(b.avg_return)}
                      </div>
                      <div style={s.scoreCount}>{b.count}건</div>
                    </div>
                  )
                })}
              </div>
            </>
          )}
        </>
      )}
    </div>
  )
}

const s: Record<string, React.CSSProperties> = {
  wrap: { padding: '12px 16px', color: '#e2e8f0', fontSize: 13, height: '100%', overflowY: 'auto', boxSizing: 'border-box' },
  hdr: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 },
  title: { fontWeight: 700, fontSize: 15, color: '#f1f5f9' },
  btnRow: { display: 'flex', gap: 6 },
  btn: {
    background: '#1e3a5f', color: '#93c5fd', border: '1px solid #1d4ed8',
    borderRadius: 6, padding: '5px 10px', cursor: 'pointer', fontSize: 11, fontWeight: 600,
  },
  trackBtn: { background: '#1a2e1a', color: '#86efac', border: '1px solid #15803d' },
  meta: { fontSize: 11, color: '#475569', marginBottom: 10 },
  rowCount: { color: '#64748b' },
  baseline: { display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12, padding: '6px 10px', background: '#1e293b', borderRadius: 6 },
  baselineLabel: { fontSize: 11, color: '#64748b' },
  baselineChip: { fontSize: 12, color: '#94a3b8', background: '#0f172a', padding: '2px 8px', borderRadius: 4 },
  empty: { color: '#64748b', textAlign: 'center', padding: '40px 0', fontSize: 13 },
  sectionTitle: { fontSize: 11, color: '#64748b', marginBottom: 6, marginTop: 14, textTransform: 'uppercase', letterSpacing: 0.5 },
  tableWrap: { overflowX: 'auto' },
  table: { width: '100%', borderCollapse: 'collapse' as const, fontSize: 12 },
  th: { background: '#1e293b', color: '#64748b', padding: '6px 8px', textAlign: 'center' as const, fontWeight: 600, fontSize: 11 },
  tdLabel: { padding: '8px 8px', color: '#cbd5e1', fontWeight: 600, whiteSpace: 'nowrap' as const },
  td: { padding: '6px 8px', textAlign: 'center' as const, borderBottom: '1px solid #1e293b' },
  tdEmpty: { padding: '6px 8px', textAlign: 'center' as const, color: '#334155', borderBottom: '1px solid #1e293b' },
  cellCount: { color: '#475569', fontSize: 10 },
  scoreWrap: { display: 'flex', gap: 8 },
  scoreCard: {
    flex: 1, background: '#1e293b', borderRadius: 8, padding: '12px',
    textAlign: 'center' as const,
  },
  scoreLabel: { fontSize: 11, color: '#64748b', marginBottom: 4 },
  scoreCount: { fontSize: 11, color: '#475569', marginTop: 4 },
}
