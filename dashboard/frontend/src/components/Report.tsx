import { useState, useEffect } from 'react'

// ── 타입 ────────────────────────────────────────────────────
type StageRow = { ticker: string; name: string; sector: string | null; s1_high: number | null; s1_volume: number | null; peakout_flag: boolean }

interface StageData {
  date: string
  summary: { stage: number; count: number; peakout: number }[]
  stage1: StageRow[]
  stage2: StageRow[]
  stage3: StageRow[]
}

interface ScreenerData {
  week: string
  total: number
  enhanced: number
  gapjum: number
  items: { ticker: string; name: string; close: number | null; ma_20w: number | null; cloud_top: number | null; is_enhanced: boolean; has_gapjum: boolean; sector: string | null }[]
}

interface PaperData {
  model_summary: Record<string, Record<string, { count: number; avg_return: number | null }>>
  open: { ticker: string; name: string; model: string; signal_date: string; entry_actual: number | null; current_price: number | null; unrealized_pct: number | null; status: string; qty: number | null }[]
  closed: { ticker: string; name: string; model: string; signal_date: string; exit_date: string | null; exit_type: string | null; blended_return: number | null; tp1_date: string | null }[]
}

// ── 유틸 ────────────────────────────────────────────────────
const fmt = (v: number | null | undefined, digits = 0) =>
  v == null ? '—' : v.toLocaleString('ko-KR', { maximumFractionDigits: digits })

const pctColor = (v: number | null | undefined) => {
  if (v == null) return '#64748b'
  return v > 0 ? '#4ade80' : v < 0 ? '#f87171' : '#94a3b8'
}

const MODEL_LABEL: Record<string, string> = {
  stage: 'Stage', kosdaq: 'KOSDAQ', cross: 'Cross', ichimoku: 'Ichimoku',
}

const EXIT_LABEL: Record<string, string> = {
  hard_stop: '손절', trail: '트레일', period_end: '기간종료', manual: '수동',
}

// ── 섹션 컨테이너 ────────────────────────────────────────────
function Section({ title, badge, children, defaultOpen = true }: {
  title: string; badge?: string; children: React.ReactNode; defaultOpen?: boolean
}) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div style={s.section}>
      <button style={s.sectionHdr} onClick={() => setOpen(o => !o)}>
        <span style={s.sectionTitle}>{title}</span>
        {badge && <span style={s.badge}>{badge}</span>}
        <span style={s.chevron}>{open ? '▲' : '▼'}</span>
      </button>
      {open && <div style={s.sectionBody}>{children}</div>}
    </div>
  )
}

// ── Stage 레포트 ─────────────────────────────────────────────
function StageReport({ data }: { data: StageData }) {
  const [activeStage, setActiveStage] = useState<1 | 2 | 3>(1)

  const summary1 = data.summary.find(x => x.stage === 1)
  const summary2 = data.summary.find(x => x.stage === 2)
  const summary3 = data.summary.find(x => x.stage === 3)
  const peakout = data.summary.reduce((acc, x) => acc + (x.peakout ?? 0), 0)

  const STAGE_META: { stage: 1 | 2 | 3; label: string; color: string; rows: StageRow[]; cnt: number }[] = [
    { stage: 1, label: 'Stage 1', color: '#60a5fa', rows: data.stage1, cnt: summary1?.count ?? 0 },
    { stage: 2, label: 'Stage 2', color: '#a78bfa', rows: data.stage2, cnt: summary2?.count ?? 0 },
    { stage: 3, label: 'Stage 3', color: '#f59e0b', rows: data.stage3, cnt: summary3?.count ?? 0 },
  ]

  const activeRows = STAGE_META.find(m => m.stage === activeStage)?.rows ?? []

  return (
    <>
      <div style={s.chips}>
        {STAGE_META.map(({ stage, label, color, cnt }) => (
          <button
            key={stage}
            style={{
              ...s.chip,
              cursor: 'pointer',
              border: activeStage === stage ? `1px solid ${color}` : '1px solid transparent',
              background: activeStage === stage ? '#1e3a5f' : '#1e293b',
            }}
            onClick={() => setActiveStage(stage)}
          >
            <span style={s.chipLabel}>{label}</span>
            <span style={{ ...s.chipVal, color }}>{cnt}</span>
          </button>
        ))}
        {peakout > 0 && (
          <div style={s.chip}>
            <span style={s.chipLabel}>피크아웃</span>
            <span style={{ ...s.chipVal, color: '#f87171' }}>{peakout}</span>
          </div>
        )}
      </div>

      {activeRows.length > 0 ? (
        <>
          <div style={s.tableLabel}>Stage {activeStage} 종목 (거래량 순)</div>
          <div style={s.tableWrap}>
            <table style={s.table}>
              <thead>
                <tr>
                  {['종목', '업종', '진입 고가', '거래량'].map(h => (
                    <th key={h} style={s.th}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {activeRows.map(r => (
                  <tr key={r.ticker} style={{ background: r.peakout_flag ? '#1a0f0f' : 'transparent' }}>
                    <td style={s.td}>
                      <div style={s.tickerName}>{r.name}</div>
                      <div style={s.tickerCode}>{r.ticker}{r.peakout_flag && ' ⚠️'}</div>
                    </td>
                    <td style={{ ...s.td, color: '#64748b', fontSize: 11 }}>{r.sector ?? '—'}</td>
                    <td style={{ ...s.td, textAlign: 'right' as const }}>{fmt(r.s1_high)}</td>
                    <td style={{ ...s.td, textAlign: 'right' as const, color: '#94a3b8' }}>
                      {r.s1_volume ? (r.s1_volume / 1000).toFixed(0) + 'K' : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      ) : (
        <div style={s.empty}>Stage {activeStage} 종목 없음</div>
      )}
    </>
  )
}

// ── 스크리너 레포트 ──────────────────────────────────────────
function ScreenerReport({ data }: { data: ScreenerData }) {
  const [filter, setFilter] = useState<'all' | 'enhanced' | 'gapjum'>('all')
  const filtered = data.items.filter(r =>
    filter === 'all' ? true : filter === 'enhanced' ? r.is_enhanced : r.has_gapjum
  )

  return (
    <>
      <div style={s.chips}>
        <div style={s.chip}>
          <span style={s.chipLabel}>통과</span>
          <span style={{ ...s.chipVal, color: '#60a5fa' }}>{data.total}</span>
        </div>
        <div style={s.chip}>
          <span style={s.chipLabel}>강화</span>
          <span style={{ ...s.chipVal, color: '#a78bfa' }}>{data.enhanced}</span>
        </div>
        <div style={s.chip}>
          <span style={s.chipLabel}>갭점프</span>
          <span style={{ ...s.chipVal, color: '#34d399' }}>{data.gapjum}</span>
        </div>
      </div>

      <div style={s.filterRow}>
        {(['all', 'enhanced', 'gapjum'] as const).map(f => (
          <button key={f} style={{ ...s.filterBtn, ...(filter === f ? s.filterBtnActive : {}) }}
            onClick={() => setFilter(f)}>
            {f === 'all' ? '전체' : f === 'enhanced' ? '강화만' : '갭점프만'}
          </button>
        ))}
      </div>

      <div style={s.tableWrap}>
        <table style={s.table}>
          <thead>
            <tr>
              {['종목', '업종', '종가', 'MA20W', '구름상단', ''].map(h => (
                <th key={h} style={s.th}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filtered.map(r => (
              <tr key={r.ticker}>
                <td style={s.td}>
                  <div style={s.tickerName}>{r.name}</div>
                  <div style={s.tickerCode}>{r.ticker}</div>
                </td>
                <td style={{ ...s.td, color: '#64748b', fontSize: 11 }}>{r.sector ?? '—'}</td>
                <td style={{ ...s.td, textAlign: 'right' as const }}>{fmt(r.close)}</td>
                <td style={{ ...s.td, textAlign: 'right' as const, color: '#64748b' }}>{fmt(r.ma_20w)}</td>
                <td style={{ ...s.td, textAlign: 'right' as const, color: '#64748b' }}>{fmt(r.cloud_top)}</td>
                <td style={{ ...s.td, textAlign: 'center' as const, fontSize: 11 }}>
                  {r.is_enhanced && <span style={{ color: '#a78bfa' }}>강화</span>}
                  {r.has_gapjum && <span style={{ color: '#34d399', marginLeft: 4 }}>갭</span>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  )
}

// ── 모의투자 레포트 ──────────────────────────────────────────
function PaperReport({ data }: { data: PaperData }) {
  const models = Object.keys(data.model_summary)

  return (
    <>
      {/* 모델별 요약 */}
      <div style={s.modelGrid}>
        {models.map(m => {
          const info = data.model_summary[m]
          const open   = info['open']   ?? { count: 0, avg_return: null }
          const pending = info['pending'] ?? { count: 0, avg_return: null }
          const closed  = info['closed']  ?? { count: 0, avg_return: null }
          return (
            <div key={m} style={s.modelCard}>
              <div style={s.modelName}>{MODEL_LABEL[m] ?? m}</div>
              <div style={s.modelRow}><span style={s.modelStatLabel}>오픈</span><span>{open.count}</span></div>
              <div style={s.modelRow}><span style={s.modelStatLabel}>대기</span><span>{pending.count}</span></div>
              {closed.count > 0 && (
                <div style={s.modelRow}>
                  <span style={s.modelStatLabel}>청산</span>
                  <span>{closed.count}건</span>
                </div>
              )}
              {closed.avg_return != null && (
                <div style={{ ...s.modelRow, marginTop: 4 }}>
                  <span style={s.modelStatLabel}>평균수익</span>
                  <span style={{ color: pctColor(closed.avg_return), fontWeight: 700 }}>
                    {closed.avg_return > 0 ? '+' : ''}{closed.avg_return.toFixed(1)}%
                  </span>
                </div>
              )}
            </div>
          )
        })}
      </div>

      {/* 오픈/대기 포지션 */}
      {data.open.length > 0 && (
        <>
          <div style={s.tableLabel}>오픈·대기 포지션</div>
          <div style={s.tableWrap}>
            <table style={s.table}>
              <thead>
                <tr>
                  {['종목', '모델', '진입가', '현재가', '수익률', '상태'].map(h => (
                    <th key={h} style={s.th}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.open.map(r => (
                  <tr key={r.ticker + r.model + r.signal_date}>
                    <td style={s.td}>
                      <div style={s.tickerName}>{r.name}</div>
                      <div style={s.tickerCode}>{r.signal_date}</div>
                    </td>
                    <td style={{ ...s.td, color: '#94a3b8' }}>{MODEL_LABEL[r.model] ?? r.model}</td>
                    <td style={{ ...s.td, textAlign: 'right' as const }}>{fmt(r.entry_actual)}</td>
                    <td style={{ ...s.td, textAlign: 'right' as const }}>{fmt(r.current_price)}</td>
                    <td style={{ ...s.td, textAlign: 'right' as const, color: pctColor(r.unrealized_pct), fontWeight: 600 }}>
                      {r.unrealized_pct != null ? `${r.unrealized_pct > 0 ? '+' : ''}${r.unrealized_pct.toFixed(1)}%` : '—'}
                    </td>
                    <td style={{ ...s.td, color: r.status === 'open' ? '#4ade80' : '#94a3b8', fontSize: 11 }}>
                      {r.status === 'open' ? '보유' : '대기'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      {/* 최근 청산 이력 */}
      {data.closed.length > 0 && (
        <>
          <div style={s.tableLabel}>최근 청산 이력</div>
          <div style={s.tableWrap}>
            <table style={s.table}>
              <thead>
                <tr>
                  {['종목', '모델', '청산일', '청산유형', 'blended수익'].map(h => (
                    <th key={h} style={s.th}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.closed.map((r, i) => (
                  <tr key={i}>
                    <td style={s.td}>
                      <div style={s.tickerName}>{r.name}</div>
                      <div style={s.tickerCode}>{r.signal_date}</div>
                    </td>
                    <td style={{ ...s.td, color: '#94a3b8' }}>{MODEL_LABEL[r.model] ?? r.model}</td>
                    <td style={{ ...s.td, color: '#64748b' }}>{r.exit_date ?? '—'}</td>
                    <td style={{ ...s.td, fontSize: 11, color: '#94a3b8' }}>
                      {r.exit_type ? (EXIT_LABEL[r.exit_type] ?? r.exit_type) : '—'}
                      {r.tp1_date && <span style={{ color: '#a78bfa', marginLeft: 4 }}>TP1</span>}
                    </td>
                    <td style={{ ...s.td, textAlign: 'right' as const, color: pctColor(r.blended_return ? r.blended_return * 100 : null), fontWeight: 600 }}>
                      {r.blended_return != null
                        ? `${r.blended_return > 0 ? '+' : ''}${(r.blended_return * 100).toFixed(1)}%`
                        : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </>
  )
}

// ── 메인 컴포넌트 ────────────────────────────────────────────
export default function Report() {
  const [stage, setStage] = useState<StageData | null>(null)
  const [screener, setScreener] = useState<ScreenerData | null>(null)
  const [paper, setPaper] = useState<PaperData | null>(null)
  const [loading, setLoading] = useState(false)
  const [lastFetched, setLastFetched] = useState<Date | null>(null)

  const load = async () => {
    setLoading(true)
    try {
      const [s, sc, p] = await Promise.all([
        fetch('/api/report/stage').then(r => r.json()),
        fetch('/api/report/screener').then(r => r.json()),
        fetch('/api/report/paper').then(r => r.json()),
      ])
      setStage(s.data)
      setScreener(sc.data)
      setPaper(p.data)
      setLastFetched(new Date())
    } catch {
      // 개별 실패는 null 유지
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  return (
    <div style={s.wrap}>
      <div style={s.hdr}>
        <span style={s.hdrTitle}>레포트</span>
        {lastFetched && (
          <span style={s.hdrTime}>{lastFetched.toLocaleTimeString('ko-KR')} 기준</span>
        )}
        <button style={s.refreshBtn} onClick={load} disabled={loading}>
          {loading ? '로딩…' : '새로고침'}
        </button>
      </div>

      <Section title="Stage 분류" badge={stage?.date ?? undefined}>
        {stage
          ? <StageReport data={stage} />
          : <div style={s.empty}>데이터 없음</div>}
      </Section>

      <Section title="차트 스크리닝" badge={screener?.week ?? undefined}>
        {screener
          ? <ScreenerReport data={screener} />
          : <div style={s.empty}>데이터 없음</div>}
      </Section>

      <Section title="모의투자" defaultOpen={false}>
        {paper
          ? <PaperReport data={paper} />
          : <div style={s.empty}>데이터 없음</div>}
      </Section>
    </div>
  )
}

// ── 스타일 ───────────────────────────────────────────────────
const s: Record<string, React.CSSProperties> = {
  wrap: { height: '100%', overflowY: 'auto', fontSize: 12, color: '#e2e8f0', boxSizing: 'border-box' },

  hdr: { display: 'flex', alignItems: 'center', gap: 8, padding: '10px 14px', borderBottom: '1px solid #1e293b', flexShrink: 0 },
  hdrTitle: { fontWeight: 700, fontSize: 13, flex: 1 },
  hdrTime: { color: '#475569', fontSize: 11 },
  refreshBtn: { background: '#1e293b', color: '#94a3b8', border: '1px solid #334155', borderRadius: 5, padding: '4px 10px', cursor: 'pointer', fontSize: 11 },

  section: { borderBottom: '1px solid #1e293b' },
  sectionHdr: { width: '100%', display: 'flex', alignItems: 'center', gap: 8, padding: '9px 14px', background: 'none', border: 'none', color: '#94a3b8', cursor: 'pointer', textAlign: 'left' as const },
  sectionTitle: { fontWeight: 700, fontSize: 12, flex: 1 },
  badge: { background: '#1e293b', color: '#64748b', fontSize: 10, padding: '2px 7px', borderRadius: 10 },
  chevron: { fontSize: 10, color: '#475569' },
  sectionBody: { padding: '0 14px 12px' },

  chips: { display: 'flex', gap: 8, marginBottom: 10, flexWrap: 'wrap' as const },
  chip: { background: '#1e293b', borderRadius: 6, padding: '6px 12px', display: 'flex', flexDirection: 'column' as const, alignItems: 'center', gap: 2, border: '1px solid transparent', outline: 'none' },
  chipLabel: { fontSize: 10, color: '#64748b' },
  chipVal: { fontSize: 18, fontWeight: 700 },

  filterRow: { display: 'flex', gap: 6, marginBottom: 8 },
  filterBtn: { background: '#1e293b', color: '#64748b', border: '1px solid #334155', borderRadius: 4, padding: '3px 9px', cursor: 'pointer', fontSize: 11 },
  filterBtnActive: { background: '#1e3a5f', color: '#93c5fd', borderColor: '#1d4ed8' },

  tableLabel: { fontSize: 11, color: '#475569', marginBottom: 5, marginTop: 8 },
  tableWrap: { overflowX: 'auto', maxHeight: 280, overflowY: 'auto' as const },
  table: { width: '100%', borderCollapse: 'collapse' as const, fontSize: 11 },
  th: { position: 'sticky' as const, top: 0, background: '#0f172a', color: '#475569', padding: '5px 8px', textAlign: 'left' as const, fontWeight: 600, whiteSpace: 'nowrap' as const },
  td: { padding: '5px 8px', borderBottom: '1px solid #0f172a', verticalAlign: 'middle' as const },
  tickerName: { color: '#cbd5e1', fontWeight: 600 },
  tickerCode: { color: '#475569', fontSize: 10 },

  modelGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(110px, 1fr))', gap: 8, marginBottom: 10 },
  modelCard: { background: '#1e293b', borderRadius: 8, padding: '10px 12px' },
  modelName: { fontWeight: 700, color: '#93c5fd', marginBottom: 6, fontSize: 12 },
  modelRow: { display: 'flex', justifyContent: 'space-between', fontSize: 11, color: '#94a3b8', marginBottom: 2 },
  modelStatLabel: { color: '#475569' },

  empty: { color: '#475569', textAlign: 'center' as const, padding: '24px 0', fontSize: 12 },
}
