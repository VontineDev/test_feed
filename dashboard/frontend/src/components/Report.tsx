import { useState, useEffect, useCallback, useMemo } from 'react'
import { tokens } from '../tokens'
import DateRangeBar, { DatePreset, computeRange } from './DateRangeBar'
import HistoryStageView from './HistoryStageView'
import HistoryScreenerView from './HistoryScreenerView'

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

// ── 유틸 ────────────────────────────────────────────────────
const fmt = (v: number | null | undefined, digits = 0) =>
  v == null ? '—' : v.toLocaleString('ko-KR', { maximumFractionDigits: digits })

// ── 툴팁 ─────────────────────────────────────────────────────
function InfoTip({ text }: { text: string }) {
  const [show, setShow] = useState(false)
  return (
    <span style={{ position: 'relative', display: 'inline-flex', alignItems: 'center' }}>
      <span
        style={{ cursor: 'help', color: tokens.tx.subtle, fontSize: 11, marginLeft: 4, userSelect: 'none' }}
        onMouseEnter={() => setShow(true)}
        onMouseLeave={() => setShow(false)}
      >ⓘ</span>
      {show && (
        <span style={{
          position: 'absolute',
          left: '110%',
          top: '50%',
          transform: 'translateY(-50%)',
          background: tokens.bg.raised,
          border: `1px solid ${tokens.bd.emphasis}`,
          color: tokens.tx.secondary,
          fontSize: 11,
          lineHeight: 1.5,
          padding: '6px 10px',
          borderRadius: 4,
          whiteSpace: 'normal',
          width: 240,
          zIndex: 100,
          pointerEvents: 'none',
        }}>
          {text}
        </span>
      )}
    </span>
  )
}

// ── 섹션 컨테이너 ────────────────────────────────────────────
function Section({ title, badge, tooltip, children, defaultOpen = true }: {
  title: string; badge?: string; tooltip?: string; children: React.ReactNode; defaultOpen?: boolean
}) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div style={s.section}>
      <button style={s.sectionHdr} onClick={() => setOpen(o => !o)}>
        <span style={{ ...s.sectionTitle, display: 'inline-flex', alignItems: 'center' }}>
          {title}
          {tooltip && <InfoTip text={tooltip} />}
        </span>
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
    { stage: 1, label: 'Stage 1', color: tokens.stage[1], rows: data.stage1, cnt: summary1?.count ?? 0 },
    { stage: 2, label: 'Stage 2', color: tokens.stage[2], rows: data.stage2, cnt: summary2?.count ?? 0 },
    { stage: 3, label: 'Stage 3', color: tokens.stage[3], rows: data.stage3, cnt: summary3?.count ?? 0 },
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
              background: activeStage === stage ? tokens.bg.active : tokens.bg.raised,
            }}
            onClick={() => setActiveStage(stage)}
          >
            <span style={s.chipLabel}>{label}</span>
            <span style={{ ...s.chipVal, color }}>{cnt}</span>
          </button>
        ))}
        {peakout > 0 && (
          <div style={s.chip}>
            <span style={s.chipLabel}>
              고점 이탈
              <span onClick={e => e.stopPropagation()}>
                <InfoTip text="외국인·기관 동시 순매도 또는 윗꼬리+거래량 급증 감지. 단기 고점에서 매물 압력이 집중된 신호." />
              </span>
            </span>
            <span style={{ ...s.chipVal, color: tokens.semantic.up }}>{peakout}</span>
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
                  {([
                    '종목', '업종',
                    { label: 'S1 고가', tip: 'Stage 1으로 분류된 날의 당일 고가. 진입 시점 가격 압박을 가늠하는 기준점.' },
                    '거래량',
                  ] as const).map(h => {
                    const label = typeof h === 'string' ? h : h.label
                    const tip   = typeof h === 'object' ? h.tip : undefined
                    return (
                      <th key={label} style={s.th}>
                        {label}{tip && <InfoTip text={tip} />}
                      </th>
                    )
                  })}
                </tr>
              </thead>
              <tbody>
                {activeRows.map(r => (
                  <tr key={r.ticker} style={{ background: r.peakout_flag ? '#1a0f0f' : 'transparent' }}>
                    <td style={s.td}>
                      <div style={s.tickerName}>{r.name}</div>
                      <div style={s.tickerCode}>{r.ticker}{r.peakout_flag && ' ⚠️'}</div>
                    </td>
                    <td style={{ ...s.td, color: tokens.tx.muted, fontSize: 11 }}>{r.sector ?? '—'}</td>
                    <td style={{ ...s.td, textAlign: 'right' as const }}>{fmt(r.s1_high)}</td>
                    <td style={{ ...s.td, textAlign: 'right' as const, color: tokens.tx.secondary }}>
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
        {([
          { f: 'all',      label: '통과',   val: data.total,    color: tokens.accent.blueSoft,       tooltip: '주봉 일목균형표 기준선 위 + 20주 이동평균 위 조건을 동시에 충족한 종목. 기본 강세 구조 확인.' },
          { f: 'enhanced', label: '강화',   val: data.enhanced, color: tokens.stage[2],              tooltip: '통과 조건 + 전환선·기준선 정배열 + 구름대 위 종가 등 추가 조건 충족. 더 강한 강세 신호.' },
          { f: 'gapjum',   label: '갭점프', val: data.gapjum,   color: tokens.chart.cat.ichimoku,    tooltip: '전주 대비 갭업(시가 > 전주 종가) 발생 종목. 수급 집중 또는 강세 돌파 신호일 수 있음.' },
        ] as const).map(({ f, label, val, color, tooltip }) => (
          <button
            key={f}
            style={{
              ...s.chip,
              cursor: 'pointer',
              border: filter === f ? `1px solid ${color}` : '1px solid transparent',
              background: filter === f ? tokens.bg.active : tokens.bg.raised,
            }}
            onClick={() => setFilter(f)}
          >
            <span style={{ ...s.chipLabel, display: 'inline-flex', alignItems: 'center' }}>
              {label}
              <span onClick={e => e.stopPropagation()}><InfoTip text={tooltip} /></span>
            </span>
            <span style={{ ...s.chipVal, color }}>{val}</span>
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
                <td style={{ ...s.td, color: tokens.tx.muted, fontSize: 11 }}>{r.sector ?? '—'}</td>
                <td style={{ ...s.td, textAlign: 'right' as const }}>{fmt(r.close)}</td>
                <td style={{ ...s.td, textAlign: 'right' as const, color: tokens.tx.muted }}>{fmt(r.ma_20w)}</td>
                <td style={{ ...s.td, textAlign: 'right' as const, color: tokens.tx.muted }}>{fmt(r.cloud_top)}</td>
                <td style={{ ...s.td, textAlign: 'center' as const, fontSize: 11 }}>
                  {r.is_enhanced && <span style={{ color: tokens.stage[2] }}>강화</span>}
                  {r.has_gapjum && <span style={{ color: tokens.chart.cat.ichimoku, marginLeft: 4 }}>갭</span>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  )
}

// ── 이력 뷰용 타입 ──────────────────────────────────────────
interface HistoryStageData {
  start: string
  end: string
  stage_filter: number | null
  items: {
    ticker: string; name: string; appearance_count: number
    first_seen: string | null; last_seen: string | null
    any_peakout: boolean; stage_queried: number; latest_stage: number | null
  }[]
}

interface HistoryScreenerData {
  start: string; end: string
  items: {
    ticker: string; name: string; week_count: number
    first_week: string; last_week: string
    any_enhanced: boolean; any_gapjum: boolean
  }[]
}

// ── 메인 컴포넌트 ────────────────────────────────────────────
export default function Report() {
  const [preset, setPreset] = useState<DatePreset>('today')

  // 오늘 뷰 상태
  const [stage, setStage] = useState<StageData | null>(null)
  const [screener, setScreener] = useState<ScreenerData | null>(null)

  // 이력 뷰 상태
  const [histStage, setHistStage] = useState<HistoryStageData | null>(null)
  const [histScreener, setHistScreener] = useState<HistoryScreenerData | null>(null)

  const [loading, setLoading] = useState(false)
  const [lastFetched, setLastFetched] = useState<Date | null>(null)

  const range = useMemo(() => computeRange(preset), [preset])

  const load = useCallback(async () => {
    setLoading(true)
    try {
      if (preset === 'today') {
        const [sr, scr] = await Promise.all([
          fetch('/api/report/stage'),
          fetch('/api/report/screener'),
        ])
        if (!sr.ok || !scr.ok) throw new Error('fetch failed')
        const [s, sc] = await Promise.all([sr.json(), scr.json()])
        setStage(s.data)
        setScreener(sc.data)
        setHistStage(null)
        setHistScreener(null)
      } else {
        const { start, end } = range
        const [hsr, hscr] = await Promise.all([
          fetch(`/api/history/stage?start=${start}&end=${end}`),
          fetch(`/api/history/screener?start=${start}&end=${end}`),
        ])
        if (!hsr.ok || !hscr.ok) throw new Error('fetch failed')
        const [hs, hsc] = await Promise.all([hsr.json(), hscr.json()])
        setHistStage(hs.data)
        setHistScreener(hsc.data)
        setStage(null)
        setScreener(null)
      }
      setLastFetched(new Date())
    } catch {
      // 개별 실패는 null 유지
    } finally {
      setLoading(false)
    }
  }, [preset, range])

  useEffect(() => { load() }, [load])

  const stageBadge = preset === 'today' ? (stage?.date ?? undefined) : `${range.start}~${range.end}`
  const screenerBadge = preset === 'today' ? (screener?.week ?? undefined) : `${range.start}~${range.end}`

  return (
    <div style={s.wrap}>
      <div style={s.hdr}>
        <span style={s.hdrTitle}>종목 분석</span>
        {lastFetched && (
          <span style={s.hdrTime}>{lastFetched.toLocaleTimeString('ko-KR')} 기준</span>
        )}
        <button style={s.refreshBtn} onClick={load} disabled={loading}>
          {loading ? '로딩…' : '새로고침'}
        </button>
      </div>

      {/* 날짜 범위 선택 바 */}
      <DateRangeBar preset={preset} onChange={p => setPreset(p)} />

      <Section
        title="추세 단계"
        badge={stageBadge}
        tooltip="전 종목을 일봉 기준 3단계 추세로 분류합니다. Stage 1(상승 초기)이 매수 적기, Stage 2(고점권)는 조심, Stage 3(하락)은 관망."
      >
        {preset === 'today'
          ? (stage ? <StageReport data={stage} /> : <div style={s.empty}>데이터 없음</div>)
          : (histStage
              ? <HistoryStageView items={histStage.items} start={range.start} end={range.end} />
              : <div style={s.empty}>{loading ? '로딩…' : '데이터 없음'}</div>)
        }
      </Section>

      <Section
        title="강세 후보 발굴"
        badge={screenerBadge}
        tooltip="주봉 일목균형표 + 20주 이동평균 조건을 통과한 종목입니다. 기술적으로 강세 신호가 켜진 후보를 매주 스캔합니다."
      >
        {preset === 'today'
          ? (screener ? <ScreenerReport data={screener} /> : <div style={s.empty}>데이터 없음</div>)
          : (histScreener
              ? <HistoryScreenerView items={histScreener.items} start={range.start} end={range.end} />
              : <div style={s.empty}>{loading ? '로딩…' : '데이터 없음'}</div>)
        }
      </Section>
    </div>
  )
}

// ── 스타일 ───────────────────────────────────────────────────
const s: Record<string, React.CSSProperties> = {
  wrap: { height: '100%', overflowY: 'auto', fontSize: 12, color: tokens.tx.secondary, boxSizing: 'border-box' },

  hdr: { display: 'flex', alignItems: 'center', gap: 8, padding: '10px 14px', borderBottom: `1px solid ${tokens.bd.default}`, flexShrink: 0 },
  hdrTitle: { fontWeight: 700, fontSize: 13, flex: 1 },
  hdrTime: { color: tokens.tx.subtle, fontSize: 11 },
  refreshBtn: { background: tokens.bg.raised, color: tokens.tx.secondary, border: `1px solid ${tokens.bd.emphasis}`, borderRadius: 4, padding: '4px 10px', cursor: 'pointer', fontSize: 11, minHeight: 36 },

  section: { borderBottom: `1px solid ${tokens.bd.default}` },
  sectionHdr: { width: '100%', display: 'flex', alignItems: 'center', gap: 8, padding: '9px 14px', background: 'none', border: 'none', color: tokens.tx.secondary, cursor: 'pointer', textAlign: 'left' as const },
  sectionTitle: { fontWeight: 700, fontSize: 12, flex: 1 },
  badge: { background: tokens.bg.raised, color: tokens.tx.muted, fontSize: 10, padding: '2px 7px', borderRadius: 10 },
  chevron: { fontSize: 10, color: tokens.tx.subtle },
  sectionBody: { padding: '0 14px 12px' },

  chips: { display: 'flex', gap: 8, marginBottom: 10, flexWrap: 'wrap' as const },
  chip: { background: tokens.bg.raised, borderRadius: 6, padding: '6px 12px', display: 'flex', flexDirection: 'column' as const, alignItems: 'center', gap: 2, border: '1px solid transparent', outline: 'none' },
  chipLabel: { fontSize: 10, color: tokens.tx.muted },
  chipVal: { fontSize: 18, fontWeight: 700 },

  filterRow: { display: 'flex', gap: 6, marginBottom: 8 },
  filterBtn: { background: tokens.bg.raised, color: tokens.tx.muted, border: `1px solid ${tokens.bd.emphasis}`, borderRadius: 4, padding: '3px 9px', cursor: 'pointer', fontSize: 11 },
  filterBtnActive: { background: tokens.bg.active, color: tokens.accent.blueLight, borderColor: tokens.accent.blue },

  tableLabel: { fontSize: 11, color: tokens.tx.subtle, marginBottom: 5, marginTop: 8 },
  tableWrap: { overflowX: 'auto', maxHeight: 280, overflowY: 'auto' as const },
  table: { width: '100%', borderCollapse: 'collapse' as const, fontSize: 11 },
  th: { position: 'sticky' as const, top: 0, background: tokens.bg.row, color: tokens.tx.subtle, padding: '5px 8px', textAlign: 'left' as const, fontWeight: 600, whiteSpace: 'nowrap' as const, borderRight: `1px solid ${tokens.bd.default}`, borderBottom: `1px solid ${tokens.bd.default}` },
  td: { padding: '5px 8px', borderBottom: `1px solid ${tokens.bd.default}`, borderRight: `1px solid ${tokens.bd.default}`, verticalAlign: 'middle' as const },
  tickerName: { color: tokens.tx.secondary, fontWeight: 600 },
  tickerCode: { color: tokens.tx.subtle, fontSize: 10 },

  empty: { color: tokens.tx.subtle, textAlign: 'center' as const, padding: '24px 0', fontSize: 12 },
}
