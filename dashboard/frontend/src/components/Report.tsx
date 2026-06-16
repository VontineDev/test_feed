import { useState, useEffect, useRef, useCallback } from 'react'
import { tokens } from '../tokens'
import DateRangeBar, { DateRange, computeRange } from './DateRangeBar'
import StageHistoryPopup from './StageHistoryPopup'
import Narrative from './Narrative'

// ── 파이프라인 상태 배너 ──────────────────────────────────────
type PipeStatus = 'ok' | 'warn' | 'error'
interface PipelineData {
  flow:     { date: string | null; tickers: number; status: PipeStatus }
  stage:    { date: string | null; status: PipeStatus }
  screener: { date: string | null; status: PipeStatus }
  youtube:  { date: string | null; status: PipeStatus }
}

const STATUS_COLOR: Record<PipeStatus, string> = {
  ok:    '#22c55e',
  warn:  '#f59e0b',
  error: '#f87171',
}

function PipelineStatusBar({ refreshKey }: { refreshKey: number }) {
  const [data, setData] = useState<PipelineData | null>(null)

  useEffect(() => {
    fetch('/api/report/pipeline-status')
      .then(r => r.json())
      .then(setData)
      .catch(() => setData(null))
  }, [refreshKey])

  if (!data) return null

  const items: { label: string; date: string | null; status: PipeStatus; detail?: string }[] = [
    { label: '수급',     date: data.flow.date,     status: data.flow.status,     detail: `${data.flow.tickers}개 티커` },
    { label: '스테이지', date: data.stage.date,    status: data.stage.status },
    { label: '스크리너', date: data.screener.date, status: data.screener.status },
    { label: '유튜브',   date: data.youtube.date,  status: data.youtube.status },
  ]

  return (
    <div style={ps.bar}>
      <span style={ps.label}>파이프라인</span>
      {items.map(({ label, date, status, detail }) => (
        <span
          key={label}
          style={ps.item}
          title={detail ? `${label}: ${date ?? '—'} (${detail})` : `${label}: ${date ?? '—'}`}
        >
          <span style={{ ...ps.dot, background: STATUS_COLOR[status] }} />
          <span style={ps.itemLabel}>{label}</span>
          <span style={ps.itemDate}>{date ? date.slice(5) : '—'}</span>
        </span>
      ))}
    </div>
  )
}

const ps: Record<string, React.CSSProperties> = {
  bar:       { display: 'flex', alignItems: 'center', gap: 12, padding: '5px 14px', borderBottom: `1px solid ${tokens.bd.default}`, background: tokens.bg.panel, flexShrink: 0, flexWrap: 'wrap' as const },
  label:     { fontSize: 10, color: tokens.tx.subtle, fontWeight: 600, letterSpacing: '0.05em', textTransform: 'uppercase' as const, marginRight: 4 },
  item:      { display: 'flex', alignItems: 'center', gap: 4, cursor: 'default' },
  dot:       { width: 6, height: 6, borderRadius: '50%', flexShrink: 0 },
  itemLabel: { fontSize: 11, color: tokens.tx.secondary },
  itemDate:  { fontSize: 11, color: tokens.tx.subtle },
}

export default function Report() {
  const [range, setRange] = useState<DateRange>(computeRange('today'))

  // Narrative 로드 상태 — 헤더에서 통합 관리
  const [fetchedAt, setFetchedAt]         = useState<Date | null>(null)
  const [asOf, setAsOf]                   = useState<{ stage: string | null; screener: string | null; narrative: string | null } | null>(null)
  const [narrativeRefreshKey, setRefreshKey] = useState(0)

  // 우측 패널 선택 상태
  const [selectedTicker, setSelectedTicker] = useState<string | null>(null)
  const [selectedName, setSelectedName]     = useState<string>('')
  const splitRightRef = useRef<HTMLDivElement>(null)

  const handleLoad = useCallback((at: Date, ao: typeof asOf) => {
    setFetchedAt(at)
    setAsOf(ao)
  }, [])

  // 프리셋/범위 변경 시 패널 닫기
  useEffect(() => { setSelectedTicker(null); setSelectedName('') }, [range.preset, range.start, range.end])

  // 패널 열릴 때 화면에 보이도록 스크롤 (모바일 세로 스택 대응)
  useEffect(() => {
    if (selectedTicker && splitRightRef.current) {
      splitRightRef.current.scrollIntoView({ behavior: 'smooth', block: 'nearest' })
    }
  }, [selectedTicker])

  const handleSelect = (ticker: string, name: string) => {
    if (selectedTicker === ticker) {
      setSelectedTicker(null)
      setSelectedName('')
    } else {
      setSelectedTicker(ticker)
      setSelectedName(name)
    }
  }

  // today는 파라미터 없이 최신 스냅샷, 나머지는 기간 전달
  const dateProps = range.preset === 'today'
    ? {}
    : { start: range.start, end: range.end }

  return (
    <div style={s.wrap}>
      {/* 헤더 */}
      <div style={s.hdr}>
        <span style={s.hdrTitle}>종목 분석</span>
        {fetchedAt && (
          <span style={s.hdrTime} title={fetchedAt ? `조회: ${fetchedAt.toLocaleTimeString('ko-KR')}` : undefined}>
            데이터 기준: {asOf?.stage ?? asOf?.screener ?? asOf?.narrative ?? '—'}
            {asOf && (
              <span style={{ marginLeft: 4, color: 'inherit', opacity: 0.6 }}
                title={`스테이지: ${asOf.stage ?? '—'} / 스크리너: ${asOf.screener ?? '—'} / 내러티브: ${asOf.narrative ?? '—'}`}>
                (?)
              </span>
            )}
          </span>
        )}
        <button style={s.refreshBtn} onClick={() => setRefreshKey(k => k + 1)}>
          새로고침
        </button>
      </div>

      {/* 파이프라인 수집 상태 */}
      <PipelineStatusBar refreshKey={narrativeRefreshKey} />

      {/* 날짜 범위 선택 바 */}
      <DateRangeBar range={range} onChange={setRange} />

      {/* 콘텐츠 — 좌우 분할 */}
      <div className="report-split-container" style={s.splitWrap}>
        <div className="report-split-left" style={{
          ...s.splitLeft,
          flex: selectedTicker ? '0 0 55%' : 1,
          borderRight: selectedTicker ? `1px solid ${tokens.bd.default}` : undefined,
        }}>
          <Narrative
            onSelect={handleSelect}
            selectedTicker={selectedTicker}
            onLoad={handleLoad}
            refreshKey={narrativeRefreshKey}
            {...dateProps}
          />
        </div>

        {/* 오른쪽: 종목별 상세 패널 */}
        {selectedTicker && (
          <div ref={splitRightRef} className="report-split-right" style={s.splitRight}>
            <StageHistoryPopup
              ticker={selectedTicker}
              name={selectedName}
              start={range.start}
              end={range.end}
              onClose={() => { setSelectedTicker(null); setSelectedName('') }}
              mode="panel"
            />
          </div>
        )}
      </div>
    </div>
  )
}

// ── 스타일 ────────────────────────────────────────────────────
const s: Record<string, React.CSSProperties> = {
  wrap:      { height: '100%', display: 'flex', flexDirection: 'column', fontSize: 12, color: tokens.tx.secondary, boxSizing: 'border-box', overflow: 'hidden' },
  splitWrap: { flex: 1, display: 'flex', overflow: 'hidden', minHeight: 0 },
  splitLeft: { minWidth: 0, overflowY: 'auto', transition: 'flex 0.15s' },
  splitRight: { flex: '0 0 45%', minWidth: 0, overflowY: 'auto' },

  hdr:        { display: 'flex', alignItems: 'center', gap: 8, padding: '10px 14px', borderBottom: `1px solid ${tokens.bd.default}`, flexShrink: 0 },
  hdrTitle:   { fontWeight: 700, fontSize: 13, flex: 1 },
  hdrTime:    { color: tokens.tx.subtle, fontSize: 11 },
  refreshBtn: { background: tokens.bg.raised, color: tokens.tx.secondary, border: `1px solid ${tokens.bd.emphasis}`, borderRadius: 4, padding: '4px 10px', cursor: 'pointer', fontSize: 11, minHeight: 36 },

  empty: { color: tokens.tx.subtle, textAlign: 'center' as const, padding: '24px 0', fontSize: 12 },
}
