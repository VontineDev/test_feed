import { useState, useEffect, useRef, useCallback } from 'react'
import { tokens } from '../tokens'
import DateRangeBar, { DateRange, computeRange } from './DateRangeBar'
import StageHistoryPopup from './StageHistoryPopup'
import Narrative from './Narrative'
import { useRole } from '../hooks/useRole'

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

const JOB_MAP: { label: string; job: string; date: (d: PipelineData) => string | null; status: (d: PipelineData) => PipeStatus; detail?: (d: PipelineData) => string }[] = [
  { label: '수급',     job: 'flow',     date: d => d.flow.date,     status: d => d.flow.status,     detail: d => `${d.flow.tickers}개 티커` },
  { label: '스테이지', job: 'stage',    date: d => d.stage.date,    status: d => d.stage.status },
  { label: '스크리너', job: 'screener', date: d => d.screener.date, status: d => d.screener.status },
  { label: '유튜브',   job: 'youtube',  date: d => d.youtube.date,  status: d => d.youtube.status },
]

interface PipelineStatusBarProps {
  refreshKey: number
  isAdmin: boolean
  onRefresh: () => void
  runningJob: string | null
  onTrigger: (job: string) => void
}

function PipelineStatusBar({ refreshKey, isAdmin, onRefresh, runningJob, onTrigger }: PipelineStatusBarProps) {
  const [data, setData] = useState<PipelineData | null>(null)

  useEffect(() => {
    fetch('/api/report/pipeline-status')
      .then(r => r.json())
      .then(setData)
      .catch(() => setData(null))
  }, [refreshKey])

  if (!data) return null

  return (
    <div style={ps.bar}>
      <span style={ps.label}>파이프라인</span>
      {JOB_MAP.map(({ label, job, date, status, detail }) => {
        const d = date(data)
        const st = status(data)
        const det = detail ? detail(data) : undefined
        const isRunning = runningJob === job
        return (
          <span
            key={job}
            style={ps.item}
            title={det ? `${label}: ${d ?? '—'} (${det})` : `${label}: ${d ?? '—'}`}
          >
            <span style={{ ...ps.dot, background: STATUS_COLOR[st] }} />
            <span style={ps.itemLabel}>{label}</span>
            <span style={ps.itemDate}>{d ? d.slice(5) : '—'}</span>
            {isAdmin && (
              <button
                style={{ ...ps.runBtn, opacity: isRunning ? 0.5 : 1 }}
                disabled={isRunning}
                onClick={() => onTrigger(job)}
                title={isRunning ? '실행 중' : `${label} 파이프라인 실행`}
              >
                {isRunning ? '●' : '▶'}
              </button>
            )}
          </span>
        )
      })}
      <button style={ps.refreshBtn} onClick={onRefresh} title="상태 새로고침">↺</button>
    </div>
  )
}

const ps: Record<string, React.CSSProperties> = {
  bar:        { display: 'flex', alignItems: 'center', gap: 12, padding: '5px 14px', borderBottom: `1px solid ${tokens.bd.default}`, background: tokens.bg.panel, flexShrink: 0, flexWrap: 'wrap' as const },
  label:      { fontSize: 10, color: tokens.tx.subtle, fontWeight: 600, letterSpacing: '0.05em', textTransform: 'uppercase' as const, marginRight: 4 },
  item:       { display: 'flex', alignItems: 'center', gap: 4, cursor: 'default' },
  dot:        { width: 6, height: 6, borderRadius: '50%', flexShrink: 0 },
  itemLabel:  { fontSize: 11, color: tokens.tx.secondary },
  itemDate:   { fontSize: 11, color: tokens.tx.subtle },
  runBtn:     { background: 'transparent', border: 'none', color: tokens.tx.subtle, cursor: 'pointer', fontSize: 9, padding: '0 2px', lineHeight: 1 },
  refreshBtn: { marginLeft: 'auto', background: 'transparent', border: 'none', color: tokens.tx.subtle, cursor: 'pointer', fontSize: 13, padding: '0 2px', lineHeight: 1 },
}

export default function Report() {
  const [range, setRange] = useState<DateRange>(computeRange('today'))
  const [refreshKey, setRefreshKey] = useState(0)
  const [runningJob, setRunningJob] = useState<string | null>(null)

  // 우측 패널 선택 상태
  const [selectedTicker, setSelectedTicker] = useState<string | null>(null)
  const [selectedName, setSelectedName]     = useState<string>('')
  const splitRightRef = useRef<HTMLDivElement>(null)

  const role = useRole()
  const isAdmin = role === 'admin'

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

  const handleTrigger = useCallback(async (job: string) => {
    if (runningJob) return
    setRunningJob(job)
    try {
      const r = await fetch('/api/scheduler/trigger', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ job }),
      })
      if (r.status === 403) {
        alert('관리자 권한이 필요합니다')
        return
      }
      if (!r.ok) {
        alert(`요청 실패 (${r.status})`)
        return
      }
      const j = await r.json()
      if (j.status === 'already_queued') {
        alert(`이미 실행 대기 중입니다`)
      }
    } catch {
      alert('요청 실패')
    } finally {
      setRunningJob(null)
    }
  }, [runningJob])

  // today는 파라미터 없이 최신 스냅샷, 나머지는 기간 전달
  const dateProps = range.preset === 'today'
    ? {}
    : { start: range.start, end: range.end }

  return (
    <div style={s.wrap}>
      {/* 헤더 */}
      <div style={s.hdr}>
        <span style={s.hdrTitle}>종목 분석</span>
      </div>

      {/* 파이프라인 수집 상태 */}
      <PipelineStatusBar
        refreshKey={refreshKey}
        isAdmin={isAdmin}
        onRefresh={() => setRefreshKey(k => k + 1)}
        runningJob={runningJob}
        onTrigger={handleTrigger}
      />

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
            refreshKey={refreshKey}
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

  hdr:      { display: 'flex', alignItems: 'center', gap: 8, padding: '10px 14px', borderBottom: `1px solid ${tokens.bd.default}`, flexShrink: 0 },
  hdrTitle: { fontWeight: 700, fontSize: 13, flex: 1 },
}
