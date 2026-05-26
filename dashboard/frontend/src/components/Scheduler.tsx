import { useEffect, useRef, useState } from 'react'
import { tokens } from '../tokens'

interface TriggerRecord {
  id: number
  job_name: string
  requested_at: string
  executed_at: string | null
  status: string
}

const JOB_LABELS: Record<string, string> = {
  stage: '추세 단계',
  screener: '강세 후보',
  paper_sample: '모의투자 샘플링',
}

const STATUS_META: Record<string, { label: string; color: string; pulse: boolean }> = {
  pending:  { label: '대기 중',  color: tokens.tx.secondary,          pulse: false },
  running:  { label: '실행 중',  color: tokens.chart.significance,    pulse: true  },
  done:     { label: '완료',    color: tokens.chart.cat.ichimoku,    pulse: false },
  error:    { label: '오류',    color: tokens.semantic.up,           pulse: false },
}

function elapsed(from: string): string {
  const secs = Math.floor((Date.now() - new Date(from).getTime()) / 1000)
  if (secs < 60) return `${secs}초`
  const mins = Math.floor(secs / 60)
  const rem  = secs % 60
  return `${mins}분 ${rem}초`
}

function relTime(iso: string): string {
  const secs = Math.floor((Date.now() - new Date(iso).getTime()) / 1000)
  if (secs < 60) return `${secs}초 전`
  if (secs < 3600) return `${Math.floor(secs / 60)}분 전`
  return `${Math.floor(secs / 3600)}시간 전`
}

export default function Scheduler() {
  const [history, setHistory] = useState<TriggerRecord[]>([])
  const [connected, setConnected] = useState(false)
  const [busy, setBusy] = useState<string | null>(null)
  const [, setTick] = useState(0)   // 1초 ticker — running 경과시간 갱신용
  const esRef = useRef<EventSource | null>(null)

  // SSE 연결
  useEffect(() => {
    const connect = () => {
      const es = new EventSource('/api/scheduler/stream')
      esRef.current = es

      es.onopen = () => setConnected(true)

      es.onmessage = (e: MessageEvent) => {
        try {
          setHistory(JSON.parse(e.data))
        } catch {}
      }

      es.onerror = () => {
        setConnected(false)
        es.close()
        // 5초 후 재연결
        setTimeout(connect, 5000)
      }
    }

    connect()
    return () => {
      esRef.current?.close()
      esRef.current = null
    }
  }, [])

  // running 잡이 있으면 1초마다 tick → 경과시간 갱신
  useEffect(() => {
    const hasRunning = history.some(h => h.status === 'running')
    if (!hasRunning) return
    const id = setInterval(() => setTick(t => t + 1), 1000)
    return () => clearInterval(id)
  }, [history])

  const trigger = async (job: string) => {
    if (busy) return
    setBusy(job)
    try {
      const r = await fetch('/api/scheduler/trigger', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ job }),
      })
      if (r.status === 403) {
        alert('관리자 전용 기능입니다. 관리자 계정으로 로그인해 주세요.')
        return
      }
      if (!r.ok) {
        alert(`요청 실패 (${r.status})`)
        return
      }
      const j = await r.json()
      if (j.status === 'already_queued') {
        alert(`${JOB_LABELS[job]}: 이미 대기 중입니다`)
      }
    } catch {
      alert('요청 실패')
    } finally {
      setBusy(null)
    }
  }

  const hasActive = history.some(h => h.status === 'pending' || h.status === 'running')

  return (
    <div style={s.wrap}>
      {/* 헤더 */}
      <div style={s.hdr}>
        <span style={s.title}>스케줄러 컨트롤</span>
        <span style={{ ...s.dot, background: connected ? tokens.chart.cat.ichimoku : tokens.semantic.up }}
              title={connected ? '실시간 연결됨' : '연결 끊김 — 재연결 중…'} />
      </div>

      {/* 트리거 버튼 */}
      <div style={s.btnRow}>
        {Object.entries(JOB_LABELS).map(([job, label]) => {
          const isRunning = history.some(
            h => h.job_name === job && (h.status === 'pending' || h.status === 'running')
          )
          return (
            <button
              key={job}
              style={{ ...s.btn, opacity: (busy || isRunning) ? 0.55 : 1 }}
              onClick={() => trigger(job)}
              disabled={!!busy || isRunning}
              title={isRunning ? '실행 중 — 완료 후 재시도 가능' : undefined}
            >
              {isRunning ? `${label} ●` : label}
            </button>
          )
        })}
      </div>

      {/* 활성 잡 강조 */}
      {hasActive && (
        <div style={s.activeWrap}>
          {history.filter(h => h.status === 'pending' || h.status === 'running').map(h => {
            const meta = STATUS_META[h.status] ?? STATUS_META.pending
            const since = h.status === 'running' && h.executed_at
              ? elapsed(h.executed_at)
              : elapsed(h.requested_at)
            return (
              <div key={h.id} style={s.activeRow}>
                <span style={{ ...s.activeDot, background: meta.color,
                               boxShadow: meta.pulse ? `0 0 6px ${meta.color}` : 'none' }} />
                <span style={s.activeJob}>{JOB_LABELS[h.job_name] ?? h.job_name}</span>
                <span style={{ ...s.activeStatus, color: meta.color }}>{meta.label}</span>
                <span style={s.activeElapsed}>
                  {h.status === 'running' ? `경과 ${since}` : `대기 ${since}`}
                </span>
              </div>
            )
          })}
        </div>
      )}

      {/* 이력 */}
      {history.filter(h => h.status !== 'pending' && h.status !== 'running').length > 0 && (
        <div style={s.historyWrap}>
          <div style={s.historyHdr}>완료 이력</div>
          {history
            .filter(h => h.status !== 'pending' && h.status !== 'running')
            .map(h => {
              const meta = STATUS_META[h.status] ?? STATUS_META.done
              return (
                <div key={h.id} style={s.historyRow}>
                  <span style={s.historyJob}>{JOB_LABELS[h.job_name] ?? h.job_name}</span>
                  <span style={{ ...s.historyStatus, color: meta.color }}>{meta.label}</span>
                  <span style={s.historyTime}>{relTime(h.requested_at)}</span>
                </div>
              )
            })}
        </div>
      )}
    </div>
  )
}

const s: Record<string, React.CSSProperties> = {
  wrap:  { padding: 12 },
  hdr:   { display: 'flex', alignItems: 'center', gap: 6, marginBottom: 10 },
  title: { fontWeight: 700, fontSize: 13, color: tokens.tx.secondary, flex: 1 },
  dot:   { width: 7, height: 7, borderRadius: '50%', flexShrink: 0 },

  btnRow: { display: 'flex', gap: 8, flexWrap: 'wrap' as const, marginBottom: 12 },
  btn: {
    background: tokens.bg.active, color: tokens.accent.blueLight, border: `1px solid ${tokens.accent.blue}`,
    borderRadius: 4, padding: '11px 12px', cursor: 'pointer', fontSize: 12,
    fontWeight: 600, transition: 'opacity 0.15s', minHeight: 44,
  },

  activeWrap:    { background: tokens.bg.panel, borderRadius: 8, padding: '8px 10px', marginBottom: 12 },
  activeRow:     { display: 'flex', alignItems: 'center', gap: 8, padding: '4px 0', fontSize: 12 },
  activeDot:     { width: 8, height: 8, borderRadius: '50%', flexShrink: 0 },
  activeJob:     { color: tokens.tx.secondary, flex: 1 },
  activeStatus:  { fontWeight: 700, fontSize: 11 },
  activeElapsed: { color: tokens.tx.muted, fontSize: 11, fontVariantNumeric: 'tabular-nums' as const },

  historyWrap:   { borderTop: `1px solid ${tokens.bd.default}`, paddingTop: 8 },
  historyHdr:    { fontSize: 11, color: tokens.tx.separator, marginBottom: 6 },
  historyRow:    { display: 'flex', gap: 8, alignItems: 'center', padding: '3px 0', fontSize: 12 },
  historyJob:    { color: tokens.tx.muted, flex: 1 },
  historyStatus: { fontWeight: 600, fontSize: 11 },
  historyTime:   { color: tokens.tx.separator, fontSize: 11 },
}
