import { useEffect, useState } from 'react'

interface TriggerRecord {
  id: number
  job_name: string
  requested_at: string
  executed_at: string | null
  status: string
}

const JOB_LABELS: Record<string, string> = {
  stage: 'Stage 분류 실행',
  screener: '차트 스크리너 실행',
  paper_sample: '모의투자 샘플링 실행',
}

const statusColor = (s: string): string => {
  if (s === 'done') return '#4ade80'
  if (s === 'running') return '#facc15'
  if (s === 'error') return '#f87171'
  return '#94a3b8'
}

export default function Scheduler() {
  const [history, setHistory] = useState<TriggerRecord[]>([])
  const [busy, setBusy] = useState<string | null>(null)

  const loadHistory = () => {
    fetch('/api/scheduler/status')
      .then(r => r.json())
      .then(j => setHistory(j.data))
      .catch(() => {})
  }

  useEffect(() => {
    loadHistory()
    const id = setInterval(loadHistory, 10_000)
    return () => clearInterval(id)
  }, [])

  const trigger = async (job: string) => {
    if (busy) return
    setBusy(job)
    try {
      const r = await fetch('/api/scheduler/trigger', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ job }),
      })
      const j = await r.json()
      if (j.status === 'already_queued') {
        alert(`${JOB_LABELS[job]}: 이미 대기 중입니다`)
      }
      loadHistory()
    } catch {
      alert('요청 실패')
    } finally {
      setBusy(null)
    }
  }

  return (
    <div style={styles.wrap}>
      <div style={styles.hdr}>스케줄러 컨트롤</div>
      <div style={styles.btnRow}>
        {Object.entries(JOB_LABELS).map(([job, label]) => (
          <button
            key={job}
            style={{ ...styles.btn, opacity: busy === job ? 0.6 : 1 }}
            onClick={() => trigger(job)}
            disabled={!!busy}
          >
            {busy === job ? '실행 중…' : label}
          </button>
        ))}
      </div>
      {history.length > 0 && (
        <div style={styles.historyWrap}>
          <div style={styles.historyHdr}>최근 실행 이력</div>
          {history.map(h => (
            <div key={h.id} style={styles.historyRow}>
              <span style={styles.historyJob}>{JOB_LABELS[h.job_name] ?? h.job_name}</span>
              <span style={{ ...styles.historyStatus, color: statusColor(h.status) }}>{h.status}</span>
              <span style={styles.historyTime}>
                {new Date(h.requested_at).toLocaleTimeString('ko-KR')}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  wrap: { padding: 12 },
  hdr: { fontWeight: 700, fontSize: 13, color: '#94a3b8', marginBottom: 10 },
  btnRow: { display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 16 },
  btn: {
    background: '#1e3a5f', color: '#93c5fd', border: '1px solid #1d4ed8',
    borderRadius: 6, padding: '8px 14px', cursor: 'pointer', fontSize: 12,
    fontWeight: 600, transition: 'opacity 0.15s',
  },
  historyWrap: { borderTop: '1px solid #1e293b', paddingTop: 10 },
  historyHdr: { fontSize: 11, color: '#475569', marginBottom: 6 },
  historyRow: { display: 'flex', gap: 10, alignItems: 'center', padding: '3px 0', fontSize: 12 },
  historyJob: { color: '#cbd5e1', flex: 1 },
  historyStatus: { fontWeight: 600, fontSize: 11, minWidth: 50 },
  historyTime: { color: '#475569', fontSize: 11 },
}
