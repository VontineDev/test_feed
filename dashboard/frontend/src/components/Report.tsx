import { useState, useEffect, useRef } from 'react'
import { tokens } from '../tokens'
import DateRangeBar, { DateRange, computeRange } from './DateRangeBar'
import StageHistoryPopup from './StageHistoryPopup'
import Narrative from './Narrative'

export default function Report() {
  const [range, setRange] = useState<DateRange>(computeRange('today'))

  // Narrative 로드 상태 — 헤더에서 통합 관리
  const [fetchedAt, setFetchedAt]         = useState<Date | null>(null)
  const [narrativeRefreshKey, setRefreshKey] = useState(0)

  // 우측 패널 선택 상태
  const [selectedTicker, setSelectedTicker] = useState<string | null>(null)
  const [selectedName, setSelectedName]     = useState<string>('')
  const splitRightRef = useRef<HTMLDivElement>(null)

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
          <span style={s.hdrTime}>{fetchedAt.toLocaleTimeString('ko-KR')} 기준</span>
        )}
        <button style={s.refreshBtn} onClick={() => setRefreshKey(k => k + 1)}>
          새로고침
        </button>
      </div>

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
            onLoad={setFetchedAt}
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
