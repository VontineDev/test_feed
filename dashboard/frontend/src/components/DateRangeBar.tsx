import { useState, useRef, useEffect } from 'react'
import { tokens } from '../tokens'

export type DatePreset = 'today' | '3d' | '1w' | '2w' | '1m' | 'custom'

export interface DateRange {
  preset: DatePreset
  start: string   // YYYY-MM-DD
  end: string     // YYYY-MM-DD
}

function toYMD(d: Date): string {
  return d.toISOString().slice(0, 10)
}

export function computeRange(preset: Exclude<DatePreset, 'custom'>): DateRange {
  const today = new Date()
  today.setHours(0, 0, 0, 0)
  const end = toYMD(today)
  const offsets: Record<string, number> = { today: 0, '3d': 3, '1w': 7, '2w': 14, '1m': 30 }
  const start = new Date(today)
  start.setDate(start.getDate() - offsets[preset])
  return { preset, start: toYMD(start), end }
}

function parseYYMMDD(s: string): string | null {
  const clean = s.trim().replace(/\D/g, '')
  if (clean.length !== 6) return null
  const full = `20${clean.slice(0, 2)}-${clean.slice(2, 4)}-${clean.slice(4, 6)}`
  const d = new Date(full)
  if (isNaN(d.getTime())) return null
  return full
}


const PRESETS: { key: DatePreset; label: string }[] = [
  { key: 'today',  label: '오늘'    },
  { key: '3d',     label: '-3일'    },
  { key: '1w',     label: '-1주'    },
  { key: '2w',     label: '-2주'    },
  { key: '1m',     label: '-1달'    },
  { key: 'custom', label: '직접입력' },
]

interface Props {
  range: DateRange
  onChange: (range: DateRange) => void
}

export default function DateRangeBar({ range, onChange }: Props) {
  // activeBtn 은 버튼 하이라이트용 (실제 range.preset 과 분리)
  const [activeBtn, setActiveBtn]   = useState<DatePreset>(range.preset)
  const [startInput, setStartInput] = useState('')
  const [endInput, setEndInput]     = useState('')
  const [error, setError]           = useState(false)
  const startRef = useRef<HTMLInputElement>(null)
  const endRef   = useRef<HTMLInputElement>(null)

  // 외부에서 range가 바뀌면 activeBtn 동기화
  useEffect(() => { setActiveBtn(range.preset) }, [range.preset])

  // custom 진입 시 입력 필드 초기화 후 시작일 포커스 (숫자패드 자동 열림)
  useEffect(() => {
    if (activeBtn === 'custom') {
      setStartInput('')
      setEndInput('')
      setError(false)
      setTimeout(() => startRef.current?.focus(), 0)
    }
  }, [activeBtn]) // eslint-disable-line react-hooks/exhaustive-deps

  const handlePresetClick = (key: DatePreset) => {
    setActiveBtn(key)
    if (key !== 'custom') {
      onChange(computeRange(key as Exclude<DatePreset, 'custom'>))
    }
  }

  const applyCustom = () => {
    const s = parseYYMMDD(startInput)
    const e = parseYYMMDD(endInput)
    if (!s || !e || s > e) {
      setError(true)
      return
    }
    setError(false)
    // 키패드 닫기 (모바일)
    startRef.current?.blur()
    endRef.current?.blur()
    onChange({ preset: 'custom', start: s, end: e })
  }

  const handleStartKey = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') endRef.current?.focus()
  }

  const handleEndKey = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') applyCustom()
    if (e.key === 'Escape') {
      setActiveBtn(range.preset)
      setError(false)
    }
  }

  // 6자 입력 시 다음 필드로 자동 이동
  const handleStartChange = (v: string) => {
    setStartInput(v)
    setError(false)
    if (v.replace(/\D/g, '').length === 6) endRef.current?.focus()
  }

  return (
    <div style={s.wrap}>
      <div style={s.btns}>
        {PRESETS.map(({ key, label }) => (
          <button
            key={key}
            style={{
              ...s.btn,
              background: activeBtn === key ? tokens.bg.active  : tokens.bg.raised,
              border:     activeBtn === key
                ? `1px solid ${tokens.accent.blue}`
                : `1px solid ${tokens.bd.emphasis}`,
              color: activeBtn === key ? tokens.accent.blueLight : tokens.tx.muted,
            }}
            onClick={() => handlePresetClick(key)}
          >
            {label}
          </button>
        ))}
      </div>

      {activeBtn === 'custom' ? (
        <div style={s.customWrap}>
          <input
            ref={startRef}
            style={{ ...s.input, borderColor: error ? '#e55' : tokens.bd.emphasis }}
            placeholder="yymmdd"
            value={startInput}
            maxLength={6}
            inputMode="numeric"
            onChange={e => handleStartChange(e.target.value)}
            onKeyDown={handleStartKey}
          />
          <span style={s.sep}>~</span>
          <input
            ref={endRef}
            style={{ ...s.input, borderColor: error ? '#e55' : tokens.bd.emphasis }}
            placeholder="yymmdd"
            value={endInput}
            maxLength={6}
            inputMode="numeric"
            onChange={e => { setEndInput(e.target.value); setError(false) }}
            onKeyDown={handleEndKey}
          />
          <button
            style={s.confirmBtn}
            onPointerDown={e => e.preventDefault()}
            onClick={applyCustom}
          >
            확인
          </button>
          {error && <span style={s.err}>날짜 형식 오류</span>}
        </div>
      ) : (
        range.preset !== 'today' && (
          <span style={s.rangeLabel}>{range.start} ~ {range.end}</span>
        )
      )}
    </div>
  )
}

const s: Record<string, React.CSSProperties> = {
  wrap: {
    display: 'flex', alignItems: 'center', gap: 10,
    padding: '8px 14px', borderBottom: `1px solid ${tokens.bd.default}`,
    background: '#0a0f1a', flexShrink: 0,
  },
  btns: { display: 'flex', gap: 5, flexWrap: 'wrap' as const },
  btn: {
    borderRadius: 4, padding: '5px 10px', cursor: 'pointer',
    fontSize: 11, fontWeight: 600, outline: 'none',
    minHeight: 36, whiteSpace: 'nowrap' as const,
  },
  rangeLabel: { fontSize: 10, color: tokens.tx.subtle, marginLeft: 4 },
  customWrap: { display: 'flex', alignItems: 'center', gap: 6 },
  input: {
    width: 64, padding: '4px 7px', fontSize: 11, fontFamily: 'monospace',
    background: tokens.bg.raised, color: tokens.tx.primary,
    border: `1px solid ${tokens.bd.emphasis}`, borderRadius: 4,
    outline: 'none', textAlign: 'center' as const, minHeight: 28,
  },
  sep: { fontSize: 11, color: tokens.tx.subtle },
  confirmBtn: {
    padding: '4px 10px', fontSize: 11, fontWeight: 600, cursor: 'pointer',
    background: tokens.accent.blue, color: '#fff',
    border: 'none', borderRadius: 4, minHeight: 28,
  },
  err: { fontSize: 10, color: '#e55' },
}
