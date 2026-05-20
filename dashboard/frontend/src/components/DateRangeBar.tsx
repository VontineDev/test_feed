import { useMemo } from 'react'

export type DatePreset = 'today' | '3d' | '1w' | '2w' | '1m'

export interface DateRange {
  preset: DatePreset
  start: string   // YYYY-MM-DD
  end: string     // YYYY-MM-DD
}

function toYMD(d: Date): string {
  return d.toISOString().slice(0, 10)
}

export function computeRange(preset: DatePreset): DateRange {
  const today = new Date()
  today.setHours(0, 0, 0, 0)
  const end = toYMD(today)

  const offsets: Record<DatePreset, number> = {
    today: 0,
    '3d': 3,
    '1w': 7,
    '2w': 14,
    '1m': 30,
  }
  const start = new Date(today)
  start.setDate(start.getDate() - offsets[preset])
  return { preset, start: toYMD(start), end }
}

const PRESETS: { key: DatePreset; label: string }[] = [
  { key: 'today', label: '오늘' },
  { key: '3d',    label: '-3일' },
  { key: '1w',    label: '-1주' },
  { key: '2w',    label: '-2주' },
  { key: '1m',    label: '-1달' },
]

interface Props {
  preset: DatePreset
  onChange: (preset: DatePreset) => void
}

export default function DateRangeBar({ preset, onChange }: Props) {
  const range = useMemo(() => computeRange(preset), [preset])

  const label =
    preset === 'today'
      ? `${range.end} (오늘)`
      : `${range.start} ~ ${range.end}`

  return (
    <div style={s.wrap}>
      <div style={s.btns}>
        {PRESETS.map(({ key, label: btnLabel }) => (
          <button
            key={key}
            style={{
              ...s.btn,
              background: preset === key ? '#1e3a5f' : '#1e293b',
              border: preset === key ? '1px solid #3b82f6' : '1px solid #334155',
              color: preset === key ? '#93c5fd' : '#64748b',
            }}
            onClick={() => onChange(key)}
          >
            {btnLabel}
          </button>
        ))}
      </div>
      {preset !== 'today' && (
        <span style={s.rangeLabel}>{label}</span>
      )}
    </div>
  )
}

const s: Record<string, React.CSSProperties> = {
  wrap: {
    display: 'flex', alignItems: 'center', gap: 10,
    padding: '8px 14px', borderBottom: '1px solid #1e293b',
    background: '#0a0f1a', flexShrink: 0,
  },
  btns: { display: 'flex', gap: 5, flexWrap: 'wrap' as const },
  btn: {
    borderRadius: 5, padding: '5px 10px', cursor: 'pointer',
    fontSize: 11, fontWeight: 600, outline: 'none',
    minHeight: 32, whiteSpace: 'nowrap' as const,
  },
  rangeLabel: { fontSize: 10, color: '#475569', marginLeft: 4 },
}
