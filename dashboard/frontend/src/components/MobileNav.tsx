export type MobileTab = 'top' | 'stage' | 'heatmap' | 'more'

interface Props {
  active: MobileTab
  onChange: (tab: MobileTab) => void
}

const TABS: { key: MobileTab; icon: string; label: string }[] = [
  { key: 'top',     icon: '📊', label: 'Top'   },
  { key: 'stage',   icon: '📈', label: 'Stage' },
  { key: 'heatmap', icon: '🗺',  label: '맵'    },
  { key: 'more',    icon: '☰',  label: '더보기' },
]

export default function MobileNav({ active, onChange }: Props) {
  return (
    <nav className="app-mobile-nav">
      {TABS.map(({ key, icon, label }) => (
        <button
          key={key}
          className={`app-mobile-nav-btn${active === key ? ' active' : ''}`}
          onClick={() => onChange(key)}
        >
          <span style={{ fontSize: 18, lineHeight: 1 }}>{icon}</span>
          <span>{label}</span>
        </button>
      ))}
    </nav>
  )
}
