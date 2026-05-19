import { TAB_CONFIG, MORE_TAB, type MobileTabKey } from '../tabs'

interface Props {
  active: MobileTabKey
  onChange: (tab: MobileTabKey) => void
}

const ALL_TABS = [...TAB_CONFIG, MORE_TAB]

export default function MobileNav({ active, onChange }: Props) {
  return (
    <nav className="app-mobile-nav">
      {ALL_TABS.map(({ key, icon, label }) => (
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
