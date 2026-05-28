import { MORE_TAB, type TabConfig, type MobileTabKey } from '../tabs'

interface Props {
  tabs:     TabConfig[]
  active:   MobileTabKey
  onChange: (tab: MobileTabKey) => void
}

export default function MobileNav({ tabs, active, onChange }: Props) {
  const allTabs = [...tabs, MORE_TAB]
  return (
    <nav className="app-mobile-nav">
      {allTabs.map(({ key, icon, label }) => (
        <button
          key={key}
          className={`app-mobile-nav-btn${active === key ? ' active' : ''}`}
          onClick={() => onChange(key as MobileTabKey)}
        >
          <span style={{ fontSize: 18, lineHeight: 1 }}>{icon}</span>
          <span>{label}</span>
        </button>
      ))}
    </nav>
  )
}
