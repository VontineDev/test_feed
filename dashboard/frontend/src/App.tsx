import { Suspense, lazy, useState } from 'react'
import MobileNav from './components/MobileNav'
import Feedback from './components/Feedback'
import { DEFAULT_TAB, getVisibleTabs, type TabKey, type MobileTabKey } from './tabs'
import { useRole } from './hooks/useRole'
import { tokens } from './tokens'

const SignalFeed = lazy(() => import('./components/SignalFeed'))

const Loading = () => (
  <div style={{ padding: 24, color: tokens.tx.muted, textAlign: 'center' as const }}>로딩 중…</div>
)

export default function App() {
  const role = useRole()
  const visibleTabs = getVisibleTabs(role)

  const [leftTab, setLeftTab]           = useState<TabKey>(DEFAULT_TAB)
  const [mobileTab, setMobileTab]       = useState<MobileTabKey>(DEFAULT_TAB)
  const [feedbackOpen, setFeedbackOpen] = useState(false)

  // 현재 선택된 탭이 역할 변경으로 숨겨지면 기본 탭으로 복귀
  const safeLeftTab   = visibleTabs.find(t => t.key === leftTab)   ? leftTab   : DEFAULT_TAB
  const safeMobileTab = (visibleTabs.find(t => t.key === mobileTab) || mobileTab === 'more')
    ? mobileTab : DEFAULT_TAB

  return (
    <div style={styles.root} className="app-root">
      <header style={styles.header}>
        <span style={styles.logo}>
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" style={{ marginRight: 6, verticalAlign: 'middle' }}>
            <rect x="1" y="9" width="3" height="6" rx="1" fill={tokens.accent.blue}/>
            <rect x="6" y="5" width="3" height="10" rx="1" fill={tokens.accent.blueSoft}/>
            <rect x="11" y="1" width="3" height="14" rx="1" fill={tokens.accent.blueLight}/>
            <path d="M2 7 L7 4 L12 2" stroke={tokens.semantic.up} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
          Trading Dashboard
        </span>
        <span style={styles.sub} className="app-header-sub">KOSPI + KOSDAQ Stage 시스템</span>
        <button style={styles.feedbackBtn} onClick={() => setFeedbackOpen(true)}>피드백</button>
      </header>

      {/* 데스크탑 레이아웃 */}
      <main style={styles.main} className="app-main app-desktop-layout">
        <section style={styles.leftPane} className="app-left-pane">
          <div style={styles.tabBar}>
            {visibleTabs.map(({ key, label }) => (
              <button
                key={key}
                className="app-tab-btn"
                style={{ ...styles.tab, ...(safeLeftTab === key ? styles.tabActive : {}) }}
                onClick={() => setLeftTab(key)}
              >
                {label}
              </button>
            ))}
          </div>
          <div style={styles.tabContent}>
            {visibleTabs.map(({ key, component: Comp }) =>
              safeLeftTab === key && <Suspense key={key} fallback={<Loading />}><Comp /></Suspense>
            )}
          </div>
        </section>
        <aside style={styles.sidebar} className="app-sidebar">
          <div style={styles.panel}><Suspense fallback={<Loading />}><SignalFeed /></Suspense></div>
        </aside>
      </main>

      {/* 모바일 레이아웃 */}
      <div className="app-mobile-layout">
        {visibleTabs.map(({ key, component: Comp }) =>
          safeMobileTab === key && <Suspense key={key} fallback={<Loading />}><Comp /></Suspense>
        )}
        {safeMobileTab === 'more' && (
          <Suspense fallback={<Loading />}><SignalFeed /></Suspense>
        )}
      </div>

      <MobileNav tabs={visibleTabs} active={safeMobileTab} onChange={setMobileTab} />
      <Feedback open={feedbackOpen} onClose={() => setFeedbackOpen(false)} />
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: { display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden', background: tokens.bg.root },
  header: {
    display: 'flex', alignItems: 'center', gap: 12, padding: '10px 20px',
    background: tokens.bg.panel, borderBottom: `1px solid ${tokens.bd.default}`, flexShrink: 0,
  },
  logo: { fontWeight: 700, fontSize: 16, letterSpacing: 0.5 },
  sub: { fontSize: 12, color: tokens.tx.subtle },
  main: { display: 'flex', flex: 1, minHeight: 0, overflow: 'hidden' },
  leftPane: { flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', borderRight: `1px solid ${tokens.bd.default}` },
  tabBar: { display: 'flex', borderBottom: `1px solid ${tokens.bd.default}`, background: tokens.bg.panel, flexShrink: 0 },
  tab: {
    padding: '10px 22px', border: 'none', background: 'transparent',
    color: tokens.tx.muted, cursor: 'pointer', fontSize: 13, fontWeight: 600,
    borderBottom: '2px solid transparent',
  },
  tabActive: { color: tokens.accent.blueLight, borderBottom: `2px solid ${tokens.accent.blue}` },
  tabContent: { flex: 1, minHeight: 0, overflow: 'hidden', display: 'flex', flexDirection: 'column' },
  sidebar: { width: 340, display: 'flex', flexDirection: 'column', overflowY: 'auto', flexShrink: 0 },
  panel: { flex: 1, minHeight: 200, borderBottom: `1px solid ${tokens.bd.default}`, overflow: 'auto' },
  feedbackBtn: {
    marginLeft: 'auto', background: 'none', border: `1px solid ${tokens.bd.emphasis}`,
    color: tokens.tx.muted, borderRadius: 5, padding: '5px 12px',
    cursor: 'pointer', fontSize: 12, fontWeight: 600, flexShrink: 0,
  },
}
