import { Suspense, lazy, useState } from 'react'
import MobileNav, { type MobileTab } from './components/MobileNav'

const Heatmap    = lazy(() => import('./components/Heatmap'))
const Positions  = lazy(() => import('./components/Positions'))
const SignalFeed = lazy(() => import('./components/SignalFeed'))
const Scheduler  = lazy(() => import('./components/Scheduler'))
const Report     = lazy(() => import('./components/Report'))
const Top        = lazy(() => import('./components/Top'))

const Loading = () => (
  <div style={{ padding: 24, color: '#64748b', textAlign: 'center' as const }}>로딩 중…</div>
)

type LeftTab = 'heatmap' | 'report' | 'top'

export default function App() {
  const [leftTab, setLeftTab] = useState<LeftTab>('heatmap')
  const [mobileTab, setMobileTab] = useState<MobileTab>('top')

  return (
    <div style={styles.root}>
      <header style={styles.header}>
        <span style={styles.logo}>
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" style={{ marginRight: 6, verticalAlign: 'middle' }}>
            <rect x="1" y="9" width="3" height="6" rx="1" fill="#3b82f6"/>
            <rect x="6" y="5" width="3" height="10" rx="1" fill="#60a5fa"/>
            <rect x="11" y="1" width="3" height="14" rx="1" fill="#93c5fd"/>
            <path d="M2 7 L7 4 L12 2" stroke="#f87171" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
          Trading Dashboard
        </span>
        <span style={styles.sub} className="app-header-sub">KOSPI + KOSDAQ Stage 시스템</span>
      </header>

      {/* 데스크탑 레이아웃: 769px 이상에서만 표시 */}
      <main style={styles.main} className="app-main app-desktop-layout">
        <section style={styles.leftPane} className="app-left-pane">
          <div style={styles.tabBar}>
            {([['heatmap', '히트맵'], ['report', '레포트'], ['top', 'Top']] as const).map(([key, label]) => (
              <button
                key={key}
                className="app-tab-btn"
                style={{ ...styles.tab, ...(leftTab === key ? styles.tabActive : {}) }}
                onClick={() => setLeftTab(key)}
              >
                {label}
              </button>
            ))}
          </div>
          <div style={styles.tabContent}>
            {leftTab === 'heatmap' && <Suspense fallback={<Loading />}><Heatmap /></Suspense>}
            {leftTab === 'report'  && <Suspense fallback={<Loading />}><Report /></Suspense>}
            {leftTab === 'top'     && <Suspense fallback={<Loading />}><Top /></Suspense>}
          </div>
        </section>
        <aside style={styles.sidebar} className="app-sidebar">
          <div style={styles.panel}><Suspense fallback={<Loading />}><SignalFeed /></Suspense></div>
          <div style={styles.panel}><Suspense fallback={<Loading />}><Positions /></Suspense></div>
          <div style={styles.schedulerPanel}><Suspense fallback={<Loading />}><Scheduler /></Suspense></div>
        </aside>
      </main>

      {/* 모바일 레이아웃: 768px 이하에서만 표시, 활성 탭만 마운트 */}
      <div className="app-mobile-layout">
        {mobileTab === 'top'     && <Suspense fallback={<Loading />}><Top /></Suspense>}
        {mobileTab === 'stage'   && <Suspense fallback={<Loading />}><Report /></Suspense>}
        {mobileTab === 'heatmap' && <Suspense fallback={<Loading />}><Heatmap /></Suspense>}
        {mobileTab === 'more'    && (
          <>
            <Suspense fallback={<Loading />}><SignalFeed /></Suspense>
            <Suspense fallback={<Loading />}><Positions /></Suspense>
          </>
        )}
      </div>

      <MobileNav active={mobileTab} onChange={setMobileTab} />
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: { display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden', background: '#0f1117' },
  header: {
    display: 'flex', alignItems: 'center', gap: 12, padding: '10px 20px',
    background: '#1a1d2e', borderBottom: '1px solid #1e293b', flexShrink: 0,
  },
  logo: { fontWeight: 700, fontSize: 16, letterSpacing: 0.5 },
  sub: { fontSize: 12, color: '#475569' },
  main: { display: 'flex', flex: 1, minHeight: 0, overflow: 'hidden' },
  leftPane: { flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', borderRight: '1px solid #1e293b' },
  tabBar: { display: 'flex', borderBottom: '1px solid #1e293b', background: '#1a1d2e', flexShrink: 0 },
  tab: {
    padding: '8px 20px', border: 'none', background: 'transparent',
    color: '#64748b', cursor: 'pointer', fontSize: 13, fontWeight: 600,
    borderBottom: '2px solid transparent',
  },
  tabActive: { color: '#93c5fd', borderBottom: '2px solid #3b82f6' },
  tabContent: { flex: 1, minHeight: 0, overflow: 'hidden', display: 'flex', flexDirection: 'column' },
  sidebar: { width: 380, display: 'flex', flexDirection: 'column', overflowY: 'auto', flexShrink: 0 },
  panel: { flex: 1, minHeight: 200, borderBottom: '1px solid #1e293b', overflow: 'auto' },
  schedulerPanel: { flexShrink: 0 },
}
