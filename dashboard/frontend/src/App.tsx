import { Suspense, lazy, useState } from 'react'

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

  return (
    <div style={styles.root}>
      <header style={styles.header}>
        <span style={styles.logo}>📈 Trading Dashboard</span>
        <span style={styles.sub} className="app-header-sub">KOSPI + KOSDAQ Stage 시스템</span>
      </header>

      <main style={styles.main} className="app-main">
        {/* 좌측 패널 */}
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
            {leftTab === 'heatmap' && (
              <Suspense fallback={<Loading />}><Heatmap /></Suspense>
            )}
            {leftTab === 'report' && (
              <Suspense fallback={<Loading />}><Report /></Suspense>
            )}
            {leftTab === 'top' && (
              <Suspense fallback={<Loading />}><Top /></Suspense>
            )}
          </div>
        </section>

        {/* 우측 사이드바 */}
        <aside style={styles.sidebar} className="app-sidebar">
          <div style={styles.panel}>
            <Suspense fallback={<Loading />}><SignalFeed /></Suspense>
          </div>
          <div style={styles.panel}>
            <Suspense fallback={<Loading />}><Positions /></Suspense>
          </div>
          <div style={styles.schedulerPanel}>
            <Suspense fallback={<Loading />}><Scheduler /></Suspense>
          </div>
        </aside>
      </main>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: { display: 'flex', flexDirection: 'column', height: '100vh', overflow: 'hidden', background: '#0f1117' },
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
