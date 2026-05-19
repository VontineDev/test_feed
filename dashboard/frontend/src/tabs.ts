import { lazy } from 'react'

const Heatmap = lazy(() => import('./components/Heatmap'))
const Report  = lazy(() => import('./components/Report'))
const Top     = lazy(() => import('./components/Top'))

export type TabKey = 'heatmap' | 'report' | 'top'
export type MobileTabKey = TabKey | 'more'

export interface TabConfig {
  key:       TabKey
  label:     string
  icon:      string
  component: ReturnType<typeof lazy>
}

export const TAB_CONFIG: TabConfig[] = [
  { key: 'heatmap', label: '히트맵', icon: '🗺',  component: Heatmap },
  { key: 'report',  label: '레포트', icon: '📈', component: Report  },
  { key: 'top',     label: 'Top',   icon: '📊', component: Top     },
]

export const MORE_TAB = { key: 'more' as const, label: '더보기', icon: '☰' }
export const DEFAULT_TAB: TabKey = 'heatmap'
