import { lazy } from 'react'

const Heatmap        = lazy(() => import('./components/Heatmap'))
const Report         = lazy(() => import('./components/Report'))
const Top            = lazy(() => import('./components/Top'))
const PaperPortfolio = lazy(() => import('./components/PaperPortfolio'))
const Macro          = lazy(() => import('./components/Macro'))

export type TabKey = 'heatmap' | 'report' | 'top' | 'paper' | 'macro'
export type MobileTabKey = TabKey | 'more'

export interface TabConfig {
  key:       TabKey
  label:     string
  icon:      string
  component: ReturnType<typeof lazy>
}

export const TAB_CONFIG: TabConfig[] = [
  { key: 'heatmap', label: '히트맵',  icon: '🗺',  component: Heatmap        },
  { key: 'report',  label: '레포트',  icon: '📈', component: Report         },
  { key: 'top',     label: 'Top',    icon: '📊', component: Top            },
  { key: 'paper',   label: '모의투자', icon: '💼', component: PaperPortfolio },
  { key: 'macro',   label: '매크로',  icon: '🌐', component: Macro          },
]

export const MORE_TAB = { key: 'more' as const, label: '시그널', icon: '📡' }
export const DEFAULT_TAB: TabKey = 'heatmap'
