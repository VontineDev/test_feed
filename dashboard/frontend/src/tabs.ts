import { lazy } from 'react'
import type { Role } from './hooks/useRole'

const Heatmap        = lazy(() => import('./components/Heatmap'))
const Report         = lazy(() => import('./components/Report'))
const Top            = lazy(() => import('./components/Top'))
const PaperPortfolio = lazy(() => import('./components/PaperPortfolio'))
const Macro          = lazy(() => import('./components/Macro'))
const Portfolio      = lazy(() => import('./components/Portfolio'))
const Narrative      = lazy(() => import('./components/Narrative'))

export type TabKey = 'heatmap' | 'report' | 'top' | 'paper' | 'macro' | 'portfolio' | 'narrative'
export type MobileTabKey = TabKey | 'more'

export interface TabConfig {
  key:       TabKey
  label:     string
  icon:      string
  component: ReturnType<typeof lazy>
  roles?:    Role[]   // undefined = 모든 역할 접근 가능
}

export const TAB_CONFIG: TabConfig[] = [
  { key: 'heatmap',   label: '히트맵',    icon: '🗺',  component: Heatmap        },
  { key: 'report',    label: '종목 분석',  icon: '📈', component: Report         },
  { key: 'top',       label: 'Top',      icon: '📊', component: Top            },
  { key: 'paper',     label: '모의투자',   icon: '💼', component: PaperPortfolio },
  { key: 'macro',     label: '매크로',    icon: '🌐', component: Macro          },
  { key: 'portfolio', label: '내 자산',   icon: '💰', component: Portfolio,
    roles: ['admin', 'special'] },
  { key: 'narrative', label: '내러티브',  icon: '📡', component: Narrative  },
]

export const MORE_TAB = { key: 'more' as const, label: '시그널', icon: '📡' }
export const DEFAULT_TAB: TabKey = 'heatmap'

export function getVisibleTabs(role: Role | null): TabConfig[] {
  return TAB_CONFIG.filter(t => !t.roles || (role && t.roles.includes(role)))
}
