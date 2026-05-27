// 순수 유틸 함수 모음 — Macro.tsx에서 import하고 테스트에서도 사용

export interface FactorSnap {
  name: string
  current: number
  change_1d: number
  change_3d: number
  change_5d: number
  change_10d: number
  change_20d: number
  change_60d: number
  z_score_60d: number
}

export interface StockResult {
  ticker: string
  name: string
  n_obs: number
  r_squared: number
  adj_r_squared: number
  residual_std: number
  macro_score: number
  macro_score_5d: number
  macro_score_20d: number
  significant_factors: string[]
  betas: Record<string, number>
  alpha: number
  t_stats: Record<string, number>
  p_values: Record<string, number>
  factor_contribs_5d: Record<string, number>
}

export const FACTOR_KEYS = ['rate', 'fx', 'oil', 'vix', 'dxy', 'export']

export const FACTOR_LABELS: Record<string, string> = {
  rate: '미국10년금리', fx: 'USD/KRW', oil: '브렌트유',
  vix: 'VIX', dxy: '달러인덱스', export: '아이셰어즈 대한민국 ETF(EWY)',
}

export const FACTOR_SHORT_LABELS: Record<string, string> = {
  rate: '금리', fx: '환율', oil: '유가', vix: 'VIX', dxy: '달러', export: 'EWY',
}

export const FACTOR_UNITS: Record<string, string> = {
  rate: '%p', fx: '%', oil: '%', vix: '%', dxy: '%', export: '%',
}

// 팩터 카드 1줄 해석 (change_5d > 0 → up 문구, <= 0 → down 문구)
export const FACTOR_CARD_INTERPRET: Record<string, { up: string; down: string }> = {
  rate:   { up: '금리 상승 — 성장주·부동산 부담, 금융주 유리', down: '금리 하락 — 성장주 유리, 금융주 부담' },
  fx:     { up: '원화 약세 — 수출주 수혜, 항공·내수 부담',    down: '원화 강세 — 수입비용 감소, 수출 마진 압박' },
  oil:    { up: '유가 상승 — 정유·에너지 수혜, 항공·운수 부담', down: '유가 하락 — 항공·운수 유리, 정유 마진 압박' },
  vix:    { up: '공포 증가 — 전반적 위험자산 부담',            down: '공포 완화 — 위험선호 회복' },
  dxy:    { up: '달러 강세 — 신흥국 자본 유출 우려',           down: '달러 약세 — 신흥국·원자재 유리' },
  export: { up: 'EWY 상승 — 미장 투자심리 개선, 외국인 수급 유입 기대', down: 'EWY 하락 — 미장 한국 기피, 외국인 이탈 압력' },
}

export const FACTOR_IMPACT_UP: Record<string, { good: string; bad: string }> = {
  rate:   { good: '금융주',    bad: '성장주·부동산' },
  fx:     { good: '수출주',    bad: '항공·내수' },
  oil:    { good: '정유·에너지', bad: '항공·운수' },
  vix:    { good: '방어주',    bad: '전반적 위험자산' },
  dxy:    { good: '달러자산',  bad: '신흥국·원자재' },
  export: { good: '외국인 수급 수혜주', bad: '외국인 이탈 취약주' },
}

export function getFactorState(z: number): { text: string; borderColor: string } {
  if (z > 2)  return { text: '극단 ▲▲', borderColor: '#f97316' }
  if (z < -2) return { text: '극단 ▼▼', borderColor: '#3b82f6' }
  if (z > 1)  return { text: '주의 ▲',  borderColor: '#eab308' }
  if (z < -1) return { text: '주의 ▼',  borderColor: '#eab308' }
  return { text: '정상', borderColor: '#334155' }
}

export function getSensitivity(r2: number): { label: string; color: string } {
  if (r2 >= 0.20) return { label: '고민감', color: '#f59e0b' }
  if (r2 >= 0.10) return { label: '중민감', color: '#64748b' }
  return { label: '저민감', color: '#334155' }
}

export function generateVerdict(stock: StockResult): string {
  const parts: string[] = []
  for (const f of stock.significant_factors.slice(0, 2)) {
    const beta = stock.betas[f] ?? 0
    const label = FACTOR_SHORT_LABELS[f] ?? f
    parts.push(beta > 0 ? `${label} 수혜` : `${label} 부담`)
  }
  return parts.length > 0 ? parts.join(', ') : '매크로 영향 낮음'
}

export function generateBanner(snapshot: Record<string, FactorSnap>): string | null {
  const extremeWithZ = FACTOR_KEYS
    .map(k => ({ key: k, z: snapshot[k]?.z_score_60d ?? 0 }))
    .filter(item => Math.abs(item.z) > 2)
    .sort((a, b) => Math.abs(b.z) - Math.abs(a.z))
    .slice(0, 3)

  if (extremeWithZ.length === 0) return null

  const parts = extremeWithZ.map(({ key: k, z }) => {
    const s = snapshot[k]
    const dir = z > 0 ? '▲' : '▼'
    const val = Math.abs(s.change_5d).toFixed(k === 'rate' ? 2 : 1)
    return `${FACTOR_LABELS[k]} ${dir}${val}${FACTOR_UNITS[k]}`
  })

  const impact = extremeWithZ.map(({ key: k, z }) => {
    const m = FACTOR_IMPACT_UP[k]
    return z > 0 ? `${m.good} 유리` : `${m.bad} 불리`
  }).join(', ')

  return `현재 시장: ${parts.join(' + ')} 구도\n→ ${impact}`
}
