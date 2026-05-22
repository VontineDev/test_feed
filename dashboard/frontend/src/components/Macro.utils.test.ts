import { describe, it, expect } from 'vitest'
import {
  getFactorState,
  getSensitivity,
  generateVerdict,
  generateBanner,
  type FactorSnap,
  type StockResult,
} from './Macro.utils'

// ── getFactorState ────────────────────────────────────────────

describe('getFactorState', () => {
  it('z > 2 → 극단 ▲▲ (orange)', () => {
    const s = getFactorState(2.5)
    expect(s.text).toBe('극단 ▲▲')
    expect(s.borderColor).toBe('#f97316')
  })

  it('z < -2 → 극단 ▼▼ (blue)', () => {
    const s = getFactorState(-3.1)
    expect(s.text).toBe('극단 ▼▼')
    expect(s.borderColor).toBe('#3b82f6')
  })

  it('1 < z ≤ 2 → 주의 ▲ (yellow)', () => {
    const s = getFactorState(1.5)
    expect(s.text).toBe('주의 ▲')
    expect(s.borderColor).toBe('#eab308')
  })

  it('-2 ≤ z < -1 → 주의 ▼ (yellow)', () => {
    const s = getFactorState(-1.8)
    expect(s.text).toBe('주의 ▼')
    expect(s.borderColor).toBe('#eab308')
  })

  it('|z| ≤ 1 → 정상 (grey)', () => {
    const s = getFactorState(0.5)
    expect(s.text).toBe('정상')
    expect(s.borderColor).toBe('#334155')
  })

  it('경계값 z=2 → 주의 ▲ (not 극단)', () => {
    expect(getFactorState(2).text).toBe('주의 ▲')
  })

  it('경계값 z=-2 → 주의 ▼ (not 극단)', () => {
    expect(getFactorState(-2).text).toBe('주의 ▼')
  })
})

// ── getSensitivity ────────────────────────────────────────────

describe('getSensitivity', () => {
  it('R² ≥ 0.20 → 고민감 (gold)', () => {
    const s = getSensitivity(0.25)
    expect(s.label).toBe('고민감')
    expect(s.color).toBe('#f59e0b')
  })

  it('0.10 ≤ R² < 0.20 → 중민감', () => {
    expect(getSensitivity(0.15).label).toBe('중민감')
  })

  it('R² < 0.10 → 저민감', () => {
    expect(getSensitivity(0.05).label).toBe('저민감')
  })

  it('경계값 R²=0.20 → 고민감', () => {
    expect(getSensitivity(0.20).label).toBe('고민감')
  })

  it('경계값 R²=0.10 → 중민감', () => {
    expect(getSensitivity(0.10).label).toBe('중민감')
  })
})

// ── generateVerdict ────────────────────────────────────────────

const baseStock = (): StockResult => ({
  ticker: '005930.KS', name: '삼성전자', n_obs: 250,
  r_squared: 0.15, adj_r_squared: 0.13, residual_std: 0.01,
  macro_score: 47, macro_score_5d: 2.1, macro_score_20d: -0.5,
  significant_factors: [], betas: {}, alpha: 0.001,
  t_stats: {}, p_values: {}, factor_contribs_5d: {},
})

describe('generateVerdict', () => {
  it('significant_factors 없음 → 매크로 영향 낮음', () => {
    expect(generateVerdict(baseStock())).toBe('매크로 영향 낮음')
  })

  it('beta > 0 → 수혜', () => {
    const s = { ...baseStock(), significant_factors: ['fx'], betas: { fx: 0.8 } }
    expect(generateVerdict(s)).toBe('환율 수혜')
  })

  it('beta < 0 → 부담', () => {
    const s = { ...baseStock(), significant_factors: ['rate'], betas: { rate: -0.4 } }
    expect(generateVerdict(s)).toBe('금리 부담')
  })

  it('팩터 2개 — 수혜 + 부담', () => {
    const s = {
      ...baseStock(),
      significant_factors: ['fx', 'rate'],
      betas: { fx: 0.8, rate: -0.3 },
    }
    expect(generateVerdict(s)).toBe('환율 수혜, 금리 부담')
  })

  it('significant_factors 3개 이상 → 최대 2개만 표시', () => {
    const s = {
      ...baseStock(),
      significant_factors: ['fx', 'rate', 'vix'],
      betas: { fx: 0.8, rate: -0.3, vix: -1.1 },
    }
    const v = generateVerdict(s)
    expect(v).toBe('환율 수혜, 금리 부담')
    expect(v).not.toContain('VIX')
  })
})

// ── generateBanner ────────────────────────────────────────────

const makeSnap = (z: number, change5d = 1.0): FactorSnap => ({
  name: '', current: 1300, change_1d: 0.1, change_5d: change5d,
  change_20d: 0.5, change_60d: 2.0, z_score_60d: z,
})

const emptySnapshot = () =>
  Object.fromEntries(['rate', 'fx', 'oil', 'vix', 'dxy', 'export'].map(k => [k, makeSnap(0.3)]))

describe('generateBanner', () => {
  it('모든 팩터 |z| ≤ 2 → null 반환 (배너 미표시)', () => {
    expect(generateBanner(emptySnapshot())).toBeNull()
  })

  it('|z| > 2인 팩터 있으면 banner string 반환', () => {
    const snap = { ...emptySnapshot(), fx: makeSnap(2.5, 2.3) }
    const result = generateBanner(snap)
    expect(result).not.toBeNull()
    expect(result).toContain('USD/KRW')
    expect(result).toContain('▲')
  })

  it('z < 0 → ▼ 방향, bad 키워드 포함', () => {
    const snap = { ...emptySnapshot(), rate: makeSnap(-2.8, -0.15) }
    const result = generateBanner(snap)!
    expect(result).toContain('▼')
    expect(result).toContain('성장주·부동산')
  })

  it('최대 3개 팩터만 포함', () => {
    const snap = {
      ...emptySnapshot(),
      fx:  makeSnap(3.0, 2.0),
      rate: makeSnap(2.5, 0.1),
      vix: makeSnap(-2.2, -5.0),
      oil: makeSnap(2.1, 3.0),
    }
    const result = generateBanner(snap)!
    // 4개 극단 팩터 중 3개만 = 4번째(oil, z=2.1) 제외
    // 분리자 '+' 개수 최대 2개 (3개 팩터)
    const plusCount = (result.split('현재 시장: ')[1].split('\n')[0].match(/ \+ /g) ?? []).length
    expect(plusCount).toBeLessThanOrEqual(2)
  })

  it('snapshot에 팩터 키가 없어도 크래시 없음', () => {
    expect(() => generateBanner({})).not.toThrow()
    expect(generateBanner({})).toBeNull()
  })
})
