import { describe, it, expect } from 'vitest'
import { pivotSeries } from './PaperAnalytics'

describe('pivotSeries', () => {
  it('빈 series → 빈 배열', () => {
    expect(pivotSeries({})).toEqual([])
  })

  it('단일 모델 → 날짜별 누적값 그대로', () => {
    const result = pivotSeries({
      stage: [
        { date: '2026-04-01', cumulative: 0.05 },
        { date: '2026-04-10', cumulative: 0.12 },
      ],
    })
    expect(result).toHaveLength(2)
    expect(result[0]).toMatchObject({ date: '2026-04-01', stage: 0.05 })
    expect(result[1]).toMatchObject({ date: '2026-04-10', stage: 0.12 })
  })

  it('모델 2개, 날짜 겹치지 않음 → null fill', () => {
    const result = pivotSeries({
      stage:   [{ date: '2026-04-01', cumulative: 0.05 }],
      kosdaq: [{ date: '2026-04-15', cumulative: 0.03 }],
    })
    // 날짜 2개: 2026-04-01, 2026-04-15
    expect(result).toHaveLength(2)

    const first = result[0]
    expect(first.date).toBe('2026-04-01')
    expect(first.stage).toBe(0.05)
    expect(first.kosdaq).toBeNull()  // 아직 거래 없음

    const second = result[1]
    expect(second.date).toBe('2026-04-15')
    expect(second.stage).toBe(0.05)   // 마지막값 유지 (connectNulls용)
    expect(second.kosdaq).toBe(0.03)
  })

  it('같은 날짜에 두 모델 모두 거래 → 둘 다 채워짐', () => {
    const result = pivotSeries({
      stage:   [{ date: '2026-05-01', cumulative: 0.10 }],
      cross:   [{ date: '2026-05-01', cumulative: 0.20 }],
    })
    expect(result).toHaveLength(1)
    expect(result[0]).toMatchObject({ date: '2026-05-01', stage: 0.10, cross: 0.20 })
  })

  it('날짜 정렬 보장 — 입력 순서 무관', () => {
    const result = pivotSeries({
      stage: [
        { date: '2026-05-10', cumulative: 0.15 },
        { date: '2026-04-01', cumulative: 0.05 },
      ],
    })
    expect(result[0].date).toBe('2026-04-01')
    expect(result[1].date).toBe('2026-05-10')
  })
})
