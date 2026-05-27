/**
 * vtradingDashboard — 디자인 토큰
 *
 * 모든 색·치수의 단일 진실원천(SSOT).
 * 새 색을 즉석에서 만들지 말고 반드시 이 안에서 골라 쓰세요.
 *
 * 위치: dashboard/frontend/src/tokens.ts
 * 가이드: docs/DESIGN.md (또는 Design System.html)
 */

export const tokens = {
  // ── 배경 ───────────────────────────────────────────────
  bg: {
    root:   '#0f1117',  // 앱 루트 배경
    panel:  '#1a1d2e',  // 헤더, 사이드 패널, 모바일 nav
    raised: '#1e293b',  // 버튼, 툴팁, 정보바
    row:    '#0f172a',  // 테이블 행 구분선
    active: '#1e3a5f',  // 선택된 칩/행 배경 (deep blue highlight)
  },

  // ── 보더 ───────────────────────────────────────────────
  bd: {
    default:  '#1e293b',
    emphasis: '#334155',
  },

  // ── 텍스트 (4단계 위계) ────────────────────────────────
  tx: {
    primary:   '#ffffff',
    secondary: '#94a3b8',
    muted:     '#64748b',
    subtle:    '#475569',
    separator: '#334155',
  },

  // ── 액센트 ─────────────────────────────────────────────
  accent: {
    blue:      '#3b82f6',  // 활성 탭, 포커스, 1차 CTA
    blueSoft:  '#60a5fa',
    blueLight: '#93c5fd',  // 활성 탭 라벨
  },

  // ── Stage 분류 (stage_classifier.py 산출물) ─────────────
  stage: {
    1: '#3b82f6',  // blue
    2: '#a78bfa',  // purple
    3: '#f59e0b',  // amber
  } as Record<1 | 2 | 3, string>,

  // ── 시멘틱 — 등락 (한국식 — 절대 뒤집지 말 것) ──────────
  semantic: {
    up:   '#f87171',  // 빨강 · change_pct > 0
    down: '#60a5fa',  // 파랑 · change_pct < 0
    flat: '#64748b',  // 회색 · change_pct == 0
  },

  // ── 히트맵 셀 그라데이션 — 트리맵 전용 (한국식: 빨강=상승, 파랑=하락) ──
  // 일반 등락 텍스트에는 사용 금지. semantic.up/down을 쓰세요.
  heat: {
    hot:     ['#fca5a5', '#f87171', '#ef4444', '#b91c1c'],  // ≥0, +1, +3, +5 (빨강·상승)
    cold:    ['#93c5fd', '#60a5fa', '#3b82f6', '#1e40af'],  // <0, -1, -3, -5 (파랑·하락)
    neutral: '#334155',
  },

  // ── 차트 ───────────────────────────────────────────────
  chart: {
    // 카테고리컬 — 모델·시리즈 구분 (최대 6개)
    cat: {
      stage:    '#3b82f6',  // blue
      kosdaq:   '#a78bfa',  // purple
      cross:    '#f97316',  // orange
      ichimoku: '#4ade80',  // green
      extra1:   '#22d3ee',  // cyan (예약)
      extra2:   '#f472b6',  // pink (예약)
    },
    // 점수 임계치 — 신호등식 디버지스
    score: {
      positive: '#22c55e',  // ≥ +20  유리
      neutral:  '#f59e0b',  // -20~+20 중립
      negative: '#ef4444',  // ≤ -20  불리
    },
    significance: '#fbbf24',  // p-value 유의(*/**/*** 표시)

    // 크롬 (Recharts 공통)
    axis: {
      tick:   { fontSize: 9, fill: '#475569' },
      stroke: '#334155',
    },
    grid: '#1e293b',
    tooltip: {
      background: '#1e293b',
      border:     '1px solid #334155',
      fontSize:   11,
    },
    line: {
      solid:  { strokeWidth: 1.5, dot: false },
      dashed: { strokeWidth: 1, strokeDasharray: '4 2' },
    },
  },

  // ── 스페이싱 ───────────────────────────────────────────
  space: { xs: 4, sm: 6, md: 8, lg: 12, xl: 20, xxl: 24 },

  // ── 라운드 ─────────────────────────────────────────────
  radius: { sm: 4, md: 6, lg: 8 },

  // ── 타이포 ─────────────────────────────────────────────
  type: {
    display: { size: 32, weight: 700 },
    h2:      { size: 22, weight: 600 },
    logo:    { size: 16, weight: 700, tracking: 0.5 },
    tab:     { size: 13, weight: 600 },
    body:    { size: 12, weight: 400 },
    meta:    { size: 11, weight: 400 },
    micro:   { size: 10, weight: 400 },
  },

  // ── 그림자 ─────────────────────────────────────────────
  shadow: {
    tooltip: '0 4px 12px rgba(0,0,0,.4)',
  },
} as const

// ─────────────────────────────────────────────────────────
// 헬퍼 — 자주 쓰는 색 매핑 함수
// ─────────────────────────────────────────────────────────

/** 등락률 → 텍스트 색 (한국식: 빨강=상승, 파랑=하락) */
export const pctTextColor = (pct: number | null | undefined): string => {
  if (pct == null) return tokens.semantic.flat
  if (pct > 0)  return tokens.semantic.up
  if (pct < 0)  return tokens.semantic.down
  return tokens.semantic.flat
}

/** 등락률 → 히트맵 셀 배경색 (그린/레드 9단계 — 트리맵 전용) */
export const heatCellColor = (pct: number): string => {
  if (pct >=  5) return tokens.heat.hot[3]
  if (pct >=  3) return tokens.heat.hot[2]
  if (pct >=  1) return tokens.heat.hot[1]
  if (pct >   0) return tokens.heat.hot[0]
  if (pct === 0) return tokens.heat.neutral
  if (pct >= -1) return tokens.heat.cold[0]
  if (pct >= -3) return tokens.heat.cold[1]
  if (pct >= -5) return tokens.heat.cold[2]
  return tokens.heat.cold[3]
}

/** Stage → 색 (트리맵 보더, 뱃지 등) */
export const stageColor = (stage: number | null): string => {
  if (stage === 1 || stage === 2 || stage === 3) return tokens.stage[stage]
  return tokens.bd.default
}

/** 점수(-100~+100) → 색 */
export const scoreColor = (score: number): string => {
  if (score >=  20) return tokens.chart.score.positive
  if (score <= -20) return tokens.chart.score.negative
  return tokens.chart.score.neutral
}
