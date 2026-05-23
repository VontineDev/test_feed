# Claude Code 마이그레이션 프롬프트

> 아래 블록 전체를 복사해 Claude Code에 붙여넣으세요. 한 번에 1개 컴포넌트씩 진행하길 권장합니다.

---

## 마이그레이션 작업 지시

`dashboard/frontend/src/tokens.ts` 파일이 새로 추가되었습니다 (이미 첫 두 컴포넌트 `Heatmap.tsx`·`Top.tsx`는 마이그레이션 완료). 이제 나머지 컴포넌트들의 인라인 16진수 색상을 토큰으로 교체해 주세요.

### 토큰 매핑표

기존 16진수 → 토큰 참조로 1:1 치환합니다.

**배경**
- `'#0f1117'` → `tokens.bg.root`
- `'#1a1d2e'` → `tokens.bg.panel`
- `'#1e293b'` (배경 용도) → `tokens.bg.raised`
- `'#0f172a'` (배경 용도) → `tokens.bg.row`

**보더**
- `'#1e293b'` (보더 용도) → `tokens.bd.default`
- `'#334155'` (보더 용도) → `tokens.bd.emphasis`

**텍스트**
- `'#ffffff'` / `'#fff'` (텍스트 용도) → `tokens.tx.primary`
- `'#94a3b8'` → `tokens.tx.secondary`
- `'#cbd5e1'` → `tokens.tx.secondary` (가까운 값으로 흡수)
- `'#e2e8f0'` → `tokens.tx.secondary` (가까운 값으로 흡수)
- `'#64748b'` → `tokens.tx.muted`
- `'#475569'` → `tokens.tx.subtle`
- `'#334155'` (텍스트 용도) → `tokens.tx.separator`

**액센트**
- `'#3b82f6'` → `tokens.accent.blue`
- `'#60a5fa'` (배경/보더/액센트 용도) → `tokens.accent.blueSoft`
  - **주의:** 등락 텍스트의 파랑은 `tokens.semantic.down`을 쓸 것
- `'#93c5fd'` → `tokens.accent.blueLight`

**Stage**
- `'#3b82f6'` (Stage 1 용도) → `tokens.stage[1]`
- `'#a78bfa'` (Stage 2 용도) → `tokens.stage[2]`
- `'#f59e0b'` (Stage 3 용도) → `tokens.stage[3]`
- **Stage 1을 `#60a5fa`로 쓰고 있는 곳이 있다면 `tokens.stage[1]` (즉 `#3b82f6`)로 통일**

**등락 (한국식 — 빨강 상승, 파랑 하락)**
- 양수 → `tokens.semantic.up` (`#f87171`)
- 음수 → `tokens.semantic.down` (`#60a5fa`)
- 0/null → `tokens.semantic.flat` (`#64748b`)
- 가능하면 `pctTextColor(value)` 헬퍼 사용

**차트 — 모델 카테고리컬**
- `'#3b82f6'` (모델 stage) → `tokens.chart.cat.stage`
- `'#a78bfa'` (모델 kosdaq) → `tokens.chart.cat.kosdaq`
- `'#f97316'` (모델 cross) → `tokens.chart.cat.cross`
- `'#4ade80'` (모델 ichimoku) → `tokens.chart.cat.ichimoku`

**차트 — 점수 임계치**
- `'#22c55e'` (≥+20) → `tokens.chart.score.positive`
- `'#f59e0b'` (-20~+20 점수 임계치) → `tokens.chart.score.neutral`
  - **주의:** Stage 3 용도가 아닐 때만
- `'#ef4444'` (≤-20) → `tokens.chart.score.negative`
- `'#fbbf24'` (p-value 유의) → `tokens.chart.significance`
- 가능하면 `scoreColor(score)` 헬퍼 사용

**기타**
- `'#34d399'` (갭점프 표시 등) → `tokens.chart.cat.ichimoku` (가까운 값으로 흡수)
- `'#f87171'` (빨강 전반) → 컨텍스트 따라:
  - 에러/경고 텍스트 → `tokens.semantic.up`
  - 피크아웃 등 경고 표시 → `tokens.semantic.up`

### 작업 단계 (컴포넌트당)

1. 파일 상단에 import 추가: `import { tokens, pctTextColor, scoreColor, heatCellColor, stageColor } from '../tokens'` (필요한 헬퍼만)
2. 모든 인라인 16진수 색상 문자열을 위 매핑표에 따라 토큰 참조로 교체
3. 등락률·EPS 등 부호로 색이 바뀌는 곳은 `pctTextColor()` 헬퍼로 단순화
4. 점수 임계치(`>= 20 ? ... : >= -20 ? ... : ...`) 패턴은 `scoreColor()` 헬퍼로 단순화
5. `font-variant-numeric: tabular-nums`가 누락된 숫자 칼럼 발견 시 추가
6. 직접 테스트: 페이지가 시각적으로 동일한지 확인

### 마이그레이션 대상 파일 (체크리스트)

이미 완료:
- [x] `dashboard/frontend/src/components/Heatmap.tsx`
- [x] `dashboard/frontend/src/components/Top.tsx`

남은 파일 (우선순위 순):
- [ ] `dashboard/frontend/src/App.tsx` — 헤더·탭바·사이드바
- [ ] `dashboard/frontend/src/components/Macro.tsx` — **가장 색이 흩어져 있음, 점수 임계치 다수**
- [ ] `dashboard/frontend/src/components/PaperAnalytics.tsx` — 차트 카테고리컬 색 적용
- [ ] `dashboard/frontend/src/components/Report.tsx` — **Stage 1 색 #60a5fa → tokens.stage[1] (#3b82f6) 수정 필요**
- [ ] `dashboard/frontend/src/components/PaperPortfolio.tsx`
- [ ] `dashboard/frontend/src/components/Positions.tsx`
- [ ] `dashboard/frontend/src/components/SignalFeed.tsx`
- [ ] `dashboard/frontend/src/components/Scheduler.tsx`
- [ ] `dashboard/frontend/src/components/MobileNav.tsx`
- [ ] `dashboard/frontend/src/components/DateRangeBar.tsx`
- [ ] `dashboard/frontend/src/components/HistoryStageView.tsx`
- [ ] `dashboard/frontend/src/components/HistoryScreenerView.tsx`
- [ ] `dashboard/frontend/src/components/StageHistoryPopup.tsx`
- [ ] `dashboard/frontend/src/components/TickerHistory.tsx`
- [ ] `dashboard/frontend/src/index.css` — `:root`에 CSS 변수 추가 (선택)

### 금지 사항

- 새 색상 16진수를 즉석에서 추가하지 말 것. 토큰에 없는 색이 꼭 필요하면 먼저 `tokens.ts`에 등록 후 사용
- 인라인 스타일 객체 이름을 통일하지 말 것 — 컴포넌트마다 `styles`라는 동일 변수명을 쓰면 충돌하므로 기존대로 `S`·`s`·`styles` 등 컴포넌트별 명명 유지
- 동작 변경 금지 — 색·라운드·간격만 토큰으로 교체. 로직·레이아웃·prop 시그니처는 그대로

### 작업 진행 방식 권장

한 PR에 1~3개 파일만 묶어 작업하세요. 각 파일은 시각적 회귀 없는지 직접 보고 확인. 한꺼번에 16개를 바꾸면 리뷰 불가능합니다.

### 첫 번째 작업 요청

먼저 **`App.tsx`** 부터 시작합니다. `dashboard/frontend/src/App.tsx`를 위 매핑표에 따라 마이그레이션해 주세요. 변경 후 페이지가 시각적으로 동일한지 확인하고, diff를 보여주세요.
