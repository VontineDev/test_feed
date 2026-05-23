## 디자인 컨텍스트 — vtradingDashboard

스타일: 미니멀 다크. 한국 주식 시장 트레이딩 대시보드.
폰트: 시스템 sans (또는 IBM Plex Sans). 숫자는 항상 tabular-nums + monospace.

### 색 (절대 새로 만들지 말 것)
배경:    bg/0 #0f1117 · bg/1 #1a1d2e · bg/2 #1e293b · bg/3 #0f172a
보더:    bd/1 #1e293b · bd/2 #334155
텍스트:  tx/0 #fff · tx/1 #94a3b8 · tx/2 #64748b · tx/3 #475569
액센트:  blue #3b82f6 · purple #a78bfa · amber #f59e0b

### 등락 (한국식 — 고정. 절대 뒤집지 말 것)
up   #f87171  (빨강, change_pct > 0)
down #60a5fa  (파랑, change_pct < 0)
flat #64748b  (회색, change_pct == 0)

### Stage 색 (전역 통일)
stage1 #3b82f6  · stage2 #a78bfa  · stage3 #f59e0b

### 차트 — 모델별 카테고리컬 (새 색 만들지 말 것)
stage    #3b82f6  ·  kosdaq   #a78bfa
cross    #f97316  ·  ichimoku #4ade80

### 차트 — 점수 임계치
≥ +20  positive #22c55e
-20~+20 neutral  #f59e0b
≤ -20  negative #ef4444
p-value 표시:    #fbbf24

### 차트 크롬 (Recharts 공용)
XAxis/YAxis tick: fontSize 9, fill #475569
grid: stroke #1e293b
Tooltip: bg #1e293b, border 1px #334155, fontSize 11
Line 실제값: strokeWidth 1.5, dot=false
Line 예측/미실현: strokeWidth 1, strokeDasharray="4 2"

### 컴포넌트 패턴
- 헤더: bg-1 · padding 10px 20px · border-bottom 1px bd-1
- 탭: padding 10px 22px · 활성 = #93c5fd + 2px #3b82f6 underline
- 버튼: bg-2 · 1px bd-2 · radius 4 · min 36×36
- 테이블: 12px · 셀 padding 4–6px 8px · 숫자 칼럼 우측 + tabular-nums
- 차트(Recharts): 전역 chartTheme 첨부 · 새 색 만들지 말고 chart.cat.*에서만 선택
- 트리맵: 보더 색 = stage·셀 색 = changePct
- 툴팁: bg-2 · 1px bd-2 · radius 6 · shadow 0 4px 12px rgba(0,0,0,.4)
- 빈 상태: SVG(opacity 0.35) + 굵은 1줄 + 가는 힌트 1줄
- 로딩: "…" 텍스트 · 스피너 ✕
- 에러: 인라인 빨강 텍스트 11–12px

### 레이아웃
- 컴포넌트 루트는 flex-column · height 100%
- 모바일에서 컴포넌트 자체 스크롤 금지 (.app-mobile-layout이 스크롤)
- 데스크탑 사이드바는 340px 고정

### 새 화면을 만들 땐
1) Heatmap.tsx 또는 Top.tsx 패턴을 그대로 본떠 시작
2) tokens 외 색·치수를 추가하지 말 것
3) 추가가 꼭 필요하면 PR 설명에 사유 명시