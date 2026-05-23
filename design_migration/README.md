# 마이그레이션 패키지

이 폴더는 `vtradingDashboard/dashboard/frontend/`에 적용할 마이그레이션 산출물입니다.

## 파일 목록

| 파일 | 대상 경로 | 설명 |
|---|---|---|
| `tokens.ts` | `dashboard/frontend/src/tokens.ts` | **신규** — 디자인 토큰 단일 진실원천 + 헬퍼 함수 4개 |
| `Heatmap.tsx` | `dashboard/frontend/src/components/Heatmap.tsx` | **덮어쓰기** — 토큰화 + 한국식 등락 색상 적용 (텍스트는 KR, 셀은 heat scale 유지) |
| `Top.tsx` | `dashboard/frontend/src/components/Top.tsx` | **덮어쓰기** — 토큰화 (이미 한국식이라 색 로직은 변화 없음) |
| `CLAUDE_CODE_PROMPT.md` | (참조용) | 나머지 14개 컴포넌트 마이그레이션 시 Claude Code에 그대로 붙여넣는 프롬프트 |

## 적용 순서

1. `tokens.ts`를 먼저 추가 (의존성)
2. `Heatmap.tsx`·`Top.tsx`를 교체하고 시각 회귀 없는지 확인
3. PR을 분리해 머지 — 토큰 도입과 시범 컴포넌트 2개까지가 1차 PR
4. `CLAUDE_CODE_PROMPT.md`의 작업 지시를 사용해 나머지 14개 파일 마이그레이션 (1 PR 당 1~3개 파일)

## 주요 변경점

### Heatmap.tsx
- `changePctColor()` 함수 제거 → `heatCellColor()` (셀 배경, heat scale 유지) + `pctTextColor()` (툴팁·정보바 텍스트, 한국식)로 분리
- `stageBorderColor()` 함수 제거 → `stageColor()` 토큰 헬퍼로 통합
- 모든 인라인 16진수 → `tokens.*` 참조

### Top.tsx
- 기존 한국식 등락 색 로직 → `pctTextColor()` 헬퍼로 단순화
- 모든 인라인 16진수 → `tokens.*` 참조

## 검증

각 파일 교체 후 다음을 눈으로 확인:
- 히트맵 셀 색이 그대로 (그린/레드) 유지되는지
- 히트맵 툴팁·정보바의 등락률 텍스트가 빨강(상승)/파랑(하락)인지
- Top 테이블의 등락률 텍스트가 빨강(상승)/파랑(하락)인지
- Stage 보더 색(블루·퍼플·앰버)이 그대로인지
- 전체 톤이 시각적으로 동일한지

문제 없다면 1차 PR로 머지 → 다음 컴포넌트로 이동.
