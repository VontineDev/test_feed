# 종목명 해석 (Name Resolution) 가이드

## 문제 개요

종목명을 DB에서 가져올 때 여러 테이블을 순서대로 시도한다.
어떤 소스에도 이름이 없으면 티커 코드(`005930`)가 화면에 그대로 표시된다.
이 문서는 표준 패턴과 각 엔드포인트의 현황을 기록한다.

---

## 이름 소스 테이블 우선순위

| 우선순위 | 테이블 | 컬럼 | JOIN 키 | 비고 |
|---------|--------|------|---------|------|
| 1 | `ticker_names` | `name_ko` | `ticker = sc.ticker` | 수동 입력 / 정확도 최고 |
| 2 | `krx_listings` | `name_ko` | `yfinance_symbol = sc.ticker` | KRX 상장 종목 공식명 |
| 3 | `chart_signals` | `name` | `ticker = sc.ticker` (LATERAL) | 주봉 스크리너 저장명 |
| 4 | `youtube_mention_raw` | `stock_name_raw` | `ticker = yt.ticker` | 유튜브에서 추출한 이름 |
| 5 | ticker 코드 | — | — | **표시 금지** (버그 지표) |

---

## 표준 COALESCE 패턴

### 일반 엔드포인트 (ticker 원형 유지)

```sql
COALESCE(tn.name_ko, k.name_ko, cs.name, SPLIT_PART(sc.ticker, '.', 1)) AS name
```

```sql
LEFT JOIN ticker_names tn ON tn.ticker = sc.ticker
LEFT JOIN krx_listings k  ON k.yfinance_symbol = sc.ticker
LEFT JOIN LATERAL (
    SELECT name FROM chart_signals
    WHERE ticker = sc.ticker ORDER BY screened_at DESC LIMIT 1
) cs ON TRUE
```

### 정규화된 티커(`.KS` 제거) 환경 — unified 엔드포인트

CTE에서 모두 `SPLIT_PART(ticker, '.', 1)` 정규화 후 JOIN:

```sql
tn AS (SELECT SPLIT_PART(ticker, '.', 1) AS t, name_ko FROM ticker_names),
kl AS (SELECT SPLIT_PART(yfinance_symbol, '.', 1) AS t, name_ko FROM krx_listings),
mr AS (
    SELECT DISTINCT ON (ticker) ticker AS t, stock_name_raw
    FROM youtube_mention_raw ORDER BY ticker, created_at DESC
)

COALESCE(tn.name_ko, kl.name_ko, cs.name, mr.stock_name_raw, at.t) AS name
```

---

## 엔드포인트별 현황 (2026-06-12 기준)

| 엔드포인트 | ticker_names | krx_listings | chart_signals | youtube_raw | 비고 |
|-----------|:---:|:---:|:---:|:---:|------|
| `/api/report/unified` | O | O | O | O | 4단계 폴백 |
| `/api/report/stage` | O | O | O | — | 3단계 |
| `/api/history/stage` | O | — | O (LATERAL) | — | krx_listings 없음 |
| `/api/youtube/screener` | O | — | O | O | krx_listings 없음 |
| `/api/heatmap` (폴백) | O | O | — | — | 개선됨 |
| `/api/aftermarket` | O | O | — | — | 개선됨 |
| `paper_positions` 계열 | O | O | O | — | 3단계 |

---

## Python 레벨 최종 안전망

SQL COALESCE가 ticker 코드를 반환한 경우에도 Python에서 한 번 더 가드:

```python
"name": r["name"] or r["ticker"],
```

이 패턴은 모든 items 딕셔너리 생성 시 반드시 포함한다.

---

## 신규 엔드포인트 체크리스트

새 API에서 종목명을 반환할 때 아래를 확인:

- [ ] `ticker_names` LEFT JOIN 포함
- [ ] `krx_listings` LEFT JOIN 포함 (`yfinance_symbol = ticker`)
- [ ] COALESCE에 최소 2개 소스 이상
- [ ] 최종 폴백이 `SPLIT_PART(ticker, '.', 1)` (`.KS`/`.KQ` 제거)
- [ ] Python 딕셔너리에 `r["name"] or r["ticker"]` 패턴

---

## 티커 코드 노출 탐지 방법

`ticker_names` 미수록 종목을 찾으려면:

```sql
-- unified 뷰에서 이름이 티커 코드와 같은 행 (= 폴백 발생)
SELECT ticker, name
FROM (
    SELECT at.t AS ticker,
           COALESCE(tn.name_ko, kl.name_ko, cs.name, mr.stock_name_raw, at.t) AS name
    FROM ...
) sub
WHERE name ~ '^[0-9]{6}$';  -- 6자리 숫자 = 티커 코드
```

또는 백엔드 로그에서 `name == ticker` 패턴을 모니터링:

```python
for item in items:
    if item["name"] == item["ticker"]:
        logger.warning("[name_resolution] 종목명 미확인 ticker=%s", item["ticker"])
```

---

## 발생 이력

| 날짜 | 현상 | 원인 | 수정 |
|------|------|------|------|
| 2026-06-12 | unified 뷰 일부 종목 티커 코드 표시 | `krx_listings`, `youtube_mention_raw` 누락 | `kl`/`mr` CTE 추가 |
| 2026-06-12 | aftermarket 뷰 동일 문제 | `krx_listings` JOIN 누락 | LEFT JOIN 추가 |
