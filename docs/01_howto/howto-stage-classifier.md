# 3단계 분류기 설정 가이드

매일 16:30 KST에 KOSPI + KOSDAQ 전 종목을 Stage 1 / Stage 2 / Stage 3 / None으로 분류합니다. 주봉 Ichimoku 스크리너와 독립된 시스템입니다.

---

## Stage 정의

| Stage | 명칭 | 조건 (요약) |
|-------|------|------------|
| **Stage 1** | 랠리 초입 | 당일 +5%(KOSPI) / +7%(KOSDAQ) 이상 상승. MA20 > MA20(5일 전). RSI ≥ 50. 거래량 증가. |
| **Stage 2** | 중간 조정·재매집 | Stage 1 발동 후 14일 이내. 고점 대비 -5%~-20% 구간. MA20 근처 지지. 거래량 30~60% 수준. |
| **Stage 3** | 과열 재가속 | Stage 2 이후 재상승. 고점 이탈 신호 동시 감지 가능. |

Stage 분류는 우선순위 **Stage 3 > Stage 2 > Stage 1** 순으로 적용됩니다.

---

## 전제 조건

- `stage_classifications` 테이블 (`db.py init_db()`로 자동 생성)
- `daily_flow` 테이블 — 수급 streak 데이터 (없어도 동작하지만 Stage 2 Condition 4 skip)
- `SCREENER_WORKERS` 설정 — 분류기가 같은 워커 수를 사용

---

## 실행

### 자동 실행 (매일 16:30 KST)

스케줄러 실행 중이면 자동 실행됩니다.

```bash
python run_scheduler.py
```

### 즉시 실행

```bash
python run_scheduler.py --once stage
```

---

## 티커 캡 설정

기본값으로 일봉 분류기는 최대 150종목을 처리합니다. Ichimoku 주봉 통과 종목은 이 한도에 관계없이 항상 포함됩니다.

```env
# .env
DAILY_CLASSIFIER_TICKERS=150   # 기본값
```

### 캡 확장 절차

1. **2주 이상 로그에서 fetch latency 확인:**

```bash
grep "\[일봉\].*API 수집" logs/run.log | tail -50
```

2. **p99 latency < 0.5초이면 캡 확장:**

```env
DAILY_CLASSIFIER_TICKERS=300
```

3. **5회 연속 데드라인(17:00 KST) 이내 완료 확인 후 유지.**

| 캡 | 예상 소요 (SCREENER_WORKERS=8) |
|----|-------------------------------|
| 150 | ~3분 |
| 300 | ~6분 |
| 2770 (전종목) | ~50분 |

---

## 결과 확인

### 오늘 분류 결과

```sql
SELECT stage, COUNT(*) AS cnt
FROM stage_classifications
WHERE classified_date = CURRENT_DATE
GROUP BY stage
ORDER BY stage;
```

### Stage 1 진입 종목

```sql
SELECT ticker, classified_date, s1_high, s1_volume
FROM stage_classifications
WHERE stage = 1
  AND classified_date >= CURRENT_DATE - 7
ORDER BY classified_date DESC;
```

### Stage 2 전환 종목 (최근 3일)

```sql
SELECT DISTINCT s.ticker
FROM stage_classifications s
WHERE s.stage = 2
  AND s.classified_date >= CURRENT_DATE - 3
  AND EXISTS (
    SELECT 1 FROM stage_classifications s1
    WHERE s1.ticker = s.ticker AND s1.stage = 1
      AND s1.classified_date BETWEEN s.classified_date - 14 AND s.classified_date - 1
  );
```

---

## 뉴스 게이팅 연동

일봉 분류기 완료 후 `_active_stage_tickers` 캐시가 자동 갱신됩니다. 이 캐시는 최근 7일 이내 Stage 1/2/3 분류 종목 집합입니다.

- Ichimoku 주봉 스크리너 미통과 종목이라도 Stage 활성이면 뉴스 신호가 Telegram으로 전달됩니다.
- 스케줄러 재시작 시 DB에서 캐시를 자동 복구합니다.

자세한 내용은 [신호 파이프라인 설계 해설](explanation-signal-pipeline.md)을 참고하세요.

---

## Telegram 알림

분류기 실행 완료 후 자동 발송:

- **Stage 분류 비교** — 오늘 Stage 1/2/3 종목과 Ichimoku 스크리너 교차 결과
- **Stage 1→2 전환 알림** — 해당 종목 즉시 알림 (워치리스트 일보와 별도)
- **고점 이탈 경고** — Stage 3 발동 종목 중 고점 이탈 신호 감지 시

---

## 백테스트

```bash
# Stage 1 조건 백테스트
/backtest stage 2025-01-01 2026-01-01

# Stage 2 조건 백테스트
/backtest stage2 2025-01-01 2026-01-01
```

---

## 트러블슈팅

**`[3단계] OHLCV 수집: 50/150종목`처럼 수집 수가 낮은 경우:**
- yfinance 일봉 60일치 조회 실패. 최근 상장 종목이나 거래 정지 종목에서 발생합니다.
- `SCREENER_WORKERS`를 줄이면 rate limit 압박이 줄어들 수 있습니다.

**분류기가 데드라인(17:00 KST) 전에 완료되지 않는 경우:**
1. `SCREENER_WORKERS` 증가
2. `DAILY_CLASSIFIER_TICKERS` 감소 (150이 기본, 100으로 낮춰서 확인)

**`stage_classifications` 테이블이 없는 경우:**
```bash
python -c "import asyncio; from db import init_db; asyncio.run(init_db(None))"
```
또는 스케줄러를 한 번 실행하면 `init_db()`가 자동 호출됩니다.

---

## 관련 문서

- [reference-env-vars.md](reference-env-vars.md) — `DAILY_CLASSIFIER_TICKERS`, `SCREENER_WORKERS`
- [howto-watchlist.md](howto-watchlist.md) — Stage 1 이후 워치리스트 추적
- [howto-screener.md](howto-screener.md) — 주봉 Ichimoku 스크리너 (교차 시스템)
- [explanation-signal-pipeline.md](explanation-signal-pipeline.md) — 게이팅 동작 원리
- [howto-backtest.md](howto-backtest.md) — Stage 백테스트 가이드
