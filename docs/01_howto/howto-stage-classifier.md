# 3단계 분류기 설정 가이드

매일 16:30 KST에 KOSPI + KOSDAQ 전 종목을 Stage 1 / Stage 2 / Stage 3 / None으로 분류합니다. 주봉 Ichimoku 스크리너와 독립된 시스템입니다.

---

## Stage 정의

Stage 분류는 우선순위 **Stage 3 > Stage 2 > Stage 1** 순으로 적용됩니다.

구현체는 `analysis/stage_classifier.py`의 `classify_stage_v15`이며, 2026-06-22부터
`jobs/stage_job.py`(라이브 일별 분류)와 `jobs/stage_backfill.py`(과거 이력 백필)
양쪽 모두 이 함수를 호출합니다. v1.0~v1.4는 동일 파일에 남아있는 백테스트 비교용
구버전(`/backtest stage_v11` 등)이며 운영에는 쓰이지 않습니다.

### Stage 1 — 랠리 초입

**의미:** 강한 양봉 + 거래대금 폭증 + 수급 유입이 처음 나타나는 지점.

| 축 | 조건 |
|----|------|
| **가격·기술** | 당일 상승률 ≥ +5%(KOSPI) / +7%(KOSDAQ). close > MA20 AND close > MA60. RSI(14) ≥ 50. 52주 고점 대비 괴리율 0~20% 구간. 종가 > 직전 20일(당일 제외) 최고가 — 박스권 상단 돌파. |
| **거래대금·거래량** | 당일 거래대금 ≥ 20일 평균의 200% **AND** ≥ 30일 평균의 200% (두 조건 모두 통과해야 함). |
| **수급** | (당일 외국인 순매수 > 0 AND 기관 순매수 ≥ 0) **또는** 기관 순매수 3일 이상 연속. 최근 3거래일 외인+기관 합산 순매수량 ≥ 유통주식수의 0.2% (`krx_listings.listed_shares` 없으면 이 조건은 스킵). |

---

### Stage 2 — 중간 조정·재매집

**의미:** 급등 이후 가격은 쉬면서 기관·외인이 물량을 유지하거나 추가 매집하는 구간.

**전제:** 직전 14일 이내 Stage 1 발동 기록 있어야 함.

| 축 | 조건 |
|----|------|
| **가격·기술** | 현재가가 Stage 1 고점 대비 -5%~-20% 구간. close ≥ MA20×0.95. 일일 고저폭이 직전 20일 평균 고저폭의 70% 이하로 축소. |
| **거래대금·거래량** | 거래대금이 Stage 1 급등일 대비 30~60% 수준. 20일 평균 대비 100~150% 유지. |
| **수급** | 기관 순매수 streak ≥ 0 (음수면 탈락). 외국인 14일 누적 순매수 ≥ −유통주식수의 1% (그 이상 이탈 시 탈락, listed_shares 없으면 스킵). 개인: `personal_net ≤ 0` 하드 게이트 ("개인 출회 + 기관 받는" 패턴이 양수면 탈락). |

---

### Stage 3 — 과열 재가속

**의미:** 조정 박스 상단 또는 52주 고점을 돌파하면서 거래대금·모멘텀·수급이 재폭발하는 구간.

**전제:** 직전 14일 이내 Stage 2 발동 기록이 있어야 함(`get_stage2_history`). 라이브/백필 모두 이 이력을 항상 전달하므로 실제로 강제됩니다.

| 축 | 조건 |
|----|------|
| **가격·기술** | 직전 10일 고가(당일 제외) 또는 52주 고가 돌파. 일일 상승률 ≥ +5%. RSI(14) ≥ 70. |
| **거래대금·거래량** | 거래대금 ≥ 30일 평균의 150%. |
| **수급** | 외국인·기관 모두 당일 순매수 **AND** 모두 streak ≥ 2(2거래일 이상 연속 순매수). 최근 3거래일 합산 외인+기관 순매수량 ≥ 유통주식수의 0.2% (listed_shares 없으면 스킵). |
| **피크아웃 신호** | 외국인·기관 동시 2~3일 이상 연속 순매도 전환, 또는 윗꼬리 긴 음봉 + 거래대금 급증. 해당 시 보유 비중 축소. |

---

## 공통 필요 데이터

분류기가 매일 수집·계산하는 데이터 목록입니다. 어떤 Stage 판단에 어느 데이터가 쓰이는지 매핑합니다.

| 데이터 | 항목 | 사용 Stage |
|--------|------|------------|
| **가격·차트** | 최근 60일 이상 시가·고가·저가·종가 | 1·2·3 |
| | MA5·MA10·MA20·MA60 | 1·2·3 |
| | 52주 고점·저점, 현재가의 52주 고점 대비 위치(%) | 1·3 |
| **거래량·거래대금** | 일별 거래량, 20·60일 평균 거래량 | 1·2·3 |
| | 일별 거래대금, 20·60일 평균 거래대금 | 1·2·3 |
| **수급** | 일별 외국인·기관·개인 순매수(수량·금액) | 1·2·3 |
| | 외국인·기관 연속 순매수/순매도 일수 (foreign_streak, inst_streak) | 1·2·3 + 피크아웃 |
| | 유통주식수 (`krx_listings.listed_shares`) | 1·2·3 (0.2%/1% 임계값 계산용) |
| | 외국인 지분율 변화 — 14일 누적 외국인 순매수 / 상장주식수 (`foreign_chg_14d_pct`) | 2 (필터). 1·3은 저장만 하고 분류에는 미사용 |
| | 수급 컨빅션 점수 (`flow_score`, -1.0 ~ +1.0) | 분류 자체엔 미사용 — 워치리스트 확신도 정렬에만 사용 |
| **시장·섹터** | KOSPI·KOSDAQ 지수의 52주 고점 대비 위치 | 참고 |
| | 섹터 평균 수익률·수급 동향 (`sector_daily_stats`) | 참고 |

수급 데이터는 `daily_flow` 테이블에서 읽습니다. 해당 테이블이 없으면 수급 조건을 skip하고 가격·거래대금 조건만으로 분류합니다.

---

## 매수/매도 전략

Stage 분류는 진입·관리·청산 전략을 결정하는 기준입니다. 아래는 각 Stage별 기본 가이드라인입니다.

### Stage 1 — 공격적 추세추종

| 구분 | 기준 |
|------|------|
| **진입** | 강한 양봉+거래대금 폭증+수급 유입 첫 날 또는 직후. 추격 리스크가 크므로 분할 진입. |
| **손절** | 전일 저가 또는 MA5 이탈 시 빠른 손절. |
| **익절** | 단기 목표: Stage 1 고점 대비 +10~20% 구간에서 부분 익절. Stage 2 조정 진입 가능성 염두에 두고 전량 청산보다 일부 유지. |

### Stage 2 — 보수적 눌림 매수·추가 매집

| 구분 | 기준 |
|------|------|
| **진입** | Stage 1 이후 5~20% 조정 구간. MA20 부근(또는 박스 하단) + 거래량 감소 + 기관 순매수 유지 확인 후 분할 매수. |
| **손절** | MA20 명확 이탈 + 거래량 증가 + 기관·외인 동반 매도 시 손절. |
| **익절** | Stage 3 재가속 진입 시, 조정 구간 고점 또는 52주 고점 돌파 이후 +10~20% 구간에서 부분 익절. |

### Stage 3 — 익절·비중 축소 중심

| 구분 | 기준 |
|------|------|
| **신규 진입** | 일반적으로 비추천. 돌파 초반+거래대금 폭발 시 소량 단타 포지션만 제한적으로. |
| **손절** | 돌파 실패(가격이 돌파선 아래로 재진입) + 거래량 동반 시 즉시 손절. |
| **익절 (기존 보유)** | 외인·기관 동반 순매도 전환, 윗꼬리 긴 음봉+거래대금 급증, RSI 과열+가격 둔화 조합 시 강하게 비중 축소. |

### 수급 기반 타이밍 미세조정

분류기는 매일 두 가지 수급 파생 지표를 `stage_classifications`에 함께 저장합니다.

**flow_score (-1.0 ~ +1.0)**

| 조건 | 점수 |
|------|------|
| f_streak ≥ 3 AND i_streak ≥ 3 | +1.0 |
| f_streak ≥ 3 OR i_streak ≥ 3 | +0.5 |
| f_streak ≤ -2 OR i_streak ≤ -2 | -0.5 |
| f_streak ≤ -2 AND i_streak ≤ -2 | -1.0 |
| 오늘 거래대금 > 20일 평균 × 1.2 (보너스) | +0.3 |

워치리스트 확신도 정렬이 이 점수를 사용합니다 (`확신도 = vol_ratio + flow_score × 0.2`).

**foreign_chg_14d_pct**

14일 누적 외국인 순매수를 상장주식수로 나눈 지분율 변화 근사값입니다.  
음수(-1% 이하)이면 외국인이 지분을 축소하는 구간으로 해석합니다.

- **매수 가중치 증가:** flow_score ≥ +0.5 (외국인+기관 동시 3일 이상 순매수) + 거래대금 모멘텀.
- **매도 신호:** flow_score ≤ -0.5 또는 foreign_chg_14d_pct ≤ -0.01(−1%p) 시 비중 축소 검토.

---

## 전제 조건

- `stage_classifications` 테이블 (`db.py init_db()`로 자동 생성)
  - 컬럼: `ticker`, `classified_date`, `stage`, `s1_entry_date`, `s1_high`, `s1_volume`, `s1_txamt`, `peakout_flag`, `foreign_chg_14d_pct`, `flow_score`
- `daily_flow` 테이블 — 수급 streak·personal_net 데이터 (없어도 동작하지만 수급 조건 다수 skip)
- `krx_listings` 테이블 — `listed_shares` 컬럼 (Stage 1·3의 0.2% 합산 순매수 조건, Stage 2의 -1% 외국인 이탈 조건, foreign_chg_14d_pct 계산에 필요. 없으면 해당 조건만 skip)
- `sector_daily_stats` 테이블 — 섹터별 일별 수급·수익률 집계 (`init_db()`로 자동 생성)
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
SELECT ticker, classified_date, s1_high, s1_volume, flow_score, foreign_chg_14d_pct
FROM stage_classifications
WHERE stage = 1
  AND classified_date >= CURRENT_DATE - 7
ORDER BY classified_date DESC, flow_score DESC NULLS LAST;
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

### 섹터 일별 통계

매일 19:00 KST에 `sector_stats_job`이 실행되어 섹터별 수급·수익률을 집계합니다.

```sql
-- 오늘 섹터별 수급 현황 (stage1 종목 많고 flow_score 높은 순)
SELECT sector, ticker_count, avg_return_pct, foreign_net_sum, avg_flow_score,
       stage1_count, stage2_count, stage3_count
FROM sector_daily_stats
WHERE trade_date = CURRENT_DATE
ORDER BY avg_flow_score DESC NULLS LAST, stage1_count DESC;
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
