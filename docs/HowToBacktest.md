# 통합 백테스트 엔진 사용 가이드

`backtest_engine.py` 기준 (v0.7.3~)

---

## 개요

4가지 모드로 과거 구간 백테스트를 실행합니다.

| 모드 | 내용 |
|------|------|
| `ichimoku` | 주봉 이치모쿠 7조건 walk-forward 재현 |
| `stage` | 일봉 Stage 1 가격 조건 재현 (5/5 조건, 수급 제외 기본) |
| `cross` | 이치모쿠 + Stage 1이 동일 ISO 주에 발동한 종목만 |
| `stage2` | Stage 1 신호 후 14일 이내 Stage 2 재진입 조건 재현 |

출력 지표: 승률(7d/28d/91d), 평균·중앙값 수익률, KOSPI 초과수익률, 샤프비율(연환산), MDD(equity curve), 매도 신호 통계, S2/S3 단계 진행률, 종목별 MDD(91d), 업종

---

## 텔레그램 명령어

```
/backtest2 ichimoku 2025-01-01 2026-01-01
/backtest2 stage    2025-01-01 2026-01-01 KOSDAQ
/backtest2 cross    2025-01-01 2026-01-01
/backtest2 stage2   2025-01-01 2026-01-01 ALL --max 100
```

형식: `/backtest2 <mode> <start> <end> [market] [--max N] [--tx-cost F]`

| 인수 | 기본값 | 설명 |
|------|--------|------|
| `mode` | 필수 | `ichimoku` \| `stage` \| `stage2` \| `cross` |
| `start` / `end` | 필수 | `YYYY-MM-DD` |
| `market` | `ALL` | `KOSPI` \| `KOSDAQ` \| `ALL` |
| `--max N` | `200` | 최대 티커 수 (0 = 전종목) |
| `--tx-cost F` | KRX 실비 | 왕복 거래비용 소수 (예: `0.0021`) |

결과는 백테스트 완료 후 텍스트 요약 + HTML 리포트 경로로 전송됩니다.  
중복 실행 방지 Lock 내장 — 실행 중 재시도 시 "백테스트 실행 중" 안내.

---

## 거래비용 기본값 (KRX 2025)

| 항목 | 비율 |
|------|------|
| 매수 수수료 | 0.014% |
| 매도 수수료 | 0.014% |
| 증권거래세 | 0.180% |
| 농어촌특별세 | 0.002% |
| **왕복 합계** | **0.210%** |

---

## 신호 재현 로직

### ichimoku 모드 (주봉 7조건)

일봉 → 주봉(W-FRI 리샘플) 후 조건 확인. 선행스팬B 계산에 52주 필요해 데이터 62주 미만이면 스킵.

| 조건 | 내용 |
|------|------|
| A | `close > cloud_top` (구름 상향 돌파) |
| B | `prev_close ≤ prev_cloud_top` (전 주 구름 내/하부에서 돌파) |
| C | `close > MA20w` |
| D | `close > MA60w` |
| E | `MA20w > prev_MA20w` (우상향) |
| F | `MA60w > prev_MA60w` |
| G | `close > MA120w` (데이터 부족 시 통과) |

### stage 모드 (일봉 Stage 1)

| 조건 | 내용 |
|------|------|
| 1 | 일일 상승률 ≥ 5%(KOSPI) / 7%(KOSDAQ) |
| 2 | 거래량 ≥ 2× 20일 평균 |
| 3 | `close > MA20` AND `close > MA60` |
| 4 | 52주 고점 대비 괴리율 ≤ 20% |
| 5 | 외인+기관 순매수 양수 (DSN 설정 + daily_flow 있을 때만 적용) |

### stage2 모드 (Stage 2 재진입)

Stage 1 신호 발생 후 14일 이내, 아래 3조건 동시 충족 첫째 날을 Stage 2 진입일로 기록.

| 조건 | 내용 |
|------|------|
| C1 | `S1 종가 × 0.80 ≤ close ≤ S1 종가 × 0.95` (−5% ~ −20% 되돌림) |
| C2 | `close ≥ MA20 × 0.95` (MA20 5% 이내) |
| C3 | `0.30 ≤ 당일 거래량 / S1 거래량 ≤ 0.60` (저거래량 조정) |

### cross 모드

ichimoku 신호와 stage 신호가 동일 ISO 주에 발동한 종목. 해당 주의 stage 신호 첫 발생일을 기준 날짜로 사용.

---

## 매도 신호 계산

모든 모드에서 신호 발생 이후 일봉/주봉을 스캔하여 최초 매도 조건을 기록합니다.

### ichimoku 모드 — 주봉 매도 (우선순위 순)

| 순위 | 조건 | 기록값 |
|------|------|--------|
| 1 | `close ≤ 진입가 × (1 − 8%)` | `손절 -8%` |
| 2 | `close < cloud_bottom` (구름 하향 이탈) | `구름 이탈` |
| 3 | 전환선이 기준선 아래로 하향 돌파 (데드크로스) | `전환<기준 DC` |

### stage / stage2 / cross 모드 — 일봉 매도 (우선순위 순)

| 순위 | 조건 | 기록값 |
|------|------|--------|
| 1 | `close ≤ 진입가 × (1 − 8%)` | `손절 -8%` |
| 2 | `close < MA20` (20일 이동평균 이탈) | `MA20 이탈` |

매도 신호가 없으면 `보유 중`으로 표시.

---

## S2 / S3 단계 진행 감지

`stage` 모드 신호에 한해 단계 진행일을 추적합니다.

- **S2 감지**: S1 신호일 +14일 이내 C1·C2·C3 동시 충족 → `s2_date` 기록
- **S3 감지**: S2 이후, 아래 4조건 동시 충족 첫째 날 → `s3_date` 기록

| 조건 | 내용 |
|------|------|
| S3-1 | `close > 직전 10일 High` (10일 고점 돌파) |
| S3-2 | 일일 상승률 ≥ +5% |
| S3-3 | RSI(14) ≥ 70 (Wilder EMA 방식) |
| S3-4 | 거래량 ≥ 1.5× 30일 평균 |

> 외인+기관 순매수(S3 원래 조건 5번)는 yfinance 데이터 미지원으로 생략.

---

## 종목별 MDD(91d)

신호 발생일 기준 91일간 일봉을 추적해 각 종목의 최대낙폭을 계산합니다.  
리포트에 `MDD(91d)` 컬럼으로 표시되며, 집계 지표 `avg_mdd_91d`로도 확인 가능합니다.

---

## 사용자 지정 보유 기간 (hold_weeks)

`BacktestConfig(hold_weeks=N)` 설정 시 N주(N×7일) 후 종가 기준 수익률을 추가 계산합니다.

```python
cfg = BacktestConfig("ichimoku", date(2025, 1, 1), date(2026, 1, 1), hold_weeks=8)
```

집계 결과에 `win_rate_custom`, `avg_return_custom`, `sharpe_custom` 등이 추가됩니다.

---

## HTML 리포트

`BacktestResult.to_html_report(path)` 호출 시 생성. 텔레그램 봇은 `reports/backtest/backtest_{mode}_{ts}.html` 경로에 자동 저장합니다.

### 컬럼 목록 (정렬 가능)

| 컬럼 | 설명 |
|------|------|
| 신호일 | 신호 발생 날짜 |
| 종목명 | 한글 종목명 |
| 업종 | KIND 업종 분류 |
| 티커 | KRX 티커 (예: `005930.KS`) |
| 단계 | 신호 모드 (이치모쿠/3단계/교차/Stage2) |
| 진입가 | 신호 발생 시 종가 |
| 7d / 28d / 91d | 보유 기간별 수익률 |
| 초과(28d) | KOSPI 대비 초과 수익률 |
| S2진행일 | Stage 2 전환 확인일 |
| S3진행일 | Stage 3 재가속 확인일 |
| 매도신호 | 매도 사유 (MA20 이탈 / 손절 / 구름 이탈 / 전환<기준 DC / 보유 중) |
| 매도일 | 실제 매도 날짜 |
| 보유일 | 신호~매도일 달력일수 |
| 매도수익 | 매도 시점 수익률 (거래비용 차감) |
| MDD(91d) | 진입 후 91일 최대낙폭 |

---

## Python API

```python
from datetime import date
from backtest_engine import BacktestConfig, run_backtest

cfg = BacktestConfig(
    mode="ichimoku",
    start=date(2019, 1, 1),
    end=date(2026, 5, 9),
    market="ALL",
    max_tickers=0,      # 0 = 전종목
    workers=8,
    hold_weeks=None,    # None = 표준 1/4/13주
)

result = run_backtest(cfg)
print(result.to_text_report())
result.to_html_report("reports/backtest/my_report.html")
```

---

## 수급 조건 생략 안내

`stage` 모드는 기본적으로 4/5 조건(가격·거래량·MA·52주 고점)만 재현합니다.  
조건 5(외국인·기관 순매수)는 yfinance에서 과거 데이터를 제공하지 않아 생략됩니다.  
결과 리포트에 이 사실이 경고로 표시됩니다.  
실제 live 분류기(`stage_classifier.py`)는 Naver Finance에서 수급 데이터를 수집하여 5조건 모두 적용합니다.
