# 통합 백테스트 엔진 사용 가이드

`backtest_engine.py` 기준 (v0.7.3~ / compose 모드 v1.0.0~)

---

## 개요

5가지 모드로 과거 구간 백테스트를 실행합니다.

| 모드 | 내용 |
|------|------|
| `ichimoku` | 주봉 이치모쿠 7조건 walk-forward 재현 |
| `stage` | 일봉 Stage 1 가격 조건 재현 (5/5 조건, 수급 제외 기본) |
| `cross` | 이치모쿠 + Stage 1이 동일 ISO 주에 발동한 종목만 |
| `stage2` | Stage 1 신호 후 14일 이내 Stage 2 재진입 조건 재현 |
| `compose` | Tier-1 조합전략 (AND-gate / Composite Score / Funnel) — 백필 precompute 테이블 기반 |

출력 지표: 승률(7d/28d/91d), 평균·중앙값 수익률, KOSPI 초과수익률, 샤프비율(연환산), MDD(equity curve), 매도 신호 통계, S2/S3 단계 진행률, 종목별 MDD(91d), 업종

---

## 텔레그램 명령어

```
/backtest ichimoku 2025-01-01 2026-01-01
/backtest stage    2025-01-01 2026-01-01 KOSDAQ
/backtest cross    2025-01-01 2026-01-01
/backtest stage2   2025-01-01 2026-01-01 ALL --max 100

# compose 모드 (Tier-1 조합전략)
/backtest compose FUNNEL-1 2025-01-01 2026-06-14
/backtest compose ALL      2025-01-01 2026-06-14
```

형식: `/backtest <mode> <start> <end> [market] [--max N] [--tx-cost F]`

| 인수 | 기본값 | 설명 |
|------|--------|------|
| `mode` | 필수 | `ichimoku` \| `stage` \| `stage2` \| `cross` \| `compose` |
| `start` / `end` | 필수 | `YYYY-MM-DD` |
| `market` | `ALL` | `KOSPI` \| `KOSDAQ` \| `ALL` |
| `--max N` | `200` | 최대 티커 수 (0 = 전종목) |
| `--tx-cost F` | KRX 실비 | 왕복 거래비용 소수 (예: `0.0021`) |

### compose 모드 형식

```
/backtest compose <strategy> <start> <end> [market]
```

| 인수 | 기본값 | 설명 |
|------|--------|------|
| `strategy` | 필수 | `AND-1` ~ `AND-6` \| `SCORE-1` \| `FUNNEL-1` \| `ALL` \| `TXAMT` \| `RELAX` |
| `market` | `ALL` | `KOSPI` \| `KOSDAQ` \| `ALL` |

- `ALL`: 전체 전략 순차 실행 후 샤프28d 내림차순 비교표 전송
- `TXAMT`: AND-1/2/3/4 (거래량 vs 거래대금 비교셋)
- `RELAX`: AND-1/3/5/6 (stage2+ vs stage1+ 완화 비교셋)

DB 연결(DSN) 필수.

결과는 백테스트 완료 후 텍스트 요약 + HTML 리포트 경로로 전송됩니다.  
중복 실행 방지 Lock 내장 — 실행 중 재시도 시 "백테스트 실행 중" 안내.

---

## 거래비용 기본값 (KRX 2026)

| 항목 | 비율 |
|------|------|
| 매수 수수료 | 0.015% |
| 매도 수수료 | 0.015% |
| 증권거래세 | 0.05% |
| 농어촌특별세 | 0.15% |
| **왕복 합계** | **0.23%** |

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
| 5 | `foreign_net > 0` **OR** `inst_net > 0` (외인·기관 중 하나 이상 순매수, DSN 설정 시 적용) |

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
| S3-5 | `foreign_net > 0` **AND** `inst_net > 0` (외인·기관 동시 순매수) |

> S3-5는 `daily_flow` DB 데이터 기반. DSN 미설정 또는 해당 날짜 데이터 없으면 조건 생략(통과 처리).

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

---

## compose 모드 (Tier-1 조합전략)

### CLI

```bash
# 단일 전략
python scripts/run_compose.py --strategy AND-1 --start 2025-01-01 --end 2026-06-14

# 전체 Tier-1 비교표 (샤프28d 내림차순)
python scripts/run_compose.py --strategy ALL --start 2025-01-01 --end 2026-06-14

# HTML 리포트 저장
python scripts/run_compose.py --strategy FUNNEL-1 --start 2025-01-01 --end 2026-06-14 \
    --html results/funnel1.html --workers 8
```

### Tier-1 전략 목록

| 전략 | kind | 조건 | 신호수(25W01~26W24) | 샤프28d | 승률28d |
|------|------|------|---------------------|---------|---------|
| AND-1 | AND-gate | 이치모쿠 ∩ Stage2+ ∩ 수급 비동시매도 | ~8 | 1.75 | 80% |
| AND-2 | AND-gate | AND-1 ∩ 거래량 주내 중앙값 이상 | ~3 | — | 100% |
| AND-3 | AND-gate | AND-1 ∩ 거래대금 주내 중앙값 이상 (txamt_above_med_cs) | ~5 | N/A | 100% |
| AND-4 | AND-gate | AND-1 ∩ 거래대금 주내 상위 30% (txamt_top30_cs) | ~3 | — | — |
| AND-5 | AND-gate | 이치모쿠 ∩ Stage1+ ∩ 수급 비동시매도 (stage 완화) | ~30 | 0.34 | 48% |
| AND-6 | AND-gate | AND-5 ∩ 거래대금 주내 중앙값 이상 | ~22 | 0.41 | 46% |
| SCORE-1 | Composite Score | Stage·거래대금·수급 z-score top-20/주 | ~1500 | 1.17 | 67% |
| FUNNEL-1 | Funnel | 수급 스크린 → 4주 내 이치모쿠 트리거 | ~2009 | 0.74 | 67% |

> AND-3/4는 신호 수가 적어 샤프 계산 불가. AND-5/6은 신호 확대 목적의 실험적 전략으로 품질이 AND-1/3 대비 낮습니다(샤프·MDD 모두 열위).

### 플래그 정의 (derive_flags)

| 플래그 | 소스 | 정의 |
|--------|------|------|
| `ichimoku` | chart_signals | 주봉 7조건 통과 여부 |
| `stage2plus` | stage_classifications | stage >= 2 |
| `stage_any` | stage_classifications | stage >= 1 |
| `flow_pos` | daily_flow | (외국인 > 0) OR (기관 > 0) — 엄격 |
| `flow_loose` | daily_flow | NOT (외국인 < 0 AND 기관 < 0) — 완화 |
| `vol_above_med` | chart_signals | 주내 이치모쿠 통과 종목 거래량(volume_w) 중앙값 이상 |
| `txamt_above_med_cs` | chart_signals | `volume_w × close` (주간 거래대금) 주내 중앙값 이상 |
| `txamt_top30_cs` | chart_signals | 주간 거래대금 주내 70th percentile 이상 (상위 30%) |

> `txamt_*_cs` 플래그는 `chart_signals.volume_w × close_chart`로 산출하며 daily_ohlcv가 불필요합니다. 플래그 계산은 동일 ISO 주 내 신호 종목 간 cross-sectional 비교입니다.

### 데이터 제약

- `chart_signals`: 2025-W01 ~ 현재 (백필 완료, `jobs/screener_backfill.py` 재실행으로 갱신)
- `stage_classifications`: 2025-W01 ~ 현재 (백필 완료, `jobs/stage_backfill.py` 재실행으로 갱신)
- `daily_flow`: 2025-01-02 ~ 현재 (flow 의존 전략 하한)

### Python API (compose 모드)

```python
from datetime import date
from analysis.backtest_engine import BacktestConfig, run_backtest

cfg = BacktestConfig(
    mode="compose",
    strategy="FUNNEL-1",   # AND-1~6 | SCORE-1 | FUNNEL-1
    start=date(2025, 1, 1),
    end=date(2026, 6, 14),
    market="ALL",
    workers=8,
    dsn="postgresql://...",
)
result = run_backtest(cfg)
print(result.to_telegram_report())
result.to_html_report("reports/backtest/funnel1.html")
```

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

## 수급 조건 (daily_flow DB)

외인·기관 순매수 데이터는 yfinance가 아닌 `daily_flow` PostgreSQL 테이블에서 읽습니다.  
DSN이 설정된 환경에서만 동작하며, 해당 날짜 데이터가 없으면 조건을 생략(통과 처리)합니다.

| 모드 | 수급 조건 | 논리 |
|------|-----------|------|
| Stage 1 (조건 5) | 외인 순매수 > 0 **또는** 기관 순매수 > 0 | OR |
| S3 감지 (조건 5) | 외인 순매수 > 0 **그리고** 기관 순매수 > 0 | AND |

실제 live 분류기(`stage_classifier.py`)는 Naver Finance에서 수급 데이터를 수집하여 동일한 조건을 적용합니다.

> DSN 미설정 시 리포트 하단에 `"S3 수급 조건 제외 — daily_flow 없음"` 경고가 표시됩니다.
