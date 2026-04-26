# 통합 백테스트 엔진 사용 가이드

`backtest_engine.py` + `run_backtest.py` 기준 (v0.7.0.0~)

---

## 개요

3가지 모드로 과거 구간 백테스트를 실행합니다.

| 모드 | 내용 |
|------|------|
| `ichimoku` | 주봉 Ichimoku 7조건 walk-forward 재현 |
| `stage` | 일봉 Stage 1 가격 조건 재현 (4/5 조건, 수급 제외) |
| `cross` | 이치모쿠 + Stage 1이 동일 ISO 주에 발동한 종목만 |

출력 지표: 승률(7d/28d/91d), 평균·중앙값 수익률, KOSPI 초과수익률, 샤프비율(연환산), MDD(equity curve)

---

## CLI 사용법

```bash
# 이치모쿠 모드 (전종목, 1년)
python run_backtest.py --mode ichimoku --start 2025-01-01 --end 2026-01-01

# Stage 1 모드 (KOSDAQ만, 최대 50종목)
python run_backtest.py --mode stage --start 2025-01-01 --end 2026-01-01 --market KOSDAQ --max 50

# 교차 모드 (거래비용 사용자 정의)
python run_backtest.py --mode cross --start 2025-01-01 --end 2026-01-01 --tx-cost 0.0025

# 전체 옵션 보기
python run_backtest.py --help
```

### 주요 파라미터

| 파라미터 | 기본값 | 설명 |
|----------|--------|------|
| `--mode` | (필수) | ichimoku / stage / cross |
| `--start` | (필수) | 백테스트 시작일 (YYYY-MM-DD) |
| `--end` | (필수) | 백테스트 종료일 (YYYY-MM-DD) |
| `--market` | ALL | KOSPI / KOSDAQ / ALL |
| `--max` | 200 | 최대 종목 수 (0 = 전종목, 수십 분 소요) |
| `--tx-cost` | 0.0021 | 왕복 거래비용 (소수점, 0.0021 = 0.21%) |
| `--rf` | 0.03 | 무위험수익률 연환산 (샤프비율 계산용) |

---

## 텔레그램 명령어

```
/backtest2 ichimoku 2025-01-01 2026-01-01
/backtest2 stage 2025-01-01 2026-01-01 KOSDAQ
/backtest2 cross 2025-01-01 2026-01-01
```

형식: `/backtest2 <mode> <start> <end> [market]`

결과는 백테스트 완료 후 자동 전송됩니다. 전종목 기준 약 5~20분 소요.
중복 실행 방지 Lock 내장 — 실행 중 재시도 시 "백테스트 실행 중" 안내 메시지.

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

## 수급 조건 생략 안내

`stage` 모드는 4/5 조건(가격·거래량·MA·52주 고점)만 재현합니다.
조건 5(외국인·기관 순매수)는 yfinance에서 과거 데이터를 제공하지 않아 생략됩니다.
결과 리포트에 이 사실이 경고로 표시됩니다.
실제 live 분류기(`stage_classifier.py`)는 Naver Finance에서 수급 데이터를 수집하여 5조건 모두 적용합니다.

---

## 거래대금(거래대금) 근사 검증

Stage 2 backtest를 위해 `compare_tx_amt.py`로 거래대금 근사 오차를 검증할 수 있습니다.

```bash
python compare_tx_amt.py                   # 기본 10종목 검증
python compare_tx_amt.py 005930 000660     # 특정 종목
```

검증 결과: yfinance `Volume × Close`는 Naver Finance 실제 거래대금 대비 평균 1.4% 오차 (최대 3.6%).
Stage 2 비율 체크(30~60% 밴드)에서 양쪽 모두 동일 근사식 사용 시 오차 상쇄 — 실사용 허용 범위.
