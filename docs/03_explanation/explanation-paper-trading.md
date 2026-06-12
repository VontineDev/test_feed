# 모의투자 파이프라인 설계 해설

키움 모의투자 시스템이 왜 3-잡 구조로 나뉘어 있는지, 가격 소스를 어떻게 분리하는지, exit 조건이 어떤 상태 머신으로 동작하는지를 설명한다.

---

## 문제: 주문과 가격 조회는 다른 서버가 필요하다

키움증권은 **실 API**(`api.kiwoom.com`)와 **모의투자 API**(`mockapi.kiwoom.com`)를 분리 운영한다. 모의투자 서버는 주문 체결(kt10000/kt10001)과 계좌 조회(kt00018)만 지원하고, **시장 데이터 API(ka10001 — 현재가 조회)는 지원하지 않는다.**

초기 설계에서는 `paper_exit_checker_job`이 `KiwoomPaperTrader.get_current_price()`를 호출해 exit 조건을 판단했다. 이 함수는 내부에서 `KiwoomClient(use_mock=True)`를 사용하므로 모의투자 서버의 ka10001 엔드포인트를 호출한다. 결과는 항상 실패였고, 매일 23개 포지션 전체가 스킵되면서 손절·익절이 한 번도 실행되지 않았다.

대시보드는 같은 종목들에 대해 yfinance로 현재가를 정상 조회하고 있었다. exit checker가 대시보드와 같은 가격 소스를 쓰지 않아 생긴 불일치였다.

**수정 원칙**: 주문 실행은 Kiwoom mock API를 유지하고, 가격 조회는 대시보드와 동일한 yfinance로 통일한다.

```
[잘못된 설계]
paper_exit_checker → KiwoomPaperTrader.get_current_price()
                       └─ KiwoomClient(use_mock=True)
                            └─ mockapi.kiwoom.com/ka10001 → 실패

[수정 후]
paper_exit_checker → _fetch_prices_yf(tickers)        ← 가격 (yfinance)
                   → paper_trader.place_sell(ticker, qty) ← 주문 (mockapi)
```

---

## 3-잡 파이프라인

모의투자는 세 개의 독립된 APScheduler 잡으로 구성된다. 잡 간에 공유 상태는 없고, DB(`paper_positions` 테이블)가 유일한 상태 저장소다.

```
매일 평일

16:30 KST  daily_stage_classifier ─────────┐
                                            │ stage_classifications 테이블
16:10 KST  paper_exit_checker_job  ←── DB  │
           (exit 조건 판정 → 청산 주문)         │
                                            │
16:40 KST  paper_eod_sampler_job   ────────┘
           (Stage1 × Ichimoku 신호 샘플링
            → pending 삽입)

09:05 KST (다음날) paper_open_entry_job
           (pending → 시가 주문 → open)
```

### paper_exit_checker_job (16:10 KST)

1. DB에서 `status='open'` 포지션 전체 조회
2. yfinance로 전 종목 현재가 일괄 조회 (1분봉 1d)
3. 종목별 exit 조건 판정 (우선순위 순)
4. 청산 조건 해당 시 Kiwoom mock API로 매도주문 → DB `closed` 업데이트
5. Telegram 알림

### paper_eod_sampler_job (16:40 KST)

1. 오늘 `stage_classifications` Stage 1 진입 종목 조회
2. 최신 주봉 Ichimoku 통과 종목 조회 (`chart_signals`)
3. 모델별 4개 신호 큐 구성:
   - `stage`: Stage1 KOSPI 종목
   - `kosdaq`: Stage1 KOSDAQ 종목
   - `cross`: Stage1 ∩ Ichimoku (교차 확인)
   - `ichimoku`: Ichimoku 단독 통과 전체
4. 모델별 남은 슬롯만큼 랜덤 샘플링 → `pending` 삽입
5. 랜덤 시드는 `YYYYMMDDxxx(model hash)`로 고정 → 같은 날 재실행 시 동일 결과

### paper_open_entry_job (09:05 KST)

1. 당일 또는 최근 4일 이내 `pending` 포지션 조회 (주말 대응)
2. 종목별 당일 시가 조회 (`get_open_price` → ka10001, 단 **실 API** 아닌 mock API)
3. 시가로 Kiwoom mock API 매수주문 → DB `open` 업데이트, 슬리피지 기록

> **주의**: `get_open_price`는 mockapi.kiwoom.com의 ka10001을 호출한다. 시가는 장 시작 직후에는 조회 가능할 수 있지만, 장 마감 후에는 응답이 없다. 이 때문에 `get_current_price` 폴백이 있으나, 두 함수 모두 mock 서버에서 지원이 불안정하다. 현재는 장 시작 직후(09:05) 시가 조회는 작동하는 것으로 확인되어 있다.

---

## DB 상태 머신

`paper_positions.status` 컬럼은 세 가지 상태를 순서대로 전이한다.

```
INSERT (eod_sampler)        UPDATE (open_entry)       UPDATE (exit_checker)
     │                           │                          │
     ▼                           ▼                          ▼
  pending  ──────────────►  open  ─────────────────►  closed
   (이론가만 있음)          (실제 체결가, qty, 주문번호)  (청산가, exit_type, 수익률)
```

| 필드 | pending | open | closed |
|------|---------|------|--------|
| `status` | `'pending'` | `'open'` | `'closed'` |
| `entry_theory` | ✅ 신호일 종가 | ✅ | ✅ |
| `entry_actual` | — | ✅ 실 체결가 | ✅ |
| `qty` | — | ✅ | ✅ |
| `watermark` | — | ✅ entry_actual | ✅ 최고가 |
| `tp1_date` / `tp1_price` | — | nullable | nullable |
| `exit_price` / `exit_type` | — | — | ✅ |
| `blended_return` | — | — | ✅ |

슬리피지(`slippage_pct`)는 `(entry_actual - entry_theory) / entry_theory`로 DB에서 계산된다.

---

## Exit 조건 상태 머신

exit checker는 오픈 포지션마다 네 가지 조건을 **우선순위 순서**로 검사한다. 먼저 해당하는 조건에서 멈춘다.

```
1. period_end   (today - signal_date).days >= 91
2. hard_stop    close <= entry * (1 - hard_stop_pct)
3. tp1          (tp1_done=False) AND close >= entry * (1 + tp1_pct)
4. trail        (tp1_done=True) AND close <= watermark * (1 - trail_pct)
```

### 각 조건 상세

**period_end** — 91일 만기. 모든 모델 공통. 손익과 무관하게 청산한다. 전략 설계상 91일이 기대 보유 기간의 상한이다.

**hard_stop** — 하드 스탑. 진입가 대비 `hard_stop_pct`(기본 10%) 손실 시 즉시 청산. `tp1_done` 여부와 무관하게 동작한다. TP1 이후에도 큰 하락이 발생하면 hard_stop이 trail보다 먼저 발동될 수 있다.

**tp1 (1차 익절)** — TP1 미발동 상태에서 `tp1_pct` 수익 달성 시. 전량 청산이 아니라 DB 기록만 업데이트(`tp1_date`, `tp1_price`)하고 잔여 포지션은 계속 보유한다. 실제로는 50% 청산이지만, 현재 모의투자 시스템은 분할 주문을 지원하지 않으므로 주문은 나가지 않는다 — 기록만 한다. `tp1_ratio`가 blended_return 계산에 반영된다.

**trail (트레일링 스탑)** — TP1 발동 후, `watermark`(이 포지션의 최고가) 대비 `trail_pct`(기본 10%) 하락 시 청산. watermark는 매일 exit checker에서 현재가가 기존 watermark를 초과할 때마다 갱신된다.

### blended_return 계산

청산 수익률은 TP1 여부에 따라 다르게 계산된다:

```python
if tp1_done:
    tp1_ret  = (tp1_price - entry) / entry    # TP1 시점 수익률
    final    = (close - entry) / entry        # 최종 청산 수익률
    blended  = tp1_ratio * tp1_ret + (1 - tp1_ratio) * final
else:
    blended  = (close - entry) / entry
```

예: `tp1_pct=0.15, tp1_ratio=0.50, trail_pct=0.10`이고 TP1에서 +15%, 최종 +8% 청산이면:
`blended = 0.50 × 0.15 + 0.50 × 0.08 = +11.5%`

---

## 모델별 파라미터 출처

exit 파라미터(`tp1_pct`, `trail_pct`, `hard_stop_pct` 등)는 `analysis/backtest_engine.py`의 `OPTIMAL_EXIT_PARAMS*` 상수에서 온다. 이 값들은 `scripts/run_sweep.py`의 그리드서치로 산출됐다.

| 모델 | tp1_pct | tp1_ratio | trail_pct | hard_stop_pct | 학습 성과 |
|------|---------|-----------|-----------|----------------|-----------|
| stage (KOSPI) | 25% | 50% | 10% | 10% | val_sharpe=4.70, win=45.7% |
| kosdaq | 25% | 50% | 15% | 10% | val_sharpe=5.48, win=46.7% |
| cross | 15% | 50% | 10% | 10% | val_sharpe=5.11, win=54.3% |
| ichimoku | 25% | 70% | 10% | 10% | val_sharpe=7.50, win=55.8% |

KOSDAQ의 trail이 15%인 이유는 KOSPI(10%)보다 변동성이 크기 때문이다. Ichimoku의 tp1_ratio가 70%인 이유는 주봉 전략 특성상 조기 익절 비중을 높이는 것이 더 유리했기 때문이다. Cross의 tp1_pct가 15%인 이유는 과적합 없는 승률 극대화를 위해서다 (0.25 대비 overfit_gap 안전).

파라미터는 `eod_sampler_job`이 `insert_pending()` 호출 시 각 포지션에 저장되므로, 이후 OPTIMAL_EXIT_PARAMS 값이 바뀌어도 기존 오픈 포지션에는 영향을 주지 않는다.

---

## 모델 슬롯과 포지션 금액

`MODEL_CONFIG`는 `kiwoom_paper_trader.py`에 하드코딩되어 있다:

```python
MODEL_CONFIG = {
    "stage":    {"max_slots": 10, "position_krw": 10_000_000},
    "kosdaq":   {"max_slots": 10, "position_krw": 10_000_000},
    "cross":    {"max_slots":  5, "position_krw": 20_000_000},
    "ichimoku": {"max_slots": 10, "position_krw": 10_000_000},
}
```

`eod_sampler_job`은 `get_open_slot_count(model)` — `open + pending` 합산 — 으로 남은 슬롯을 계산한다. 슬롯이 0이면 해당 모델의 신규 신호는 샘플링되지 않는다.

Cross 모델의 `max_slots=5, position_krw=20M`은 나머지 모델 대비 포지션 크기가 2배다. Cross는 Stage1 + Ichimoku 이중 조건이므로 신호 빈도가 낮고, 확인된 신호에 더 큰 금액을 배정하는 설계다.

---

## 가격 소스 통일 원칙

시스템 내에서 동일 종목의 현재가를 조회하는 지점이 여럿이다:

| 컴포넌트 | 가격 소스 | 용도 |
|----------|----------|------|
| 대시보드 `/api/positions` | yfinance (1d 1m, 5분 캐시) | 미실현 손익 표시 |
| `paper_exit_checker_job` | yfinance (1d 1m, 배치) | exit 조건 판정 |
| `paper_open_entry_job` | Kiwoom mock API ka10001 | 당일 시가 조회 |
| 텔레그램 `/paper` | yfinance (대시보드 캐시 재사용 없음, 직접 조회) | 포지션 조회 |

exit checker와 대시보드가 동일 소스(yfinance)를 사용하므로, 대시보드에서 손실로 표시되는 종목은 다음 16:10 exit checker 실행 시 동일한 가격 기준으로 손절 처리된다.

open_entry의 시가 조회만 Kiwoom mock API를 사용하는 이유: 당일 시가(`open_pric`)는 yfinance의 1분봉에서도 조회 가능하지만, Kiwoom mock API가 이 용도로는 장 시작 직후(09:05) 안정적으로 동작하는 것이 확인됐고, 주문과 가격을 같은 서버 응답에서 가져오는 것이 일관성 면에서 단순하다.

---

## 관련 문서

- [키움 모의투자 설정 방법](howto-kiwoom-paper-trade.md) — API 키 발급부터 포지션 확인까지
- [키움 연동 레퍼런스](reference-kiwoom.md) — API 코드 목록, 환경변수, 스케줄러 잡
- [스케줄러 레퍼런스](reference-scheduler.md) — 잡 실행 시각, 의존 환경변수
