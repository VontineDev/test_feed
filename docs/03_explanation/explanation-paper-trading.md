# 모의투자 파이프라인 설계 해설

키움 모의투자 시스템이 왜 3-잡 구조로 나뉘어 있는지, 가격 소스를 어떻게 분리하는지, exit 조건이 어떤 상태 머신으로 동작하는지를 설명한다.

---

## 문제: 주문과 가격 조회는 다른 서버가 필요하다

키움증권은 **실 API**(`api.kiwoom.com`)와 **모의투자 API**(`mockapi.kiwoom.com`)를 분리 운영한다. 모의투자 서버는 주문 체결(kt10000/kt10001)과 계좌 조회(kt00018)만 지원하고, **시장 데이터 API(ka10001 — 현재가 조회)는 지원하지 않는다.**

초기 설계에서는 `paper_exit_checker_job`이 장 마감 후에 실행되며 `KiwoomPaperTrader.get_current_price()`를 호출해 exit 조건을 판단했다. 모의투자 서버는 ka10001(현재가 조회)을 지원하지 않으므로 결과는 항상 실패였고, 매일 전 포지션이 스킵되면서 손절·익절이 한 번도 실행되지 않았다.

**1차 수정** (v0.10.0.1): 가격 조회를 대시보드와 동일한 yfinance 배치 조회(`_fetch_prices_yf()`)로 교체했다. 주문 실행은 Kiwoom mock API를 유지했다.

**2차 수정** (v1.0.4.1): exit checker 실행 시점이 장 마감 후 → **정규장 마감 직전(15:20 KST) 시장가 매도**로 바뀌면서 정규장 중 실시간가가 필요해졌다. yfinance의 1분봉은 지연이 있어 장중 판단에는 부적합하므로, 가격 조회를 Kiwoom mock API(`paper_trader.get_current_price()`, 종목당 0.5초 딜레이)로 되돌렸다. 하지만 이 서버는 애초에 ka10001을 지원하지 않으므로(위 문단), 다시 매일 "현재가 조회 실패"로 전 포지션이 스킵되는 상태로 돌아갔다 — hard_stop이 발동해야 할 포지션이 며칠~몇 주씩 방치되며 -10% 설계 손절선을 훨씬 넘는 손실(-20~-38%)로 청산되는 결과를 낳았다.

**3차 수정**: `KiwoomPaperTrader`에 **시세 조회 전용 실 API 클라이언트**(`_quote_client`, `api.kiwoom.com`, `KIWOOM_APPKEY`/`KIWOOM_SECRETKEY`)를 별도로 추가했다. `get_current_price()`/`get_open_price()`는 이 실 API 클라이언트로 ka10001을 호출하고, 주문(`place_buy`/`place_sell`)과 계좌 조회는 계속 모의투자 서버(`self._client`, mock)를 사용한다 — 실제 매매가 발생하지 않도록 주문 경로는 그대로 유지.

```
[초기 설계 / 2차 수정 — 실패]
paper_exit_checker(15:20 KST) → KiwoomPaperTrader.get_current_price()
                                   └─ mockapi.kiwoom.com/ka10001 → 항상 실패 (시장데이터 미지원)

[1차 수정 — v0.10.0.1]
paper_exit_checker(장 마감 후) → _fetch_prices_yf(tickers)        ← 가격 (yfinance)
                                → paper_trader.place_sell(...)     ← 주문 (mockapi)

[3차 수정]
paper_exit_checker(15:20 KST) → paper_trader.get_current_price(ticker)  ← 가격 (api.kiwoom.com, 실 API)
                              → paper_trader.place_sell(...)             ← 주문 (mockapi.kiwoom.com, 모의투자)
```

**4차 수정 — 현재 상태** (2026-08-03): 매도주문(`place_sell`) 호출이 예외를 던지면 `kiwoom_sell_no`에 `"FAILED"`만 기록하고도 `update_to_closed()`를 그대로 호출해 `status='closed'`로 확정해버리는 버그가 있었다. 브로커에는 주식이 그대로 남아있는데 DB는 청산된 걸로 착각해, 실제 계좌 보유 종목과 `paper_positions`가 어긋나는 문제(26건 발견, 2026-08-03 investigate 세션)로 이어졌다. 매도주문이 실패하면 `status`를 `closed`로 넘기지 않고 `open`을 유지하도록 수정 — 다음 실행(익일 15:20 KST)에서 exit 조건이 다시 판정되며 자동 재시도된다. 실패 시 텔레그램 경고(`⚠️ 매도주문 실패`)도 추가했다.

---

## 4-잡 파이프라인

모의투자는 네 개의 독립된 APScheduler 잡으로 구성된다. 잡 간에 공유 상태는 없고, DB(`paper_positions` 테이블)가 유일한 상태 저장소다.

```
매주 일요일

21:15 KST  compose_paper_entry_job
           (FUNNEL-1/AND-1 주간 신호 추출
            → pending 삽입, Kiwoom 불필요)

매일 평일

16:30 KST  daily_stage_classifier ─────────┐
                                            │ stage_classifications 테이블
15:20 KST  paper_exit_checker_job  ←── DB  │
           (exit 조건 판정 → 청산 주문)         │
                                            │
16:40 KST  paper_eod_sampler_job   ────────┘
           (Stage1 × Ichimoku 신호 샘플링
            → pending 삽입)

09:05 KST (다음날) paper_open_entry_job
           (pending → 시가 주문 → open)
```

### paper_exit_checker_job (15:20 KST, 정규장 마감 직전)

1. DB에서 `status='open'` 포지션 전체 조회
2. Kiwoom 실 API(ka10001, `api.kiwoom.com`)로 종목별 현재가 순차 조회 (0.5초 딜레이로 rate limit 방지)
3. 종목별 exit 조건 판정 (우선순위 순)
4. 청산 조건 해당 시 Kiwoom mock API(모의투자 서버)로 시장가 매도주문 → DB `closed` 업데이트
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

### compose_paper_entry_job (일요일 21:15 KST)

1. `strategy_compose.load_signal_frame()` + 각 전략 `.run()`으로 이번 주(ISO) 신호 추출 (8주 lookback)
2. FUNNEL-1 상위 10개, AND-1 상위 5개, SCORE-1 상위 5개를 각 모델(`compose-funnel1` / `compose-and1` / `compose-score1`)로 `pending` 삽입 (`_STRATEGIES_CFG`의 `top_n`으로 모두 캡 — "전체"는 아님)
3. `entry_theory=0.0` (이론 진입가 없음 — `paper_open_entry_job`이 실제 시가로 채움)
4. 같은 주에 이미 `pending/open`인 종목은 중복 스킵
5. Kiwoom 계정 불필요 — DB 접근만으로 동작

> **ISO 주 경계**: 일요일은 ISO 8601상 직전 주의 마지막 날. `cur_week`로 조회 시 신호가 없으면 `prev_week`로 자동 fallback.

> **slippage_pct**: compose 포지션은 `entry_theory=0.0`이므로 `NULLIF(0,0) = NULL` → slippage_pct는 항상 NULL. 설계상 한계.

### paper_open_entry_job (09:05 KST)

1. `signal_date` 무관하게 `status='pending'` 포지션 **전체** 조회 (이전엔 최근 4일 중 가장 최근 날짜 1개만 처리하고 멈추는 버그가 있었음 — 특정 회차 진입 실패분이 더 최근 날짜의 pending에 가려져 영구히 재시도 안 되는 문제. 2026-07 수정)
2. 종목별 당일 시가 조회 (`get_open_price` → ka10001, 실 API `api.kiwoom.com`) — **종목 간 0.5초 딜레이로 rate limit 방지**
3. 시가로 Kiwoom mock API(모의투자 서버) 매수주문 → DB `open` 업데이트, 슬리피지 기록

> **가격 소스**: `get_open_price`/`get_current_price`는 `KiwoomPaperTrader._quote_client`(실 API, `api.kiwoom.com`)를 호출한다. 모의투자 서버(mockapi.kiwoom.com)는 ka10001을 지원하지 않으므로 시세 조회에 쓰지 않는다 — 주문 제출(`place_buy`/`place_sell`)에만 모의투자 서버를 사용.

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

exit 파라미터(`tp1_pct`, `trail_pct`, `hard_stop_pct` 등)는 `analysis/backtest/config.py`의 `OPTIMAL_EXIT_PARAMS*` 상수에서 온다(2026-07-16 리팩토링으로 옛 `analysis/backtest_engine.py`에서 이관 — 심까지 삭제됨). 이 값들은 `scripts/run_sweep.py`의 그리드서치로 산출됐다.

> **주의**: `analysis/backtest/config.py`에는 이 4개 모델용 상수 외에 `OPTIMAL_EXIT_PARAMS_FUNNEL1`/`OPTIMAL_EXIT_PARAMS_SCORE1`도 정의돼 있지만, 라이브 모의투자는 이 두 상수를 쓰지 않는다 — `analysis/backtest/engine.py`(백테스트 전용)에서만 참조된다. compose 모델의 실제 청산 파라미터는 아래 표 대신 바로 다음 문단(`compose_paper_entry_job`에서 직접 전달)을 참고.

| 모델 | tp1_pct | tp1_ratio | trail_pct | hard_stop_pct | 학습 성과 |
|------|---------|-----------|-----------|----------------|-----------|
| stage (KOSPI) | 25% | 50% | 10% | 10% | val_sharpe=4.70, win=45.7% |
| kosdaq | 25% | 50% | 15% | 10% | val_sharpe=5.48, win=46.7% |
| cross | 15% | 50% | 10% | 10% | val_sharpe=5.11, win=54.3% |
| ichimoku | 25% | 70% | 10% | 10% | val_sharpe=7.50, win=55.8% |
| compose-funnel1 | 15% | 50% | 10% | 10% | backtest sharpe=0.74, win=67% |
| compose-and1 | 15% | 50% | 10% | 10% | backtest sharpe=1.75, win=80% |
| compose-score1 | 15% | 50% | 10% | 10% | backtest sharpe=1.17, win=67% |

compose 모델의 exit 파라미터는 backtest 기본값을 사용한다. `OPTIMAL_EXIT_PARAMS`에서 오지 않고 `compose_paper_entry_job`에서 `insert_pending()` 호출 시 직접 전달된다. 4주 실전 결과 후 최적화 예정.

KOSDAQ의 trail이 15%인 이유는 KOSPI(10%)보다 변동성이 크기 때문이다. Ichimoku의 tp1_ratio가 70%인 이유는 주봉 전략 특성상 조기 익절 비중을 높이는 것이 더 유리했기 때문이다. Cross의 tp1_pct가 15%인 이유는 과적합 없는 승률 극대화를 위해서다 (0.25 대비 overfit_gap 안전).

파라미터는 `eod_sampler_job`이 `insert_pending()` 호출 시 각 포지션에 저장되므로, 이후 OPTIMAL_EXIT_PARAMS 값이 바뀌어도 기존 오픈 포지션에는 영향을 주지 않는다.

---

## 모델 슬롯과 포지션 금액

`MODEL_CONFIG`는 `kiwoom_paper_trader.py`에 하드코딩되어 있다 (슬롯 수만 — 포지션 금액은 아래 참고):

```python
MODEL_CONFIG = {
    "stage":           {"max_slots": 10},
    "kosdaq":          {"max_slots": 10},
    "cross":           {"max_slots":  5},
    "ichimoku":        {"max_slots": 10},
    "compose-funnel1": {"max_slots": 10},
    "compose-and1":    {"max_slots":  5},
    "compose-score1":  {"max_slots":  5},  # txamt z-score top-20
}
```

`eod_sampler_job`은 `get_open_slot_count(model)` — `open + pending` 합산 — 으로 남은 슬롯을 계산한다. 슬롯이 0이면 해당 모델의 신규 신호는 샘플링되지 않는다.

### 포지션 금액 (2026-07-31 재설계)

원래는 모델별 고정 원화 금액(`position_krw`)이었으나, 계좌 실제 자산 규모(당시 약 3.75억원, 이미 -18.6%
손실)를 무시한 채 합계 약 7억원어치를 배정해 신규 진입이 "RC4025 모의투자 매수증거금이 부족합니다"로
상시 실패하는 문제가 발견됐다. `compute_slot_krw()`로 교체:

1. 계좌 추정예탁자산(`prsm_dpst_aset_amt`) × `(1 - CASH_RESERVE_RATIO)`(기본 20%)로 "배포 가능 자본"을 구한다.
2. `ACTIVE_MODELS`(kosdaq 제외 — 신호가 전혀 생성되지 않는 별도 버그, 죽은 모델에 자본을 배정하면 나머지가 손해) 수로 균등 분배해 **모델별 동일 금액**을 배정한다.
3. 그 금액을 각 모델의 `max_slots`로 나눠 슬롯당 금액을 정한다.

Cross/compose-and1/compose-score1(슬롯 5개)은 stage/ichimoku/compose-funnel1(슬롯 10개)보다 슬롯당 금액이
2배 크지만, 모델 총 배정액은 동일하다 — Cross류는 Stage1+Ichimoku 이중 조건 등으로 신호 빈도가 낮으니
확인된 신호에 더 큰 금액을 싣는 기존 설계 의도를 유지한 것이다.

`paper_open_entry_job`은 실행 시작 시 계좌 잔고를 한 번 조회해 슬롯당 금액과 배포 가능 자본을 계산하고,
각 매수 주문 전에 "이번 실행에서 이미 낸 주문 포함 기투자금액 + 이번 주문 금액"이 배포 가능 자본을
넘으면 스킵한다(현금 비중 보호) — 다음 실행에서 재시도된다.

---

## 가격 소스

시스템 내에서 동일 종목의 현재가를 조회하는 지점이 여럿이다. **주문에 영향을 주는 조회(exit, open_entry)는 Kiwoom 실 API**(`api.kiwoom.com`, `_quote_client`), **표시 전용 조회(대시보드, 텔레그램)는 yfinance**로 나뉜다. 주문 제출(매수/매도) 자체는 항상 Kiwoom mock API(모의투자 서버)로 간다:

| 컴포넌트 | 가격 소스 | 용도 |
|----------|----------|------|
| 대시보드 `/api/positions` | yfinance (1d 1m, 5분 캐시) | 미실현 손익 표시 |
| `paper_exit_checker_job` | Kiwoom 실 API ka10001 (종목별 순차) | exit 조건 판정 → 매도주문(mock) |
| `paper_open_entry_job` | Kiwoom 실 API ka10001 | 당일 시가 조회 → 매수주문(mock) |
| 텔레그램 `/paper` | yfinance (대시보드 캐시 재사용 없음, 직접 조회) | 포지션 조회 |

exit checker와 대시보드가 **다른** 소스를 쓰므로, 대시보드에 표시되는 손익과 15:20 KST exit checker가 판단하는 손익이 일시적으로 어긋날 수 있다(둘 다 정규장 중 시세이므로 보통 근접하지만 동일 틱은 아니다).

exit checker와 open_entry가 모두 Kiwoom mock API를 쓰는 이유: 두 잡 모두 정규장 중에 실행되고 그 결과로 실제 mock 주문을 내야 하므로, 주문 체결가와 같은 서버에서 가격을 가져오는 것이 일관성 면에서 단순하고, 1분봉 지연이 있는 yfinance보다 실시간성이 낫다.

---

## 관련 문서

- [키움 모의투자 설정 방법](howto-kiwoom-paper-trade.md) — API 키 발급부터 포지션 확인까지
- [키움 연동 레퍼런스](reference-kiwoom.md) — API 코드 목록, 환경변수, 스케줄러 잡
- [스케줄러 레퍼런스](reference-scheduler.md) — 잡 실행 시각, 의존 환경변수
- [분할 청산 모델 개선 제안 ↔ 코드 대조](../02_reference/reference-paper-exit-model-proposal.md) — 외부 제안과 실제 exit checker 구현 비교, 미구현 항목(Breakeven Rule) 정리
