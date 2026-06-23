# 모의투자 분할 청산(Scale-out) 모델 — 개선 제안 및 코드 대조

외부에서 받은 분할 청산 모델 개선 제안 원문과, 실제 구현(`jobs/paper_jobs.py` exit checker)을 대조한 결과를 함께 보관합니다. 제안이 지적한 "잠재적 허점" 중 무엇이 이미 현재 코드에서 처리되어 있고 무엇이 실제로 미구현 상태인지 구분하는 것이 이 문서의 목적입니다.

## 코드 대조 결과 (먼저 확인할 것)

`jobs/paper_jobs.py` exit checker(`paper_exit_checker_job`, 라인 95-147 기준)와 대조한 결과:

| 제안의 지적 | 실제 코드 상태 | 판정 |
|---|---|---|
| 1. TP1과 Trailing Stop 우선순위 역전 리스크 | `elif not _tp1_done and ...` (TP1, 라인 108) / `elif _tp1_done and ...` (trail, 라인 124) — `tp1_done` 플래그로 두 조건이 상호 배타적이라 같은 날 동시 충돌 불가. hard_stop(라인 104, 손실 조건)과 tp1(라인 108, 수익 조건)도 같은 날 동시에 참일 수 없음 | **이미 해결됨** — 우려한 if-elif 오작동 케이스가 설계상 발생하지 않음 |
| 2. Trailing Stop 기준점("전체 최고점"인지 "TP1 이후 최고점"인지 애매함) | `_watermark = _pos["watermark"] or _entry`(라인 67), `if _close > _watermark: _watermark = _close`(라인 83-84) — **진입(entry) 시점부터의 전체 최고점**, TP1 시점에 리셋하지 않음. Breakeven(본전 락인)도 구현 안 됨 | **미구현 — 실제 동작 확인됨.** 제안된 Breakeven Rule은 적용 안 됨 (아래 "검토 메모" 참고) |
| 3. Blended Return 계산 오류 가능성 (TP1 못 가고 청산 시 blended 공식이 왜곡될 수 있음) | `if _tp1_done: blended = ratio*tp1_ret + (1-ratio)*final else: blended = _ret`(라인 142-147) — `_tp1_done`은 **이번 루프 진입 시점**(이전 날까지의 상태)을 읽으므로, hard_stop/period_end로 청산되는 날 TP1이 처음 발동하는 경우는 없음(TP1은 그 자체로 전량청산이 아니라 플래그 기록이며 같은 elif 체인에서 hard_stop/period_end보다 늦게 검사됨) | **이미 해결됨** — 우려한 오염 케이스가 발생하지 않는 구조 |

**검토 메모 (2026-06-23):** 유일하게 실제로 미구현인 항목은 #2의 Breakeven Rule(TP1 발동 후 트레일링 기준을 진입가 이상으로 락인)입니다. `analysis/backtest_engine.py`에 이미 `_compute_exit_logic_model_a()`(ATR 손절 + Breakeven + Chandelier Exit, 라인 2475-2607)로 구현되어 있었고 — 모의투자 잡에는 연결되지 않은 상태 — `scripts/compare_exit_models.py`로 백테스트 비교가 가능했습니다. 아래 "백테스트 검증 결과" 참고.

## 백테스트 검증 결과 (2026-06-23, 전종목 재검증으로 갱신)

### 1차 시도 (200종목 샘플) — 표본 편향 + 도구 버그로 무효 처리

처음 `--max-tickers 200`으로 돌렸을 때는 모델A가 원본보다 나쁘게 나왔으나(win_sell -3.2pp, avg_sell -1.7pp), 이어서 `--max-tickers 0`(전종목)으로 돌리자 **모델B의 신호 수가 21건**(원본 9027건/모델A 8352건 대비)으로 붕괴됐습니다. 원인은 `compare_exit_models.py`가 모델 3개를 비교하기 위해 `run_backtest()`를 3번 호출하면서 **동일한 전종목 OHLCV를 yfinance에서 3번 반복 다운로드**했기 때문 — `daily_ohlcv` 캐시가 760일 lookback을 요구하는데 DB에 쌓인 KRX OpenAPI 데이터는 2025-01-01부터만 있어 캐시가 한 번도 히트하지 못하고, 누적 요청량이 Yahoo Finance rate limit("Too Many Requests")에 걸려 3번째 패스(모델B)는 거의 전부 실패했습니다. 200종목 샘플도 표본이 `get_all_tickers()` 목록의 앞 200개(코드 순)로 편향돼 있어 결론을 신뢰할 수 없었습니다.

**도구 수정**: `scripts/compare_exit_models.py`를 OHLCV·수급·streak·상장주식수·진입신호(stage_v13, exit_model과 무관)를 **1회만 수집**해 모델 3개가 공유하도록 재작성(`_build_shared_data()` + `run_one()` 분리). 200종목 샘플로 기존 결과(승률·수익률)가 동일하게 재현되는 것을 먼저 확인한 뒤 전종목 재실행.

### 2차 시도 (전종목, 신호 8,311건) — 신뢰 가능한 결과

`scripts/compare_exit_models.py --max-tickers 0 --start 2025-01-01 --end 2026-06-17` (신호 소스 `stage_v13`, KOSPI+KOSDAQ 전종목, **세 모델 모두 동일하게 신호 8,311건** — 더 이상 표본이 갈리지 않음):

| 지표 | 원본 (현재 운영) | 모델A (ATR+Breakeven) | 모델B (3단계분할) |
|---|---|---|---|
| `win_sell` (실 청산 승률) | **32.5%** | 34.4% (+1.9pp) | 36.5% (+4.0pp) |
| `avg_sell` (실 청산 평균수익률) | **2.0%** | 2.3% (+0.3pp) | 1.6% (-0.4pp) |
| `sharpe_28d`/`win_28d`/`avg_28d`/`mdd` | 동일 | 동일 | 동일 |

(`sharpe_28d` 등은 28일 고정 윈도우 forward return 기준이라 exit_model과 무관하게 동일. `mdd`가 세 모델 모두 -100.0%인 것은 표본 내 극단치 1종목이 모든 모델에 동일하게 반영된 것 — exit_model 차이가 아님.)

**결론 (정정): 전종목 표본에서는 Breakeven Rule을 포함한 모델A가 원본보다 승률·평균수익률 모두 더 좋습니다** (200종목 샘플에서의 1차 결론과 반대). 단 개선폭은 제안문이 기대한 수준("MDD가 눈에 띄게 개선")보다는 작습니다(win_sell +1.9pp, avg_sell +0.3pp). 모델B는 승률 개선이 가장 크지만(+4.0pp) 평균수익률은 오히려 소폭 하락(-0.4pp) — 빠른 1차 익절(15%)이 승률을 높이는 대신 큰 수익 구간 일부를 포기하는 트레이드오프로 보입니다.

**권고**: 모델A(Breakeven Rule)는 전종목 표본 기준 원본보다 우위이므로 **모의투자 잡(`jobs/paper_jobs.py`) 적용을 검토할 가치가 있습니다** — 단, 이 백테스트는 `OPTIMAL_EXIT_PARAMS`처럼 train/val 분리 그리드서치를 거치지 않은 단일 구간(2025-01~2026-06) 결과이므로, 실제 전환 전에 ① 기간을 나눠 holdout 검증, ② `paper_jobs.py`의 모델A 구현(Breakeven + Chandelier Exit, ATR 기반 hard stop)을 실제 운영 가격소스(Kiwoom mock API)에 맞게 연결, ③ 4주 정도 모의투자로 실거래 검증을 권장합니다. 모델B는 평균수익률이 낮아 전환 근거가 약합니다.

---

## 원문 (외부 제안, 코드 미반영 — 검토용)

공유해주신 분할 청산(Scale-out) 모델은 손실 제한(Hard Stop), 익절(TP), 추세 추종(Trailing Stop), 시간 제한(Time-out)까지 트레이딩의 필수 요소를 아주 짜임새 있게 갖춘 훌륭한 로직입니다. 1차 익절 후 잔여 물량으로 수익을 극대화하려는 의도가 잘 보입니다.

다만, 백테스트 엔진을 실제로 구동하고 고도화하는 과정에서 몇 가지 논리적 허점(Loophole)이나 **수익률을 더 끌어올릴 수 있는 개선 포인트**가 보입니다. 이를 보완할 수 있는 더 나은 모델과 수정 아이디어를 추천해 드립니다.

### 1. 현재 모델의 잠재적 리스크 & 보완점

* **우선순위 역전 리스크 (1차 TP vs Trailing Stop):** 현재 1차 TP(2번)가 Trailing Stop(4번)보다 우선순위가 높습니다. 만약 주가가 entry 대비 +24%까지 올랐다가 급락하여 고점 대비 -10%를 건드렸다면, 아직 1차 TP를 만나지 못했으므로 4번 Trailing Stop이 작동해야 합니다. 하지만 조건문 순서상 1차 TP 조건이 먼저 검사되므로 코딩할 때 `if-elif` 구조를 잘못 쓰면 트레일링 스탑이 씹히거나 오작동할 수 있습니다.
* **Trailing Stop의 기준점 애매함:** 현재 'High 고점 - 10%'로 되어 있는데, 이 고점이 '진입 이후 전체 최고점'인지, 아니면 '1차 TP 달성 이후의 최고점'인지 명확해야 합니다. 보통은 1차 익절을 하고 나면 본전 확보(Breakeven)를 위해 Trailing Stop의 기준을 진입가(Entry) 이상으로 락인(Lock-in)하는 것이 안전합니다.
* **Blended Return 계산 오류 가능성:** 만약 1차 TP에 도달하지 못하고 1번(Hard Stop)이나 4번(Trailing Stop)으로 전량 청산되면, `tp1_ratio`만큼 분할 매도가 안 일어납니다. 이때도 `0.5 × tp1_ret + 0.5 × final_exit_ret` 공식을 일괄 적용하면 수익률이 왜곡됩니다. **실제 청산된 물량의 비중(Weight)을 동적으로 반영하는 로직**이 필요합니다.

### 2. 더 나은 개선 모델 추천 (업그레이드 버전)

현재 모델의 큰 틀을 유지하면서, **안정성과 수익률을 극대화할 수 있는 2가지 업그레이드 모델**을 제안합니다.

#### 모델 A: ATR 기반 가변형 트레일링 스탑 모델 (수익 극대화형)

고정 % (10%) 대신 시장의 변동성(ATR)을 반영하여 Stop 라인을 유동적으로 조절하는 방식입니다. 변동성이 낮을 때는 타이트하게, 변동성이 클 때는 널널하게 잡아서 억울하게 털리는 일을 방지합니다.

* **우선순위 구조 변경:**
  1. **Hard Stop:** `Close <= Entry - 2 × ATR` (진입 당시 변동성의 2배수 손절)
  2. **1차 TP:** `Close >= Entry × (1 + 25%)` → 50% 청산 + **나머지 50%의 Hard Stop을 Entry(본전)로 상향 조정 (Breakeven Rule)**
  3. **Trailing Stop (Chandelier Exit):** `Close <= 익절후 최고가 - 3 × ATR` → 잔여분 청산
  4. **Stage3 피크 / 기간 종료:** 잔여분 전량 강제 청산

> **추천 이유:** 1차 익절(25%)을 성공한 시점에서 이미 이 매매는 절대 손실을 보지 않는 구조(Breakeven)로 전환되므로, 심리적으로나 자산 곡선(Equity Curve) 측면에서 매우 안정적입니다.

#### 모델 B: 3단계 분할 청산 & 매수단가 하향 연동 모델 (MDD 방어형)

50:50 분할은 대세 상승장에서 아쉬울 수 있고, 하락장에서 타격이 클 수 있습니다. 청산 단계를 3단계로 쪼개고 수익을 담보하는 모델입니다.

* **청산 로직:**
  1. **Hard Stop:** `Close <= Entry × (1 - 8%)` → 손절 폭을 조금 축소
  2. **1차 TP (30% 물량):** `Entry × (1 + 15%)` 달성 시 청산 (빠른 익절로 현금 확보)
  3. **2차 TP (40% 물량):** `Entry × (1 + 30%)` 달성 시 청산
  4. **3차 Trailing (잔여 30% 물량):** 고점 대비 10% 하락 시 청산 (이 물량으로 대세 상승장을 끝까지 먹음)
  5. **Time-out:** 기간 종료 시 전량 청산

> **추천 이유:** 1차 목표가를 15%로 낮추어 승률(Win Rate)을 크게 끌어올립니다. 일단 30% 물량이라도 익절을 해두면, 손절이 나가더라도 전체 자산에 미치는 타격(MDD)이 획기적으로 줄어듭니다.

### 3. 백테스트 코드 구현 시 주의할 점 (팁)

수식으로 적어주신 `blended_return`을 코드화할 때는 아래와 같이 상태 플래그(State Flag)를 두어 물량이 실제로 나갔는지 체크해야 데이터 오염이 없습니다.

```python
# 로직 예시 (Pseudocode)
is_tp1_triggered = False
tp1_return = 0.0
final_return = 0.0

for row in df_after_signal:
    # 1. Hard Stop (전량 청산)
    if not is_tp1_triggered and row['Close'] <= entry * 0.90:
        blended_return = (row['Close'] / entry) - 1
        break

    # 2. 1차 TP (50% 분할)
    if not is_tp1_triggered and row['Close'] >= entry * 1.25:
        tp1_return = (row['Close'] / entry) - 1
        is_tp1_triggered = True
        # 이때부터 Stop 조건을 수정하거나 Trailing 최고가 추적 시작
        continue

    # 3. Trailing Stop (잔여분 혹은 전량)
    if row['High'] >= max_high_after_entry:
        max_high_after_entry = row['High']

    if row['Close'] <= max_high_after_entry * 0.90:
        final_return = (row['Close'] / entry) - 1
        if is_tp1_triggered:
            blended_return = 0.5 * tp1_return + 0.5 * final_return
        else:
            blended_return = final_return  # 1차 TP 못 가고 트레일링 컷 당한 경우
        break

    # 4. Stage3 피크 및 만기 강제 청산 조건...
```

현재 모델도 훌륭하니, 먼저 현재 버전으로 백테스트를 돌려 승률(Win Rate)과 프로핏 팩터(Profit Factor)를 뽑아보신 후, 추천된 '1차 익절 후 본전 보장(Breakeven) 규칙'을 딱 하나만 추가해 보세요. MDD가 눈에 띄게 개선되는 걸 보실 수 있을 겁니다.

---

## 관련 문서

- [모의투자 파이프라인 설계 해설](../03_explanation/explanation-paper-trading.md) — 실제 Exit 조건 상태 머신, blended_return 계산, 모델별 파라미터
- [키움 연동 레퍼런스](reference-kiwoom.md)
