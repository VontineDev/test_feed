# 백테스트 모델 레지스트리

지금까지 테스트된 모든 진입/유니버스/청산 조합을 성과 순으로 정리한 카탈로그.
개별 실험의 근거·전체 그리드서치 표·과최적화 검증 과정 등 **서술은 여전히
[`TechnicalQuant.md`](../../TechnicalQuant.md)가 원본**이다 — 이 문서는 거기서
"결과가 뭐였는지, 어디서 재현하는지"만 빠르게 찾기 위한 색인이다. 실제 라이브
운용 성과(백테스트와 다름, 아래 참고)는
[reference-paper-trading-gen1-result-20260818.md](reference-paper-trading-gen1-result-20260818.md)
참고.

**코드**: 아래 "코드 위치"는 [`analysis/backtest/model_registry.py`](../../analysis/backtest/model_registry.py)의
`ENTRY_COMPONENTS`/`UNIVERSE_COMPONENTS`/`EXIT_COMPONENTS` 딕셔너리 키와 1:1 대응한다.
새 조합을 만들 때는 여기서 진입/유니버스 하나 + 청산 하나를 골라 섞으면 된다 —
`scripts/run_cross_combo_backtest.py`가 이미 이 방식으로 15개 조합을 실행한 전례가 있다.

**표기 규칙**: 한 그리드서치에서 수십~수백 조합을 스윕한 경우, 여기엔 보고된
최선/대표 조합만 1~2행으로 남긴다(전체 그리드는 TechnicalQuant.md의 해당 CSV
참고). 표본 30건 미만은 "표본↓"로 표시 — 노이즈를 최적값으로 착각하지 말 것.

⚠️ **가장 중요한 단서 — 이 문서 전체에 적용**: 아래는 전부 **백테스트** 성과다.
실제 모의투자 1기 라이브 결과(2026-05-17~08-17, 130건)는 헤드라인상 전체
승률 9.5%/평균수익 -4.86%로 정반대였으나, **2026-08-23 `/investigate`로
재분석한 결과 이 수치의 상당 부분은 전략 성과가 아니라 운영 버그(스케줄러
크래시, Kiwoom 토큰 만료로 인한 손절 감시 중단, 유령보유 재조정)로 만들어진
근사치였다** — 종료 84건 중 실제 시장 결과를 가진 건 28건(33%)뿐이고, 이를
걷어내면 승률 28.6%/평균 -1.74%로 여전히 백테스트보다 낮지만 헤드라인만큼
재앙적이지는 않다. 상세 분해와 모델별 클린 수치는
[reference-paper-trading-gen1-result-20260818.md](reference-paper-trading-gen1-result-20260818.md)의
"정정" 섹션 참고. 결론은 바뀌지 않는다 — 백테스트 승률·수익률을 실전
기대치로 그대로 믿지 말 것. 다만 "gen1이 백테스트를 반증한다"고 보기도
어렵다(표본 오염 + 역풍 구간 겹침) — 진짜 검증은 gen2 데이터가 쌓여야 가능.

---

## Tier S — 프로젝트 전체 최고 기록 (신규 발견, 미채택)

`UNIVERSE_COMPONENTS["MOMENTUM_top20pct_mktcap200"]`(모멘텀 단독, 시총200∩상위20%,
51종목) × compose 분할청산. 표본 103건 — FUNNEL-1/SCORE-1(수천 건)보다 훨씬 작다.

| 모델명 | 구성요소 (유니버스 / 청산) | 코드 위치 | 승률 | 평균수익 | MDD | 표본 | 상태 |
|---|---|---|---|---|---|---|---|
| **모멘텀단독×FUNNEL-1분할청산** | MOMENTUM_top20pct_mktcap200 / funnel1 | `UNIVERSE_COMPONENTS["MOMENTUM_top20pct_mktcap200"]` × `EXIT_COMPONENTS["funnel1"]` | 72.8% | **+14.4%** | **-10.4%** | 103 | 유망후보 |
| 모멘텀단독×SCORE-1분할청산 | MOMENTUM_top20pct_mktcap200 / score1 | × `EXIT_COMPONENTS["score1"]` | **74.8%** | +11.8% | -18.0% | 103 | 유망후보 |
| 모멘텀단독×CROSS분할청산 | MOMENTUM_top20pct_mktcap200 / cross | × `EXIT_COMPONENTS["cross"]` | 72.8% | +14.0% | -20.2% | 103 | 유망후보 |
| 모멘텀단독 (원안청산) | MOMENTUM_top20pct_mktcap200 / RSI30진입·70청산·-7%손절 | × `EXIT_COMPONENTS["quant_original"]` | 64.1% | +11.4% | -12.6% | 103 | 유망후보 (팩터분해 원본) |

재현: `python scripts/run_cross_combo_backtest.py --combo MOMENTUM_TOP20:funnel1` (등)

## Tier A — 현재 실전 운용 중인 주력 모델 (4.5년 검증, 표본 충분)

| 모델명 | 구성요소 (진입 / 청산) | 코드 위치 | 승률 | 평균수익 | MDD | 표본 | 상태 |
|---|---|---|---|---|---|---|---|
| **FUNNEL-1** | 수급스크린→4주내 Ichimoku돌파 / 분할청산 | `ENTRY_COMPONENTS["FUNNEL-1"]` × `EXIT_COMPONENTS["funnel1"]` | 64.9% | **+10.4%** | -32.3% | 7,427 | **생산중(주력)** |
| SCORE-1 | Stage·거래대금·수급 z-score top20 / 분할청산 | `ENTRY_COMPONENTS["SCORE-1"]` × `EXIT_COMPONENTS["score1"]` | 65.8% | +7.3% | -34.5% | 4,793 | **생산중(보조)** |

(1.6년 구간 수치: FUNNEL-1 64.1%/+9.7%/-35.9%·4,643건, SCORE-1 70.6%/+9.2%/-39.5%·1,658건 —
4.5년 재검증에서도 방향 유지. SCORE-1 승률이 다소 낮아진 건 표본이 3배 늘며
신뢰도가 오히려 상승한 것으로 해석.)

## Tier B — 저위험/보조 후보 (표본 35~140건)

| 모델명 | 구성요소 | 코드 위치 | 승률 | 평균수익 | MDD | 표본 | 상태 |
|---|---|---|---|---|---|---|---|
| SCENARIO2_PBR×SCORE-1분할청산 | SCENARIO2(PBR단독) / score1 | `ENTRY_COMPONENTS["SCENARIO2"]`(PBR 유니버스) × `EXIT_COMPONENTS["score1"]` | 72.0% | +7.6% | 미계산 | 125 | 후보 |
| SCENARIO2_PBR×CROSS/FUNNEL1분할청산 | SCENARIO2(PBR단독) / cross,funnel1 | 〃 × `cross`/`funnel1` | 68.8% | +9.5~10.0% | 미계산 | 125 | 후보 |
| QVM_top20pct_mktcap200 (원안청산) | 퀄리티+밸류+모멘텀 3팩터 top20 / RSI30/70/-7% | `UNIVERSE_COMPONENTS["QVM_top20pct_mktcap200"]` × `quant_original` | 57.4% | +8.5% | **-12.6%**(저위험) | 47 | 후보(저위험 포지션) |
| QVM_top30pct_mktcap200 (원안청산) | 3팩터 top30 / 원안청산 | `UNIVERSE_COMPONENTS["QVM_top30pct_mktcap200"]` × `quant_original` | 58.6% | +8.4% | -18.4% | 70 | 후보 |
| QVM_top10pct_mktcap200 (원안청산) | 3팩터 top10 / 원안청산 | `UNIVERSE_COMPONENTS["QVM_top10pct_mktcap200"]` × `quant_original` | 54.3% | +6.8% | -16.5% | 35 | 후보(표본↓) |
| SCENARIO2_PER18×compose분할청산 | SCENARIO2(PER≤18) / score1,cross,funnel1 | `ENTRY_COMPONENTS["SCENARIO2"]`(PER 유니버스) × compose exit | 59.3~64.3% | +6.1~7.1% | 미계산 | 140 | 후보 |
| QVM(3팩터top20)×cross/score1/funnel1분할청산 | 3팩터 top20 / compose exit | `UNIVERSE_COMPONENTS["QVM_top20pct_mktcap200"]` × compose exit | 59.6~63.8% | +3.6~6.1% | -8.8~-11.9% | 47 | 후보 |

## Tier C — 순수 기술적 퀀트 2안 SCENARIO2 (참고용, 과최적화 위험 확인됨)

| 모델명 | 구성요소 | 코드 위치 | 승률 | 평균수익 | 표본 | 상태 |
|---|---|---|---|---|---|---|
| SCENARIO2 이론적 상한 | PBR단독+시총200 / RSI30진입·80청산·-12%손절 | `ENTRY_COMPONENTS["SCENARIO2"]` × `EXIT_COMPONENTS["quant_optimized"]` | 55.2% | +16.82% | 116 | 참고용 — 과최적화 위험 매우 큼 |
| SCENARIO2 보수안 | PER≤18+시총200 / 동일청산 | 〃 | 50.4% | +9.47% | 129 | 참고용 — walk-forward 8폴드 중 5개 test 마이너스 전환 |
| SCENARIO2 필터스윕 최선 | PER≤12+시총300 | 〃 | 46.2% | +4.02% | 143 | 참고용 |
| SCENARIO5 (PBR단독, 원안청산) | PBR단독+시총200 / RSI30/70/-7% | `EXIT_COMPONENTS["quant_original"]` | 51.2% | +5.4% | 121 | 참고용 — 필터조합 중 최고, 6단계 발견 |
| **SCENARIO2 원안 (4.5년 재검증, 가장 신뢰)** | PER≤15+시총200 / 원안청산 | `ENTRY_COMPONENTS["SCENARIO2"]` × `quant_original` | 39.6% | +2.2% | 535 | 순수 기술조건 중 유일하게 유의미 |
| SCENARIO3(ROE단독) | ROE≥8%+시총200 / 원안청산 | | 42.8% | +4.1% | 152 | 참고용 |
| SCENARIO4(PER+ROE) | PER≤18 AND ROE≥8% / 원안청산 | | 42.2% | +2.7% | 83 | 참고용 |

**Walk-forward 메타 결론**(개별 수치 아님): train→test 평균수익 하락폭 평균 -26.7%p,
8개 폴드×필터 중 test 플러스 유지는 3개뿐. 단 "청산을 넓히면 개선"이라는 방향성
자체는 8폴드 중 6개에서 재현 — **방향성은 신뢰 가능, 절대 수익률(+9.47%/+16.82%)은
표본이 작을 때의 순차 그리드서치 착시에 가깝다.**

## Tier D — compose AND 계열 (표본 부족, 대부분 노이즈)

| 모델명 | 구성요소 | 코드 위치 | 승률 | 평균수익 | MDD | 표본 | 상태 |
|---|---|---|---|---|---|---|---|
| AND-1~4 | Ichimoku∩Stage2+∩수급 (+변형) | `ENTRY_COMPONENTS["AND-1"]`~`["AND-4"]` | 50~80% | +12~24% | -0~18% | 3~10 | 표본 미달(판단 보류) |
| AND-5 | +stage_any 완화 | `ENTRY_COMPONENTS["AND-5"]` | 38.8% | -0.1% | -35.6% | 49 | 사실상 마이너스 |
| AND-6 | +거래대금 필터 | `ENTRY_COMPONENTS["AND-6"]` | 26.5% | -4.3% | -38.7% | 34 | 사실상 마이너스 |

## Tier E — 폐기 / 무의미 (breakeven 이하 또는 신뢰 불가)

| 모델명 | 구성요소 | 코드 위치 | 승률 | 평균수익 | 표본 | 상태 |
|---|---|---|---|---|---|---|
| SCENARIO1 (1안: 밸류+추세추종) | PBR≤0.8+ROE≥10%+부채≤100% / MA20+거래량 | `ENTRY_COMPONENTS["SCENARIO1"]` × `quant_scenario1` | 21.7~22.2% | -0.5~-1.3% | 9~46 | **폐기 확정**(유니버스 6종 완화해도 전부 breakeven 이하) |
| A_ma20_breakout | 이평선 돌파 단독 | `ENTRY_COMPONENTS["A_ma20_breakout"]` | 24.5~24.8% | +0.4~0.6% | 41K~113K | 폐기 |
| B_ma_alignment | 정배열 단독 | `ENTRY_COMPONENTS["B_ma_alignment"]` | 24.8~28.0% | +0.1~0.8% | 12K~27K | 폐기 |
| C_rsi_macd_rebound | RSI/MACD 반등 단독 | `ENTRY_COMPONENTS["C_rsi_macd_rebound"]` | 31.6~31.9% | -0.0~0.2% | 53K~148K | 폐기 |
| D_new_high20 | 20일 신고가 단독 | `ENTRY_COMPONENTS["D_new_high20"]` | 29.4~33.1% | +0.5~1.7% | 29K~72K | 폐기(그나마 최선) |
| D×ATR/Donchian청산 (H) | D_new_high20 / atr2x_donchian10 | × `EXIT_COMPONENTS["atr2x_donchian10"]` | 31.1% | +2.7% | 29,380 | 참고용(승률↓ 평균↑ 패턴 재확인, 여전히 breakeven급) |
| F(볼린저+RSI+거래량) | 3중확인 / bb_center_rsi50 | `ENTRY_COMPONENTS["F_bb_rsi_volume"]` × `EXIT_COMPONENTS["bb_center_rsi50"]` | 35.2% | +2.6% | 5,594 | 폐기(확인조건 늘려도 노이즈만 증가) |
| G(Larry Williams 변동성돌파) | k=0.5 상태전이 없는 매일판정 | `ENTRY_COMPONENTS["G_volatility_breakout"]` | 28.7% | +13.7%(액면신뢰금지) | 208,727 | **신뢰 금지** — 스퀴즈 필터 부재로 시장베타 포착 |
| QVM 시총제한없음 (top10/20%) | 3팩터, 유니버스 제한없음 | `UNIVERSE_COMPONENTS["QVM_top10pct_all"]`/`["QVM_top20pct_all"]` | 33.1~35.8% | +13.2~20.2%(액면신뢰금지) | 1,334~2,472 | 참고용 — 소수 대박종목 의존(G와 동일 패턴) |
| 팩터 분해 비교군 (quality/value 단독·조합) | quality/value/quality+value/quality+momentum/value+momentum | (시총200∩top20 고정) | 41.5~58.3% | +3.1~8.3% | 46~127 | 참고용 — 전부 모멘텀 단독보다 열위 |

---

## 재현 커맨드 요약

```bash
# 순수 기술적 퀀트 (A~E, SCENARIO1/2)
python scripts/run_quant_backtest.py --condition ALL --start 2022-01-01 --end 2026-08-14 --use-fundamentals

# QVM / 모멘텀 단독 + 팩터분해
python scripts/run_quant_qvm_backtest.py --start 2025-01-02 --end 2026-08-06
python scripts/run_quant_qvm_factor_ablation.py

# compose 전략 (FUNNEL-1/SCORE-1/AND-1~6)
python scripts/run_compose.py --strategy ALL --start 2022-01-01 --end 2026-08-14

# entry×exit 교차 조합 (Tier S/B의 대부분)
python scripts/run_cross_combo_backtest.py --start 2025-01-02 --end 2026-08-06
```

전체 서술·그리드서치 원본표·과최적화 검증 과정은 [`TechnicalQuant.md`](../../TechnicalQuant.md) 참고.
