# How to: TechnicalQuant.md 기반 퀀트 백테스트 실행 및 결과 해석

사용자가 제공한 퀀트 전략 문서(`TechnicalQuant.md`, 저장소 루트)의 종목선택(펀더멘털)
+ 매매타이밍(기술적 지표) 조건을 실제 전종목 데이터로 검증한다.

## 전제 조건

- `daily_ohlcv`, `daily_flow`, `krx_listings`에 전종목 데이터가 있어야 한다(기존 백테스트 인프라 재사용).
- `dart_fundamentals`에 재무 스냅샷이 있어야 한다(펀더멘털 필터 사용 시) — 없으면 먼저 백필:
  ```bash
  python scripts/dart_fundamentals_backfill.py --year 2025
  ```
- `.env`에 `DATABASE_URL`(또는 `DB_USER`/`DB_PASSWORD` 등) 필요.

## 구성 요소

| 파일 | 역할 |
|------|------|
| `analysis/backtest/quant_signals.py` | 진입조건 5종(A~E) + 시나리오1/2 재현, 자기완결 청산 로직(라이브 모의투자 `exit_models.py`와 무관) |
| `analysis/fundamentals.py` | PBR/PER/ROE/부채비율/매출증가율 계산 + 시나리오별 스크리닝 |
| `scripts/run_quant_backtest.py` | 조건별/시나리오별 백테스트 실행 CLI |
| `scripts/dart_fundamentals_backfill.py` | 전체시장 DART 재무제표 백필 |
| `scripts/run_quant_filter_sweep.py` | SCENARIO2 필터(PER 상한·시가총액 유니버스) 그리드서치 |

## 1단계: 기술적 조건만 (펀더멘털 없이)

```bash
# 개별 조건 5종 + 시나리오 2종 전체
python scripts/run_quant_backtest.py --start 2025-01-02 --end 2026-08-06

# 특정 조건만
python scripts/run_quant_backtest.py --condition D_new_high20 --start 2025-01-02 --end 2026-08-06
```

조건 목록:

| 키 | 내용 |
|----|------|
| `A_ma20_breakout` | 이평선 돌파: 주가>MA20 AND MA5>MA20 |
| `B_ma_alignment` | 정배열: MA5>MA20>MA60>MA120 |
| `C_rsi_macd_rebound` | RSI(14) 30 상향돌파 OR MACD 골든크로스 |
| `D_new_high20` | 신고가 돌파: 종가 >= 최근 20일 최고가 |
| `E_flow_streak` | 외국인 또는 기관 3일 연속 순매수 |
| `SCENARIO1` | 문서 1안(밸류+추세추종): MA20돌파+거래량200%↑ 매수 / -5%손절·+20%익절·MA20이탈 매도 |
| `SCENARIO2` | 문서 2안(역발상 과매도반등): RSI30 상향돌파 매수 / RSI70익절·-7%손절 매도 |

유니버스 필터(거래대금 상위20%/시가총액 상위200)는 `daily_ohlcv`+`krx_listings`만으로
계산 — 펀더멘털 데이터 불필요. **주의**: 기간 평균 거래대금/시가총액으로 순위를
매기는 근사치이며(일별 cross-sectional 아님), 조건은 전부 "상태 지속"이 아니라
"전이"(어제 미충족 → 오늘 충족)로 판정한다.

## 2단계: 시나리오1/2에 실제 펀더멘털 필터 적용

```bash
python scripts/run_quant_backtest.py --condition SCENARIO1 --start 2025-01-02 --end 2026-08-06 --use-fundamentals
python scripts/run_quant_backtest.py --condition SCENARIO2 --start 2025-01-02 --end 2026-08-06 --use-fundamentals
```

**중요**: 1안과 2안은 서로 다른 종목선택 숫자를 쓴다 — 동일한 범용 필터를
공유하면 안 된다(`analysis/fundamentals.py`의 `SCENARIO1_THRESHOLDS`/
`SCENARIO2_THRESHOLDS` 참고).

| 시나리오 | 종목선택 조건 |
|----------|----------------|
| 1안 | PBR ≤ 0.8, ROE ≥ 10%, 부채비율 ≤ 100% (PER·매출증가율 조건 없음) |
| 2안 | PER ≤ 15 (PBR·ROE·부채비율·매출증가율 조건 없음) |

## 3단계: SCENARIO2 필터 최적화 (PER 상한 / 시가총액 유니버스)

매매타이밍(RSI 30 진입/RSI 70 익절/-7% 손절)은 고정하고, "필터"(종목선택 조건)만
그리드서치한다 — 진입/청산까지 바꾸면 다른 전략이 되므로 범위를 의도적으로 좁혔다.

```bash
python scripts/run_quant_filter_sweep.py --start 2025-01-02 --end 2026-08-06
```

기본 그리드: PER 상한 `[10, 12, 15, 18, 20, 25]` × 시가총액 상위 `[100, 150, 200, 300, 500]`
(30개 조합). 신호 30건 미만 조합은 결과 표에서 제외(CSV에는 전부 남음) — 소표본
그리드서치가 노이즈를 "최적값"으로 착각하게 만드는 걸 방지(2026-08-06 AND-1
스윕에서 관측된 문제, `project_compose_strategies` 메모리 참고).

## 결과 해석

- `win_rate`: 청산 완료된 신호 중 수익률 > 0 비율.
- `avg_return`: 청산 신호의 평균 실현수익률(거래비용 반영, `TX_COST_DEFAULT` 왕복 0.21%).
- 표본이 30건 미만이면 승률/평균수익 숫자를 신뢰하지 말 것 — 특히 시나리오1은
  구조적으로(거래대금 상위20% ∩ PBR/ROE/부채비율 필터) 유니버스가 3~19종목까지
  줄어들어 판단 자체가 어렵다.

### 2026-08-06 검증 결과 요약

| 조건 | 신호수 | 승률 | 평균수익 |
|------|--------|------|----------|
| 개별 조건 A~E | 12,721~83,519 | 24.8~34.2% | +0.2~1.3% (거래비용 감안 시 사실상 무의미) |
| 1안 (문서 정확 재현) | 9 | 22.2% | **-1.3%**(마이너스, 표본 극소) |
| 2안 (문서 정확 재현) | 100 | **43.0%** | **+2.9%** |

2안만 세 가지 검증 방식(기술적 단독/범용 펀더멘털/문서 정확 재현) 모두에서 일관되게
양호했으나, 기존 조합전략 FUNNEL-1(승률28d 62.8%)/SCORE-1(60.2%) 대비로는 여전히
열위 — 새 주력 전략으로 채택하기보다 보조 후보로만 취급. 상세 근거는
`~/.claude/projects/.../memory/project_technicalquant_backtest.md` 참고(세션 메모리,
저장소에는 없음).

## 트러블슈팅

**`캐시 히트: 0 미스: N개`가 매번 뜬다**
→ 요청 구간이 `daily_ohlcv` 백필 시작일(2025-01-02)보다 앞선 워밍업 구간(MA120용
`start - 400일`)까지 포함해서 발생 — `_get_coverage`가 그 구간을 커버 못 하면
전체를 yfinance로 재수집한다. 느리지만(전종목 기준 2~3분) 매 실행마다 DB에
저장되므로 다음 실행부터는 점점 빨라진다.

**시나리오1 신호가 너무 적음(한 자릿수)**
→ 정상 — "거래대금 상위 20%"(대형·유동주)와 "PBR≤0.8·ROE≥10%·부채비율≤100%"
(저평가·고수익·저부채)의 교집합이 원래 좁다. 완화하려면 `--start`를 더 길게
잡거나(신호 자체가 시간에 비례해 늘어남), 유니버스 조건(거래대금 상위 20%→50%)을
`scripts/run_quant_backtest.py`의 `_select_universe` 호출부에서 완화.

## 관련

- [TechnicalQuant.md](../../TechnicalQuant.md) — 원본 전략 문서
- [howto-backtest.md](howto-backtest.md) — 기존 조합전략(FUNNEL-1/SCORE-1 등) 백테스트, 비교 기준선
- [howto-dart-setup.md](howto-dart-setup.md) — DART API 연동 설정
- [dart-pipeline.md](dart-pipeline.md) — DART 파이프라인 구조(내러티브용 `dart_xbrl` vs 재무비율용 `dart_fundamentals` 구분)
