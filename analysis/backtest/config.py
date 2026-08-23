"""백테스트 상수·튜닝 파라미터 (backtest_engine.py에서 이동, Phase C)."""

from __future__ import annotations

# ── 거래 비용 ─────────────────────────────────────────────────────
_TX_BUY  = 0.000140                              # 증권사 매수 수수료
_TX_SELL = 0.000140 + 0.001800 + 0.000020        # 수수료 + 증권거래세 + 농특세
TX_COST_DEFAULT: float = _TX_BUY + _TX_SELL      # ≈ 0.0021 (0.210%)

_S1_THRESHOLD = {"KOSPI": 0.05, "KOSDAQ": 0.07}  # Stage 1 일봉 상승률 기준

_STOP_LOSS_PCT: float = 0.08  # 기본 손절 기준 (−8%)

MODE_KOR: dict[str, str] = {
    "ichimoku":   "이치모쿠(주봉)",
    "stage":      "3단계 v1.0(일봉)",
    "cross":      "이치모쿠×3단계",
    "stage2":     "Stage2 v1.0(일봉)",
    "compose":    "조합전략",
    "stage_v11":  "3단계 v1.1(일봉)",
    "stage2_v11": "Stage2 v1.1(일봉)",
    "stage_v12":  "3단계 v1.2(일봉)",
    "stage2_v12": "Stage2 v1.2(일봉)",
    "stage_v13":  "3단계 v1.3(일봉)",
    "stage2_v13": "Stage2 v1.3(일봉)",
    "stage_v14":  "3단계 v1.4(일봉)",
    "stage_v15":  "3단계 v1.5(일봉)",
}

# ── 그리드서치로 검증된 최적 청산 파라미터 (모드별) ─────────────────
# 출처: scripts/run_sweep.py, 2024-01-01~2025-06-30 학습 / 2025-07-01~2026-05-12 검증
#
# Stage 모드 (KOSPI 200, 신호 ~1006건/검증기간)
#   val_sharpe=4.70, val_win_rate=45.7%, val_cagr=46.5%, overfit_gap=-0.23
OPTIMAL_EXIT_PARAMS: dict = {
    "tp1_pct":         0.25,   # 1차 익절 목표 +25%
    "tp1_ratio":       0.50,   # 1차에서 50% 청산, 나머지 50% 트레일링
    "trail_pct":       0.10,   # 고점 대비 -10% 트레일링 스탑
    "hard_stop_pct":   0.10,   # 진입가 대비 -10% 하드 스탑
    "use_stage3_peak": False,
}

# Stage 모드 KOSDAQ (신호 ~1227건/검증기간, val>>train — 검증기간 강세장 반영)
#   val_sharpe=5.48, val_win_rate=46.7%, val_cagr=67.7%
#   trail=15%: KOSDAQ 변동성 특성상 더 넓은 트레일이 유리.
OPTIMAL_EXIT_PARAMS_KOSDAQ: dict = {
    "tp1_pct":         0.25,
    "tp1_ratio":       0.50,
    "trail_pct":       0.15,   # KOSPI(10%)보다 넓음 — KOSDAQ 변동성 반영
    "hard_stop_pct":   0.10,
    "use_stage3_peak": False,
}

# Cross 모드 (Ichimoku × Stage, ALL시장, 신호 ~105건/검증기간, 과적합 필터 적용)
#   val_sharpe=5.11, val_win_rate=54.3%, val_cagr=46.6%, overfit_gap=0.29
#   tp1_pct=0.15: 과적합 없이 승률 극대화. 0.25 대비 낮지만 gap 안전.
OPTIMAL_EXIT_PARAMS_CROSS: dict = {
    "tp1_pct":         0.15,
    "tp1_ratio":       0.50,
    "trail_pct":       0.10,
    "hard_stop_pct":   0.10,
    "use_stage3_peak": False,
}

# SCORE-1 / FUNNEL-1 전용 청산 파라미터
# 출처: scripts/run_sweep.py --mode compose, 학습 2025-01-01~2025-10-31 /
#       검증 2025-11-01~2026-07-24 (검증기간에 2026년 3~4월 급변동장 포함).
#
# 이 기간 분할에서는 전 조합에서 overfit_gap(train_sharpe-val_sharpe)이
# 2~5대로 CROSS 모드 기준(0.29)보다 훨씬 크게 나오는데, 이는 특정 파라미터의
# 과최적화가 아니라 학습기간(저변동)과 검증기간(2026 급변동장)의 시장 레짐
# 차이 자체가 원인임을 별도 확인함(compose 백테스트 기간분할 검증: 학습
# MDD -0.2~-2.5% vs 검증 MDD -33~-35%). 따라서 여기서는 overfit_gap 최소화
# 대신 검증기간(더 가혹한 구간) 기준 val_mdd/val_win_rate로 선정 —
# hard_stop_pct는 두 전략 모두 0.10(스윕 최대값)이 승률·MDD 모두 최선이었음
# (타이트한 손절이 급변동장에서 휩쏘 손실을 키움).
#
# SCORE-1: val_sharpe=6.80, val_win_rate=65.4%, val_mdd=-36.7%, val_n=758
OPTIMAL_EXIT_PARAMS_SCORE1: dict = {
    "tp1_pct":         0.10,
    "tp1_ratio":       0.70,
    "trail_pct":       0.15,
    "hard_stop_pct":   0.10,
    "use_stage3_peak": False,
}

# FUNNEL-1: val_sharpe=6.20, val_win_rate=60.2%, val_mdd=-15.1%, val_n=2920
OPTIMAL_EXIT_PARAMS_FUNNEL1: dict = {
    "tp1_pct":         0.15,
    "tp1_ratio":       0.70,
    "trail_pct":       0.15,
    "hard_stop_pct":   0.10,
    "use_stage3_peak": False,
}

# Ichimoku 단독 모드 (주봉 7조건, ALL시장, 신호 ~1097건/전체, val 439건)
#   val_sharpe=7.50, val_win_rate=55.8%, overfit_gap=0.24
#   tp1_ratio=0.70: 1차 익절에서 70% 청산 — 주봉 전략 특성상 조기 익절 비중 높임.
#   use_stage3_peak=False: Stage3 peakout 데이터 없어도 결과 동일.
OPTIMAL_EXIT_PARAMS_ICHIMOKU: dict = {
    "tp1_pct":         0.25,
    "tp1_ratio":       0.70,
    "trail_pct":       0.10,
    "hard_stop_pct":   0.10,
    "use_stage3_peak": False,
}

# ── quant 자기완결 청산(RSI 익절/MA20이탈/목표가/손절) 3변형 ──────────────
# analysis/backtest/quant_signals.py::_scan_exit가 이 kwargs를 그대로 받는다.
# 원래 scripts/run_cross_combo_backtest.py에만 있었는데 analysis/backtest/
# model_registry.py가 손으로 옮겨 적으면서 두 값이 따로 놀 위험이 생겼음
# (2026-08-22 review 발견) — 여기로 옮겨 양쪽 다 이 상수를 가져다 쓰게 통일
# (2026-08-23).
QUANT_EXIT_VARIANTS: dict[str, dict] = {
    "quant_original": dict(     # 2안 문서 원안: RSI70 익절 / -7% 손절
        hard_stop_pct=0.07, target_pct=None, use_ma20_exit=False,
        use_rsi70_exit=True, rsi_overbought=70.0,
    ),
    "quant_optimized": dict(    # 2안 최적화(5단계 그리드서치 1위): RSI80 익절 / -12% 손절
        hard_stop_pct=0.12, target_pct=None, use_ma20_exit=False,
        use_rsi70_exit=True, rsi_overbought=80.0,
    ),
    "quant_scenario1": dict(    # 1안 문서 원안: +20%익절 / -5%손절 / MA20 하향이탈
        hard_stop_pct=0.05, target_pct=0.20, use_ma20_exit=True,
        use_rsi70_exit=False,
    ),
}
