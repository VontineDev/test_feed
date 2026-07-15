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
