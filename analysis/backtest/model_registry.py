"""백테스트 컴포넌트 카탈로그 — 지금까지 테스트된 모든 진입/필터/청산 조각을
한곳에서 찾아 조합할 수 있도록 모아놓은 인덱스.

**이 파일은 새 로직을 만들지 않는다.** 기존에 이미 검증된 dict/함수를 그대로
재참조(re-export)만 한다 — `strategy_compose.STRATEGIES`, `quant_signals.
ENTRY_CONDITIONS`, `config.OPTIMAL_EXIT_PARAMS*` 등 기존 import 경로는 전부
그대로 동작한다. 목적은 "어떤 조합이 있었는지, 어디서 가져다 쓰면 되는지"를
한 파일에서 훑어볼 수 있게 하는 것 — `docs/02_reference/reference-model-registry.md`
(성과·상태 포함 전체 카탈로그)의 코드 짝.

**신규 조합을 만들 때**: 아래 ENTRY_COMPONENTS/EXIT_COMPONENTS에서 이름을 골라
`ref`(callable/dict)를 실제 백테스트 함수에 넘기면 된다 — 이미 이 방식으로
작동하는 예시가 `scripts/run_cross_combo_backtest.py`(entry×exit 교차 조합
엔진, 15개 조합 실행 이력 있음).

컴포넌트 3종류:
  - ENTRY_COMPONENTS:    "이 신호가 뜨면 산다" — compose 전략 + quant 개별조건
  - UNIVERSE_COMPONENTS: "이 종목들만 대상으로 한다" — QVM/모멘텀 팩터 유니버스
  - EXIT_COMPONENTS:     "이렇게 판다" — 분할청산(TP1+트레일링) / quant 자기완결청산
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, Optional

from analysis.backtest import config as _config
from analysis.backtest.quant_signals import ENTRY_CONDITIONS as _QUANT_ENTRY_CONDITIONS
from analysis.fundamentals import QVM_UNIVERSE_VARIANTS as _QVM_UNIVERSE_VARIANTS
from analysis.strategy_compose import STRATEGIES as _COMPOSE_STRATEGIES


@dataclass(frozen=True)
class Component:
    """카탈로그 한 항목 — 실제 로직이 아니라 로직에 대한 포인터."""

    name: str
    kind: str          # "entry" | "universe" | "exit"
    source: str        # "파일경로::심볼명" — 실제 정의 위치
    desc: str          # 한 줄 설명(무엇을 하는지, TechnicalQuant.md 어느 섹션인지)
    ref: Optional[Any] = None  # 부작용 없이 바로 import 가능한 경우의 실제 callable/dict


# ──────────────────────────────────────────────────────────────
# ENTRY_COMPONENTS — "이 신호가 뜨면 산다"
# ──────────────────────────────────────────────────────────────

ENTRY_COMPONENTS: dict[str, Component] = {}

# compose 전략 (analysis/strategy_compose.py::STRATEGIES) — 이미 그 자체로
# 선언적 레지스트리(StrategySpec.kind: and/score/funnel)라 그대로 재참조.
for _name, _spec in _COMPOSE_STRATEGIES.items():
    ENTRY_COMPONENTS[_name] = Component(
        name=_name, kind="entry",
        source=f"analysis/strategy_compose.py::STRATEGIES[{_name!r}]",
        desc=f"compose {_spec.kind} 전략 — sources={_spec.sources}",
        # deepcopy: StrategySpec은 non-frozen dataclass에 list/dict 필드(flags/
        # weights)를 갖고 있어, 원본을 그대로 넘기면 카탈로그에서 실험 삼아
        # spec.flags.append(...) 했을 때 실제 운용 중인 STRATEGIES 원본까지
        # 조용히 오염된다(2026-08-22 adversarial review 발견).
        ref=copy.deepcopy(_spec),
    )

# quant 개별 기술조건 (analysis/backtest/quant_signals.py::ENTRY_CONDITIONS)
_QUANT_ENTRY_DESC = {
    "A_ma20_breakout":       "이평선 돌파 — 승률24.5~24.8%/평균+0.4~0.6% (breakeven 근처, 폐기)",
    "B_ma_alignment":        "정배열(5>20>60>120일선) — 승률24.8~28.0%/평균+0.1~0.8% (breakeven, 폐기)",
    "C_rsi_macd_rebound":    "RSI/MACD 반등 — 승률31.6~31.9%/평균-0.0~0.2% (breakeven, 폐기)",
    "D_new_high20":          "20일 신고가 돌파 — 승률29.4~33.1%/평균+0.5~1.7% (breakeven 근처)",
    "F_bb_rsi_volume":       "볼린저+RSI+거래량 3중확인 — 승률35.2%/평균+2.6% (F, 노이즈만 키움)",
    "G_volatility_breakout": "Larry Williams 변동성 돌파(k=0.5) — 신뢰 금지(스퀴즈 필터 부재, 시장베타 포착)",
    "SCENARIO1":             "1안: 밸류+추세추종 — 승률21.7~22.2%/평균-0.5~-1.3% (폐기 확정)",
    "SCENARIO2":             "2안: 대형주+RSI과매도반등 — 승률39.6~43.5%/평균+2.2~4.8% (유일하게 유의미한 순수기술조건)",
}
for _name, _cond in _QUANT_ENTRY_CONDITIONS.items():
    ENTRY_COMPONENTS[_name] = Component(
        name=_name, kind="entry",
        source=f"analysis/backtest/quant_signals.py::ENTRY_CONDITIONS[{_name!r}]",
        desc=_QUANT_ENTRY_DESC.get(_name, ""),
        ref=_cond,
    )


# ──────────────────────────────────────────────────────────────
# UNIVERSE_COMPONENTS — "이 종목들만 대상으로 한다" (QVM/모멘텀 팩터 유니버스)
#
# 실제 계산 함수는 analysis/fundamentals.py::compute_qvm_score +
# screen_qvm_top_pct — 유니버스마다 새 코드가 아니라 factors/top_pct/
# mktcap_restrict 파라미터만 다르다. 사용법:
#   ratios_df = compute_ratios(dsn); momentum_df = load_momentum(dsn)
#   qvm_df = compute_qvm_score(ratios_df, momentum_df, factors=<factors>)
#   universe = screen_qvm_top_pct(qvm_df, top_pct=<top_pct>)
#   (mktcap_restrict=True면 시총상위200으로 추가 교집합 — run_quant_qvm_backtest.py 참고)
#
# QVM 5개 변형의 factors/top_pct/mktcap_restrict는 analysis/fundamentals.py::
# QVM_UNIVERSE_VARIANTS가 단일 출처(2026-08-23, scripts/run_quant_qvm_backtest.py
# 쪽도 같은 상수를 가져다 씀 — 손으로 옮겨 적어 값이 따로 놀던 문제 해소).
# ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class UniverseParams:
    factors: tuple[str, ...]
    top_pct: float
    mktcap_restrict: bool


_QVM_UNIVERSE_DESC = {
    "QVM_top10pct_mktcap200": "퀄리티+밸류+모멘텀 3팩터, 시총200∩상위10% (8종목) — 승률54.3%/평균+6.8%/MDD-16.5%, 신호35",
    "QVM_top20pct_mktcap200": "퀄리티+밸류+모멘텀 3팩터, 시총200∩상위20% (15종목) — 승률57.4%/평균+8.5%/MDD-12.6%, 신호47 [비과최적화 조합 중 최고]",
    "QVM_top30pct_mktcap200": "퀄리티+밸류+모멘텀 3팩터, 시총200∩상위30% (27종목) — 승률58.6%/평균+8.4%/MDD-18.4%, 신호70",
    "QVM_top10pct_all": "3팩터, 시총제한없음∩상위10% (237종목) — 승률33.1%/평균+20.2%, 소수 대박종목 의존 구조(액면신뢰 금지)",
    "QVM_top20pct_all": "3팩터, 시총제한없음∩상위20% (475종목) — 승률35.8%/평균+13.2%, 위와 동일 패턴",
}

UNIVERSE_COMPONENTS: dict[str, Component] = {}
for _v in _QVM_UNIVERSE_VARIANTS:
    UNIVERSE_COMPONENTS[_v["name"]] = Component(
        name=_v["name"], kind="universe",
        source="analysis/fundamentals.py::QVM_UNIVERSE_VARIANTS",
        desc=_QVM_UNIVERSE_DESC.get(_v["name"]) or str(_v.get("note", "")),
        ref=UniverseParams(
            factors=("quality", "value", "momentum"),
            top_pct=_v["top_pct"], mktcap_restrict=_v["mktcap_restrict"],
        ),
    )

# 모멘텀 단독 유니버스는 QVM_UNIVERSE_VARIANTS에 없는 별도 발견(4-2 팩터분해
# 결론) — build_factor_combos()의 itertools 조합 중 하나를 top_pct=0.20으로
# 고른 것이라 "그리드에서 손으로 옮겨 적은 값"이 애초에 존재하지 않는다
# (재현: scripts/run_quant_qvm_factor_ablation.py --top-pct 0.20).
UNIVERSE_COMPONENTS["MOMENTUM_top20pct_mktcap200"] = Component(
    name="MOMENTUM_top20pct_mktcap200", kind="universe",
    source="scripts/run_quant_qvm_factor_ablation.py --top-pct 0.20 (momentum 단독)",
    desc=(
        "모멘텀(6개월수익률) 단독, 시총200∩상위20% (51종목) — 승률64.1%/평균+11.4%/"
        "MDD-12.6%, 신호103 [퀄리티+밸류를 섞으면 오히려 희석됨, 4-2 팩터분해 결론]. "
        "run_cross_combo_backtest.py에서 'MOMENTUM_TOP20'으로 참조."
    ),
    ref=UniverseParams(factors=("momentum",), top_pct=0.20, mktcap_restrict=True),
)


# ──────────────────────────────────────────────────────────────
# EXIT_COMPONENTS — "이렇게 판다"
# ──────────────────────────────────────────────────────────────

EXIT_COMPONENTS: dict[str, Component] = {}

# 분할청산(TP1 분할 + 트레일링 스탑) — analysis/backtest/exit_models.py::
# _compute_exit_logic가 BacktestConfig 필드로 아래 파라미터를 받아 실행.
_SPLIT_EXIT_PARAMS: dict[str, dict] = {
    "stage_kospi": _config.OPTIMAL_EXIT_PARAMS,
    "stage_kosdaq": _config.OPTIMAL_EXIT_PARAMS_KOSDAQ,
    "cross": _config.OPTIMAL_EXIT_PARAMS_CROSS,
    "score1": _config.OPTIMAL_EXIT_PARAMS_SCORE1,
    "funnel1": _config.OPTIMAL_EXIT_PARAMS_FUNNEL1,
    "ichimoku": _config.OPTIMAL_EXIT_PARAMS_ICHIMOKU,
}
for _name, _params in _SPLIT_EXIT_PARAMS.items():
    EXIT_COMPONENTS[_name] = Component(
        name=_name, kind="exit",
        source="analysis/backtest/config.py + analysis/backtest/exit_models.py::_compute_exit_logic",
        desc=f"분할청산(TP1 {_params['tp1_pct']*100:.0f}%/{_params['tp1_ratio']*100:.0f}% + 트레일 {_params['trail_pct']*100:.0f}% + 손절 {_params['hard_stop_pct']*100:.0f}%)",
        # dict() 복사: _params는 config.py의 OPTIMAL_EXIT_PARAMS* 원본 dict 그
        # 자체 — engine.py가 실제 운용에 쓰는 것과 동일 객체라, 카탈로그에서
        # 실험 삼아 ref['trail_pct'] = ... 하면 운용 파라미터까지 조용히
        # 바뀐다(2026-08-22 adversarial review 발견). 얕은 복사로 충분(전부 스칼라).
        ref=dict(_params),
    )

# quant 자기완결 청산(RSI 익절/MA20이탈/목표가/손절) — analysis/backtest/
# quant_signals.py::_scan_exit가 아래 kwargs를 그대로 받는다. 정의는
# analysis/backtest/config.py::QUANT_EXIT_VARIANTS가 단일 출처(2026-08-23,
# scripts/run_cross_combo_backtest.py도 같은 상수를 가져다 씀 — 예전엔 여기서
# 손으로 복제하고 있었음).
_QUANT_EXIT_VARIANTS: dict[str, dict] = _config.QUANT_EXIT_VARIANTS
_QUANT_EXIT_DESC = {
    "quant_original": "2안 문서원안 청산 — SCENARIO2와 짝지으면 승률43.0~43.5%/평균+2.9~4.8%",
    "quant_optimized": "2안 최적화 청산(5단계 최적) — SCENARIO2와 짝지으면 승률50.4~55.2%/평균+9.47~16.82% (과최적화 위험, walk-forward에서 8폴드 중 5개 test 마이너스 전환)",
    "quant_scenario1": "1안 문서원안 청산 — SCENARIO1과 짝지으면 폐기된 조합(승률22.2%/평균-1.3%)",
}
for _name, _params in _QUANT_EXIT_VARIANTS.items():
    EXIT_COMPONENTS[_name] = Component(
        name=_name, kind="exit",
        source="analysis/backtest/quant_signals.py::_scan_exit (파라미터 출처: scripts/run_cross_combo_backtest.py::QUANT_EXIT_VARIANTS)",
        desc=_QUANT_EXIT_DESC[_name],
        ref=_params,
    )
del _name, _params

# 방법론별 전용 자기완결 청산 (F/H) — 파라미터 dict가 아니라 진입조건과 1:1로
# 묶인 전용 replay 함수. 다른 진입조건에 옮겨 쓸 수 없음(설계상 그 진입에
# 맞춰진 로직).
EXIT_COMPONENTS["bb_center_rsi50"] = Component(
    name="bb_center_rsi50", kind="exit",
    source="analysis/backtest/quant_signals.py::replay_quant_bb_exit (_scan_exit_bb)",
    desc="F 전용: 진입캔들 저점 손절 → MA20 복귀 목표 → RSI>50 탈출 → 기간종료. F_bb_rsi_volume과만 짝지어 사용.",
)
EXIT_COMPONENTS["atr2x_donchian10"] = Component(
    name="atr2x_donchian10", kind="exit",
    source="analysis/backtest/quant_signals.py::replay_quant_donchian_atr (_scan_exit_donchian_atr)",
    desc="H 전용: 진입가-2×ATR14 손절 → 종가<10일 저가 이탈 → 기간종료. D_new_high20과 짝지으면 승률31.1%/평균+2.7%(D 원안 대비 승률↓ 평균↑).",
)


def all_components() -> dict[str, list[str]]:
    """디버그/확인용 — 종류별 컴포넌트 이름 목록."""
    return {
        "entry": sorted(ENTRY_COMPONENTS),
        "universe": sorted(UNIVERSE_COMPONENTS),
        "exit": sorted(EXIT_COMPONENTS),
    }


__all__ = [
    "Component",
    "UniverseParams",
    "ENTRY_COMPONENTS",
    "UNIVERSE_COMPONENTS",
    "EXIT_COMPONENTS",
    "all_components",
]
