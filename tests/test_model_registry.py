"""analysis/backtest/model_registry.py — 카탈로그 정합성 회귀 테스트.

이 레지스트리는 새 로직이 아니라 기존 dict/함수에 대한 포인터 모음이므로,
정말 지켜야 할 건 "이름이 실제로 존재하는 대상을 가리키는지"뿐이다 —
underlying 파일이 리팩터링되면서 조용히 끊기는 걸 잡아낸다.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis.backtest.model_registry import (
    EXIT_COMPONENTS,
    ENTRY_COMPONENTS,
    UNIVERSE_COMPONENTS,
    UniverseParams,
    all_components,
)


def test_entry_components_cover_compose_and_quant():
    """compose 전략(STRATEGIES)과 quant 조건(ENTRY_CONDITIONS)이 모두 들어있다."""
    from analysis.strategy_compose import STRATEGIES
    from analysis.backtest.quant_signals import ENTRY_CONDITIONS

    for name in STRATEGIES:
        assert name in ENTRY_COMPONENTS, f"compose 전략 {name} 누락"
    for name in ENTRY_CONDITIONS:
        assert name in ENTRY_COMPONENTS, f"quant 조건 {name} 누락"


def test_every_component_has_name_kind_source():
    """모든 컴포넌트는 name/kind/source가 비어있지 않아야 한다 — 카탈로그로서 최소 조건."""
    for bucket, expected_kind in (
        (ENTRY_COMPONENTS, "entry"),
        (UNIVERSE_COMPONENTS, "universe"),
        (EXIT_COMPONENTS, "exit"),
    ):
        for key, comp in bucket.items():
            assert comp.name == key
            assert comp.kind == expected_kind
            assert comp.source, f"{key}: source가 비어있음"


def test_universe_params_shape():
    """UNIVERSE_COMPONENTS의 ref는 전부 UniverseParams — factors/top_pct/mktcap_restrict."""
    for key, comp in UNIVERSE_COMPONENTS.items():
        assert isinstance(comp.ref, UniverseParams), key
        assert 0 < comp.ref.top_pct <= 1.0
        assert set(comp.ref.factors) <= {"quality", "value", "momentum"}


def test_split_exit_params_have_required_keys():
    """분할청산 계열 exit(cross/score1/funnel1 등)은 exit_models._compute_exit_logic이
    기대하는 4개 필수 키를 전부 갖는다."""
    required = {"tp1_pct", "tp1_ratio", "trail_pct", "hard_stop_pct"}
    split_exit_names = {"stage_kospi", "stage_kosdaq", "cross", "score1", "funnel1", "ichimoku"}
    for name in split_exit_names:
        comp = EXIT_COMPONENTS[name]
        assert isinstance(comp.ref, dict)
        assert required <= set(comp.ref)


def test_quant_exit_params_have_required_keys():
    """quant 자기완결청산(_scan_exit)이 기대하는 4개 필수 키를 전부 갖는다."""
    required = {"hard_stop_pct", "target_pct", "use_ma20_exit", "use_rsi70_exit"}
    for name in ("quant_original", "quant_optimized", "quant_scenario1"):
        comp = EXIT_COMPONENTS[name]
        assert isinstance(comp.ref, dict)
        assert required <= set(comp.ref)


def test_all_components_returns_sorted_name_lists():
    result = all_components()
    assert set(result) == {"entry", "universe", "exit"}
    for names in result.values():
        assert names == sorted(names)
        assert len(names) > 0
