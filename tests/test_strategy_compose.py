"""
test_strategy_compose.py — 신호 합성 조합기 단위 테스트

순수 함수(조합기·플래그·주차 유틸)만 합성 프레임으로 검증. DB 의존
load_signal_frame은 통합 테스트 대상으로 제외.
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from analysis.strategy_compose import (
    iso_week,
    week_to_friday,
    week_distance,
    derive_flags,
    and_gate,
    composite_score,
    funnel,
    STRATEGIES,
)


# ── 주차 유틸 ─────────────────────────────────────────────────
class TestWeekUtils:
    def test_iso_week_format(self):
        assert iso_week(date(2026, 4, 24)) == "2026-W17"
        assert iso_week(date(2026, 1, 9)) == "2026-W02"  # 제로패딩

    def test_week_to_friday(self):
        assert week_to_friday("2026-W17") == date(2026, 4, 24)
        assert week_to_friday("2026-W17").isoweekday() == 5

    def test_week_distance(self):
        assert week_distance("2026-W17", "2026-W17") == 0
        assert week_distance("2026-W17", "2026-W20") == 3
        assert week_distance("2026-W20", "2026-W17") == -3

    def test_week_distance_across_year(self):
        assert week_distance("2025-W52", "2026-W01") == 1

    def test_week_string_lexical_sort_is_chronological(self):
        weeks = ["2026-W02", "2025-W52", "2026-W10", "2026-W01"]
        assert sorted(weeks) == ["2025-W52", "2026-W01", "2026-W02", "2026-W10"]


# ── derive_flags ─────────────────────────────────────────────
class TestDeriveFlags:
    def test_stage2plus(self):
        df = pd.DataFrame({"ticker": ["A", "B", "C"], "week": ["2026-W01"] * 3,
                           "stage": [1, 2, 3]})
        out = derive_flags(df)
        assert out["stage2plus"].tolist() == [False, True, True]

    def test_stage_nan_is_false(self):
        df = pd.DataFrame({"ticker": ["A"], "week": ["2026-W01"], "stage": [np.nan]})
        out = derive_flags(df)
        assert out["stage2plus"].tolist() == [False]

    def test_flow_pos_foreign_or_inst(self):
        df = pd.DataFrame({
            "ticker": ["A", "B", "C", "D"], "week": ["2026-W01"] * 4,
            "foreign_net_w": [10, -5, -1, 0], "inst_net_w": [-3, 8, -2, 0],
        })
        out = derive_flags(df)
        assert out["flow_pos"].tolist() == [True, True, False, False]

    def test_vol_spike_threshold(self):
        df = pd.DataFrame({"ticker": ["A", "B"], "week": ["2026-W01"] * 2,
                           "vol_ratio": [2.5, 1.9]})
        out = derive_flags(df, vol_spike_x=2.0)
        assert out["vol_spike"].tolist() == [True, False]

    def test_narrative_hi_is_per_week_quantile(self):
        # 두 주, 각 주 내에서 상위 분위로 평가되어야 함
        df = pd.DataFrame({
            "ticker": ["A", "B", "C", "D", "E"],
            "week": ["2026-W01", "2026-W01", "2026-W01", "2026-W01", "2026-W02"],
            "attention_score": [10, 20, 30, 40, 1],
        })
        out = derive_flags(df, narrative_q=0.75)
        flags = dict(zip(out["ticker"], out["narrative_hi"]))
        assert flags["D"] is True or bool(flags["D"]) is True  # W01 최상위
        assert bool(flags["A"]) is False                       # W01 최하위
        assert bool(flags["E"]) is True                        # W02 단독 → 분위 1.0

    def test_ichimoku_nan_false(self):
        df = pd.DataFrame({"ticker": ["A", "B"], "week": ["2026-W01"] * 2,
                           "ichimoku": [True, np.nan]})
        out = derive_flags(df)
        assert out["ichimoku"].tolist() == [True, False]


# ── and_gate ─────────────────────────────────────────────────
class TestAndGate:
    def _frame(self):
        return pd.DataFrame({
            "ticker": ["A", "B", "C"],
            "week": ["2026-W01"] * 3,
            "ichimoku":   [True, True, False],
            "stage2plus": [True, False, True],
            "flow_pos":   [True, True, True],
        })

    def test_intersection_of_three(self):
        out = and_gate(self._frame(), ["ichimoku", "stage2plus", "flow_pos"])
        assert out["ticker"].tolist() == ["A"]  # A만 셋 다 참

    def test_two_flags_wider(self):
        out = and_gate(self._frame(), ["ichimoku", "flow_pos"])
        assert sorted(out["ticker"].tolist()) == ["A", "B"]

    def test_missing_flag_raises(self):
        with pytest.raises(ValueError):
            and_gate(self._frame(), ["ichimoku", "no_such_col"])

    def test_empty_flags_raises(self):
        with pytest.raises(ValueError):
            and_gate(self._frame(), [])

    def test_empty_frame_returns_empty(self):
        empty = pd.DataFrame(columns=["ticker", "week", "ichimoku"])
        out = and_gate(empty, ["ichimoku"])
        assert out.empty
        assert list(out.columns) == ["ticker", "week"]

    def test_nan_treated_as_false(self):
        df = pd.DataFrame({"ticker": ["A"], "week": ["2026-W01"], "f": [np.nan]})
        out = and_gate(df, ["f"])
        assert out.empty


# ── composite_score ──────────────────────────────────────────
class TestCompositeScore:
    def test_top_n_per_week_by_weighted_zscore(self):
        df = pd.DataFrame({
            "ticker": ["A", "B", "C", "D"],
            "week": ["2026-W01"] * 4,
            "x": [1.0, 2.0, 3.0, 4.0],
        })
        out = composite_score(df, {"x": 1.0}, top_n=2)
        assert out["ticker"].tolist() == ["D", "C"]  # 높은 x 순
        assert (out["week"] == "2026-W01").all()

    def test_weights_direction(self):
        df = pd.DataFrame({
            "ticker": ["A", "B"], "week": ["2026-W01"] * 2,
            "x": [1.0, 2.0],
        })
        out = composite_score(df, {"x": -1.0}, top_n=1)
        assert out["ticker"].tolist() == ["A"]  # 음수 가중 → 낮은 x가 상위

    def test_two_columns_combined(self):
        # A가 두 컬럼 모두 우위 → 합산 z 최대. (x,y가 정확히 상쇄되지 않는 값)
        df = pd.DataFrame({
            "ticker": ["A", "B", "C"], "week": ["2026-W01"] * 3,
            "x": [5.0, 1.0, 2.0], "y": [4.0, 3.0, 1.0],
        })
        out = composite_score(df, {"x": 1.0, "y": 1.0}, top_n=2)
        assert out["ticker"].tolist() == ["A", "B"]  # A 최상위, B 차상위

    def test_two_columns_perfect_tie_breaks_by_ticker(self):
        # z가 정확히 상쇄되어 전원 0점 → ticker 오름차순 결정적
        df = pd.DataFrame({
            "ticker": ["A", "B", "C"], "week": ["2026-W01"] * 3,
            "x": [3.0, 1.0, 2.0], "y": [1.0, 3.0, 2.0],
        })
        out = composite_score(df, {"x": 1.0, "y": 1.0}, top_n=1)
        assert out["ticker"].tolist() == ["A"]
        assert out["score"].iloc[0] == pytest.approx(0.0)

    def test_single_sample_week_neutral_no_nan(self):
        df = pd.DataFrame({"ticker": ["A"], "week": ["2026-W01"], "x": [5.0]})
        out = composite_score(df, {"x": 1.0}, top_n=5)
        assert out["ticker"].tolist() == ["A"]
        assert out["score"].iloc[0] == 0.0  # 표본 1 → z=0

    def test_per_week_independent(self):
        df = pd.DataFrame({
            "ticker": ["A", "B", "C", "D"],
            "week": ["2026-W01", "2026-W01", "2026-W02", "2026-W02"],
            "x": [1.0, 2.0, 5.0, 1.0],
        })
        out = composite_score(df, {"x": 1.0}, top_n=1)
        got = dict(zip(out["week"], out["ticker"]))
        assert got == {"2026-W01": "B", "2026-W02": "C"}

    def test_top_n_larger_than_available(self):
        df = pd.DataFrame({"ticker": ["A", "B"], "week": ["2026-W01"] * 2, "x": [1.0, 2.0]})
        out = composite_score(df, {"x": 1.0}, top_n=10)
        assert len(out) == 2

    def test_tie_broken_by_ticker(self):
        df = pd.DataFrame({"ticker": ["B", "A"], "week": ["2026-W01"] * 2, "x": [1.0, 1.0]})
        out = composite_score(df, {"x": 1.0}, top_n=1)
        assert out["ticker"].tolist() == ["A"]  # 동점 → ticker 오름차순

    def test_missing_column_raises(self):
        df = pd.DataFrame({"ticker": ["A"], "week": ["2026-W01"], "x": [1.0]})
        with pytest.raises(ValueError):
            composite_score(df, {"nope": 1.0}, top_n=1)

    def test_invalid_top_n_raises(self):
        df = pd.DataFrame({"ticker": ["A"], "week": ["2026-W01"], "x": [1.0]})
        with pytest.raises(ValueError):
            composite_score(df, {"x": 1.0}, top_n=0)


# ── funnel ───────────────────────────────────────────────────
class TestFunnel:
    def test_screen_then_trigger_within_window(self):
        df = pd.DataFrame({
            "ticker": ["A"] * 3,
            "week": ["2026-W01", "2026-W02", "2026-W03"],
            "screen":  [True, False, False],
            "trigger": [False, True, False],
        })
        out = funnel(df, "screen", "trigger", max_weeks=4)
        assert out.values.tolist() == [["A", "2026-W02"]]

    def test_same_week_trigger_allowed(self):
        df = pd.DataFrame({
            "ticker": ["A"], "week": ["2026-W01"],
            "screen": [True], "trigger": [True],
        })
        out = funnel(df, "screen", "trigger", max_weeks=4)
        assert out.values.tolist() == [["A", "2026-W01"]]

    def test_no_trigger_no_signal(self):
        df = pd.DataFrame({
            "ticker": ["A"] * 2, "week": ["2026-W01", "2026-W02"],
            "screen": [True, False], "trigger": [False, False],
        })
        out = funnel(df, "screen", "trigger", max_weeks=4)
        assert out.empty

    def test_trigger_outside_window_expires(self):
        df = pd.DataFrame({
            "ticker": ["A"] * 3,
            "week": ["2026-W01", "2026-W06", "2026-W07"],
            "screen":  [True, False, False],
            "trigger": [False, True, False],  # W06은 스크린(W01)에서 5주 → max 4 초과
        })
        out = funnel(df, "screen", "trigger", max_weeks=4)
        assert out.empty

    def test_rearm_after_signal(self):
        df = pd.DataFrame({
            "ticker": ["A"] * 4,
            "week": ["2026-W01", "2026-W02", "2026-W03", "2026-W04"],
            "screen":  [True, False, True, False],
            "trigger": [False, True, False, True],
        })
        out = funnel(df, "screen", "trigger", max_weeks=4)
        assert out.values.tolist() == [["A", "2026-W02"], ["A", "2026-W04"]]

    def test_per_ticker_independent(self):
        df = pd.DataFrame({
            "ticker": ["A", "A", "B", "B"],
            "week": ["2026-W01", "2026-W02", "2026-W01", "2026-W02"],
            "screen":  [True, False, False, False],
            "trigger": [False, True, False, True],
        })
        out = funnel(df, "screen", "trigger", max_weeks=4)
        # A는 스크린 후 트리거 → 신호. B는 스크린 없음 → 없음.
        assert out["ticker"].tolist() == ["A"]

    def test_missing_column_raises(self):
        df = pd.DataFrame({"ticker": ["A"], "week": ["2026-W01"], "screen": [True]})
        with pytest.raises(ValueError):
            funnel(df, "screen", "no_trigger", max_weeks=4)


# ── 전략 로스터 ───────────────────────────────────────────────
class TestStrategyRoster:
    def test_tier1_strategies_present(self):
        assert set(STRATEGIES) == {"AND-1", "AND-2", "SCORE-1", "FUNNEL-1"}

    def test_and1_runs_via_spec(self):
        df = pd.DataFrame({
            "ticker": ["A", "B"], "week": ["2026-W01"] * 2,
            "ichimoku": [True, True], "stage2plus": [True, False], "flow_pos": [True, True],
        })
        out = STRATEGIES["AND-1"].run(df)
        assert out["ticker"].tolist() == ["A"]

    def test_funnel1_runs_via_spec(self):
        df = pd.DataFrame({
            "ticker": ["A"] * 2, "week": ["2026-W01", "2026-W02"],
            "flow_pos": [True, False], "ichimoku": [False, True],
        })
        out = STRATEGIES["FUNNEL-1"].run(df)
        assert out.values.tolist() == [["A", "2026-W02"]]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
