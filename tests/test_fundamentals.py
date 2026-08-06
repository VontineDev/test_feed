"""analysis/fundamentals.py 단위 테스트 (compute_ratio_columns, screen)."""
import math

import pandas as pd
import pytest

from analysis.fundamentals import (
    RatioThresholds,
    SCENARIO1_THRESHOLDS,
    SCENARIO2_THRESHOLDS,
    compute_ratio_columns,
    screen,
)


def _row(ticker, market_cap, revenue, revenue_prev, net_income, equity, liabilities):
    return {
        "ticker": ticker, "stock_code": ticker.split(".")[0], "market_cap": market_cap,
        "revenue": revenue, "revenue_prev": revenue_prev, "net_income": net_income,
        "equity": equity, "liabilities": liabilities,
    }


class TestComputeRatioColumns:
    def test_computes_expected_ratios(self):
        df = pd.DataFrame([
            _row("005930.KS", market_cap=1_000_000, revenue=2_000_000,
                 revenue_prev=1_500_000, net_income=100_000,
                 equity=500_000, liabilities=250_000),
        ])
        out = compute_ratio_columns(df)
        row = out.iloc[0]
        assert row["pbr"] == pytest.approx(2.0)       # 1,000,000 / 500,000
        assert row["per"] == pytest.approx(10.0)      # 1,000,000 / 100,000
        assert row["roe"] == pytest.approx(0.2)        # 100,000 / 500,000
        assert row["debt_ratio"] == pytest.approx(0.5)  # 250,000 / 500,000
        assert row["revenue_growth"] == pytest.approx(1 / 3)  # (2M-1.5M)/1.5M

    def test_zero_denominator_yields_nan_not_error(self):
        df = pd.DataFrame([
            _row("A.KS", market_cap=1_000, revenue=100, revenue_prev=0,
                 net_income=0, equity=0, liabilities=100),
        ])
        out = compute_ratio_columns(df)
        row = out.iloc[0]
        assert math.isnan(row["pbr"])
        assert math.isnan(row["per"])
        assert math.isnan(row["roe"])
        assert math.isnan(row["revenue_growth"])

    def test_negative_net_income_produces_negative_per_and_roe(self):
        """적자기업 — PER/ROE가 음수로 계산돼야 screen()의 per_min>0 필터가 걸러낼 수 있음."""
        df = pd.DataFrame([
            _row("B.KS", market_cap=1_000, revenue=500, revenue_prev=500,
                 net_income=-50, equity=200, liabilities=100),
        ])
        out = compute_ratio_columns(df)
        row = out.iloc[0]
        assert row["per"] < 0
        assert row["roe"] < 0


class TestScreen:
    def _base_df(self):
        return pd.DataFrame([
            {"ticker": "GOOD.KS", "pbr": 0.5, "per": 8.0, "roe": 0.12,
             "debt_ratio": 0.8, "revenue_growth": 0.05},
            {"ticker": "HIGH_PBR.KS", "pbr": 1.5, "per": 8.0, "roe": 0.12,
             "debt_ratio": 0.8, "revenue_growth": 0.05},
            {"ticker": "LOSS.KS", "pbr": 0.5, "per": -5.0, "roe": -0.1,
             "debt_ratio": 0.8, "revenue_growth": 0.05},
            {"ticker": "HIGH_DEBT.KS", "pbr": 0.5, "per": 8.0, "roe": 0.12,
             "debt_ratio": 2.0, "revenue_growth": 0.05},
            {"ticker": "SHRINKING.KS", "pbr": 0.5, "per": 8.0, "roe": 0.12,
             "debt_ratio": 0.8, "revenue_growth": -0.02},
            {"ticker": "NAN_ROE.KS", "pbr": 0.5, "per": 8.0, "roe": float("nan"),
             "debt_ratio": 0.8, "revenue_growth": 0.05},
        ])

    def test_default_thresholds_match_document(self):
        th = RatioThresholds()
        assert (th.pbr_min, th.pbr_max) == (0.2, 1.0)
        assert (th.per_min, th.per_max) == (0.0, 12.0)
        assert th.roe_min == 0.08
        assert th.debt_ratio_max == 1.5
        assert th.revenue_growth_min == 0.0

    def test_only_fully_passing_ticker_survives(self):
        result = screen(self._base_df())
        assert result == {"GOOD.KS"}

    def test_empty_df_returns_empty_set(self):
        assert screen(pd.DataFrame()) == set()

    def test_custom_thresholds_relax_filter(self):
        th = RatioThresholds(pbr_max=2.0, debt_ratio_max=3.0)
        result = screen(self._base_df(), th)
        assert "HIGH_PBR.KS" in result
        assert "HIGH_DEBT.KS" in result
        assert "LOSS.KS" not in result  # PER 여전히 음수라 걸러짐

    def test_none_threshold_disables_that_condition_even_with_nan(self):
        """조건이 None이면 해당 컬럼이 NaN이어도 그 종목을 걸러내지 않는다."""
        th = RatioThresholds(pbr_min=None, pbr_max=None, per_min=None, per_max=None,
                              roe_min=None, debt_ratio_max=None, revenue_growth_min=None)
        result = screen(self._base_df(), th)
        assert result == {"GOOD.KS", "HIGH_PBR.KS", "LOSS.KS", "HIGH_DEBT.KS",
                           "SHRINKING.KS", "NAN_ROE.KS"}  # 전부 통과(검사 조건 없음)


class TestScenarioThresholds:
    """TechnicalQuant.md 1안/2안의 종목선택 조건 그대로 재현하는지 확인 —
    문서 1절의 범용 필터(RatioThresholds 기본값)와 혼동하지 않도록 별도 검증."""

    def test_scenario1_matches_document_1an_exactly(self):
        """1안: "PBR 0.8 이하 + ROE 10% 이상 + 부채비율 100% 이하" (PER·매출증가율 조건 없음)."""
        th = SCENARIO1_THRESHOLDS
        assert th.pbr_min is None and th.pbr_max == 0.8
        assert th.per_min is None and th.per_max is None
        assert th.roe_min == 0.10
        assert th.debt_ratio_max == 1.0
        assert th.revenue_growth_min is None

    def test_scenario2_matches_document_2an_exactly(self):
        """2안: "PER 15 이하" (PBR·ROE·부채비율·매출증가율 조건 없음)."""
        th = SCENARIO2_THRESHOLDS
        assert th.pbr_min is None and th.pbr_max is None
        assert th.per_max == 15.0
        assert th.roe_min is None
        assert th.debt_ratio_max is None
        assert th.revenue_growth_min is None

    def test_scenario1_screen_ignores_per_and_revenue_growth(self):
        df = pd.DataFrame([
            # PBR/ROE/부채비율은 1안 통과, PER은 극단적으로 나쁘고 매출은 역성장
            # — 1안엔 그 조건이 없으므로 그래도 통과해야 함
            {"ticker": "PASS.KS", "pbr": 0.6, "per": 999.0, "roe": 0.15,
             "debt_ratio": 0.5, "revenue_growth": -0.5},
            {"ticker": "FAIL_PBR.KS", "pbr": 0.9, "per": 5.0, "roe": 0.15,
             "debt_ratio": 0.5, "revenue_growth": 0.05},
        ])
        result = screen(df, SCENARIO1_THRESHOLDS)
        assert result == {"PASS.KS"}

    def test_scenario2_screen_ignores_pbr_roe_debt(self):
        df = pd.DataFrame([
            # PBR/ROE/부채비율이 전부 나쁘지만 2안엔 그 조건이 없으므로 PER만 보면 통과
            {"ticker": "PASS.KS", "pbr": 5.0, "per": 10.0, "roe": -0.2,
             "debt_ratio": 3.0, "revenue_growth": -0.5},
            {"ticker": "FAIL_PER.KS", "pbr": 0.5, "per": 20.0, "roe": 0.15,
             "debt_ratio": 0.5, "revenue_growth": 0.05},
        ])
        result = screen(df, SCENARIO2_THRESHOLDS)
        assert result == {"PASS.KS"}
