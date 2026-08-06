"""analysis/fundamentals.py 단위 테스트 (compute_ratio_columns, screen)."""
import math

import pandas as pd
import pytest

from analysis.fundamentals import RatioThresholds, compute_ratio_columns, screen


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
