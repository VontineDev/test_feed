"""
test_backtest_engine.py — 백테스트 엔진 단위 테스트

모든 테스트는 네트워크 없이 합성 DataFrame 사용.
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import pytest

from analysis.backtest.models import (
    BacktestConfig, BacktestResult, GroupMetrics, SignalRecord,
)
from analysis.backtest.config import TX_COST_DEFAULT, _STOP_LOSS_PCT
from analysis.backtest.helpers import (
    _build_price_lookup,
    _build_weekly_ichimoku,
    _compute_group_metrics,
    _compute_mdd,
    _compute_rsi,
    _compute_sharpe,
    _fill_returns,
    _find_ichimoku_sell,
    _nearest_price,
    _week_label,
)
from analysis.backtest.replay import _apply_cross_filter, _replay_ichimoku, _replay_stage
from analysis.backtest.exit_models import _compute_sell_signals_and_s2

# ── BacktestConfig.hold_weeks ─────────────────────────────────────

class TestBacktestConfigHoldWeeks:
    def test_none_by_default(self):
        cfg = BacktestConfig("ichimoku", date(2025, 1, 1), date(2026, 1, 1))
        assert cfg.hold_weeks is None

    def test_valid_non_standard(self):
        cfg = BacktestConfig("ichimoku", date(2025, 1, 1), date(2026, 1, 1), hold_weeks=8)
        assert cfg.hold_weeks == 8

    def test_valid_standard_weeks(self):
        for w in (1, 4, 13):
            cfg = BacktestConfig("stage", date(2025, 1, 1), date(2026, 1, 1), hold_weeks=w)
            assert cfg.hold_weeks == w

    def test_zero_raises(self):
        with pytest.raises(ValueError, match="hold_weeks"):
            BacktestConfig("ichimoku", date(2025, 1, 1), date(2026, 1, 1), hold_weeks=0)

    def test_negative_raises(self):
        with pytest.raises(ValueError, match="hold_weeks"):
            BacktestConfig("ichimoku", date(2025, 1, 1), date(2026, 1, 1), hold_weeks=-3)


# ── _fill_returns — custom hold period ───────────────────────────

class TestFillReturnsCustomHold:
    def test_non_standard_hold_populates_custom(self):
        """hold_weeks=8 (56d) → return_custom computed from N*7 days offset."""
        sig = _make_signal(close=10_000.0, signal_date=date(2025, 1, 6))
        # 2025-01-06 + 56d = 2025-03-03
        stock = {date(2025, 3, 3): 11_200.0}
        _fill_returns(sig, stock, {}, tx_cost_rt=0.0, hold_weeks=8)
        assert sig.return_custom == pytest.approx(0.12, abs=0.001)

    def test_standard_4w_custom_matches_28d(self):
        """hold_weeks=4 → return_custom == return_28d (same 28-day target)."""
        sig = _make_signal(close=10_000.0, signal_date=date(2025, 1, 6))
        # 2025-01-06 + 28d = 2025-02-03
        stock = {date(2025, 2, 3): 10_800.0}
        _fill_returns(sig, stock, {}, tx_cost_rt=0.0, hold_weeks=4)
        assert sig.return_28d   == pytest.approx(0.08, abs=0.001)
        assert sig.return_custom == pytest.approx(0.08, abs=0.001)

    def test_no_hold_weeks_custom_stays_none(self):
        """hold_weeks not set → return_custom/excess_custom remain None."""
        sig = _make_signal(close=10_000.0, signal_date=date(2025, 1, 6))
        stock = {date(2025, 1, 13): 10_500.0}
        _fill_returns(sig, stock, {}, tx_cost_rt=0.0)
        assert sig.return_custom is None
        assert sig.excess_custom is None

    def test_tx_cost_deducted_from_custom(self):
        """Transaction cost subtracted from return_custom same as standard returns."""
        sig = _make_signal(close=10_000.0, signal_date=date(2025, 1, 6))
        # 2025-01-06 + 56d = 2025-03-03, flat price
        stock = {date(2025, 3, 3): 10_000.0}
        _fill_returns(sig, stock, {}, tx_cost_rt=0.005, hold_weeks=8)
        assert sig.return_custom == pytest.approx(-0.005, abs=0.0001)

    def test_missing_price_custom_is_none(self):
        """No price data at custom hold target → return_custom is None."""
        sig = _make_signal(close=10_000.0, signal_date=date(2025, 1, 6))
        _fill_returns(sig, {}, {}, tx_cost_rt=0.0, hold_weeks=8)
        assert sig.return_custom is None
        assert sig.excess_custom is None

    def test_custom_excess_uses_kospi(self):
        """excess_custom = return_custom − KOSPI return over same period."""
        sig = _make_signal(close=10_000.0, signal_date=date(2025, 1, 6))
        # 2025-01-06 + 56d = 2025-03-03
        stock = {date(2025, 3, 3): 11_200.0}   # +12%
        kospi = {date(2025, 1, 6): 2_700.0, date(2025, 3, 3): 2_754.0}  # +2%
        _fill_returns(sig, stock, kospi, tx_cost_rt=0.0, hold_weeks=8)
        assert sig.return_custom == pytest.approx(0.12, abs=0.001)
        assert sig.excess_custom == pytest.approx(0.10, abs=0.001)  # 0.12 − 0.02


# ── _compute_group_metrics — custom hold ─────────────────────────

class TestComputeGroupMetricsCustomHold:
    def test_custom_metrics_populated(self):
        """Signals with return_custom → GroupMetrics custom fields computed."""
        sigs = [
            _make_signal(r7=0.01, r28=0.03, r91=0.10),
            _make_signal(r7=0.02, r28=0.05, r91=0.12),
        ]
        sigs[0].return_custom = 0.06
        sigs[1].return_custom = -0.02
        m = _compute_group_metrics(sigs, rf_annual=0.03, hold_weeks=8)
        assert m.hold_days_custom == 56
        assert m.win_rate_custom == pytest.approx(0.5)   # 1/2 positive
        assert m.avg_return_custom == pytest.approx(0.02, abs=0.001)
        assert m.median_return_custom == pytest.approx(0.02, abs=0.001)

    def test_no_hold_weeks_custom_fields_are_none(self):
        """Without hold_weeks → all custom GroupMetrics fields stay None."""
        sigs = [_make_signal(r7=0.01, r28=0.03, r91=0.10)]
        m = _compute_group_metrics(sigs, rf_annual=0.03)
        assert m.hold_days_custom is None
        assert m.win_rate_custom is None
        assert m.avg_return_custom is None
        assert m.sharpe_custom is None

    def test_hold_days_custom_correct(self):
        """hold_days_custom = hold_weeks * 7."""
        sigs = [_make_signal()]
        sigs[0].return_custom = 0.05
        m = _compute_group_metrics(sigs, rf_annual=0.03, hold_weeks=6)
        assert m.hold_days_custom == 42

    def test_custom_excess_aggregated(self):
        """avg_excess_custom is mean of excess_custom across signals."""
        sigs = [_make_signal(), _make_signal()]
        sigs[0].return_custom = 0.10
        sigs[0].excess_custom = 0.08
        sigs[1].return_custom = 0.04
        sigs[1].excess_custom = 0.02
        m = _compute_group_metrics(sigs, rf_annual=0.03, hold_weeks=8)
        assert m.avg_excess_custom == pytest.approx(0.05, abs=0.001)

    def test_all_custom_none_custom_rates_none(self):
        """If all return_custom are None → win_rate_custom/avg_return_custom stay None."""
        sigs = [_make_signal(), _make_signal()]
        # return_custom stays None (not set)
        m = _compute_group_metrics(sigs, rf_annual=0.03, hold_weeks=8)
        assert m.hold_days_custom == 56
        assert m.win_rate_custom is None
        assert m.avg_return_custom is None


# ── 공통 헬퍼 ─────────────────────────────────────────────────────

def _make_daily_df(
    n: int = 120,
    start_price: float = 10_000.0,
    trend: float = 0.0,
    volume: int = 100_000,
    start_date: date = date(2023, 1, 2),
) -> pd.DataFrame:
    """평일 일봉 DataFrame 생성 (단순 선형 추세)."""
    dates, opens, highs, lows, closes, vols = [], [], [], [], [], []
    current = start_date
    price = start_price
    for _ in range(n):
        while current.weekday() >= 5:
            current += timedelta(days=1)
        dates.append(pd.Timestamp(current, tz="UTC"))
        next_price = price * (1 + trend)
        opens.append(price * 0.999)
        highs.append(next_price * 1.002)
        lows.append(price  * 0.998)
        closes.append(next_price)
        vols.append(volume)
        price = next_price
        current += timedelta(days=1)
    return pd.DataFrame(
        {"Open": opens, "High": highs, "Low": lows, "Close": closes, "Volume": vols},
        index=dates,
    )


def _make_signal(
    ticker: str = "TEST.KS",
    signal_date: date = date(2025, 6, 1),
    close: float = 10_000.0,
    mode: str = "ichimoku",
    market: str = "KOSPI",
    r7: Optional[float] = None,
    r28: Optional[float] = None,
    r91: Optional[float] = None,
    ex28: Optional[float] = None,
    ex91: Optional[float] = None,
) -> SignalRecord:
    s = SignalRecord(
        ticker=ticker, name="테스트", signal_date=signal_date,
        close_at_signal=close, mode=mode, market=market,
    )
    s.return_7d  = r7
    s.return_28d = r28
    s.return_91d = r91
    s.excess_28d = ex28
    s.excess_91d = ex91
    return s


# ── _week_label ───────────────────────────────────────────────────

class TestWeekLabel:
    def test_monday_week2(self):
        assert _week_label(date(2025, 1, 6)) == "2025-W02"

    def test_friday_same_week(self):
        assert _week_label(date(2025, 1, 10)) == "2025-W02"

    def test_sunday_same_week(self):
        # ISO week: Mon start, Sun end
        assert _week_label(date(2025, 1, 12)) == "2025-W02"

    def test_year_boundary_jan1(self):
        label = _week_label(date(2026, 1, 1))
        assert label.startswith("2026-")

    def test_format_zero_padded(self):
        label = _week_label(date(2025, 1, 1))
        assert "W0" in label  # single-digit week zero-padded


# ── _build_price_lookup ───────────────────────────────────────────

class TestBuildPriceLookup:
    def test_basic(self):
        df = _make_daily_df(n=5, start_price=10_000.0)
        lookup = _build_price_lookup(df)
        assert len(lookup) == 5
        for d, p in lookup.items():
            assert isinstance(p, float)
            assert p > 0

    def test_skips_nan(self):
        df = _make_daily_df(n=4)
        df.iloc[1, df.columns.get_loc("Close")] = float("nan")
        lookup = _build_price_lookup(df)
        assert len(lookup) == 3  # NaN row skipped

    def test_date_keys(self):
        df = _make_daily_df(n=3, start_date=date(2025, 1, 2))
        lookup = _build_price_lookup(df)
        assert date(2025, 1, 2) in lookup


# ── _nearest_price ────────────────────────────────────────────────

class TestNearestPrice:
    def test_exact_match(self):
        lookup = {date(2025, 1, 6): 10_000.0}
        assert _nearest_price(lookup, date(2025, 1, 6)) == 10_000.0

    def test_next_day(self):
        # Monday missing, Tuesday found
        lookup = {date(2025, 1, 7): 10_500.0}
        result = _nearest_price(lookup, date(2025, 1, 6))
        assert result == 10_500.0

    def test_none_beyond_max_days(self):
        lookup = {date(2025, 1, 15): 10_000.0}
        assert _nearest_price(lookup, date(2025, 1, 6), max_days=7) is None

    def test_max_days_boundary(self):
        lookup = {date(2025, 1, 13): 9_500.0}
        assert _nearest_price(lookup, date(2025, 1, 6), max_days=7) == 9_500.0

    def test_empty_lookup(self):
        assert _nearest_price({}, date(2025, 1, 6)) is None


# ── _compute_sharpe ───────────────────────────────────────────────

class TestComputeSharpe:
    def test_zero_std_returns_none(self):
        returns = [0.05] * 20
        assert _compute_sharpe(returns, 28, 0.03) is None

    def test_mixed_returns_not_none(self):
        returns = [0.05, -0.02, 0.03, -0.01, 0.04, 0.06, -0.03] * 3
        result = _compute_sharpe(returns, 28, 0.03)
        assert result is not None
        assert isinstance(result, float)

    def test_fewer_than_three_returns_none(self):
        assert _compute_sharpe([], 28, 0.03) is None
        assert _compute_sharpe([0.05], 28, 0.03) is None
        assert _compute_sharpe([0.05, 0.03], 28, 0.03) is None

    def test_exactly_three_ok(self):
        assert _compute_sharpe([0.05, -0.02, 0.03], 28, 0.03) is not None

    def test_hold_period_affects_annualization(self):
        returns = [0.05, -0.02, 0.03, 0.04, -0.01] * 4
        s28 = _compute_sharpe(returns, 28, 0.03)
        s7  = _compute_sharpe(returns, 7, 0.03)
        assert s28 is not None and s7 is not None
        # 7d hold → more periods/year → larger sqrt multiplier → different ratio
        assert abs(s28 - s7) > 0.001

    def test_negative_excess_gives_negative_sharpe(self):
        # Returns consistently below risk-free rate
        returns = [0.001] * 20  # tiny returns, well below 3% / (252/28) per period
        result = _compute_sharpe(returns, 28, 0.03)
        # std will be 0 → None
        assert result is None

    def test_high_returns_positive_sharpe(self):
        import random
        random.seed(42)
        returns = [0.10 + random.uniform(-0.01, 0.01) for _ in range(30)]
        result = _compute_sharpe(returns, 28, 0.03)
        assert result is not None
        assert result > 0


# ── _compute_mdd ─────────────────────────────────────────────────

class TestComputeMdd:
    def test_empty_returns_none(self):
        assert _compute_mdd([]) is None

    def test_all_positive_zero_mdd(self):
        returns = [0.01, 0.02, 0.01, 0.03]
        mdd = _compute_mdd(returns)
        assert mdd == pytest.approx(0.0, abs=1e-9)

    def test_single_loss(self):
        # equity: 1.0 → 1.1 → 0.55 → 0.605
        returns = [0.10, -0.50, 0.10]
        mdd = _compute_mdd(returns)
        # peak=1.10, valley=0.55 → MDD = -(1.10 - 0.55) / 1.10 ≈ -0.50
        assert mdd == pytest.approx(-0.50, abs=0.001)

    def test_partial_recovery(self):
        # equity: 1.0 → 1.20 → 1.08 → 1.134 → 1.191
        returns = [0.20, -0.10, 0.05, 0.05]
        mdd = _compute_mdd(returns)
        # peak=1.20, valley=1.08 → MDD = -0.12/1.20 = -0.10
        assert mdd == pytest.approx(-0.10, abs=0.001)

    def test_monotone_decline(self):
        returns = [-0.05] * 5
        mdd = _compute_mdd(returns)
        # equity goes 1.0 → 0.95^5 ≈ 0.7738; MDD ≈ -(1 - 0.7738) = -0.2262
        assert mdd == pytest.approx(-(1 - 0.95**5), abs=0.001)

    def test_mdd_not_positive(self):
        returns = [0.03, -0.01, 0.02, -0.02, 0.04]
        mdd = _compute_mdd(returns)
        assert mdd <= 0.0


# ── _compute_group_metrics ────────────────────────────────────────

class TestComputeGroupMetrics:
    def test_empty_signals(self):
        m = _compute_group_metrics([], rf_annual=0.03)
        assert m.n == 0
        assert m.win_rate_28d is None
        assert m.sharpe_28d is None
        assert m.mdd is None

    def test_all_positive_returns(self):
        sigs = [_make_signal(r7=0.02, r28=0.05, r91=0.12, ex28=0.03, ex91=0.08) for _ in range(5)]
        m = _compute_group_metrics(sigs, rf_annual=0.03)
        assert m.n == 5
        assert m.win_rate_7d  == pytest.approx(1.0)
        assert m.win_rate_28d == pytest.approx(1.0)
        assert m.avg_return_28d == pytest.approx(0.05)
        assert m.median_return_28d == pytest.approx(0.05)
        assert m.avg_excess_28d == pytest.approx(0.03)

    def test_mixed_win_rate(self):
        sigs = [
            _make_signal(r28=0.10, r7=0.05, r91=0.20, ex28=0.05),
            _make_signal(r28=-0.05, r7=-0.02, r91=-0.10, ex28=-0.02),
        ]
        m = _compute_group_metrics(sigs, rf_annual=0.03)
        assert m.win_rate_28d == pytest.approx(0.50)
        assert m.avg_return_28d == pytest.approx(0.025)

    def test_mdd_computed(self):
        sigs = [_make_signal(r28=r) for r in [0.05, -0.10, 0.03]]
        m = _compute_group_metrics(sigs, rf_annual=0.03)
        assert m.mdd is not None

    def test_partial_none_returns(self):
        # Only 2 of 4 signals have 28d returns
        sigs = [
            _make_signal(r7=0.01, r28=0.05),
            _make_signal(r7=0.02, r28=0.08),
            _make_signal(r7=0.01),  # no r28
            _make_signal(r7=0.02),  # no r28
        ]
        m = _compute_group_metrics(sigs, rf_annual=0.03)
        assert m.n == 4
        assert m.win_rate_28d == pytest.approx(1.0)  # 2/2 positive


# ── _fill_returns ─────────────────────────────────────────────────

class TestFillReturns:
    def test_basic_return(self):
        sig = _make_signal(close=10_000.0, signal_date=date(2025, 1, 6))
        stock = {date(2025, 1, 13): 10_500.0}  # +5% after 7 days
        kospi = {date(2025, 1, 6): 3_000.0, date(2025, 1, 13): 3_060.0}  # +2%
        _fill_returns(sig, stock, kospi, tx_cost_rt=0.0021)
        # return_7d = (10500/10000 - 1) - 0.0021 = 0.05 - 0.0021 = 0.0479
        assert sig.return_7d == pytest.approx(0.0479, abs=0.0001)
        # excess_7d = 0.0479 - 0.02 = 0.0279
        assert sig.excess_7d == pytest.approx(0.0279, abs=0.0001)

    def test_tx_cost_subtracted(self):
        sig = _make_signal(close=10_000.0, signal_date=date(2025, 1, 6))
        stock = {date(2025, 2, 3): 10_000.0}  # flat 28d
        _fill_returns(sig, stock, {}, tx_cost_rt=0.005)
        # flat price → return = 0 - 0.005 = -0.005
        assert sig.return_28d == pytest.approx(-0.005, abs=0.0001)

    def test_none_when_no_price(self):
        sig = _make_signal(close=10_000.0, signal_date=date(2025, 1, 6))
        _fill_returns(sig, {}, {}, tx_cost_rt=0.0021)
        assert sig.return_7d is None
        assert sig.return_28d is None
        assert sig.return_91d is None

    def test_zero_base_skipped(self):
        sig = _make_signal(close=0.0, signal_date=date(2025, 1, 6))
        stock = {date(2025, 1, 13): 10_000.0}
        _fill_returns(sig, stock, {}, tx_cost_rt=0.0021)
        assert sig.return_7d is None

    def test_no_kospi_excess_is_none(self):
        sig = _make_signal(close=10_000.0, signal_date=date(2025, 1, 6))
        stock = {date(2025, 1, 13): 10_500.0}
        _fill_returns(sig, stock, {}, tx_cost_rt=0.0021)
        assert sig.return_7d is not None
        assert sig.excess_7d is None  # no KOSPI data


# ── _apply_cross_filter ───────────────────────────────────────────

class TestApplyCrossFilter:
    def test_same_ticker_same_week(self):
        ichi  = _make_signal("A.KS", date(2025, 1, 10), mode="ichimoku")  # Friday W2
        stage = _make_signal("A.KS", date(2025, 1, 8),  mode="stage")     # Wednesday W2
        result = _apply_cross_filter([ichi, stage])
        assert len(result) == 1
        assert result[0].mode == "cross"
        assert result[0].ticker == "A.KS"
        assert result[0].signal_date == date(2025, 1, 8)  # stage date preserved

    def test_different_weeks_no_cross(self):
        ichi  = _make_signal("A.KS", date(2025, 1, 10), mode="ichimoku")  # W2
        stage = _make_signal("A.KS", date(2025, 1, 20), mode="stage")     # W4
        assert _apply_cross_filter([ichi, stage]) == []

    def test_different_tickers_no_cross(self):
        ichi  = _make_signal("A.KS", date(2025, 1, 10), mode="ichimoku")
        stage = _make_signal("B.KS", date(2025, 1, 8),  mode="stage")
        assert _apply_cross_filter([ichi, stage]) == []

    def test_stage_only_no_cross(self):
        stage = _make_signal("A.KS", date(2025, 1, 8), mode="stage")
        assert _apply_cross_filter([stage]) == []

    def test_ichimoku_only_no_cross(self):
        ichi = _make_signal("A.KS", date(2025, 1, 10), mode="ichimoku")
        assert _apply_cross_filter([ichi]) == []

    def test_multiple_tickers(self):
        signals = [
            _make_signal("A.KS", date(2025, 1, 10), mode="ichimoku"),
            _make_signal("A.KS", date(2025, 1, 8),  mode="stage"),
            _make_signal("B.KS", date(2025, 1, 10), mode="ichimoku"),
            # B.KS has no stage → no cross
            _make_signal("C.KS", date(2025, 1, 8),  mode="stage"),
            # C.KS has no ichimoku → no cross
        ]
        result = _apply_cross_filter(signals)
        assert len(result) == 1
        assert result[0].ticker == "A.KS"

    def test_deduplicates_multiple_stage_signals_same_week(self):
        """두 번의 Stage 1 신호가 같은 주에 발동해도 cross 신호는 1건만."""
        ichi  = _make_signal("A.KS", date(2025, 1, 10), mode="ichimoku")   # W2 Friday
        stage1 = _make_signal("A.KS", date(2025, 1, 6),  mode="stage")     # W2 Monday
        stage2 = _make_signal("A.KS", date(2025, 1, 8),  mode="stage")     # W2 Wednesday
        result = _apply_cross_filter([ichi, stage1, stage2])
        assert len(result) == 1
        assert result[0].signal_date == date(2025, 1, 6)  # earliest preserved


# ── BacktestConfig 검증 ──────────────────────────────────────────

class TestBacktestConfig:
    def test_valid_ichimoku(self):
        cfg = BacktestConfig("ichimoku", date(2025, 1, 1), date(2026, 1, 1))
        assert cfg.mode == "ichimoku"
        assert cfg.tx_cost_rt == pytest.approx(TX_COST_DEFAULT, abs=1e-6)

    def test_valid_stage_kosdaq(self):
        cfg = BacktestConfig("stage", date(2025, 1, 1), date(2026, 1, 1), market="KOSDAQ")
        assert cfg.market == "KOSDAQ"

    def test_invalid_mode_raises(self):
        with pytest.raises(ValueError, match="mode"):
            BacktestConfig("bad_mode", date(2025, 1, 1), date(2026, 1, 1))

    def test_start_after_end_raises(self):
        with pytest.raises(ValueError, match="start"):
            BacktestConfig("ichimoku", date(2026, 1, 1), date(2025, 1, 1))

    def test_start_equal_end_raises(self):
        with pytest.raises(ValueError):
            BacktestConfig("ichimoku", date(2025, 6, 1), date(2025, 6, 1))

    def test_invalid_market_raises(self):
        with pytest.raises(ValueError, match="market"):
            BacktestConfig("ichimoku", date(2025, 1, 1), date(2026, 1, 1), market="NYSE")

    def test_custom_tx_cost(self):
        cfg = BacktestConfig("cross", date(2025, 1, 1), date(2026, 1, 1), tx_cost_rt=0.003)
        assert cfg.tx_cost_rt == pytest.approx(0.003)


# ── _replay_stage (합성 데이터) ───────────────────────────────────

class TestReplayStage:
    def _make_stage_trigger_df(
        self,
        n_flat: int = 80,
        spike_change: float = 0.08,
        spike_vol_mult: float = 3.0,
        base_price: float = 10_000.0,
        base_vol: int = 100_000,
        start_date: date = date(2022, 1, 3),
    ) -> pd.DataFrame:
        """n_flat 일 횡보 후 spike 1일."""
        current = start_date
        dates, opens, highs, lows, closes, vols = [], [], [], [], [], []
        for i in range(n_flat + 1):
            while current.weekday() >= 5:
                current += timedelta(days=1)
            dates.append(pd.Timestamp(current, tz="UTC"))
            if i < n_flat:
                p, v = base_price, base_vol
            else:
                p = base_price * (1 + spike_change)
                v = int(base_vol * spike_vol_mult)
            opens.append(p * 0.999)
            highs.append(p * 1.001)
            lows.append(p * 0.999)
            closes.append(p)
            vols.append(v)
            current += timedelta(days=1)
        return pd.DataFrame(
            {"Open": opens, "High": highs, "Low": lows, "Close": closes, "Volume": vols},
            index=dates,
        )

    def test_clear_signal_detected(self):
        """+8% 급등 + 3× 거래량 → Stage 1 신호 발동."""
        df = self._make_stage_trigger_df(n_flat=80, spike_change=0.08, spike_vol_mult=3.0)
        spike_date = df.index[-1].date()
        cfg = BacktestConfig("stage", spike_date, spike_date + timedelta(days=1))
        sigs = _replay_stage("TEST.KS", "테스트", df, "KOSPI", cfg)
        assert len(sigs) == 1
        assert sigs[0].signal_date == spike_date
        assert sigs[0].close_at_signal == pytest.approx(10_000.0 * 1.08, rel=0.001)

    def test_small_change_no_signal(self):
        """+2% 상승 → 5% 기준 미달, 신호 없음."""
        df = self._make_stage_trigger_df(n_flat=80, spike_change=0.02, spike_vol_mult=3.0)
        spike_date = df.index[-1].date()
        cfg = BacktestConfig("stage", spike_date, spike_date + timedelta(days=1))
        sigs = _replay_stage("TEST.KS", "테스트", df, "KOSPI", cfg)
        assert len(sigs) == 0

    def test_low_volume_no_signal(self):
        """+8% 상승, 1.5× 거래량 → 2× 미달, 신호 없음."""
        df = self._make_stage_trigger_df(n_flat=80, spike_change=0.08, spike_vol_mult=1.5)
        spike_date = df.index[-1].date()
        cfg = BacktestConfig("stage", spike_date, spike_date + timedelta(days=1))
        sigs = _replay_stage("TEST.KS", "테스트", df, "KOSPI", cfg)
        assert len(sigs) == 0

    def test_outside_window_no_signal(self):
        """신호 발생일이 백테스트 기간 외 → 수집 안 됨."""
        df = self._make_stage_trigger_df(n_flat=80, spike_change=0.08, spike_vol_mult=3.0)
        spike_date = df.index[-1].date()
        # window is 30 days BEFORE spike
        cfg = BacktestConfig("stage",
                             spike_date - timedelta(days=30),
                             spike_date - timedelta(days=1))
        sigs = _replay_stage("TEST.KS", "테스트", df, "KOSPI", cfg)
        assert len(sigs) == 0

    def test_kosdaq_higher_threshold(self):
        """+6% 상승 → KOSPI는 신호, KOSDAQ (7% 기준)은 신호 없음."""
        df = self._make_stage_trigger_df(n_flat=80, spike_change=0.06, spike_vol_mult=3.0)
        spike_date = df.index[-1].date()
        cfg = BacktestConfig("stage", spike_date, spike_date + timedelta(days=1))
        kospi_sigs  = _replay_stage("TEST.KS", "테스트", df, "KOSPI",  cfg)
        kosdaq_sigs = _replay_stage("TEST.KQ", "테스트", df, "KOSDAQ", cfg)
        assert len(kospi_sigs)  == 1
        assert len(kosdaq_sigs) == 0


# ── BacktestResult 리포트 ─────────────────────────────────────────

class TestBacktestResultReport:
    def _make_result(self, n_sigs: int = 0, mode: str = "ichimoku") -> BacktestResult:
        cfg = BacktestConfig(mode, date(2025, 1, 1), date(2026, 1, 1))
        sigs = [
            _make_signal(r7=0.03, r28=0.06, r91=0.15, ex28=0.04, ex91=0.09)
            for _ in range(n_sigs)
        ]
        m = _compute_group_metrics(sigs, 0.03)
        return BacktestResult(
            config=cfg, signals=sigs, overall=m,
            computed_at="2026-01-01T00:00:00",
        )

    def test_text_report_no_signals(self):
        report = self._make_result(0).to_text_report()
        assert "이치모쿠" in report
        assert "N/A" in report
        assert "=" in report

    def test_text_report_with_signals(self):
        report = self._make_result(10).to_text_report()
        assert "10건" in report
        assert "%" in report

    def test_telegram_report_within_limit(self):
        report = self._make_result(100, mode="cross").to_telegram_report()
        assert len(report) <= 4096

    def test_telegram_report_contains_mode(self):
        for mode, kor in [("ichimoku", "이치모쿠"), ("stage", "3단계"), ("cross", "이치모쿠×3단계")]:
            r = self._make_result(5, mode).to_telegram_report()
            assert kor in r

    def test_note_appears_in_both_reports(self):
        cfg = BacktestConfig("stage", date(2025, 1, 1), date(2026, 1, 1))
        result = BacktestResult(
            config=cfg, signals=[], overall=GroupMetrics(),
            computed_at="2026-01-01T00:00:00",
            note="테스트 주의사항",
        )
        assert "테스트 주의사항" in result.to_text_report()
        assert "테스트 주의사항" in result.to_telegram_report()


# ── top_bottom_telegram_text ─────────────────────────────────────

class TestTopBottomTelegramText:
    def _make_result_with_signals(self) -> BacktestResult:
        cfg = BacktestConfig("ichimoku", date(2025, 1, 1), date(2026, 1, 1))
        sigs = [
            _make_signal("A.KS", date(2025, 1, 6),  r28=0.15, r7=0.05, r91=0.30),
            _make_signal("B.KS", date(2025, 2, 1),  r28=0.08, r7=0.03, r91=0.12),
            _make_signal("C.KS", date(2025, 3, 1),  r28=-0.05, r7=-0.02, r91=-0.10),
            _make_signal("D.KS", date(2025, 4, 1),  r28=-0.12, r7=-0.04, r91=-0.20),
            _make_signal("E.KS", date(2025, 5, 1),  r28=0.22, r7=0.08, r91=0.40),
        ]
        m = _compute_group_metrics(sigs, 0.03)
        return BacktestResult(config=cfg, signals=sigs, overall=m,
                              computed_at="2026-01-01T00:00:00")

    def test_no_28d_returns_empty_string(self):
        cfg = BacktestConfig("ichimoku", date(2025, 1, 1), date(2026, 1, 1))
        result = BacktestResult(config=cfg, signals=[_make_signal()],
                                overall=GroupMetrics(), computed_at="2026-01-01T00:00:00")
        assert result.top_bottom_telegram_text() == ""

    def test_top_ranked_first(self):
        result = self._make_result_with_signals()
        txt = result.top_bottom_telegram_text(n=2)
        # E.KS (+22%) should be first in top section
        top_section = txt.split("📉")[0]
        assert "+22" in top_section

    def test_bottom_ranked_last(self):
        result = self._make_result_with_signals()
        txt = result.top_bottom_telegram_text(n=2)
        # D.KS (-12%) should be in bottom section
        bot_section = txt.split("📉")[1]
        assert "-12" in bot_section

    def test_contains_both_sections(self):
        result = self._make_result_with_signals()
        txt = result.top_bottom_telegram_text(n=3)
        assert "🏆" in txt
        assert "📉" in txt

    def test_n_caps_at_signal_count(self):
        """n > signals → clamps to number of signals with 28d returns."""
        cfg = BacktestConfig("ichimoku", date(2025, 1, 1), date(2026, 1, 1))
        sigs = [_make_signal(r28=0.05), _make_signal(r28=-0.03)]
        result = BacktestResult(config=cfg, signals=sigs, overall=GroupMetrics(),
                                computed_at="2026-01-01T00:00:00")
        txt = result.top_bottom_telegram_text(n=10)
        assert txt  # not empty
        assert "🏆" in txt


# ── to_html_report ────────────────────────────────────────────────

class TestToHtmlReport:
    def _make_result(self, n_sigs: int = 5) -> BacktestResult:
        cfg = BacktestConfig("stage", date(2025, 1, 1), date(2026, 1, 1))
        sigs = [
            _make_signal(f"T{i:02d}.KS", date(2025, 1, 6 + i),
                         r7=0.01 * i, r28=0.02 * i, r91=0.05 * i,
                         ex28=0.01 * i - 0.02)
            for i in range(n_sigs)
        ]
        m = _compute_group_metrics(sigs, 0.03)
        return BacktestResult(config=cfg, signals=sigs, overall=m,
                              computed_at="2026-01-01T00:00:00")

    def test_creates_file(self, tmp_path):
        result = self._make_result()
        out = str(tmp_path / "test_backtest.html")
        returned = result.to_html_report(out)
        assert returned == out
        assert (tmp_path / "test_backtest.html").exists()

    def test_creates_parent_dirs(self, tmp_path):
        result = self._make_result()
        out = str(tmp_path / "nested" / "deep" / "report.html")
        result.to_html_report(out)
        import os
        assert os.path.exists(out)

    def test_html_contains_ticker_names(self, tmp_path):
        result = self._make_result(3)
        out = str(tmp_path / "r.html")
        result.to_html_report(out)
        content = (tmp_path / "r.html").read_text(encoding="utf-8")
        for sig in result.signals:
            assert sig.ticker in content

    def test_html_contains_mode_label(self, tmp_path):
        result = self._make_result()
        out = str(tmp_path / "r.html")
        result.to_html_report(out)
        content = (tmp_path / "r.html").read_text(encoding="utf-8")
        assert "3단계" in content  # MODE_KOR["stage"]

    def test_html_contains_sort_script(self, tmp_path):
        result = self._make_result()
        out = str(tmp_path / "r.html")
        result.to_html_report(out)
        content = (tmp_path / "r.html").read_text(encoding="utf-8")
        assert "function sort(" in content

    def test_empty_signals_no_crash(self, tmp_path):
        cfg = BacktestConfig("ichimoku", date(2025, 1, 1), date(2026, 1, 1))
        result = BacktestResult(config=cfg, signals=[], overall=GroupMetrics(),
                                computed_at="2026-01-01T00:00:00")
        out = str(tmp_path / "empty.html")
        result.to_html_report(out)
        assert (tmp_path / "empty.html").exists()


# ── _compute_sell_signals_and_s2 ─────────────────────────────────

def _make_ohlcv_for_sell(
    n_flat: int = 60,
    flat_price: float = 10_000.0,
    flat_vol: int = 100_000,
    spike_pct: float = 0.08,
    spike_vol_mult: float = 3.0,
    post: Optional[list[tuple[float, int]]] = None,
    start_date: date = date(2022, 1, 3),
) -> tuple[pd.DataFrame, date, date]:
    """Returns (df, spike_date, first_post_date)."""
    if post is None:
        post = [(flat_price * (1 + spike_pct) * 0.85, flat_vol)]  # MA20 break day

    current = start_date
    dates, opens, highs, lows, closes, vols = [], [], [], [], [], []
    all_prices = [flat_price] * n_flat + [flat_price * (1 + spike_pct)] + [p for p, _ in post]
    all_vols   = [flat_vol] * n_flat + [int(flat_vol * spike_vol_mult)] + [v for _, v in post]

    for p, v in zip(all_prices, all_vols):
        while current.weekday() >= 5:
            current += timedelta(days=1)
        dates.append(pd.Timestamp(current, tz="UTC"))
        opens.append(p * 0.999); highs.append(p * 1.001)
        lows.append(p * 0.999);  closes.append(p); vols.append(v)
        current += timedelta(days=1)

    df = pd.DataFrame({"Open": opens, "High": highs, "Low": lows,
                       "Close": closes, "Volume": vols}, index=dates)
    spike_date     = dates[n_flat].date()
    first_post_date = dates[n_flat + 1].date() if post else spike_date
    return df, spike_date, first_post_date


class TestComputeSellSignalsAndS2:
    def _make_sig(self, ticker: str, signal_date: date,
                  close: float, mode: str = "stage") -> SignalRecord:
        return SignalRecord(ticker=ticker, name="테스트", signal_date=signal_date,
                            close_at_signal=close, mode=mode, market="KOSPI")

    def test_ma20_break_detected(self):
        """Price drops below MA20 but above stop loss → sell_reason = MA20 이탈.

        MA20 at post day ≈ (18×10_000 + 10_800 + 9_950)/20 ≈ 10_037.
        Stop price = 10_800 × 0.92 = 9_936.
        9_950: below MA20 (break ✓) but above stop price (no stop loss ✓).
        """
        df, spike_date, post_date = _make_ohlcv_for_sell(
            n_flat=60, flat_price=10_000.0, spike_pct=0.08,
            post=[(9_950, 100_000)])   # 9_950 < MA20≈10_037 but > stop 9_936
        sig = self._make_sig("T.KS", spike_date, 10_800.0)
        _compute_sell_signals_and_s2([sig], {"T.KS": df}, tx_cost_rt=0.0)
        assert sig.sell_date   == post_date
        assert sig.sell_reason == "MA20 이탈"
        assert sig.sell_return is not None
        assert sig.sell_return == pytest.approx(9_950 / 10_800 - 1.0, abs=0.001)
        assert sig.hold_days   == (post_date - spike_date).days

    def test_stop_loss_detected(self):
        """Price drops ≥ 8% → stop loss fires before MA20 break."""
        # stop_price = 10_800 * 0.92 = 9_936
        # post day close = 9_500 ≤ 9_936 → stop loss
        df, spike_date, post_date = _make_ohlcv_for_sell(
            n_flat=60, flat_price=10_000.0, spike_pct=0.08,
            post=[(9_500, 100_000)])
        sig = self._make_sig("T.KS", spike_date, 10_800.0)
        _compute_sell_signals_and_s2([sig], {"T.KS": df}, tx_cost_rt=0.0)
        assert sig.sell_date   == post_date
        assert "손절" in sig.sell_reason
        assert sig.sell_return == pytest.approx(9_500 / 10_800 - 1.0, abs=0.001)

    def test_stop_loss_priority_over_ma20_same_day(self):
        """Same day: both stop loss and MA20 break → stop loss reported."""
        # MA20 ≈ 10_040; 9_000 < MA20 AND 9_000 < 10_800 * 0.92 = 9_936
        df, spike_date, post_date = _make_ohlcv_for_sell(
            n_flat=60, flat_price=10_000.0, spike_pct=0.08,
            post=[(9_000, 100_000)])
        sig = self._make_sig("T.KS", spike_date, 10_800.0)
        _compute_sell_signals_and_s2([sig], {"T.KS": df}, tx_cost_rt=0.0)
        assert "손절" in sig.sell_reason

    def test_no_sell_signal_still_above_ma20(self):
        """Price stays above MA20 and stop loss → sell_reason = 보유 중."""
        # post day: same spike price (still high, no break)
        df, spike_date, _ = _make_ohlcv_for_sell(
            n_flat=60, flat_price=10_000.0, spike_pct=0.08,
            post=[(10_800, 100_000)])
        sig = self._make_sig("T.KS", spike_date, 10_800.0)
        _compute_sell_signals_and_s2([sig], {"T.KS": df}, tx_cost_rt=0.0)
        assert sig.sell_date   is None
        assert sig.sell_reason == "보유 중"
        assert sig.sell_return is None

    def test_tx_cost_deducted_from_sell_return(self):
        """Transaction cost subtracted from sell_return."""
        df, spike_date, _ = _make_ohlcv_for_sell(
            n_flat=60, flat_price=10_000.0, spike_pct=0.08,
            post=[(9_000, 100_000)])
        sig = self._make_sig("T.KS", spike_date, 10_800.0)
        _compute_sell_signals_and_s2([sig], {"T.KS": df}, tx_cost_rt=0.005)
        expected = 9_000 / 10_800 - 1.0 - 0.005  # raw return - tx cost
        assert sig.sell_return == pytest.approx(expected, abs=0.001)

    def test_s2_date_detected_for_stage1(self):
        """C1+C2+C3 met within 14 days of S1 → s2_date populated."""
        # spike=+8%=10_800, post: 9_720 (−10%, C1 OK), vol=40% of spike (C3 OK)
        # MA20 ≈ 10_040; 9_720 ≥ 10_040*0.95=9_538 (C2 OK)
        spike_vol = int(100_000 * 3.0)
        df, spike_date, post_date = _make_ohlcv_for_sell(
            n_flat=60, flat_price=10_000.0, spike_pct=0.08, spike_vol_mult=3.0,
            post=[(10_800 * 0.90, int(spike_vol * 0.40))])
        sig = self._make_sig("T.KS", spike_date, 10_800.0, mode="stage")
        _compute_sell_signals_and_s2([sig], {"T.KS": df}, tx_cost_rt=0.0)
        assert sig.s2_date == post_date

    def test_s2_date_not_set_for_stage2_mode(self):
        """S2 detection only applies to stage-mode signals, not stage2."""
        spike_vol = int(100_000 * 3.0)
        df, spike_date, _ = _make_ohlcv_for_sell(
            n_flat=60, flat_price=10_000.0, spike_pct=0.08, spike_vol_mult=3.0,
            post=[(10_800 * 0.90, int(spike_vol * 0.40))])
        sig = self._make_sig("T.KS", spike_date, 10_800.0, mode="stage2")
        _compute_sell_signals_and_s2([sig], {"T.KS": df}, tx_cost_rt=0.0)
        assert sig.s2_date is None

    def test_missing_ticker_no_crash(self):
        """Ticker not in ohlcv_map → signal unchanged (no crash)."""
        sig = self._make_sig("MISSING.KS", date(2025, 1, 6), 10_000.0)
        _compute_sell_signals_and_s2([sig], {}, tx_cost_rt=0.0)
        assert sig.sell_date   is None
        assert sig.sell_reason == ""

    def test_s3_date_detected_after_s2(self):
        """S3 conditions (10d-high break + +5% + RSI≥70 + 1.5×vol) after S2 → s3_date set.

        Setup: 50 uptrend days (+1%) to prime RSI, then S1 spike (+8%),
        S2 pullback (−10%), 12 flat days (RSI partial recovery), then S3 (+5%, 2×vol).
        After 12 flat days, RSI at S3 day is ≈72 ≥ 70.
        The 12-day consolidation pushes the S1 spike outside the 10-day high window,
        so close > 10d-high (consolidation high) is satisfied.
        """
        n_up = 50
        base = 10_000.0
        # Build uptrend: each day +1%
        current = date(2021, 1, 4)
        dates_, opens_, highs_, lows_, closes_, vols_ = [], [], [], [], [], []

        p = base
        for _ in range(n_up):
            while current.weekday() >= 5:
                current += timedelta(days=1)
            next_p = p * 1.01
            dates_.append(pd.Timestamp(current, tz="UTC"))
            opens_.append(p * 0.999); highs_.append(next_p * 1.001)
            lows_.append(p * 0.999);  closes_.append(next_p); vols_.append(100_000)
            p = next_p
            current += timedelta(days=1)

        # S1 spike
        while current.weekday() >= 5:
            current += timedelta(days=1)
        s1_price = p * 1.08
        s1_vol   = 300_000
        dates_.append(pd.Timestamp(current, tz="UTC"))
        opens_.append(p); highs_.append(s1_price * 1.001)
        lows_.append(p);  closes_.append(s1_price); vols_.append(s1_vol)
        s1_date = current
        current += timedelta(days=1)

        # S2 pullback (−10%)
        while current.weekday() >= 5:
            current += timedelta(days=1)
        s2_price = s1_price * 0.90
        s2_vol   = int(s1_vol * 0.40)
        dates_.append(pd.Timestamp(current, tz="UTC"))
        opens_.append(s2_price); highs_.append(s2_price * 1.001)
        lows_.append(s2_price);  closes_.append(s2_price); vols_.append(s2_vol)
        current += timedelta(days=1)

        # 12 flat consolidation days
        for _ in range(12):
            while current.weekday() >= 5:
                current += timedelta(days=1)
            dates_.append(pd.Timestamp(current, tz="UTC"))
            opens_.append(s2_price); highs_.append(s2_price * 1.001)
            lows_.append(s2_price);  closes_.append(s2_price); vols_.append(100_000)
            current += timedelta(days=1)

        # S3 breakout (+5%, above 10d-high, 2× avg_vol30)
        while current.weekday() >= 5:
            current += timedelta(days=1)
        s3_price = s2_price * 1.05
        s3_vol   = 200_000   # > 1.5 × avg30 ≈ 107k
        dates_.append(pd.Timestamp(current, tz="UTC"))
        opens_.append(s2_price); highs_.append(s3_price * 1.001)
        lows_.append(s2_price);  closes_.append(s3_price); vols_.append(s3_vol)
        s3_expected_date = current

        df = pd.DataFrame(
            {"Open": opens_, "High": highs_, "Low": lows_,
             "Close": closes_, "Volume": vols_},
            index=dates_,
        )

        sig = self._make_sig("T.KS", s1_date, s1_price, mode="stage")
        _compute_sell_signals_and_s2([sig], {"T.KS": df}, tx_cost_rt=0.0)
        # S2 should be detected first
        assert sig.s2_date is not None, "S2 should be detected"
        # S3 should be detected after S2
        assert sig.s3_date == s3_expected_date, (
            f"Expected s3_date={s3_expected_date}, got {sig.s3_date}"
        )

    def test_s3_not_set_without_s2(self):
        """No S2 → s3_date stays None.

        post vol = 220k → txamt_ratio = (9950×220k)/(10800×300k) ≈ 0.676 > 0.65,
        C3 fails → no S2. price 9_950 < MA20 ≈ 10_037 → sell fires; no S2 → S3 also not scanned.
        """
        df, spike_date, _ = _make_ohlcv_for_sell(
            n_flat=60, flat_price=10_000.0, spike_pct=0.08, spike_vol_mult=3.0,
            post=[(9_950, 220_000)])  # txamt_ratio≈0.676 > 0.65 → C3 fails → no S2
        sig = self._make_sig("T.KS", spike_date, 10_800.0, mode="stage")
        _compute_sell_signals_and_s2([sig], {"T.KS": df}, tx_cost_rt=0.0)
        assert sig.s2_date is None
        assert sig.s3_date is None

    def test_s3_blocked_by_flow_condition(self):
        """flow_lookup 제공 + 외인·기관 중 하나가 순매도 → S3 미감지."""
        n_up = 50
        base = 10_000.0
        current = date(2021, 1, 4)
        dates_, opens_, highs_, lows_, closes_, vols_ = [], [], [], [], [], []

        p = base
        for _ in range(n_up):
            while current.weekday() >= 5:
                current += timedelta(days=1)
            next_p = p * 1.01
            dates_.append(pd.Timestamp(current, tz="UTC"))
            opens_.append(p * 0.999); highs_.append(next_p * 1.001)
            lows_.append(p * 0.999);  closes_.append(next_p); vols_.append(100_000)
            p = next_p
            current += timedelta(days=1)

        while current.weekday() >= 5:
            current += timedelta(days=1)
        s1_price = p * 1.08
        dates_.append(pd.Timestamp(current, tz="UTC"))
        opens_.append(p); highs_.append(s1_price * 1.001)
        lows_.append(p);  closes_.append(s1_price); vols_.append(300_000)
        s1_date = current
        current += timedelta(days=1)

        while current.weekday() >= 5:
            current += timedelta(days=1)
        s2_price = s1_price * 0.90
        dates_.append(pd.Timestamp(current, tz="UTC"))
        opens_.append(s2_price); highs_.append(s2_price * 1.001)
        lows_.append(s2_price);  closes_.append(s2_price); vols_.append(int(300_000 * 0.40))
        current += timedelta(days=1)

        for _ in range(12):
            while current.weekday() >= 5:
                current += timedelta(days=1)
            dates_.append(pd.Timestamp(current, tz="UTC"))
            opens_.append(s2_price); highs_.append(s2_price * 1.001)
            lows_.append(s2_price);  closes_.append(s2_price); vols_.append(100_000)
            current += timedelta(days=1)

        while current.weekday() >= 5:
            current += timedelta(days=1)
        s3_price = s2_price * 1.05
        dates_.append(pd.Timestamp(current, tz="UTC"))
        opens_.append(s2_price); highs_.append(s3_price * 1.001)
        lows_.append(s2_price);  closes_.append(s3_price); vols_.append(200_000)
        s3_day = current

        df = pd.DataFrame(
            {"Open": opens_, "High": highs_, "Low": lows_,
             "Close": closes_, "Volume": vols_},
            index=dates_,
        )
        sig = self._make_sig("T.KS", s1_date, s1_price, mode="stage")

        # Case A: 외인 순매도 → S3 차단
        flow_blocked = {("T.KS", s3_day): (-1_000, 500, None)}
        _compute_sell_signals_and_s2([sig], {"T.KS": df}, tx_cost_rt=0.0,
                                     flow_lookup=flow_blocked)
        assert sig.s2_date is not None
        assert sig.s3_date is None, "외인 순매도 시 S3 차단되어야 함"

    def test_s3_allowed_by_flow_condition(self):
        """flow_lookup 제공 + 외인·기관 모두 순매수 → S3 감지."""
        n_up = 50
        base = 10_000.0
        current = date(2021, 6, 1)
        dates_, opens_, highs_, lows_, closes_, vols_ = [], [], [], [], [], []

        p = base
        for _ in range(n_up):
            while current.weekday() >= 5:
                current += timedelta(days=1)
            next_p = p * 1.01
            dates_.append(pd.Timestamp(current, tz="UTC"))
            opens_.append(p * 0.999); highs_.append(next_p * 1.001)
            lows_.append(p * 0.999);  closes_.append(next_p); vols_.append(100_000)
            p = next_p
            current += timedelta(days=1)

        while current.weekday() >= 5:
            current += timedelta(days=1)
        s1_price = p * 1.08
        dates_.append(pd.Timestamp(current, tz="UTC"))
        opens_.append(p); highs_.append(s1_price * 1.001)
        lows_.append(p);  closes_.append(s1_price); vols_.append(300_000)
        s1_date = current
        current += timedelta(days=1)

        while current.weekday() >= 5:
            current += timedelta(days=1)
        s2_price = s1_price * 0.90
        dates_.append(pd.Timestamp(current, tz="UTC"))
        opens_.append(s2_price); highs_.append(s2_price * 1.001)
        lows_.append(s2_price);  closes_.append(s2_price); vols_.append(int(300_000 * 0.40))
        current += timedelta(days=1)

        for _ in range(12):
            while current.weekday() >= 5:
                current += timedelta(days=1)
            dates_.append(pd.Timestamp(current, tz="UTC"))
            opens_.append(s2_price); highs_.append(s2_price * 1.001)
            lows_.append(s2_price);  closes_.append(s2_price); vols_.append(100_000)
            current += timedelta(days=1)

        while current.weekday() >= 5:
            current += timedelta(days=1)
        s3_price = s2_price * 1.05
        dates_.append(pd.Timestamp(current, tz="UTC"))
        opens_.append(s2_price); highs_.append(s3_price * 1.001)
        lows_.append(s2_price);  closes_.append(s3_price); vols_.append(200_000)
        s3_day = current

        df = pd.DataFrame(
            {"Open": opens_, "High": highs_, "Low": lows_,
             "Close": closes_, "Volume": vols_},
            index=dates_,
        )
        sig = self._make_sig("T.KS", s1_date, s1_price, mode="stage")

        # 외인+기관 모두 순매수
        flow_ok = {("T.KS", s3_day): (5_000, 3_000, None)}
        _compute_sell_signals_and_s2([sig], {"T.KS": df}, tx_cost_rt=0.0,
                                     flow_lookup=flow_ok)
        assert sig.s2_date is not None
        assert sig.s3_date == s3_day, "외인+기관 모두 순매수 시 S3 감지되어야 함"

    def test_s3_passes_when_flow_date_missing(self):
        """flow_lookup 있지만 해당 날짜 데이터 없음 → 조건 5 생략, S3 감지."""
        n_up = 50
        base = 10_000.0
        current = date(2021, 9, 1)
        dates_, opens_, highs_, lows_, closes_, vols_ = [], [], [], [], [], []

        p = base
        for _ in range(n_up):
            while current.weekday() >= 5:
                current += timedelta(days=1)
            next_p = p * 1.01
            dates_.append(pd.Timestamp(current, tz="UTC"))
            opens_.append(p * 0.999); highs_.append(next_p * 1.001)
            lows_.append(p * 0.999);  closes_.append(next_p); vols_.append(100_000)
            p = next_p
            current += timedelta(days=1)

        while current.weekday() >= 5:
            current += timedelta(days=1)
        s1_price = p * 1.08
        dates_.append(pd.Timestamp(current, tz="UTC"))
        opens_.append(p); highs_.append(s1_price * 1.001)
        lows_.append(p);  closes_.append(s1_price); vols_.append(300_000)
        s1_date = current
        current += timedelta(days=1)

        while current.weekday() >= 5:
            current += timedelta(days=1)
        s2_price = s1_price * 0.90
        dates_.append(pd.Timestamp(current, tz="UTC"))
        opens_.append(s2_price); highs_.append(s2_price * 1.001)
        lows_.append(s2_price);  closes_.append(s2_price); vols_.append(int(300_000 * 0.40))
        current += timedelta(days=1)

        for _ in range(12):
            while current.weekday() >= 5:
                current += timedelta(days=1)
            dates_.append(pd.Timestamp(current, tz="UTC"))
            opens_.append(s2_price); highs_.append(s2_price * 1.001)
            lows_.append(s2_price);  closes_.append(s2_price); vols_.append(100_000)
            current += timedelta(days=1)

        while current.weekday() >= 5:
            current += timedelta(days=1)
        s3_price = s2_price * 1.05
        dates_.append(pd.Timestamp(current, tz="UTC"))
        opens_.append(s2_price); highs_.append(s3_price * 1.001)
        lows_.append(s2_price);  closes_.append(s3_price); vols_.append(200_000)
        s3_day = current

        df = pd.DataFrame(
            {"Open": opens_, "High": highs_, "Low": lows_,
             "Close": closes_, "Volume": vols_},
            index=dates_,
        )
        sig = self._make_sig("T.KS", s1_date, s1_price, mode="stage")

        # flow_lookup은 있지만 s3_day 데이터가 없음 → 조건 5 생략 → 통과
        flow_no_s3_day: dict = {}
        _compute_sell_signals_and_s2([sig], {"T.KS": df}, tx_cost_rt=0.0,
                                     flow_lookup=flow_no_s3_day)
        assert sig.s2_date is not None
        assert sig.s3_date == s3_day, "flow 데이터 없는 날짜는 조건 5 생략 → S3 감지"

    def test_group_metrics_s3_fields(self):
        """_compute_group_metrics populates s3_progression_rate when s3_date present."""
        sig1 = _make_signal(r7=0.01, r28=0.03, r91=0.08)
        sig1.s2_date = date(2025, 1, 14)
        sig1.s3_date = date(2025, 2, 5)
        sig2 = _make_signal(r7=0.02, r28=0.04, r91=0.10)
        sig2.s2_date = date(2025, 1, 16)
        # sig2 has no s3_date
        m = _compute_group_metrics([sig1, sig2], rf_annual=0.03)
        assert m.s3_progression_rate == pytest.approx(0.5)  # 1 of 2 S2 → S3

    def test_group_metrics_sell_fields(self):
        """_compute_group_metrics populates sell/S2 fields when data present."""
        sig = _make_signal(r7=0.02, r28=0.05, r91=0.10)
        sig.sell_return = 0.03
        sig.hold_days   = 15
        sig.mode        = "stage"
        sig.s2_date     = date(2025, 1, 14)
        m = _compute_group_metrics([sig], rf_annual=0.03)
        assert m.win_rate_sell     == pytest.approx(1.0)
        assert m.avg_return_sell   == pytest.approx(0.03)
        assert m.avg_hold_days     == pytest.approx(15.0)
        assert m.s2_progression_rate == pytest.approx(1.0)


# ── _replay_ichimoku (합성 데이터) ────────────────────────────────

class TestReplayIchimoku:
    """Unit tests for _replay_ichimoku using synthetic daily DataFrames.

    No network calls. Data is resampled to weekly internally by _replay_ichimoku.
    Ichimoku math (calc_ichimoku) is not mocked — we rely on price patterns
    that reliably trigger each condition.
    """

    def _make_flat_then_jump_daily(
        self,
        flat_days: int = 700,
        flat_price: float = 10_000.0,
        jump_price: float = 20_000.0,
        jump_days: int = 40,
        start_date: date = date(2015, 1, 5),
    ) -> pd.DataFrame:
        """flat_days trading days at flat_price, then jump_days at jump_price."""
        dates, opens, highs, lows, closes, vols = [], [], [], [], [], []
        current = start_date
        total = flat_days + jump_days
        for i in range(total):
            while current.weekday() >= 5:
                current += timedelta(days=1)
            p = flat_price if i < flat_days else jump_price
            dates.append(pd.Timestamp(current, tz="UTC"))
            opens.append(p * 0.999)
            highs.append(p * 1.001)
            lows.append(p * 0.999)
            closes.append(p)
            vols.append(500_000)
            current += timedelta(days=1)
        return pd.DataFrame(
            {"Open": opens, "High": highs, "Low": lows, "Close": closes, "Volume": vols},
            index=dates,
        )

    def test_insufficient_data_returns_empty(self):
        """< 62 weekly bars → Ichimoku lookback not satisfied → []."""
        # 300 trading days ≈ 60 weeks, just below the 62-week threshold
        df = _make_daily_df(n=300, start_price=10_000.0, trend=0.0,
                            start_date=date(2020, 1, 2))
        cfg = BacktestConfig("ichimoku", date(2020, 1, 1), date(2025, 12, 31))
        sigs = _replay_ichimoku("TEST.KS", "테스트", df, "KOSPI", cfg)
        assert sigs == []

    def test_flat_price_no_signals(self):
        """Flat price → MA20w and MA60w not rising → conditions E/F fail → no signals."""
        # 400 days ≈ 80 weeks, enough to pass the 62-week check
        df = _make_daily_df(n=400, start_price=10_000.0, trend=0.0,
                            start_date=date(2018, 1, 2))
        cfg = BacktestConfig("ichimoku", date(2018, 1, 1), date(2020, 12, 31))
        sigs = _replay_ichimoku("TEST.KS", "테스트", df, "KOSPI", cfg)
        assert len(sigs) == 0

    def test_outside_window_returns_empty(self):
        """Data with a potential breakout, but config window set before the data range."""
        df = self._make_flat_then_jump_daily()
        # data runs ~2015-01-05 to ~2018-Q3; window set to 2020+
        cfg = BacktestConfig("ichimoku", date(2020, 1, 1), date(2022, 12, 31))
        sigs = _replay_ichimoku("TEST.KS", "테스트", df, "KOSPI", cfg)
        assert len(sigs) == 0

    def test_breakout_signal_detected(self):
        """flat data → 2× price jump: condition A (close > cloud_top) and B
        (prev_close <= prev_cloud_top ≈ flat_price) both hold → at least 1 signal."""
        df = self._make_flat_then_jump_daily(
            flat_days=700, flat_price=10_000.0, jump_price=20_000.0, jump_days=40,
            start_date=date(2015, 1, 5),
        )
        # jump week lands around 2017-09
        cfg = BacktestConfig("ichimoku", date(2017, 1, 1), date(2018, 12, 31))
        sigs = _replay_ichimoku("TEST.KS", "테스트", df, "KOSPI", cfg)
        assert len(sigs) >= 1
        for s in sigs:
            assert s.mode == "ichimoku"
            assert s.ticker == "TEST.KS"
            # signal date is a Friday (weekly label)
            assert s.signal_date.weekday() == 4
            assert s.close_at_signal == pytest.approx(20_000.0, rel=0.01)

    def test_signals_are_non_consecutive(self):
        """Condition B (prev_close <= prev_cloud_top) prevents back-to-back signals
        when the stock stays above the cloud. Each signal must be >= 7 days after the prior."""
        df = self._make_flat_then_jump_daily(
            flat_days=700, flat_price=10_000.0, jump_price=20_000.0, jump_days=60,
        )
        cfg = BacktestConfig("ichimoku", date(2017, 1, 1), date(2019, 12, 31))
        sigs = _replay_ichimoku("TEST.KS", "테스트", df, "KOSPI", cfg)
        for i in range(1, len(sigs)):
            gap = (sigs[i].signal_date - sigs[i - 1].signal_date).days
            assert gap >= 7, (
                f"Consecutive signals {sigs[i-1].signal_date} → {sigs[i].signal_date} "
                f"({gap} days apart) — condition B should prevent this"
            )


# ── _build_weekly_ichimoku ────────────────────────────────────────

def _make_daily_for_weekly(
    n_days: int = 450,
    price: float = 10_000.0,
    start_date: date = date(2020, 1, 6),
) -> pd.DataFrame:
    """Flat daily OHLCV for weekly ichimoku tests."""
    current = start_date
    dates, opens, highs, lows, closes, vols = [], [], [], [], [], []
    for _ in range(n_days):
        while current.weekday() >= 5:
            current += timedelta(days=1)
        dates.append(pd.Timestamp(current, tz="UTC"))
        opens.append(price * 0.999); highs.append(price * 1.001)
        lows.append(price * 0.999);  closes.append(price); vols.append(100_000)
        current += timedelta(days=1)
    return pd.DataFrame(
        {"Open": opens, "High": highs, "Low": lows, "Close": closes, "Volume": vols},
        index=dates,
    )


class TestBuildWeeklyIchimoku:
    def test_returns_dataframe_for_sufficient_data(self):
        """≥ 62 weeks of daily data → returns DataFrame with expected columns."""
        df = _make_daily_for_weekly(n_days=500)
        weekly = _build_weekly_ichimoku(df)
        assert weekly is not None
        for col in ("cloud_top", "cloud_bottom", "tenkan", "kijun"):
            assert col in weekly.columns, f"Missing column: {col}"

    def test_returns_none_for_insufficient_data(self):
        """< 62 weeks (< 434 days) → None."""
        df = _make_daily_for_weekly(n_days=300)   # ≈ 43 weeks
        result = _build_weekly_ichimoku(df)
        assert result is None

    def test_weekly_rows_are_fridays(self):
        """W-FRI resample → all index timestamps are Friday."""
        df = _make_daily_for_weekly(n_days=500)
        weekly = _build_weekly_ichimoku(df)
        assert weekly is not None
        for ts in weekly.index:
            d = ts.date() if hasattr(ts, "date") else ts
            assert d.weekday() == 4, f"Expected Friday, got {d} (weekday={d.weekday()})"

    def test_cloud_top_ge_cloud_bottom(self):
        """cloud_top ≥ cloud_bottom for all valid rows."""
        df = _make_daily_for_weekly(n_days=500)
        weekly = _build_weekly_ichimoku(df)
        assert weekly is not None
        valid = weekly.dropna(subset=["cloud_top", "cloud_bottom"])
        assert (valid["cloud_top"] >= valid["cloud_bottom"]).all()


# ── _find_ichimoku_sell ───────────────────────────────────────────

def _make_weekly_ichi_df(
    n_weeks: int = 100,
    price: float = 10_000.0,
    start_date: date = date(2020, 1, 3),
) -> pd.DataFrame:
    """Minimal weekly DataFrame with ichimoku columns for sell tests.

    All prices flat → cloud_top=cloud_bottom=price, tenkan=kijun=price.
    """
    # Find first Friday >= start_date
    current = start_date
    while current.weekday() != 4:
        current += timedelta(days=1)

    dates, closes, cloud_tops, cloud_bottoms, tenkans, kijuns = [], [], [], [], [], []
    for _ in range(n_weeks):
        dates.append(pd.Timestamp(current, tz="UTC"))
        closes.append(price)
        cloud_tops.append(price)
        cloud_bottoms.append(price)
        tenkans.append(price)
        kijuns.append(price)
        current += timedelta(weeks=1)

    return pd.DataFrame(
        {
            "Close": closes,
            "cloud_top": cloud_tops,
            "cloud_bottom": cloud_bottoms,
            "tenkan": tenkans,
            "kijun": kijuns,
        },
        index=dates,
    )


class TestFindIchimokuSell:
    def test_stop_loss_detected(self):
        """Close ≤ entry × 0.92 → sell_reason contains '손절'."""
        entry = 10_000.0
        stop_price = entry * 0.92  # 9_200
        signal_date = date(2020, 1, 10)  # a Friday

        df = _make_weekly_ichi_df(n_weeks=80, price=entry)
        # Override 3rd weekly bar after signal to be below stop
        target_row = 3
        after_signal = [i for i, ts in enumerate(df.index) if ts.date() > signal_date]
        assert len(after_signal) >= target_row
        idx = after_signal[target_row - 1]
        df.iloc[idx, df.columns.get_loc("Close")] = stop_price - 100.0

        sell_date, sell_reason, sell_ret, hold_days = _find_ichimoku_sell(
            signal_date, entry, df, tx_cost_rt=0.0, stop_loss_pct=0.08,
        )
        assert sell_date is not None
        assert "손절" in sell_reason
        assert sell_ret is not None and sell_ret < 0

    def test_cloud_breakdown_detected(self):
        """Close < cloud_bottom → '구름 이탈' (stop loss not triggered)."""
        entry = 10_000.0
        signal_date = date(2020, 1, 10)

        # Price slightly below cloud_bottom but above stop
        df = _make_weekly_ichi_df(n_weeks=80, price=entry)
        after_signal = [i for i, ts in enumerate(df.index) if ts.date() > signal_date]
        idx = after_signal[1]  # 2nd bar after signal
        close_below_cloud = entry * 0.95   # 9_500 — below cloud (10_000) but above stop (9_200)
        df.iloc[idx, df.columns.get_loc("Close")] = close_below_cloud
        # cloud_bottom stays at 10_000 (above close) → breakdown triggered

        sell_date, sell_reason, sell_ret, hold_days = _find_ichimoku_sell(
            signal_date, entry, df, tx_cost_rt=0.0, stop_loss_pct=0.08,
        )
        assert sell_date is not None
        assert sell_reason == "구름 이탈"
        assert hold_days > 0

    def test_dead_cross_detected(self):
        """tenkan crosses below kijun (prev: tenkan≥kijun, curr: tenkan<kijun) → '전환<기준 DC'."""
        entry = 10_000.0
        signal_date = date(2020, 1, 10)

        df = _make_weekly_ichi_df(n_weeks=80, price=entry)
        after_signal = [i for i, ts in enumerate(df.index) if ts.date() > signal_date]

        # Bar 1: tenkan=kijun (prev equal → no DC yet)
        # Bar 2: tenkan < kijun (DC fires) — close stays well above stop & cloud
        idx1 = after_signal[0]
        idx2 = after_signal[1]
        df.iloc[idx1, df.columns.get_loc("tenkan")] = entry          # prev_tenkan == kijun
        df.iloc[idx1, df.columns.get_loc("kijun")]  = entry
        df.iloc[idx2, df.columns.get_loc("tenkan")] = entry * 0.97   # tenkan < kijun
        df.iloc[idx2, df.columns.get_loc("kijun")]  = entry          # kijun stays
        # Keep close > stop (entry * 0.92) and above cloud_bottom (entry)
        # Close must NOT fall below cloud_bottom to avoid prior trigger
        df.iloc[idx2, df.columns.get_loc("Close")]       = entry * 1.01  # above cloud
        df.iloc[idx2, df.columns.get_loc("cloud_bottom")] = entry * 0.50  # lower cloud

        sell_date, sell_reason, sell_ret, hold_days = _find_ichimoku_sell(
            signal_date, entry, df, tx_cost_rt=0.0, stop_loss_pct=0.08,
        )
        assert sell_date is not None
        assert sell_reason == "전환<기준 DC"

    def test_stop_loss_priority_over_cloud_breakdown(self):
        """Same bar: both stop loss and cloud breakdown → stop loss takes priority."""
        entry = 10_000.0
        signal_date = date(2020, 1, 10)

        df = _make_weekly_ichi_df(n_weeks=80, price=entry)
        after_signal = [i for i, ts in enumerate(df.index) if ts.date() > signal_date]
        idx = after_signal[0]
        below_stop = entry * 0.90  # < stop(9_200) AND < cloud_bottom(10_000)
        df.iloc[idx, df.columns.get_loc("Close")] = below_stop

        _, sell_reason, _, _ = _find_ichimoku_sell(
            signal_date, entry, df, tx_cost_rt=0.0, stop_loss_pct=0.08,
        )
        assert "손절" in sell_reason

    def test_no_signal_returns_holding(self):
        """No sell condition ever met → (None, '보유 중', None, None)."""
        entry = 10_000.0
        signal_date = date(2020, 1, 10)
        df = _make_weekly_ichi_df(n_weeks=20, price=entry)  # all flat above cloud

        sell_date, sell_reason, sell_ret, hold_days = _find_ichimoku_sell(
            signal_date, entry, df, tx_cost_rt=0.0, stop_loss_pct=0.08,
        )
        assert sell_date is None
        assert sell_reason == "보유 중"
        assert sell_ret is None
        assert hold_days is None

    def test_tx_cost_deducted(self):
        """sell_return = (close/entry - 1) - tx_cost_rt."""
        entry = 10_000.0
        tx = 0.005
        signal_date = date(2020, 1, 10)

        df = _make_weekly_ichi_df(n_weeks=80, price=entry)
        after_signal = [i for i, ts in enumerate(df.index) if ts.date() > signal_date]
        idx = after_signal[0]
        close_val = entry * 0.90  # triggers stop
        df.iloc[idx, df.columns.get_loc("Close")] = close_val

        _, _, sell_ret, _ = _find_ichimoku_sell(
            signal_date, entry, df, tx_cost_rt=tx, stop_loss_pct=0.08,
        )
        expected = (close_val / entry - 1.0) - tx
        assert sell_ret == pytest.approx(expected, abs=0.0001)

    def test_bars_before_signal_date_are_skipped(self):
        """signal_date bars are excluded — sell only triggers on strictly later bars."""
        entry = 10_000.0
        signal_date = date(2020, 1, 10)

        df = _make_weekly_ichi_df(n_weeks=80, price=entry)
        # Inject stop-loss price on the bar AT signal_date (should be skipped)
        for i, ts in enumerate(df.index):
            if ts.date() == signal_date:
                df.iloc[i, df.columns.get_loc("Close")] = entry * 0.80
                break

        sell_date, sell_reason, _, _ = _find_ichimoku_sell(
            signal_date, entry, df, tx_cost_rt=0.0, stop_loss_pct=0.08,
        )
        # The signal_date bar must have been skipped; no other bars trigger
        assert sell_date is None
        assert sell_reason == "보유 중"
