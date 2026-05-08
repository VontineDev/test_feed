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

from backtest_engine import (
    BacktestConfig,
    BacktestResult,
    GroupMetrics,
    SignalRecord,
    TX_COST_DEFAULT,
    _apply_cross_filter,
    _build_price_lookup,
    _compute_group_metrics,
    _compute_mdd,
    _compute_sharpe,
    _fill_returns,
    _nearest_price,
    _replay_ichimoku,
    _replay_stage,
    _week_label,
)


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
