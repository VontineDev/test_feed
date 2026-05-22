"""
test_stage_classifier.py — stage_classifier.py 전체 코드패스 커버
25개 코드패스 (Stage 1~3 각 조건 + edge case + peakout).
"""

from __future__ import annotations

import pandas as pd
import pytest
from datetime import date, timedelta

from analysis.stage_classifier import classify_stage, check_peakout


# ── fixtures ─────────────────────────────────────────────────

def _make_price_df(
    n: int = 65,
    close_today: float = 110.0,
    close_prev: float = 100.0,
    vol_today: int = 1_000_000,
    avg_vol: int = 400_000,
    high_today: float | None = None,
    low_today: float | None = None,
) -> pd.DataFrame:
    """N일 일봉 DataFrame. 마지막 2행이 오늘(close_today) / 전일(close_prev)."""
    if high_today is None:
        high_today = close_today * 1.01
    if low_today is None:
        low_today = close_today * 0.99

    closes = [95.0] * (n - 2) + [close_prev, close_today]
    highs  = [95.0] * (n - 2) + [close_prev * 1.01, high_today]
    lows   = [95.0] * (n - 2) + [close_prev * 0.99, low_today]
    vols   = [avg_vol] * (n - 2) + [avg_vol, vol_today]
    idx = pd.date_range(end="2026-04-26", periods=n, freq="D", tz="UTC")
    return pd.DataFrame(
        {"Open": closes, "High": highs, "Low": lows, "Close": closes, "Volume": vols},
        index=idx,
    )


def _make_flow_df(
    n: int = 20,
    foreign_net: int = 100,
    inst_net: int = 50,
    foreign_streak: int = 2,
    inst_streak: int = 1,
) -> pd.DataFrame:
    idx = pd.date_range(end="2026-04-26", periods=n, freq="D")
    return pd.DataFrame(
        {
            "foreign_net":    [foreign_net] * n,
            "inst_net":       [inst_net] * n,
            "foreign_streak": [foreign_streak] * n,
            "inst_streak":    [inst_streak] * n,
        },
        index=idx,
    )


def _s1_history_14d(ticker: str, s1_high: float = 115.0, s1_volume: int = 900_000) -> dict:
    s1_txamt = int(s1_volume * s1_high) if s1_high else None
    return {
        ticker: [{
            "classified_date": date.today() - timedelta(days=7),
            "s1_high":   s1_high,
            "s1_volume": s1_volume,
            "s1_txamt":  s1_txamt,  # 거래대금 = s1_volume × s1_high
        }]
    }


# ── Stage 1 조건 ──────────────────────────────────────────────

class TestStage1:
    def test_all_conditions_pass(self):
        # inst_net=0: Stage 1 OR condition passes (foreign > 0), Stage 3 AND fails (inst not > 0)
        df = _make_price_df(close_today=106.0, close_prev=100.0, vol_today=900_000, avg_vol=400_000)
        flow = _make_flow_df(foreign_net=100, inst_net=0)
        assert classify_stage("TEST.KS", df, flow, {}, "KOSPI") == 1

    def test_price_change_below_threshold_kospi(self):
        # +4% < +5% threshold for KOSPI; Stage 3 also blocked (4% < 5%)
        df = _make_price_df(close_today=104.0, close_prev=100.0, vol_today=900_000, avg_vol=400_000)
        flow = _make_flow_df()
        assert classify_stage("TEST.KS", df, flow, {}, "KOSPI") is None

    def test_price_change_below_threshold_kosdaq(self):
        # +6% < +7% KOSDAQ threshold fails Stage 1; inst_net=-1 blocks Stage 3's AND requirement
        df = _make_price_df(close_today=106.0, close_prev=100.0, vol_today=900_000, avg_vol=400_000)
        flow = _make_flow_df(inst_net=-1)
        assert classify_stage("TEST.KQ", df, flow, {}, "KOSDAQ") is None

    def test_volume_below_2x_avg(self):
        # txamt_today ≈ 1.95× avg_txamt — 거래대금 기준 2× 미달; inst_net=-1 blocks Stage 3
        # close=106, avg_close≈95: txamt_today=700k×106=74.2M vs avg≈38.1M → ratio 1.95× < 2×
        df = _make_price_df(close_today=106.0, close_prev=100.0, vol_today=700_000, avg_vol=400_000)
        flow = _make_flow_df(inst_net=-1)
        assert classify_stage("TEST.KS", df, flow, {}, "KOSPI") is None

    def test_close_below_ma20(self):
        # close_today below the moving average (force MA to be higher)
        closes = [130.0] * 63 + [100.0, 106.0]  # MA20 ~130, close=106
        vols   = [400_000] * 63 + [400_000, 900_000]
        idx = pd.date_range(end="2026-04-26", periods=65, freq="D", tz="UTC")
        df = pd.DataFrame(
            {"Open": closes, "High": closes, "Low": closes, "Close": closes, "Volume": vols},
            index=idx,
        )
        flow = _make_flow_df()
        assert classify_stage("TEST.KS", df, flow, {}, "KOSPI") is None

    def test_52w_position_too_far_from_high(self):
        # close = 75, 52w_high = 100 → position = 25% > 20%
        closes = [100.0] * 63 + [80.0, 75.0]
        vols   = [400_000] * 63 + [400_000, 900_000]
        idx = pd.date_range(end="2026-04-26", periods=65, freq="D", tz="UTC")
        df = pd.DataFrame(
            {"Open": closes, "High": closes, "Low": closes, "Close": closes, "Volume": vols},
            index=idx,
        )
        flow = _make_flow_df()
        assert classify_stage("TEST.KS", df, flow, {}, "KOSPI") is None

    def test_no_net_buy(self):
        df = _make_price_df(close_today=106.0, close_prev=100.0, vol_today=900_000, avg_vol=400_000)
        flow = _make_flow_df(foreign_net=-100, inst_net=-50)
        assert classify_stage("TEST.KS", df, flow, {}, "KOSPI") is None

    def test_only_foreign_net_buy(self):
        df = _make_price_df(close_today=106.0, close_prev=100.0, vol_today=900_000, avg_vol=400_000)
        flow = _make_flow_df(foreign_net=100, inst_net=-50)
        assert classify_stage("TEST.KS", df, flow, {}, "KOSPI") == 1

    def test_insufficient_data(self):
        # < 21 bars → can't compute MA20
        df = _make_price_df(n=15, close_today=106.0, close_prev=100.0)
        flow = _make_flow_df()
        assert classify_stage("TEST.KS", df, flow, {}, "KOSPI") is None

    def test_empty_flow_df(self):
        df = _make_price_df(close_today=106.0, close_prev=100.0, vol_today=900_000, avg_vol=400_000)
        assert classify_stage("TEST.KS", df, pd.DataFrame(), {}, "KOSPI") is None

    def test_zero_avg_vol_guard(self):
        # avg_vol = 0 → division-by-zero guard
        df = _make_price_df(close_today=106.0, close_prev=100.0, vol_today=100, avg_vol=0)
        df["Volume"] = 0
        flow = _make_flow_df()
        assert classify_stage("TEST.KS", df, flow, {}, "KOSPI") is None

    def test_zero_52w_high_guard(self):
        # 52w_high = 0 → division-by-zero guard
        closes = [0.0] * 65
        vols   = [400_000] * 64 + [900_000]
        idx = pd.date_range(end="2026-04-26", periods=65, freq="D", tz="UTC")
        df = pd.DataFrame(
            {"Open": closes, "High": closes, "Low": closes, "Close": closes, "Volume": vols},
            index=idx,
        )
        flow = _make_flow_df()
        assert classify_stage("TEST.KS", df, flow, {}, "KOSPI") is None


# ── Stage 2 조건 ──────────────────────────────────────────────

class TestStage2:
    def _s1_hist(self, ticker: str = "TEST.KS") -> dict:
        return _s1_history_14d(ticker, s1_high=115.0, s1_volume=900_000)

    def test_all_conditions_pass(self):
        # close=100, s1_high=115 → discount=13% ✓ 30~60% vol ✓
        df   = _make_price_df(close_today=100.0, close_prev=99.0, vol_today=360_000, avg_vol=400_000)
        flow = _make_flow_df(foreign_net=-50, inst_net=0, inst_streak=0)
        assert classify_stage("TEST.KS", df, flow, self._s1_hist(), "KOSPI") == 2

    def test_no_s1_history(self):
        df   = _make_price_df(close_today=100.0, close_prev=99.0, vol_today=360_000, avg_vol=400_000)
        flow = _make_flow_df(foreign_net=-50, inst_net=0, inst_streak=0)
        assert classify_stage("TEST.KS", df, flow, {}, "KOSPI") is None

    def test_discount_too_shallow(self):
        # close=112, s1_high=115 → discount=2.6% < 5%
        df   = _make_price_df(close_today=112.0, close_prev=111.0, vol_today=360_000, avg_vol=400_000)
        flow = _make_flow_df(foreign_net=-50, inst_net=0, inst_streak=0)
        assert classify_stage("TEST.KS", df, flow, self._s1_hist(), "KOSPI") is None

    def test_discount_too_deep(self):
        # close=85, s1_high=115 → discount=26% > 20%
        df   = _make_price_df(close_today=85.0, close_prev=84.0, vol_today=360_000, avg_vol=400_000)
        flow = _make_flow_df(foreign_net=-50, inst_net=0, inst_streak=0)
        assert classify_stage("TEST.KS", df, flow, self._s1_hist(), "KOSPI") is None

    def test_volume_outside_range(self):
        # txamt_today=720k×100=72M / s1_txamt=900k×115=103.5M → ratio=0.696 > 0.65 → C3 실패
        df   = _make_price_df(close_today=100.0, close_prev=99.0, vol_today=720_000, avg_vol=400_000)
        flow = _make_flow_df(foreign_net=-50, inst_net=0, inst_streak=0)
        assert classify_stage("TEST.KS", df, flow, self._s1_hist(), "KOSPI") is None

    def test_inst_streak_negative(self):
        df   = _make_price_df(close_today=100.0, close_prev=99.0, vol_today=360_000, avg_vol=400_000)
        flow = _make_flow_df(foreign_net=-50, inst_net=-100, inst_streak=-3)
        assert classify_stage("TEST.KS", df, flow, self._s1_hist(), "KOSPI") is None

    def test_null_s1_high_skips_price_condition(self):
        # s1_high=None → price C1 skipped; s1_txamt=None(계산불가) → C3 skipped; inst_streak ✓
        hist = {"TEST.KS": [{"classified_date": date.today() - timedelta(days=7),
                              "s1_high": None, "s1_volume": 900_000, "s1_txamt": None}]}
        df   = _make_price_df(close_today=100.0, close_prev=99.0, vol_today=360_000, avg_vol=400_000)
        flow = _make_flow_df(foreign_net=-50, inst_net=0, inst_streak=0)
        # C1/C3 skipped (null s1_high, null s1_txamt); C2 ✓; C4 inst_streak ≥ 0 ✓
        assert classify_stage("TEST.KS", df, flow, hist, "KOSPI") == 2


# ── Stage 3 조건 ──────────────────────────────────────────────

class TestStage3:
    def _s1_hist(self, ticker: str = "TEST.KS") -> dict:
        return _s1_history_14d(ticker)

    def _strong_df(self) -> pd.DataFrame:
        # 50일 보합 후 오늘 +6%, 고가 돌파, 거래량 급증
        closes = [100.0] * 63 + [100.0, 106.5]
        highs  = [100.5] * 63 + [100.5, 108.0]
        lows   = [99.5]  * 63 + [99.5,  104.0]
        vols   = [400_000] * 63 + [400_000, 700_000]
        idx = pd.date_range(end="2026-04-26", periods=65, freq="D", tz="UTC")
        return pd.DataFrame(
            {"Open": closes, "High": highs, "Low": lows, "Close": closes, "Volume": vols},
            index=idx,
        )

    def test_all_conditions_pass(self):
        # _strong_df(): flat 100→106.5 (+6.5%), breakout>100.5 high, RSI≈100 (no losses), vol 1.75×
        df   = self._strong_df()
        flow = _make_flow_df(foreign_net=200, inst_net=150)
        assert classify_stage("TEST.KS", df, flow, self._s1_hist(), "KOSPI") == 3

    def test_no_breakout(self):
        # close <= max(last 10d high) → fail
        df   = _make_price_df(close_today=100.0, close_prev=99.0, vol_today=700_000, avg_vol=400_000,
                               high_today=100.5)
        flow = _make_flow_df(foreign_net=200, inst_net=150)
        # 이전 고가도 100.5이므로 돌파 안 됨
        assert classify_stage("TEST.KS", df, flow, self._s1_hist(), "KOSPI") is None

    def test_insufficient_data(self):
        df = _make_price_df(n=25, close_today=106.0, close_prev=100.0)
        flow = _make_flow_df(foreign_net=200, inst_net=150)
        assert classify_stage("TEST.KS", df, flow, self._s1_hist(), "KOSPI") is None


# ── Stage 우선순위 ─────────────────────────────────────────────

def test_stage3_priority_over_stage1():
    """Stage 3 > Stage 1: 두 조건 모두 통과하는 데이터에서 Stage 3 반환을 검증."""
    # 점진적 상승(손실 없음 → RSI=100) + 마지막 바에서 +6.25% 급등 + 10일 고가 돌파
    # Stage 1: +6.25% ≥ 5%, 거래량 2.5×, MA20/MA60 상회, 52주 괴리 0%, 수급 ✓
    # Stage 3: RSI=100, 거래량 2.5×, 10일 돌파(92.2→97.0), 외인+기관 동시 ✓
    n = 65
    closes = [85.0 + i * 0.1 for i in range(n - 1)] + [97.0]
    highs  = [c * 1.01 for c in closes]
    lows   = [c * 0.99 for c in closes]
    vols   = [400_000] * (n - 1) + [1_000_000]
    idx = pd.date_range(end="2026-04-26", periods=n, freq="D", tz="UTC")
    df = pd.DataFrame(
        {"Open": closes, "High": highs, "Low": lows, "Close": closes, "Volume": vols},
        index=idx,
    )
    flow = _make_flow_df(foreign_net=200, inst_net=150)
    result = classify_stage("TEST.KS", df, flow, {}, "KOSPI")
    assert result == 3  # Stage 3 > Stage 1 우선순위 검증


# ── check_peakout ─────────────────────────────────────────────

class TestCheckPeakout:
    def _flow_streak(self, f: int, i: int, n: int = 5) -> pd.DataFrame:
        idx = pd.date_range(end="2026-04-26", periods=n, freq="D")
        return pd.DataFrame(
            {"foreign_net": [0]*n, "inst_net": [0]*n,
             "foreign_streak": [f]*n, "inst_streak": [i]*n},
            index=idx,
        )

    def test_false_when_len_lt_3(self):
        flow = self._flow_streak(-3, -3, n=2)
        df   = _make_price_df()
        assert check_peakout("TEST.KS", flow, df) is False

    def test_true_streak_condition(self):
        flow = self._flow_streak(-3, -3)
        df   = _make_price_df()
        assert check_peakout("TEST.KS", flow, df) is True

    def test_false_streak_not_enough(self):
        flow = self._flow_streak(-1, -1)
        df   = _make_price_df()
        assert check_peakout("TEST.KS", flow, df) is False

    def test_true_upper_wick_condition(self):
        # 윗꼬리: high=120, close=100, low=99 → (high-close)=20 > 0.5*(high-low)=10.5 ✓
        # volume spike: vol_today >> avg
        df = _make_price_df(
            close_today=100.0, close_prev=95.0,
            vol_today=700_000, avg_vol=400_000,
            high_today=120.0, low_today=99.0,
        )
        # streak 조건은 통과 못하도록 설정
        flow = self._flow_streak(1, 1)
        assert check_peakout("TEST.KS", flow, df) is True

    def test_false_neither_condition(self):
        flow = self._flow_streak(1, 1)
        df   = _make_price_df()
        assert check_peakout("TEST.KS", flow, df) is False

    def test_null_streak_treated_safely(self):
        idx = pd.date_range(end="2026-04-26", periods=5, freq="D")
        flow = pd.DataFrame(
            {"foreign_net": [0]*5, "inst_net": [0]*5,
             "foreign_streak": [None]*5, "inst_streak": [None]*5},
            index=idx,
        )
        df = _make_price_df()
        # NULL streak → 조건 1 pass 안 됨 (None <= -2 is False in Python)
        # 조건 2가 없으면 False
        result = check_peakout("TEST.KS", flow, df)
        assert isinstance(result, bool)
