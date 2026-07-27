"""
test_stage_classifier.py — stage_classifier.py + stage_classifier_legacy.py 전체 코드패스 커버
25개 코드패스 (Stage 1~3 각 조건 + edge case + peakout).
구버전 디스패처(classify_stage v1.0, v11~v13)는 analysis.stage_classifier_legacy에서 import.
"""

from __future__ import annotations

import pandas as pd
from datetime import date, timedelta
from typing import Optional

from analysis.stage_classifier import check_peakout
from analysis.stage_classifier_legacy import (
    classify_stage, classify_stage_v11, classify_stage_v12, classify_stage_v13,
)


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
    personal_net: Optional[int] = 100,
) -> pd.DataFrame:
    idx = pd.date_range(end="2026-04-26", periods=n, freq="D")
    return pd.DataFrame(
        {
            "foreign_net":    [foreign_net] * n,
            "inst_net":       [inst_net] * n,
            "foreign_streak": [foreign_streak] * n,
            "inst_streak":    [inst_streak] * n,
            "personal_net":   [personal_net] * n,
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


# ── v1.1 핵심 차이점 ──────────────────────────────────────────

class TestV11VsV10:
    """v1.1이 v1.0과 다르게 동작하는 세 지점을 검증."""

    # ── Stage 1: RSI ≥ 50 조건 ──────────────────────────────

    def test_s1_v11_is_subset_of_v10(self):
        """v1.1 Stage 1 신호이면 v1.0도 반드시 Stage 1 — RSI 조건이 더 엄격하므로."""
        df   = _make_price_df(close_today=106.0, close_prev=100.0, vol_today=900_000, avg_vol=400_000)
        flow = _make_flow_df(foreign_net=100, inst_net=0)
        v10  = classify_stage("TEST.KS", df, flow, {}, "KOSPI")
        v11  = classify_stage_v11("TEST.KS", df, flow, {}, "KOSPI")
        # v1.1 Stage 1이면 v1.0도 Stage 1이어야 함 (RSI 조건은 추가이지 대체가 아님)
        if v11 == 1:
            assert v10 == 1

    def test_s1_v11_passes_high_rsi(self):
        """RSI ≥ 50이면 v1.0과 v1.1 모두 Stage 1을 반환."""
        df   = _make_price_df(close_today=106.0, close_prev=100.0, vol_today=900_000, avg_vol=400_000)
        flow = _make_flow_df(foreign_net=100, inst_net=0)
        result = classify_stage_v11("TEST.KS", df, flow, {}, "KOSPI")
        assert result == 1

    # ── Stage 2: txamt 범위 0.30~0.60 ───────────────────────

    def test_s2_v11_rejects_wide_txamt(self):
        """v1.0은 통과(0.25~0.65)하지만 v1.1은 거부(0.30~0.60 밖)하는 케이스."""
        # txamt_ratio ≈ 0.26: v1.0 허용(≥0.25), v1.1 거부(<0.30)
        # s1_txamt = 900k vol × 115 close = 103.5M
        # txamt_today = 235k × 100 ≈ 23.5M → ratio 23.5/103.5 ≈ 0.227 < 0.25 → 둘 다 거부
        # txamt_today = 280k × 100 = 28M → ratio 28/103.5 ≈ 0.27 → v1.0 ✓ v1.1 ✗
        s1_hist = {"TEST.KS": [{
            "classified_date": date.today() - timedelta(days=7),
            "s1_high": 115.0, "s1_volume": 900_000, "s1_txamt": int(900_000 * 115),
        }]}
        # s1_txamt = 103,500,000. 원하는 ratio=0.27: txamt_today = 0.27×103.5M = 27.945M
        # vol_today × close_today = 27.945M → vol = 279_450 (close=100)
        df   = _make_price_df(close_today=100.0, close_prev=99.0, vol_today=279_450, avg_vol=400_000)
        flow = _make_flow_df(foreign_net=-50, inst_net=0, inst_streak=0)
        v10 = classify_stage("TEST.KS", df, flow, s1_hist, "KOSPI")
        v11 = classify_stage_v11("TEST.KS", df, flow, s1_hist, "KOSPI")
        assert v10 == 2
        assert v11 is None  # 0.27 < 0.30, v1.1 거부

    # ── Stage 3: v1.1도 동일 조건 통과 ─────────────────────

    def test_s3_v11_passes_when_conditions_met(self):
        """Stage 3 조건이 모두 충족되면 v1.1도 Stage 3를 반환."""
        # TestStage3._strong_df()와 동일한 데이터: 50일 보합 후 +6.5% 급등 + 거래량 급증
        closes = [100.0] * 63 + [100.0, 106.5]
        highs  = [100.5] * 63 + [100.5, 108.0]
        lows   = [99.5]  * 65
        vols   = [400_000] * 63 + [400_000, 700_000]
        idx = pd.date_range(end="2026-04-26", periods=65, freq="D", tz="UTC")
        df = pd.DataFrame(
            {"Open": closes, "High": highs, "Low": lows, "Close": closes, "Volume": vols},
            index=idx,
        )
        flow    = _make_flow_df(foreign_net=200, inst_net=150)
        s1_hist = _s1_history_14d("TEST.KS")
        assert classify_stage_v11("TEST.KS", df, flow, s1_hist, "KOSPI") == 3


# ── v1.2 핵심 차이점 ──────────────────────────────────────────

class TestV12VsV11:
    """v1.2가 v1.1과 다르게 동작하는 세 지점을 검증."""

    # ── Stage 1: 수급 조건 강화 ──────────────────────────────

    def test_s1_v12_passes_with_both_buy(self):
        """외인>0 AND 기관>=0 이면 v1.2 Stage 1 통과."""
        df   = _make_price_df(close_today=106.0, close_prev=100.0, vol_today=900_000, avg_vol=400_000)
        flow = _make_flow_df(foreign_net=100, inst_net=50, foreign_streak=2, inst_streak=1)
        assert classify_stage_v12("TEST.KS", df, flow, {}, market="KOSPI") == 1

    def test_s1_v12_passes_with_inst_streak3(self):
        """외인 순매도라도 기관 streak >= 3 이면 v1.2 통과."""
        df   = _make_price_df(close_today=106.0, close_prev=100.0, vol_today=900_000, avg_vol=400_000)
        flow = _make_flow_df(foreign_net=-10, inst_net=80, foreign_streak=-1, inst_streak=3)
        assert classify_stage_v12("TEST.KS", df, flow, {}, market="KOSPI") == 1

    def test_s1_v12_blocked_when_foreign_sells_inst_streak_low(self):
        """외인 순매도 + 기관 streak < 3 이면 v1.2 거부 (v1.1은 inst_net > 0으로 통과)."""
        df   = _make_price_df(close_today=106.0, close_prev=100.0, vol_today=900_000, avg_vol=400_000)
        # inst_net=50>0: v1.1 통과. 하지만 foreign=-50 AND inst_streak=1<3: v1.2 거부
        flow = _make_flow_df(foreign_net=-50, inst_net=50, foreign_streak=-1, inst_streak=1)
        v11  = classify_stage_v11("TEST.KS", df, flow, {}, "KOSPI")
        v12  = classify_stage_v12("TEST.KS", df, flow, {}, market="KOSPI")
        assert v11 == 1
        assert v12 is None

    # ── Stage 2: 거래대금 100~150% + 고저폭 축소 ─────────────

    def test_s2_v12_blocked_by_high_txamt(self):
        """거래대금이 20일 평균의 200% → v1.1 통과, v1.2 거부 (>150%)."""
        # s1_txamt=103.5M, 원하는 ratio=0.40: txamt_today=41.4M
        # avg_txamt20(= 400k vol × 95 close ≈ 38M). txamt_today/avg ≈ 41.4/38 ≈ 1.09 → 통과
        # 높은 거래대금을 위해 txamt_today를 avg × 1.6 수준으로 설정
        s1_hist = {"TEST.KS": [{
            "classified_date": date.today() - timedelta(days=7),
            "s1_high": 115.0, "s1_volume": 900_000, "s1_txamt": int(900_000 * 115),
        }]}
        # s1_txamt=103.5M, 목표 ratio=0.45: txamt_today=46.575M
        # avg_txamt20: avg_vol × close_hist ≈ 400k × 95 = 38M
        # txamt_today / avg_txamt20 = 46.575M / 38M ≈ 1.23 → 1.0~1.5 내 통과
        # 대신 ratio_to_avg > 1.5로 만들려면 txamt_today > 57M → vol_today = 570k @ close=100
        # txamt_today = 570k × 100 = 57M → s1 ratio = 57/103.5 = 0.55 (통과 범위)
        # avg_txamt20 ≈ 400k × 95 = 38M → txamt/avg = 57/38 = 1.5 경계 (실패)
        # 조금 더: vol_today=600k: txamt=60M, ratio_to_avg=60/38=1.58>1.5 → v1.2 거부
        df   = _make_price_df(close_today=100.0, close_prev=99.0, vol_today=600_000, avg_vol=400_000)
        flow = _make_flow_df(foreign_net=-50, inst_net=0, inst_streak=0)
        v11  = classify_stage_v11("TEST.KS", df, flow, s1_hist, "KOSPI")
        v12  = classify_stage_v12("TEST.KS", df, flow, s1_hist)
        assert v11 == 2
        assert v12 is None  # txamt > 20일 평균 × 1.5

    def test_s2_v12_blocked_by_wide_range(self):
        """일일 고저폭이 평균의 70% 초과 → v1.2 거부."""
        s1_hist = {"TEST.KS": [{
            "classified_date": date.today() - timedelta(days=7),
            "s1_high": 115.0, "s1_volume": 900_000, "s1_txamt": int(900_000 * 115),
        }]}
        # 역사적 고저폭: (95.5 - 94.5)/95 ≈ 1.05%
        # 오늘 고저폭을 크게: (110 - 88)/100 = 22% >> avg × 0.7 → v1.2 거부
        n = 65
        closes = [95.0] * (n - 2) + [99.0, 100.0]
        highs  = [95.5] * (n - 2) + [99.5, 110.0]   # 오늘 high 매우 큼
        lows   = [94.5] * (n - 2) + [98.5,  88.0]   # 오늘 low 매우 낮음
        # 거래대금 비율 맞추기: s1_txamt=103.5M, ratio=0.45: vol_today=465k @ close=100
        vols   = [400_000] * (n - 2) + [400_000, 465_000]
        idx    = pd.date_range(end="2026-04-26", periods=n, freq="D", tz="UTC")
        df     = pd.DataFrame(
            {"Open": closes, "High": highs, "Low": lows, "Close": closes, "Volume": vols},
            index=idx,
        )
        flow = _make_flow_df(foreign_net=-50, inst_net=0, inst_streak=0)
        v11  = classify_stage_v11("TEST.KS", df, flow, s1_hist, "KOSPI")
        v12  = classify_stage_v12("TEST.KS", df, flow, s1_hist)
        assert v11 == 2
        assert v12 is None  # 고저폭 너무 큼

    # ── Stage 3: s2_history 전제 + streak >= 2 ───────────────

    def test_s3_v12_blocked_without_s2_history(self):
        """s2_history 제공 시 Stage 2 이력 없으면 Stage 3 거부."""
        closes = [100.0] * 63 + [100.0, 106.5]
        highs  = [100.5] * 63 + [100.5, 108.0]
        lows   = [99.5]  * 65
        vols   = [400_000] * 63 + [400_000, 700_000]
        idx = pd.date_range(end="2026-04-26", periods=65, freq="D", tz="UTC")
        df  = pd.DataFrame(
            {"Open": closes, "High": highs, "Low": lows, "Close": closes, "Volume": vols},
            index=idx,
        )
        flow    = _make_flow_df(foreign_net=200, inst_net=150, foreign_streak=3, inst_streak=3)
        s1_hist = _s1_history_14d("TEST.KS")
        s2_hist: dict = {}  # Stage 2 이력 없음
        assert classify_stage_v12("TEST.KS", df, flow, s1_hist, s2_hist) is None

    def test_s3_v12_passes_with_s2_history_and_streak(self):
        """s2_history 있고 streak >= 2 이면 v1.2 Stage 3 통과."""
        closes = [100.0] * 63 + [100.0, 106.5]
        highs  = [100.5] * 63 + [100.5, 108.0]
        lows   = [99.5]  * 65
        vols   = [400_000] * 63 + [400_000, 700_000]
        idx = pd.date_range(end="2026-04-26", periods=65, freq="D", tz="UTC")
        df  = pd.DataFrame(
            {"Open": closes, "High": highs, "Low": lows, "Close": closes, "Volume": vols},
            index=idx,
        )
        flow    = _make_flow_df(foreign_net=200, inst_net=150, foreign_streak=3, inst_streak=3)
        s1_hist = _s1_history_14d("TEST.KS")
        s2_hist = {"TEST.KS": [{"classified_date": date.today() - timedelta(days=5)}]}
        assert classify_stage_v12("TEST.KS", df, flow, s1_hist, s2_hist) == 3

    def test_s3_v12_blocked_by_low_streak(self):
        """streak < 2 이면 v1.2 Stage 3 거부 (v1.1은 통과)."""
        closes = [100.0] * 63 + [100.0, 106.5]
        highs  = [100.5] * 63 + [100.5, 108.0]
        lows   = [99.5]  * 65
        vols   = [400_000] * 63 + [400_000, 700_000]
        idx = pd.date_range(end="2026-04-26", periods=65, freq="D", tz="UTC")
        df  = pd.DataFrame(
            {"Open": closes, "High": highs, "Low": lows, "Close": closes, "Volume": vols},
            index=idx,
        )
        # streak=1 (1일 순매수): v1.2 거부
        flow    = _make_flow_df(foreign_net=200, inst_net=150, foreign_streak=1, inst_streak=1)
        s1_hist = _s1_history_14d("TEST.KS")
        s2_hist = {"TEST.KS": [{"classified_date": date.today() - timedelta(days=5)}]}
        v11 = classify_stage_v11("TEST.KS", df, flow, s1_hist, "KOSPI")
        v12 = classify_stage_v12("TEST.KS", df, flow, s1_hist, s2_hist)
        assert v11 == 3
        assert v12 is None  # streak < 2

    # ── Stage 1: 개인 순매수 조건 ─────────────────────────────

    def test_s1_v12_blocked_by_personal_net_zero(self):
        """개인 순매수 <= 0 → v1.2 Stage 1 거부, v1.1은 통과."""
        df   = _make_price_df(close_today=106.0, vol_today=900_000)
        flow = _make_flow_df(foreign_net=100, inst_net=50, personal_net=-1000)
        v11  = classify_stage_v11("TEST.KS", df, flow, {}, "KOSPI")
        v12  = classify_stage_v12("TEST.KS", df, flow, {}, market="KOSPI")
        assert v11 is not None  # v1.1: 개인 순매수 조건 없음
        assert v12 is None      # v1.2: 개인 순매도 → 거부

    def test_s1_v12_passes_with_personal_net_positive(self):
        """개인 순매수 > 0 + 외인>0 + 기관>=0 → v1.2 Stage 1 통과."""
        df   = _make_price_df(close_today=106.0, vol_today=900_000)
        flow = _make_flow_df(foreign_net=100, inst_net=50, personal_net=500)
        assert classify_stage_v12("TEST.KS", df, flow, {}, market="KOSPI") == 1

    # ── Stage 1: 상장주식수 0.2% 조건 ──────────────────────────

    def test_s1_v12_blocked_by_low_flow_ratio(self):
        """3거래일 외인+기관 합산 < 상장주식수 0.2% → v1.2 거부."""
        df   = _make_price_df(close_today=106.0, vol_today=900_000)
        # foreign_net=100, inst_net=50 → 3일 합산 450주. 상장주식수=500_000 → 0.2%=1000 → 450 < 1000
        flow = _make_flow_df(foreign_net=100, inst_net=50, personal_net=500)
        listed = {"TEST.KS": 500_000}
        assert classify_stage_v12("TEST.KS", df, flow, {}, market="KOSPI", listed_shares=listed) is None

    def test_s1_v12_passes_with_sufficient_flow_ratio(self):
        """3거래일 외인+기관 합산 ≥ 상장주식수 0.2% → v1.2 통과."""
        df   = _make_price_df(close_today=106.0, vol_today=900_000)
        # foreign_net=5000, inst_net=3000 → 3일 합산 24000. 상장주식수=500_000 → 0.2%=1000 → 24000 ≥ 1000
        flow = _make_flow_df(foreign_net=5000, inst_net=3000, personal_net=500)
        listed = {"TEST.KS": 500_000}
        assert classify_stage_v12("TEST.KS", df, flow, {}, market="KOSPI", listed_shares=listed) == 1

    def test_s1_v12_passes_when_listed_shares_missing(self):
        """listed_shares에 해당 티커 없으면 조건 9 스킵 (데이터 없음 = 통과)."""
        df   = _make_price_df(close_today=106.0, vol_today=900_000)
        flow = _make_flow_df(foreign_net=100, inst_net=50, personal_net=500)
        listed = {"OTHER.KS": 500_000}  # TEST.KS 없음
        assert classify_stage_v12("TEST.KS", df, flow, {}, market="KOSPI", listed_shares=listed) == 1

    # ── Stage 2: 외국인 지분율 1%p 이탈 차단 ──────────────────

    def _s2_hist_v12(self, ticker: str = "TEST.KS") -> dict:
        return _s1_history_14d(ticker, s1_high=115.0, s1_volume=900_000)

    def _s2_df_narrow_range(self) -> pd.DataFrame:
        """Stage 2 v1.2 테스트용 가격 데이터.

        hist 범위(high-low): ~1.05% (high=95.5, low=94.5, close=95)
        today 범위: ~0.6% (high=100.3, low=99.7, close=100)
        → today_range(0.6%) < hist_avg(1.05%) × 0.70(0.74%) → 조건 B 통과.
        거래대금 ratio: avg_txamt≈38M, today=46.5M → ratio≈1.22 (1.0~1.5 통과).
        """
        n = 65
        closes = [95.0] * (n - 2) + [99.0, 100.0]
        highs  = [95.5] * (n - 2) + [99.5, 100.3]
        lows   = [94.5] * (n - 2) + [98.5,  99.7]
        vols   = [400_000] * (n - 2) + [400_000, 465_000]
        idx = pd.date_range(end="2026-04-26", periods=n, freq="D", tz="UTC")
        return pd.DataFrame(
            {"Open": closes, "High": highs, "Low": lows, "Close": closes, "Volume": vols},
            index=idx,
        )

    def test_s2_v12_blocked_by_foreign_exit(self):
        """외국인 14일 누적 순매도가 상장주식수 1% 초과 → v1.2 거부."""
        s1_hist = self._s2_hist_v12()
        df = self._s2_df_narrow_range()
        # foreign_net=-20000/day × 14일 합산 = -280_000. 상장주식수=10_000_000 → 1%=100_000 → 초과
        flow = _make_flow_df(foreign_net=-20_000, inst_net=100, inst_streak=2)
        listed = {"TEST.KS": 10_000_000}
        v11 = classify_stage_v11("TEST.KS", df, flow, s1_hist, "KOSPI")
        v12 = classify_stage_v12("TEST.KS", df, flow, s1_hist, listed_shares=listed)
        assert v11 == 2
        assert v12 is None  # 외국인 1%p 이상 이탈

    def test_s2_v12_passes_within_foreign_limit(self):
        """외국인 14일 누적 순매도가 1%p 이내 → v1.2 통과."""
        s1_hist = self._s2_hist_v12()
        df = self._s2_df_narrow_range()
        # foreign_net=-100/day → 14일 합산 = -1400. 상장주식수=10_000_000 → 1%=100_000 → 이내
        flow = _make_flow_df(foreign_net=-100, inst_net=100, inst_streak=2)
        listed = {"TEST.KS": 10_000_000}
        assert classify_stage_v12("TEST.KS", df, flow, s1_hist, listed_shares=listed) == 2

    # ── Stage 3: 상장주식수 0.2% 조건 ──────────────────────────

    def _strong_df_v12(self) -> pd.DataFrame:
        closes = [100.0] * 63 + [100.0, 106.5]
        highs  = [100.5] * 63 + [100.5, 108.0]
        lows   = [99.5]  * 65
        vols   = [400_000] * 63 + [400_000, 700_000]
        idx = pd.date_range(end="2026-04-26", periods=65, freq="D", tz="UTC")
        return pd.DataFrame(
            {"Open": closes, "High": highs, "Low": lows, "Close": closes, "Volume": vols},
            index=idx,
        )

    def test_s3_v12_blocked_by_low_flow_ratio(self):
        """3거래일 외인+기관 합산 < 상장주식수 0.2% → v1.2 Stage 3 거부."""
        df   = self._strong_df_v12()
        # foreign_net=50, inst_net=30 → 3일 합산 240. 상장주식수=500_000 → 0.2%=1000 → 240 < 1000
        flow    = _make_flow_df(foreign_net=50, inst_net=30, foreign_streak=3, inst_streak=3)
        s1_hist = _s1_history_14d("TEST.KS")
        s2_hist = {"TEST.KS": [{"classified_date": date.today() - timedelta(days=5)}]}
        listed  = {"TEST.KS": 500_000}
        assert classify_stage_v12("TEST.KS", df, flow, s1_hist, s2_hist, listed_shares=listed) is None

    def test_s3_v12_passes_with_sufficient_flow_ratio(self):
        """3거래일 외인+기관 합산 ≥ 상장주식수 0.2% → v1.2 Stage 3 통과."""
        df   = self._strong_df_v12()
        # foreign_net=5000, inst_net=3000 → 3일 합산 24000. 상장주식수=500_000 → 0.2%=1000 → 통과
        flow    = _make_flow_df(foreign_net=5000, inst_net=3000, foreign_streak=3, inst_streak=3)
        s1_hist = _s1_history_14d("TEST.KS")
        s2_hist = {"TEST.KS": [{"classified_date": date.today() - timedelta(days=5)}]}
        listed  = {"TEST.KS": 500_000}
        assert classify_stage_v12("TEST.KS", df, flow, s1_hist, s2_hist, listed_shares=listed) == 3


class TestV13VsV12:
    """Stage v1.3 = v1.2 + MA20 기울기(Stage 1) + 개인 출회(Stage 2) - 개인 순매수 조건."""

    # ── Stage 1: MA20 기울기 ──────────────────────────────────

    def _make_s1_df_with_slope(self, slope_ok: bool) -> pd.DataFrame:
        n = 65
        if slope_ok:
            # 꾸준한 우상향 → MA20_today > MA20_5ago
            base_closes = [95.0 + i * 0.05 for i in range(n - 2)] + [99.0, 106.0]
        else:
            # 5일 전 로컬 피크(110) 후 하락 → MA20_today(99.5) < MA20_5ago(102.5) → 기울기 음수
            # MA20_today = mean(bars[45:65]) = (15×100 + 3×95 + 99 + 106)/20 = 99.5
            # MA20_5ago  = mean(bars[40:60]) = (5×110 + 15×100)/20 = 102.5
            base_closes = [90.0]*40 + [110.0]*5 + [100.0]*15 + [95.0]*3 + [99.0, 106.0]
        highs = [c * 1.01 for c in base_closes]
        lows  = [c * 0.99 for c in base_closes]
        vols  = [400_000] * (n - 2) + [400_000, 900_000]
        idx = pd.date_range(end="2026-04-26", periods=n, freq="D", tz="UTC")
        return pd.DataFrame(
            {"Open": base_closes, "High": highs, "Low": lows, "Close": base_closes, "Volume": vols},
            index=idx,
        )

    def test_s1_v13_passes_regardless_of_ma20_slope(self):
        """v1.3은 MA20 기울기 조건 없음 — 평평한 MA20이어도 통과 (close > MA20은 base에서 이미 체크)."""
        for slope_ok in (True, False):
            df   = self._make_s1_df_with_slope(slope_ok=slope_ok)
            flow = _make_flow_df(foreign_net=5000, inst_net=3000, personal_net=500)
            assert classify_stage_v13("TEST.KS", df, flow, {}, market="KOSPI") == 1, \
                f"slope_ok={slope_ok}일 때 Stage 1 통과 기대"

    def test_s1_v13_no_personal_net_condition(self):
        """v1.3 Stage 1은 개인 조건 없음 — personal_net 음수여도 통과."""
        df   = self._make_s1_df_with_slope(slope_ok=True)
        # v1.2: personal_net=-500 → 개인 순매수 > 0 미달 → 차단
        # v1.3: 개인 조건 없음 → 통과
        flow = _make_flow_df(foreign_net=5000, inst_net=3000, personal_net=-500)
        v12  = classify_stage_v12("TEST.KS", df, flow, {}, market="KOSPI")
        v13  = classify_stage_v13("TEST.KS", df, flow, {}, market="KOSPI")
        assert v12 is None  # v1.2: personal_net ≤0 → 차단
        assert v13 == 1     # v1.3: 개인 조건 없음 → 통과

    # ── Stage 2: 개인 출회 신호 ──────────────────────────────────

    def _s2_hist(self) -> dict:
        return _s1_history_14d("TEST.KS", s1_high=115.0, s1_volume=900_000)

    def _s2_df(self) -> pd.DataFrame:
        n = 65
        closes = [95.0] * (n - 2) + [99.0, 100.0]
        highs  = [95.5] * (n - 2) + [99.5, 100.3]
        lows   = [94.5] * (n - 2) + [98.5,  99.7]
        vols   = [400_000] * (n - 2) + [400_000, 465_000]
        idx = pd.date_range(end="2026-04-26", periods=n, freq="D", tz="UTC")
        return pd.DataFrame(
            {"Open": closes, "High": highs, "Low": lows, "Close": closes, "Volume": vols},
            index=idx,
        )

    def test_s2_v13_blocked_by_personal_buy(self):
        """개인 순매수(personal_net > 0) → v1.3 Stage 2 거부, v1.2는 통과."""
        s1_hist = self._s2_hist()
        df      = self._s2_df()
        flow    = _make_flow_df(foreign_net=-100, inst_net=100, inst_streak=2, personal_net=5000)
        v12 = classify_stage_v12("TEST.KS", df, flow, s1_hist)
        v13 = classify_stage_v13("TEST.KS", df, flow, s1_hist)
        assert v12 == 2     # v1.2: 개인 출회 조건 없음 → 통과
        assert v13 is None  # v1.3: 개인 순매수 → 차단

    def test_s2_v13_passes_with_personal_sell(self):
        """개인 순매도(personal_net ≤ 0) → v1.3 Stage 2 통과."""
        s1_hist = self._s2_hist()
        df      = self._s2_df()
        flow    = _make_flow_df(foreign_net=-100, inst_net=100, inst_streak=2, personal_net=-300)
        assert classify_stage_v13("TEST.KS", df, flow, s1_hist) == 2

    def test_s2_v13_passes_when_personal_net_missing(self):
        """personal_net 데이터 없으면 조건 스킵(통과)."""
        s1_hist = self._s2_hist()
        df      = self._s2_df()
        flow    = _make_flow_df(foreign_net=-100, inst_net=100, inst_streak=2, personal_net=None)
        assert classify_stage_v13("TEST.KS", df, flow, s1_hist) == 2

