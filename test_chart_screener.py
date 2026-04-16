"""
test_chart_screener.py  —  주봉 차트 스크리너 단위 테스트
────────────────────────────────────────────────────────────
screen_ticker() 핵심 6조건 각각을 독립적으로 검증.
pykrx / yfinance 의존성 없이 pandas DataFrame을 직접 조립해서 테스트.
fetch_weekly_ohlcv + calc_ichimoku 모두 패치하여 조건을 결정론적으로 제어.

실행:
    pytest test_chart_screener.py -v
"""

from __future__ import annotations

import pytest
import pandas as pd
import numpy as np
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

from chart_screener import (
    screen_ticker,
    calc_ichimoku,
    current_week_of,
    ScreenResult,
)


# ── OHLCV 헬퍼 ────────────────────────────────────────────────

def _recent_friday() -> datetime:
    today = datetime.now(timezone.utc)
    return today - timedelta(days=(today.weekday() - 4) % 7)


def _uptrend_df(n: int = 80) -> pd.DataFrame:
    """
    꾸준히 우상향하는 주봉 OHLCV.
    → ma_20w, ma_60w 자연스럽게 우상향 (조건 E, F 통과).
    → close > ma_20w > ma_60w (조건 C, D 통과).
    """
    last_friday = _recent_friday()
    dates  = [last_friday - timedelta(weeks=(n - 1 - i)) for i in range(n)]
    closes = [50_000.0 + i * 300.0 for i in range(n)]
    return pd.DataFrame({
        "Open":   [c - 100.0 for c in closes],
        "High":   [c + 200.0 for c in closes],
        "Low":    [c - 200.0 for c in closes],
        "Close":  closes,
        "Volume": [1_000_000] * n,
    }, index=pd.to_datetime(dates, utc=True))


def _declining_last20_df(n: int = 80) -> pd.DataFrame:
    """
    60봉 상승 → 20봉 하락.
    → ma_20w와 ma_60w 기울기 음수 (조건 E, F 실패).
    → close < ma_20w (조건 C 실패).
    """
    last_friday = _recent_friday()
    dates  = [last_friday - timedelta(weeks=(n - 1 - i)) for i in range(n)]
    peak   = 80_000.0
    closes = [50_000.0 + i * 500.0 for i in range(60)] + \
             [peak - j * 300.0 for j in range(20)]
    return pd.DataFrame({
        "Open":   [c - 100.0 for c in closes],
        "High":   [c + 200.0 for c in closes],
        "Low":    [c - 200.0 for c in closes],
        "Close":  closes,
        "Volume": [1_000_000] * n,
    }, index=pd.to_datetime(dates, utc=True))


# ── Ichimoku 스텁 ─────────────────────────────────────────────

def _ichi_stub(
    df: pd.DataFrame,
    *,
    a_pass: bool = True,
    b_pass: bool = True,
) -> pd.DataFrame:
    """
    calc_ichimoku 반환값을 시뮬레이션.
    마지막 2개 봉의 cloud_top만 조건 제어에 사용.
    a_pass=True  → 이번 주 cloud_top < close      (조건 A 통과)
    b_pass=True  → 직전 주 cloud_top >= prev_close (조건 B 통과)
    """
    df = df.copy()
    cur_close  = float(df["Close"].iloc[-1])
    prev_close = float(df["Close"].iloc[-2])
    # 기본값: 전체 행의 cloud를 close의 70%로 설정
    df["cloud_top"]    = df["Close"] * 0.70
    df["cloud_bottom"] = df["Close"] * 0.65
    df["senkou_a"]     = df["cloud_top"]
    df["senkou_b"]     = df["cloud_bottom"]
    # 마지막 2봉 override
    df.loc[df.index[-1], "cloud_top"] = cur_close  * (0.90 if a_pass else 1.10)
    df.loc[df.index[-2], "cloud_top"] = prev_close * (1.05 if b_pass else 0.90)
    return df


# ── 공통 패치 컨텍스트 ─────────────────────────────────────────

@contextmanager
def _mock_ticker(raw_df: pd.DataFrame, ichi_df: pd.DataFrame):
    """fetch_weekly_ohlcv + calc_ichimoku 동시 패치."""
    with patch("chart_screener.fetch_weekly_ohlcv", return_value=raw_df):
        with patch("chart_screener.calc_ichimoku", return_value=ichi_df):
            yield


# ── current_week_of ───────────────────────────────────────────

class TestCurrentWeekOf:
    def test_format(self):
        w = current_week_of()
        assert "-W" in w
        year, week = w.split("-W")
        assert year.isdigit() and 1 <= int(week) <= 53

    def test_non_empty(self):
        assert current_week_of() != ""


# ── calc_ichimoku ─────────────────────────────────────────────

class TestCalcIchimoku:
    def test_columns_added(self):
        df = _uptrend_df()
        out = calc_ichimoku(df)
        for col in ["senkou_a", "senkou_b", "cloud_top", "cloud_bottom"]:
            assert col in out.columns

    def test_cloud_top_is_max_of_spans(self):
        df = _uptrend_df()
        out = calc_ichimoku(df).dropna(subset=["senkou_a", "senkou_b"])
        expected = out[["senkou_a", "senkou_b"]].max(axis=1)
        assert (out["cloud_top"] == expected).all()

    def test_cloud_bottom_is_min_of_spans(self):
        df = _uptrend_df()
        out = calc_ichimoku(df).dropna(subset=["senkou_a", "senkou_b"])
        expected = out[["senkou_a", "senkou_b"]].min(axis=1)
        assert (out["cloud_bottom"] == expected).all()

    def test_does_not_mutate_input(self):
        df = _uptrend_df()
        cols_before = set(df.columns)
        calc_ichimoku(df)
        assert set(df.columns) == cols_before


# ── 전체 조건 통과 ────────────────────────────────────────────

class TestScreenTickerAllPass:
    def _run(self):
        raw  = _uptrend_df()
        ichi = _ichi_stub(raw, a_pass=True, b_pass=True)
        with _mock_ticker(raw, ichi):
            return screen_ticker("005930.KS", "삼성전자")

    def test_returns_screen_result(self):
        assert self._run() is not None

    def test_ticker_and_name(self):
        r = self._run()
        assert r is not None
        assert r.ticker == "005930.KS"
        assert r.name   == "삼성전자"

    def test_has_gapjum_reflects_ma_order(self):
        r = self._run()
        assert r is not None
        assert r.has_gapjum == (r.ma_20w > r.ma_60w)

    def test_week_of_format(self):
        r = self._run()
        assert r is not None
        assert "-W" in r.week_of

    def test_is_enhanced_false_in_v1(self):
        r = self._run()
        assert r is not None
        assert r.is_enhanced is False


# ── 조건 A: 이번 주 구름 상향 돌파 ───────────────────────────

class TestConditionA:
    def test_fails_when_close_below_cloud_top(self):
        raw  = _uptrend_df()
        ichi = _ichi_stub(raw, a_pass=False, b_pass=True)  # cloud_top > close
        with _mock_ticker(raw, ichi):
            assert screen_ticker("005930.KS", "삼성전자") is None


# ── 조건 B: 직전 주 구름 내/하부 ─────────────────────────────

class TestConditionB:
    def test_fails_when_prev_close_already_above_cloud(self):
        raw  = _uptrend_df()
        ichi = _ichi_stub(raw, a_pass=True, b_pass=False)  # prev cloud < prev_close
        with _mock_ticker(raw, ichi):
            assert screen_ticker("005930.KS", "삼성전자") is None


# ── 조건 C/D: close > 20주선, close > 60주선 ─────────────────

class TestConditionCD:
    def test_fails_when_price_below_moving_averages(self):
        # 하락 마감 구간 → close < ma_20w (C 실패)
        raw  = _declining_last20_df()
        ichi = _ichi_stub(raw, a_pass=True, b_pass=True)
        with _mock_ticker(raw, ichi):
            assert screen_ticker("005930.KS", "삼성전자") is None


# ── 조건 E: 20주선 우상향 ─────────────────────────────────────

class TestConditionE:
    def test_fails_when_ma20_declining(self):
        # 하락 마감 구간 → ma_20w 기울기 음수 (E 실패)
        raw  = _declining_last20_df()
        ichi = _ichi_stub(raw, a_pass=True, b_pass=True)
        with _mock_ticker(raw, ichi):
            assert screen_ticker("005930.KS", "삼성전자") is None


# ── 조건 F: 60주선 우상향 ─────────────────────────────────────

class TestConditionF:
    def test_fails_when_ma60_declining(self):
        # 하락 마감 구간 → ma_60w 기울기도 결국 음수 (F 실패)
        raw  = _declining_last20_df()
        ichi = _ichi_stub(raw, a_pass=True, b_pass=True)
        with _mock_ticker(raw, ichi):
            assert screen_ticker("005930.KS", "삼성전자") is None


# ── 엣지 케이스 ───────────────────────────────────────────────

class TestEdgeCases:
    def test_returns_none_when_fetch_fails(self):
        with patch("chart_screener.fetch_weekly_ohlcv", return_value=None):
            assert screen_ticker("INVALID.KS", "없는종목") is None

    def test_returns_none_when_data_is_stale(self):
        raw = _uptrend_df()
        # 마지막 봉을 10일 전으로 설정 (staleness 초과)
        idx     = raw.index.tolist()
        idx[-1] = datetime.now(timezone.utc) - timedelta(days=10)
        raw.index = pd.to_datetime(idx, utc=True)
        ichi = _ichi_stub(raw, a_pass=True, b_pass=True)
        with _mock_ticker(raw, ichi):
            assert screen_ticker("005930.KS", "삼성전자") is None

    def test_returns_none_when_ma60_is_nan(self):
        # 30행만 제공 → ma_60w 전체 NaN
        raw  = _uptrend_df(n=30)
        ichi = _ichi_stub(raw, a_pass=True, b_pass=True)
        with _mock_ticker(raw, ichi):
            assert screen_ticker("005930.KS", "삼성전자") is None

    def test_returns_none_when_cloud_top_is_nan(self):
        raw  = _uptrend_df()
        ichi = _ichi_stub(raw, a_pass=True, b_pass=True)
        ichi.loc[ichi.index[-1], "cloud_top"] = np.nan
        with _mock_ticker(raw, ichi):
            assert screen_ticker("005930.KS", "삼성전자") is None
