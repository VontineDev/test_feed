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


# ── 조건 G: 120주선 위 (데이터 부족 시 NaN-pass) ─────────────

class TestConditionG:
    def test_G_passes_when_close_above_120wma(self):
        """110봉 우상향: close > ma_120w → G 통과, ScreenResult 반환."""
        raw  = _uptrend_df(n=110)
        ichi = _ichi_stub(raw, a_pass=True, b_pass=True)
        with _mock_ticker(raw, ichi):
            result = screen_ticker("005930.KS", "삼성전자")
        assert result is not None
        # ma_120w should be populated (110 >= 100 min_periods)
        assert result.ma_120w is not None

    def test_G_fails_when_close_below_120wma(self):
        """close를 ma_120w 아래로 강제 설정 → G 실패 → None 반환."""
        raw  = _uptrend_df(n=110)
        # rolling(120, min_periods=100) on 110 bars = mean of all 110 closes
        ma_120w_approx = float(raw["Close"].mean())
        # Force last close well below the 120w average
        raw.iloc[-1, raw.columns.get_loc("Close")] = ma_120w_approx - 5_000.0
        ichi = _ichi_stub(raw, a_pass=True, b_pass=True)
        with _mock_ticker(raw, ichi):
            assert screen_ticker("005930.KS", "삼성전자") is None

    def test_G_nanpass_when_insufficient_data(self):
        """80봉 (< 100 min_periods): ma_120w = NaN → G 자동 통과."""
        raw  = _uptrend_df(n=80)   # 80 < 100 min_periods → ma_120w is NaN
        ichi = _ichi_stub(raw, a_pass=True, b_pass=True)
        with _mock_ticker(raw, ichi):
            result = screen_ticker("005930.KS", "삼성전자")
        assert result is not None         # G는 NaN-pass → 통과
        assert result.ma_120w is None     # 데이터 부족 시 None


# ── KIND 섹터 매핑 ─────────────────────────────────────────────

class TestFetchKindSectorMap:
    def test_happy_path_builds_dict(self):
        """유효한 EUC-KR HTML 반환 시 종목코드 → 업종 dict 반환."""
        from unittest.mock import MagicMock
        from chart_screener import fetch_kind_sector_map

        # 미니멀 EUC-KR HTML: 종목코드/업종 컬럼 포함
        html = """<html><body><table>
        <tr><th>종목코드</th><th>회사명</th><th>업종</th></tr>
        <tr><td>005930</td><td>삼성전자</td><td>전자부품</td></tr>
        <tr><td>035720</td><td>카카오</td><td>소프트웨어</td></tr>
        </table></body></html>""".encode("euc-kr")

        mock_resp = MagicMock()
        mock_resp.content = html

        with patch("httpx.get", return_value=mock_resp):
            result = fetch_kind_sector_map()

        assert "005930" in result
        assert result["005930"] == "전자부품"
        assert "035720" in result

    def test_kind_down_returns_empty_dict(self):
        """httpx.get 예외 발생 시 빈 dict 반환 (서비스 중단 대응)."""
        from chart_screener import fetch_kind_sector_map

        with patch("httpx.get", side_effect=Exception("connection refused")):
            result = fetch_kind_sector_map()

        assert result == {}

    def test_empty_html_returns_empty_dict(self):
        """tr 없는 HTML 반환 시 빈 dict 반환 (파싱 실패 대응)."""
        from unittest.mock import MagicMock
        from chart_screener import fetch_kind_sector_map

        mock_resp = MagicMock()
        mock_resp.content = b"<html></html>"

        with patch("httpx.get", return_value=mock_resp):
            result = fetch_kind_sector_map()

        assert result == {}


# ── save_chart_signals 컬럼 회귀 테스트 ─────────────────────────

class TestSaveChartSignalsColumns:
    """save_chart_signals() INSERT must include high_w ($13) and volume_w ($14).

    Regression guard: adding columns to chart_signals requires updating the
    positional INSERT in save_chart_signals(). If the parameter count mismatches
    the column count, the weekly screener silently saves zero rows on Sunday.
    """

    @pytest.mark.asyncio
    async def test_insert_includes_high_w_and_volume_w(self):
        """Verify conn.execute receives 14 positional parameters ($1..$14)."""
        from unittest.mock import AsyncMock, MagicMock, patch
        from db import save_chart_signals
        from chart_screener import ScreenResult
        from datetime import datetime, timezone

        result = ScreenResult(
            ticker="005930.KS",
            name="삼성전자",
            close=75000.0,
            ma_20w=70000.0,
            ma_60w=65000.0,
            cloud_top=74000.0,
            is_enhanced=False,
            has_gapjum=True,
            screened_at=datetime.now(timezone.utc).isoformat(),
            week_of="2026-W17",
            high_w=77000.0,
            volume_w=1_500_000,
        )

        # Capture the positional args passed to conn.execute
        execute_calls: list = []

        async def _fake_execute(sql: str, *args):
            execute_calls.append((sql, args))

        from contextlib import asynccontextmanager

        mock_conn = AsyncMock()
        mock_conn.execute = _fake_execute

        @asynccontextmanager
        async def _fake_acquire():
            yield mock_conn

        mock_pool = MagicMock()
        mock_pool.acquire = _fake_acquire

        await save_chart_signals(mock_pool, [result])

        assert execute_calls, "conn.execute was never called"
        _sql, _args = execute_calls[0]
        # Should have 14 positional args: ticker name close ma_20w ma_60w cloud_top
        # is_enhanced has_gapjum week_of screened_at sector ma_120w high_w volume_w
        assert len(_args) == 14, (
            f"Expected 14 params (including high_w, volume_w), got {len(_args)}: {_args}"
        )
        # high_w is $13 (index 12)
        assert _args[12] == 77000.0, f"high_w should be $13, got {_args[12]}"
        # volume_w is $14 (index 13)
        assert _args[13] == 1_500_000, f"volume_w should be $14, got {_args[13]}"

    @pytest.mark.asyncio
    async def test_screen_result_high_w_volume_w_default_none(self):
        """New fields default to None — existing ScreenResult() callers are backward-compatible."""
        from chart_screener import ScreenResult
        from datetime import datetime, timezone

        r = ScreenResult(
            ticker="000660.KS",
            name="SK하이닉스",
            close=180000.0,
            ma_20w=170000.0,
            ma_60w=160000.0,
            cloud_top=178000.0,
            is_enhanced=False,
            has_gapjum=False,
            screened_at=datetime.now(timezone.utc).isoformat(),
            week_of="2026-W17",
        )
        assert r.high_w is None
        assert r.volume_w is None
        assert r.foreign_net_buy is None
        assert r.inst_net_buy is None
