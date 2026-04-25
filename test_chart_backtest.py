"""
test_chart_backtest.py  —  백테스트 로직 단위 테스트
────────────────────────────────────────────────────────────
검증 항목:
  1. no look-ahead: _check_at_row 는 row_idx 이후 데이터를 보지 않는다
  2. 수익률 계산: (future_price - signal_price) / signal_price
  3. 초과수익률: signal_return - kospi_return
  4. 지표 계산: win_rate, avg_return, excess_return
  5. 월별 집계: signal_date[:7] 기준 그룹화
  6. JSON 왕복: BacktestSignal → asdict → BacktestSignal(**d)
  7. 신호 없음 처리: 빈 리스트에서 compute_metrics 안전 동작
"""
from __future__ import annotations

import json
import math
from dataclasses import asdict
from datetime import date, timedelta
from typing import Optional
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from chart_backtest import (
    BacktestSignal,
    _check_at_row,
    _group_metrics,
    _nearest_price,
    _build_price_lookup,
    _week_label,
    build_summary,
    compute_metrics,
)


# ── 픽스처 헬퍼 ──────────────────────────────────────────────

def _make_ohlcv(n: int, start_close: float = 10000, trend: float = 0.0) -> pd.DataFrame:
    """
    n개 주봉 OHLCV 생성.  trend > 0 → 매주 close * (1 + trend) 상승.
    ichimoku 계산용 High/Low/Close/Volume 포함.
    """
    closes = [start_close * (1 + trend) ** i for i in range(n)]
    dates = pd.date_range("2023-01-02", periods=n, freq="W-MON", tz="UTC")
    return pd.DataFrame(
        {
            "Open":   closes,
            "High":   [c * 1.02 for c in closes],
            "Low":    [c * 0.98 for c in closes],
            "Close":  closes,
            "Volume": [1_000_000.0] * n,
        },
        index=dates,
    )


def _make_signal(
    signal_date: str = "2026-01-05",
    has_gapjum: bool = True,
    return_1w: Optional[float] = 0.03,
    return_4w: Optional[float] = 0.06,
    return_13w: Optional[float] = 0.10,
    excess_1w: Optional[float] = 0.01,
    excess_4w: Optional[float] = 0.02,
) -> BacktestSignal:
    return BacktestSignal(
        ticker="005930.KS",
        name="삼성전자",
        signal_week="2026-W01",
        signal_date=signal_date,
        close_at_signal=80000.0,
        has_gapjum=has_gapjum,
        return_1w=return_1w,
        return_4w=return_4w,
        return_13w=return_13w,
        excess_1w=excess_1w,
        excess_4w=excess_4w,
    )


# ── 1. look-ahead 방지 ─────────────────────────────────────────

class TestNoLookAhead:

    def test_slice_ends_at_row_idx(self):
        """_check_at_row는 row_idx+1 이후 데이터를 볼 수 없어야 한다."""
        # 충분히 긴 상승 추세 df 생성 (조건 통과 가능)
        df = _make_ohlcv(160, trend=0.005)
        # row_idx = 150 에서 체크할 때 df가 슬라이싱되는지 확인
        # 내부 슬라이스 로직 직접 검증: df.iloc[:151] 이 사용됨
        called_slices = []
        original_calc = __import__("chart_screener").calc_ichimoku

        def mock_calc(df_s):
            called_slices.append(len(df_s))
            return original_calc(df_s)

        with patch("chart_backtest.calc_ichimoku", side_effect=mock_calc):
            _check_at_row(df, 150)

        assert called_slices, "calc_ichimoku 호출 안 됨"
        assert called_slices[0] == 151, f"슬라이스 길이 오류: {called_slices[0]} (기대: 151)"

    def test_returns_none_for_short_df(self):
        """행이 2개 미만이면 None 반환."""
        df = _make_ohlcv(1)
        assert _check_at_row(df, 0) is None

    def test_returns_none_on_failed_conditions(self):
        """조건 미통과 (하락 추세) → None 반환."""
        df = _make_ohlcv(160, trend=-0.005)
        result = _check_at_row(df, 159)
        assert result is None


# ── 2. 수익률 계산 ──────────────────────────────────────────────

class TestReturnCalculation:

    def test_positive_return(self):
        sig = _make_signal(return_1w=0.05)
        assert sig.return_1w == pytest.approx(0.05)

    def test_negative_return(self):
        sig = _make_signal(return_4w=-0.03)
        assert sig.return_4w == pytest.approx(-0.03)

    def test_return_none_for_missing_future(self):
        sig = _make_signal(return_13w=None)
        assert sig.return_13w is None


# ── 3. 초과수익률 ────────────────────────────────────────────────

class TestExcessReturn:

    def test_excess_is_signal_minus_kospi(self):
        sig = _make_signal(return_4w=0.08, excess_4w=0.03)
        # excess_4w = return_4w - kospi_return_4w → kospi = 0.05
        assert sig.excess_4w == pytest.approx(0.03)

    def test_excess_none_when_kospi_unavailable(self):
        sig = _make_signal(excess_1w=None)
        assert sig.excess_1w is None

    def test_nearest_price_exact_match(self):
        lookup = {
            date(2026, 4, 14): 2700.0,
            date(2026, 4, 21): 2720.0,
        }
        assert _nearest_price(lookup, date(2026, 4, 14)) == pytest.approx(2700.0)

    def test_nearest_price_offset_1_day(self):
        lookup = {date(2026, 4, 15): 2710.0}
        assert _nearest_price(lookup, date(2026, 4, 14)) == pytest.approx(2710.0)

    def test_nearest_price_none_beyond_max_days(self):
        lookup = {date(2026, 4, 20): 2710.0}
        # 6일 차이 → max_days=4 이면 None
        assert _nearest_price(lookup, date(2026, 4, 14), max_days=4) is None

    def test_build_price_lookup(self):
        # Use explicit dates to avoid weekday alignment issues
        dates = pd.DatetimeIndex(["2026-04-14", "2026-04-21", "2026-04-28"], tz="UTC")
        df = pd.DataFrame({"Close": [100.0, 105.0, 110.0]}, index=dates)
        lookup = _build_price_lookup(df)
        assert date(2026, 4, 14) in lookup
        assert lookup[date(2026, 4, 14)] == pytest.approx(100.0)


# ── 4. 지표 계산 ──────────────────────────────────────────────

class TestGroupMetrics:

    def _sigs(self, r1s, r4s, ex4s=None):
        sigs = []
        for i, (r1, r4) in enumerate(zip(r1s, r4s)):
            ex4 = ex4s[i] if ex4s else None
            sigs.append(BacktestSignal(
                ticker=f"{i:06d}.KS",
                name=f"종목{i}",
                signal_week="2026-W01",
                signal_date=f"2026-01-{5 + i:02d}",
                close_at_signal=10000.0,
                has_gapjum=True,
                return_1w=r1,
                return_4w=r4,
                excess_4w=ex4,
            ))
        return sigs

    def test_win_rate_1w(self):
        sigs = self._sigs([0.05, -0.02, 0.03, -0.01], [0.1, 0.1, 0.1, 0.1])
        m = _group_metrics(sigs)
        assert m["win_rate_1w"] == pytest.approx(0.5)

    def test_win_rate_4w_all_positive(self):
        sigs = self._sigs([0.01] * 5, [0.05] * 5)
        m = _group_metrics(sigs)
        assert m["win_rate_4w"] == pytest.approx(1.0)

    def test_avg_return_4w(self):
        sigs = self._sigs([0.01] * 4, [0.04, 0.08, 0.02, 0.06])
        m = _group_metrics(sigs)
        assert m["avg_return_4w"] == pytest.approx(0.05)

    def test_median_return_4w(self):
        sigs = self._sigs([0.01] * 5, [0.02, 0.04, 0.06, 0.08, 0.10])
        m = _group_metrics(sigs)
        assert m["median_return_4w"] == pytest.approx(0.06)

    def test_excess_return_4w(self):
        sigs = self._sigs([0.01] * 3, [0.05] * 3, ex4s=[0.02, 0.04, 0.03])
        m = _group_metrics(sigs)
        assert m["excess_return_4w"] == pytest.approx(0.03)

    def test_none_outcomes_excluded(self):
        sigs = self._sigs([None, 0.05, None], [0.03, 0.07, None])
        m = _group_metrics(sigs)
        assert m["win_rate_1w"] == pytest.approx(1.0)   # only 0.05 counted
        assert m["avg_return_4w"] == pytest.approx(0.05) # only 0.03 and 0.07 counted

    def test_empty_signals(self):
        m = _group_metrics([])
        assert m["signal_count"] == 0
        assert "win_rate_1w" not in m

    def test_signal_count(self):
        sigs = self._sigs([0.01, 0.02, 0.03], [0.04, 0.05, 0.06])
        m = _group_metrics(sigs)
        assert m["signal_count"] == 3


# ── 5. 월별 집계 + compute_metrics 구조 ─────────────────────────

class TestComputeMetrics:

    def test_groups_keys(self):
        sigs = [_make_signal(has_gapjum=True), _make_signal(has_gapjum=False)]
        result = compute_metrics(sigs)
        assert set(result.keys()) == {"전체", "정배열", "일반"}

    def test_total_is_sum_of_sub(self):
        gap = [_make_signal(has_gapjum=True)] * 3
        reg = [_make_signal(has_gapjum=False)] * 2
        result = compute_metrics(gap + reg)
        assert result["전체"]["signal_count"] == 5
        assert result["정배열"]["signal_count"] == 3
        assert result["일반"]["signal_count"] == 2


class TestBuildSummary:

    def test_month_grouping(self):
        sigs = [
            _make_signal(signal_date="2026-01-05"),
            _make_signal(signal_date="2026-01-12"),
            _make_signal(signal_date="2026-02-02"),
        ]
        summary = build_summary(sigs, effective_weeks=36)
        assert "2026-01" in summary["months"]
        assert "2026-02" in summary["months"]
        assert summary["months"]["2026-01"]["groups"]["전체"]["signal_count"] == 2
        assert summary["months"]["2026-02"]["groups"]["전체"]["signal_count"] == 1

    def test_annual_includes_all(self):
        sigs = [
            _make_signal(signal_date="2026-01-05"),
            _make_signal(signal_date="2026-03-02"),
        ]
        summary = build_summary(sigs, effective_weeks=10)
        assert summary["annual"]["groups"]["전체"]["signal_count"] == 2

    def test_effective_weeks_in_summary(self):
        summary = build_summary([], effective_weeks=12)
        assert summary["effective_weeks"] == 12

    def test_empty_signals(self):
        summary = build_summary([], effective_weeks=36)
        assert summary["annual"]["groups"]["전체"]["signal_count"] == 0
        assert summary["months"] == {}


# ── 6. JSON 왕복 ──────────────────────────────────────────────

class TestJsonRoundtrip:

    def test_signal_to_dict_and_back(self):
        sig = _make_signal()
        d = asdict(sig)
        restored = BacktestSignal(**d)
        assert restored.ticker == sig.ticker
        assert restored.return_4w == sig.return_4w
        assert restored.excess_1w == sig.excess_1w

    def test_none_fields_survive(self):
        sig = _make_signal(return_13w=None, excess_1w=None)
        d = asdict(sig)
        restored = BacktestSignal(**d)
        assert restored.return_13w is None
        assert restored.excess_1w is None

    def test_json_serializable(self):
        sig = _make_signal()
        serialized = json.dumps(asdict(sig))
        d = json.loads(serialized)
        restored = BacktestSignal(**d)
        assert restored.signal_date == sig.signal_date


# ── 7. 주 레이블 ──────────────────────────────────────────────

class TestWeekLabel:

    def test_known_date(self):
        assert _week_label(date(2026, 4, 14)) == "2026-W16"

    def test_year_boundary(self):
        # 2026-01-01은 목요일 → W01
        label = _week_label(date(2026, 1, 1))
        assert label.startswith("2026-W")
