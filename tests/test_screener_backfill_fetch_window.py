"""jobs/screener_backfill.py — OHLCV fetch 구간 산정 회귀 테스트.

2026-08-17 발견: `_fetch_ohlcv()`가 예전엔 `period="3y"`(오늘 기준 최근
3년 고정)를 써서, 백필 대상이 과거 주차(예: 2022년)면 그 주봉 자체가 3년
롤링 윈도우 밖이라 매번 빈 슬라이스 → "통과 종목 0개"로 조용히 실패했다
(에러 없음 — chart_signals 2022~2024 백필이 로그상 "성공"으로 찍혔지만
실제로는 최근 3년 안의 주차만 유효했던 사고). explicit start/end로 대상
주차 범위 전체 + ma_120w 워밍업을 커버하도록 수정 — 이 테스트는 그 날짜
계산 로직(compute_fetch_window)과 _fetch_ohlcv()가 실제로 explicit
start/end를 쓰는지를 고정한다.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from jobs.screener_backfill import (  # noqa: E402
    _FETCH_LOOKBACK_WEEKS,
    _fetch_ohlcv,
    compute_fetch_window,
    week_of_to_monday,
)


def test_compute_fetch_window_covers_earliest_week_minus_lookback():
    """가장 이른 대상 주차 - lookback주(ma_120w 워밍업)부터 시작해야 한다."""
    fetch_start, fetch_end = compute_fetch_window(["2022-W01", "2022-W05", "2024-W39"])

    expected_start = week_of_to_monday("2022-W01") - timedelta(weeks=_FETCH_LOOKBACK_WEEKS)
    assert fetch_start == expected_start
    assert fetch_end > week_of_to_monday("2024-W39")


def test_compute_fetch_window_single_week():
    """단일 주차만 대상이어도 lookback을 포함한 구간이 산정된다."""
    fetch_start, fetch_end = compute_fetch_window(["2026-W19"])

    monday = week_of_to_monday("2026-W19")
    assert fetch_start < monday
    assert fetch_end >= monday
    assert (monday - fetch_start).days >= _FETCH_LOOKBACK_WEEKS * 7


def test_fetch_ohlcv_uses_explicit_start_end_not_rolling_period():
    """_fetch_ohlcv()는 yf.Ticker().history()를 period="3y"가 아니라
    explicit start/end로 호출해야 한다 — 과거 주차 백필이 조용히 실패하던
    원인 회귀 방지."""
    fetch_start = date(2022, 1, 1)
    fetch_end = date(2022, 6, 1)

    mock_df = pd.DataFrame({"Open": [1.0], "High": [1.0], "Low": [1.0], "Close": [1.0], "Volume": [100]})
    mock_history = MagicMock(return_value=mock_df)
    mock_ticker = MagicMock()
    mock_ticker.history = mock_history

    with (
        patch("jobs.screener_backfill.yf.Ticker", return_value=mock_ticker),
        patch("jobs.screener_backfill.normalize_ohlcv", side_effect=lambda df: df),
    ):
        _fetch_ohlcv("005930.KS", fetch_start, fetch_end)

    mock_history.assert_called_once_with(
        start="2022-01-01", end="2022-06-01", interval="1wk", auto_adjust=True,
    )
    # 예전 버그의 시그니처인 고정 period 인자가 더는 전달되지 않아야 한다.
    assert "period" not in mock_history.call_args.kwargs
