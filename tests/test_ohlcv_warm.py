"""
test_ohlcv_warm.py  —  jobs/ohlcv_warm.py 단위 테스트
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch, call

import pytest


# ── _weekdays ────────────────────────────────────────────────

def test_weekdays_excludes_weekends():
    from jobs.ohlcv_warm import _weekdays

    start = date(2025, 1, 1)  # 수
    end = date(2025, 1, 7)    # 화
    days = list(_weekdays(start, end))
    for d in days:
        assert d.weekday() < 5, f"{d} is weekend"
    # 1/1(수)~1/7(화): 5 weekdays (1,2,3,6,7)
    assert len(days) == 5


def test_weekdays_single_weekend_day():
    from jobs.ohlcv_warm import _weekdays

    # 2025-01-04 토 ~ 2025-01-05 일
    days = list(_weekdays(date(2025, 1, 4), date(2025, 1, 5)))
    assert days == []


# ── backfill_ohlcv ───────────────────────────────────────────

def test_backfill_skips_filled_dates():
    """이미 채워진 날짜는 fill_daily_from_krx 호출 안 함"""
    from jobs.ohlcv_warm import backfill_ohlcv

    start = date(2025, 1, 2)
    end = date(2025, 1, 3)
    # 2025-01-02(목), 2025-01-03(금)

    filled = {date(2025, 1, 2)}  # 2일은 이미 채워짐

    with patch("jobs.ohlcv_warm._get_filled_dates", return_value=filled), \
         patch("jobs.ohlcv_warm.time.sleep"), \
         patch("core.ohlcv_cache.fill_daily_from_krx", return_value=100) as mock_fill:

        total = backfill_ohlcv("postgresql://test", start, end, delay_s=0)

    # 3일(금)만 호출
    mock_fill.assert_called_once_with("postgresql://test", date(2025, 1, 3))
    assert total == 100


def test_backfill_all_filled_returns_zero():
    """모든 날짜가 이미 채워진 경우 API 미호출"""
    from jobs.ohlcv_warm import backfill_ohlcv

    start = date(2025, 1, 2)
    end = date(2025, 1, 3)
    filled = {date(2025, 1, 2), date(2025, 1, 3)}

    with patch("jobs.ohlcv_warm._get_filled_dates", return_value=filled), \
         patch("core.ohlcv_cache.fill_daily_from_krx") as mock_fill:

        total = backfill_ohlcv("postgresql://test", start, end)

    mock_fill.assert_not_called()
    assert total == 0


def test_backfill_holiday_returns_zero_and_continues():
    """휴장일은 fill이 0 반환 — 다음 날짜로 계속"""
    from jobs.ohlcv_warm import backfill_ohlcv

    start = date(2025, 1, 2)
    end = date(2025, 1, 3)

    # 1/2은 휴장(0), 1/3은 정상(2700)
    side_effects = [0, 2700]

    with patch("jobs.ohlcv_warm._get_filled_dates", return_value=set()), \
         patch("jobs.ohlcv_warm.time.sleep"), \
         patch("core.ohlcv_cache.fill_daily_from_krx", side_effect=side_effects):

        total = backfill_ohlcv("postgresql://test", start, end, delay_s=0)

    assert total == 2700


def test_backfill_delay_called():
    """delay_s > 0 이면 time.sleep 호출됨"""
    from jobs.ohlcv_warm import backfill_ohlcv

    start = date(2025, 1, 2)
    end = date(2025, 1, 2)

    with patch("jobs.ohlcv_warm._get_filled_dates", return_value=set()), \
         patch("jobs.ohlcv_warm.time.sleep") as mock_sleep, \
         patch("core.ohlcv_cache.fill_daily_from_krx", return_value=100):

        backfill_ohlcv("postgresql://test", start, end, delay_s=0.1)

    mock_sleep.assert_called_once_with(0.1)


# ── daily_ohlcv_warm_job ─────────────────────────────────────

def test_daily_warm_job_calls_fill_yesterday():
    from jobs.ohlcv_warm import daily_ohlcv_warm_job
    from datetime import timedelta

    yesterday = date.today() - timedelta(days=1)

    if yesterday.weekday() >= 5:
        pytest.skip("전일이 주말 — 스킵 동작 테스트는 별도")

    with patch("core.ohlcv_cache.fill_daily_from_krx", return_value=2700) as mock_fill:
        n = daily_ohlcv_warm_job("postgresql://test")

    mock_fill.assert_called_once_with("postgresql://test", yesterday)
    assert n == 2700


def test_daily_warm_job_skips_weekend(monkeypatch):
    from jobs.ohlcv_warm import daily_ohlcv_warm_job
    import jobs.ohlcv_warm as _mod

    # 전일을 토요일로 고정
    monkeypatch.setattr(
        _mod, "date",
        type("FakeDate", (), {
            "today": staticmethod(lambda: date(2025, 1, 6)),  # 월요일 → 전일=일요일
            "fromisoformat": date.fromisoformat,
        })
    )

    with patch("core.ohlcv_cache.fill_daily_from_krx") as mock_fill:
        n = daily_ohlcv_warm_job("postgresql://test")

    mock_fill.assert_not_called()
    assert n == 0
