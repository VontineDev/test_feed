"""
test_ohlcv_warm.py  —  jobs/ohlcv_warm.py 단위 테스트
"""
from __future__ import annotations

from datetime import date
from unittest.mock import patch


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


def test_backfill_min_rows_passed_to_filled_dates():
    """min_rows가 _get_filled_dates까지 관통 — 부분 적재일 재수집 대상 포함"""
    from jobs.ohlcv_warm import backfill_ohlcv

    start = date(2025, 1, 2)
    end = date(2025, 1, 3)

    with patch("jobs.ohlcv_warm._get_filled_dates", return_value=set()) as mock_filled, \
         patch("jobs.ohlcv_warm.time.sleep"), \
         patch("core.ohlcv_cache.fill_daily_from_krx", return_value=100):

        backfill_ohlcv("postgresql://test", start, end, delay_s=0, min_rows=1000)

    mock_filled.assert_called_once_with("postgresql://test", start, end, min_rows=1000)


# ── daily_ohlcv_warm_job ─────────────────────────────────────

def _freeze_today(monkeypatch, today: date):
    import jobs.ohlcv_warm as _mod
    monkeypatch.setattr(
        _mod, "date",
        type("FakeDate", (), {
            "today": staticmethod(lambda: today),
            "fromisoformat": date.fromisoformat,
        })
    )


def test_daily_warm_job_monday_covers_friday(monkeypatch):
    """월요일 실행 시 직전 금요일이 수집 대상에 포함 (구 '어제 스킵' 갭 해소)"""
    from jobs.ohlcv_warm import daily_ohlcv_warm_job

    _freeze_today(monkeypatch, date(2025, 1, 6))  # 월요일

    fetched: list[date] = []

    def fake_fill(*args):
        fetched.append(args[1])
        return 2700

    with patch("jobs.ohlcv_warm._get_filled_dates", return_value=set()), \
         patch("jobs.ohlcv_warm.time.sleep"), \
         patch("core.ohlcv_cache.fill_daily_from_krx", side_effect=fake_fill):

        n = daily_ohlcv_warm_job("postgresql://test")

    # 12/30(월)~1/5(일) 중 주중만: 12/30, 12/31, 1/1, 1/2, 1/3(금)
    assert date(2025, 1, 3) in fetched          # 금요일 포함
    assert all(d.weekday() < 5 for d in fetched)  # 주말 미포함
    assert n == 2700 * len(fetched)


def test_daily_warm_job_skips_already_filled(monkeypatch):
    """기채움 날짜는 재수집하지 않음 — 평시엔 전일 1건만 수집"""
    from jobs.ohlcv_warm import daily_ohlcv_warm_job
    from jobs.ohlcv_warm import _DAILY_MIN_ROWS

    _freeze_today(monkeypatch, date(2025, 1, 7))  # 화요일

    # 전일(1/6 월)만 미채움 — 나머지 주중은 전부 채워진 상태
    filled = {date(2024, 12, 31), date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 3)}

    with patch("jobs.ohlcv_warm._get_filled_dates", return_value=filled) as mock_filled, \
         patch("jobs.ohlcv_warm.time.sleep"), \
         patch("core.ohlcv_cache.fill_daily_from_krx", return_value=2700) as mock_fill:

        n = daily_ohlcv_warm_job("postgresql://test")

    mock_fill.assert_called_once_with("postgresql://test", date(2025, 1, 6))
    assert n == 2700
    # 부분 적재 재수집 임계값 적용 확인
    assert mock_filled.call_args.kwargs["min_rows"] == _DAILY_MIN_ROWS
