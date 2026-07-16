"""core/dates.py — last_trading_day 주말 보정 테스트."""

from __future__ import annotations

from datetime import date, timedelta

from core.dates import last_trading_day


class TestLastTradingDay:
    def test_monday_returns_previous_friday(self, frozen_date):
        # 월요일 실행: '어제'=일요일 → 금요일로 보정
        assert last_trading_day(frozen_date["monday"]) == date(2026, 7, 10)

    def test_sunday_returns_friday(self, frozen_date):
        assert last_trading_day(frozen_date["sunday"]) == date(2026, 7, 17)

    def test_saturday_returns_friday(self, frozen_date):
        assert last_trading_day(frozen_date["saturday"]) == date(2026, 7, 17)

    def test_friday_returns_thursday(self, frozen_date):
        assert last_trading_day(frozen_date["friday"]) == date(2026, 7, 16)

    def test_result_is_always_weekday(self):
        d = date(2026, 1, 1)
        for offset in range(60):
            assert last_trading_day(d + timedelta(days=offset)).weekday() < 5

    def test_result_is_always_before_input(self):
        d = date(2026, 1, 1)
        for offset in range(60):
            today = d + timedelta(days=offset)
            assert last_trading_day(today) < today
