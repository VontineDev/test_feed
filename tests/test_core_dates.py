"""core/dates.py — last_trading_day 주말 보정 테스트.

기존 test_krx_flow_sync.py / test_chart_screener.py의 동명 테스트는
각 모듈의 별칭(_last_trading_day)이 살아있는지 회귀 확인용으로 유지되고,
여기는 canonical 구현을 직접 검증한다.
"""

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


class TestAliasesStillResolve:
    """이관 후에도 기존 모듈 별칭이 canonical과 동일 객체인지."""

    def test_krx_flow_sync_alias(self):
        from data.krx_flow_sync import _last_trading_day
        assert _last_trading_day is last_trading_day

    def test_chart_screener_alias(self):
        from analysis.chart_screener import _last_trading_day
        assert _last_trading_day is last_trading_day

    def test_kiwoom_aftermarket_alias(self):
        from data.kiwoom_aftermarket_sync import _prev_business_day
        assert _prev_business_day is last_trading_day
