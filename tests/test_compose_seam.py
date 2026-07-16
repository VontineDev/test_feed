"""
test_compose_seam.py — backtest_engine compose 모드 seam 단위 테스트

DB/네트워크 없는 순수 경로만: BacktestConfig 검증 + 진입가 산정 헬퍼.
전체 _run_compose 는 통합 스모크(별도)로 검증.
"""

from __future__ import annotations

from datetime import date

import pytest

from analysis.backtest.models import BacktestConfig
from analysis.backtest.helpers import _entry_on_or_after


class TestComposeConfigValidation:
    def test_compose_mode_accepted(self):
        cfg = BacktestConfig(
            mode="compose", strategy="AND-1",
            start=date(2026, 1, 1), end=date(2026, 2, 1), dsn="postgres://x",
        )
        assert cfg.mode == "compose"
        assert cfg.strategy == "AND-1"

    def test_compose_requires_strategy(self):
        with pytest.raises(ValueError, match="strategy"):
            BacktestConfig(
                mode="compose", strategy=None,
                start=date(2026, 1, 1), end=date(2026, 2, 1), dsn="postgres://x",
            )

    def test_compose_requires_dsn(self):
        with pytest.raises(ValueError, match="dsn"):
            BacktestConfig(
                mode="compose", strategy="AND-1",
                start=date(2026, 1, 1), end=date(2026, 2, 1), dsn=None,
            )

    def test_unknown_mode_still_rejected(self):
        with pytest.raises(ValueError, match="mode"):
            BacktestConfig(
                mode="bogus", start=date(2026, 1, 1), end=date(2026, 2, 1),
            )

    def test_non_compose_mode_ignores_strategy_requirement(self):
        # 기존 모드는 strategy/dsn 없이도 유효해야 함 (회귀 방지)
        cfg = BacktestConfig(mode="stage", start=date(2026, 1, 1), end=date(2026, 2, 1))
        assert cfg.strategy is None


class TestEntryOnOrAfter:
    def _lookup(self):
        # 금~월 (토일 휴장) 거래일만 존재
        return {
            date(2026, 4, 24): 100.0,  # 금
            date(2026, 4, 27): 102.0,  # 월
            date(2026, 4, 28): 103.0,  # 화
        }

    def test_exact_friday_hit(self):
        out = _entry_on_or_after(self._lookup(), date(2026, 4, 24))
        assert out == (date(2026, 4, 24), 100.0)

    def test_skips_weekend_to_monday(self):
        # 금요일에 바가 없으면(휴장) 다음 거래일 월요일로
        lookup = {date(2026, 4, 27): 102.0, date(2026, 4, 28): 103.0}
        out = _entry_on_or_after(lookup, date(2026, 4, 24))
        assert out == (date(2026, 4, 27), 102.0)

    def test_none_when_no_trading_day_within_window(self):
        lookup = {date(2026, 5, 10): 100.0}  # 16일 후 → 7일 윈도우 초과
        out = _entry_on_or_after(lookup, date(2026, 4, 24))
        assert out is None

    def test_returns_real_index_date_for_idx_map_match(self):
        # 반환 날짜는 lookup(=OHLCV 인덱스)에 실재해야 idx_map 매칭 보장
        out = _entry_on_or_after(self._lookup(), date(2026, 4, 25))  # 토요일 기준
        assert out is not None
        d, price = out
        assert d in self._lookup()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
