"""
test_resolution_diagnostics.py — unit tests for ticker resolution miss diagnostics
"""
from __future__ import annotations

import pytest


class TestResolutionMissReport:

    def setup_method(self):
        """Reset the global counter before each test."""
        import data.market_data as market_data
        market_data._resolution_misses.clear()

    def test_empty_counter_returns_zero_message(self):
        from data.market_data import get_resolution_miss_report
        assert get_resolution_miss_report() == "resolution misses: 0"

    def test_single_miss_appears_in_report(self):
        import data.market_data as market_data
        from data.market_data import get_resolution_miss_report
        market_data._resolution_misses["현대차"] += 1
        report = get_resolution_miss_report()
        assert "현대차" in report
        assert "1" in report

    def test_top_n_limits_output(self):
        import data.market_data as market_data
        from data.market_data import get_resolution_miss_report
        for i in range(15):
            market_data._resolution_misses[f"종목{i:02d}"] = i + 1
        report = get_resolution_miss_report(top_n=5)
        lines = [l for l in report.splitlines() if l.strip().startswith("'")]
        assert len(lines) == 5

    def test_most_common_first(self):
        import data.market_data as market_data
        from data.market_data import get_resolution_miss_report
        market_data._resolution_misses["흔한종목"] = 10
        market_data._resolution_misses["드문종목"] = 1
        report = get_resolution_miss_report()
        assert report.index("흔한종목") < report.index("드문종목")

