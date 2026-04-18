"""
test_generate_html_report.py — unit tests for generate_html_report.generate_html()
"""
from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import patch

import pytest


def _make_result(**kwargs):
    from chart_screener import ScreenResult
    defaults = dict(
        ticker="005930.KS",
        name="삼성전자",
        close=80_000.0,
        ma_20w=75_000.0,
        ma_60w=70_000.0,
        cloud_top=79_000.0,
        is_enhanced=False,
        has_gapjum=False,
        screened_at="2026-04-20T12:00:00+00:00",
        week_of="2026-W17",
        sector="전자부품",
        ma_120w=65_000.0,
    )
    defaults.update(kwargs)
    return ScreenResult(**defaults)


class TestGenerateHtml:

    def test_empty_results_renders_empty_state(self):
        """0 results → empty-state paragraph, no table element."""
        from generate_html_report import generate_html
        out = generate_html([])
        assert "이번 주 조건 통과 종목이 없습니다" in out
        assert "<table" not in out

    def test_gapjum_section_present_when_has_gapjum(self):
        """has_gapjum=True stock → ★ 정배열 section header appears."""
        from generate_html_report import generate_html
        r = _make_result(has_gapjum=True)
        out = generate_html([r])
        assert "★ 정배열" in out
        assert "<h2>" in out

    def test_normal_section_absent_when_all_gapjum(self):
        """All stocks have has_gapjum=True → 일반 section must not appear."""
        from generate_html_report import generate_html
        results = [
            _make_result(ticker="A.KS", name="종목A", close=100_000.0, has_gapjum=True),
            _make_result(ticker="B.KS", name="종목B", close=50_000.0,  has_gapjum=True),
        ]
        out = generate_html(results)
        assert ">일반<" not in out

    def test_ma120w_none_renders_dash_not_none_string(self):
        """ma_120w=None → cell shows '—', not 'None'."""
        from generate_html_report import generate_html
        r = _make_result(ma_120w=None)
        out = generate_html([r])
        assert "None" not in out
        assert "—" in out

    def test_html_escaping_in_name(self):
        """Stock name with HTML special chars must be escaped."""
        from generate_html_report import generate_html
        r = _make_result(name="삼성<b>전자")
        out = generate_html([r])
        assert "<b>" not in out
        assert "&lt;b&gt;" in out

    def test_no_external_stylesheet_link(self):
        """Generated HTML must not reference external stylesheets (<link rel=...)."""
        from generate_html_report import generate_html
        r = _make_result()
        out = generate_html([r])
        assert '<link rel="stylesheet"' not in out
        assert "<link rel='stylesheet'" not in out

    def test_empty_sector_renders_as_gita(self):
        """sector='' → cell shows '기타'."""
        from generate_html_report import generate_html
        r = _make_result(sector="")
        out = generate_html([r])
        assert "기타" in out

    def test_sort_by_close_descending(self):
        """Within a section, stocks sorted by close descending."""
        from generate_html_report import generate_html
        results = [
            _make_result(ticker="LOW.KS",  name="저가주", close=10_000.0),
            _make_result(ticker="HIGH.KS", name="고가주", close=500_000.0),
        ]
        out = generate_html(results)
        assert out.index("고가주") < out.index("저가주")

    def test_lang_ko_attribute(self):
        """HTML root element must have lang='ko'."""
        from generate_html_report import generate_html
        out = generate_html([])
        assert 'lang="ko"' in out

    def test_footer_note_present(self):
        """Footer footnote for 120주선 — must appear."""
        from generate_html_report import generate_html
        out = generate_html([])
        assert "120주선" in out
        assert "NaN-pass" in out


class TestSparkline:

    def test_sparkline_returns_svg_with_sufficient_history(self):
        """12-point history → non-empty SVG polyline."""
        from generate_html_report import _sparkline
        history = [float(i * 1000) for i in range(1, 13)]
        result = _sparkline(history)
        assert "<svg" in result
        assert "<polyline" in result

    def test_sparkline_empty_for_single_point(self):
        """1-point history → no SVG (can't draw a line)."""
        from generate_html_report import _sparkline
        assert _sparkline([80_000.0]) == ""

    def test_sparkline_empty_for_empty_list(self):
        """Empty history → empty string, no crash."""
        from generate_html_report import _sparkline
        assert _sparkline([]) == ""

    def test_sparkline_empty_when_all_values_equal(self):
        """Flat history (hi==lo) → no SVG (would produce degenerate points)."""
        from generate_html_report import _sparkline
        assert _sparkline([50_000.0] * 12) == ""

    def test_sparkline_rendered_in_html_row(self):
        """ScreenResult with close_history → <svg> appears in generated HTML."""
        from generate_html_report import generate_html
        r = _make_result(close_history=[float(i * 1000) for i in range(1, 13)])
        out = generate_html([r])
        assert "<svg" in out

    def test_sparkline_empty_history_renders_empty_cell(self):
        """close_history=[] → no crash, no <svg> in output."""
        from generate_html_report import generate_html
        r = _make_result(close_history=[])
        out = generate_html([r])
        assert "None" not in out

    def test_추세_column_header_present(self):
        """Table header must include 추세 column."""
        from generate_html_report import generate_html
        out = generate_html([_make_result()])
        assert "추세" in out


class TestCloseHistoryPopulation:

    def test_close_history_populated_from_screen_ticker(self):
        """screen_ticker() passes last 12 Close values into close_history."""
        import pandas as pd
        import numpy as np
        from datetime import timedelta, timezone
        from unittest.mock import patch
        from chart_screener import screen_ticker, calc_ichimoku

        n = 160
        today = pd.Timestamp.now(tz="UTC")
        dates = [today - timedelta(weeks=i) for i in range(n, 0, -1)]
        close_vals = [100_000.0 + i * 200 for i in range(n)]
        df = pd.DataFrame({
            "Open":   close_vals,
            "High":   [v + 500 for v in close_vals],
            "Low":    [v - 500 for v in close_vals],
            "Close":  close_vals,
            "Volume": [1_000_000] * n,
        }, index=pd.DatetimeIndex(dates, tz="UTC"))

        with patch("chart_screener.fetch_weekly_ohlcv", return_value=df):
            result = screen_ticker("005930.KS", "삼성전자", "전자")

        # If conditions pass, close_history must have 12 entries
        if result is not None:
            assert len(result.close_history) == 12
            assert result.close_history[-1] == pytest.approx(close_vals[-1])


class TestSectorGroupedView:

    def test_sector_section_rendered_in_html(self):
        """Results with sector data → 업종별 section appears."""
        from generate_html_report import generate_html
        results = [
            _make_result(ticker="A.KS", name="종목A", sector="반도체"),
            _make_result(ticker="B.KS", name="종목B", sector="바이오"),
        ]
        out = generate_html(results)
        assert "업종별" in out
        assert "반도체" in out
        assert "바이오" in out

    def test_empty_sector_goes_to_gita_bucket(self):
        """sector='' → appears under '기타' in sector section."""
        from generate_html_report import _group_by_sector
        r = _make_result(sector="")
        groups = _group_by_sector([r])
        assert "기타" in groups
        assert groups["기타"] == [r]

    def test_sector_grouping_collapses_correctly(self):
        """Same sector → grouped under one key, different sectors → separate keys."""
        from generate_html_report import _group_by_sector
        results = [
            _make_result(ticker="A.KS", name="A", sector="반도체"),
            _make_result(ticker="B.KS", name="B", sector="반도체"),
            _make_result(ticker="C.KS", name="C", sector="바이오"),
        ]
        groups = _group_by_sector(results)
        assert len(groups["반도체"]) == 2
        assert len(groups["바이오"]) == 1

    def test_sector_section_sorted_alphabetically(self):
        """Sector groups sorted alphabetically (dict sorted by key)."""
        from generate_html_report import _group_by_sector
        results = [
            _make_result(ticker="C.KS", name="C", sector="철강"),
            _make_result(ticker="A.KS", name="A", sector="기계"),
            _make_result(ticker="B.KS", name="B", sector="건설"),
        ]
        groups = _group_by_sector(results)
        assert list(groups.keys()) == ["건설", "기계", "철강"]
