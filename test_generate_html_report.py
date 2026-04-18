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
