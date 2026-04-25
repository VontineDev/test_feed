"""
test_screener_filters.py — unit tests for screener_filters.py
"""
from __future__ import annotations

import pytest
from dataclasses import replace
from unittest.mock import patch

from chart_screener import ScreenResult
from screener_filters import (
    저평가_우량주,
    성장주,
    배당주,
    가격건전성,
    apply_filters,
    filter_summary,
    ALL_PRESETS,
)


# ── helpers ────────────────────────────────────────────────────

def _make(
    pbr=1.0,
    per=15.0,
    eps=500.0,
    dividend_yield=3.0,
    foreign_rate=25.0,
    volume_ratio_4w=1.2,
) -> ScreenResult:
    return ScreenResult(
        ticker="005930.KS",
        name="테스트종목",
        close=50000,
        ma_20w=48000,
        ma_60w=45000,
        cloud_top=47000,
        is_enhanced=False,
        has_gapjum=True,
        screened_at="2026-04-19T00:00:00+00:00",
        week_of="2026-W16",
        per=per,
        pbr=pbr,
        eps=eps,
        dividend_yield=dividend_yield,
        foreign_rate=foreign_rate,
        volume_ratio_4w=volume_ratio_4w,
    )


# ── 저평가 우량주 ───────────────────────────────────────────────

class TestJeopPyeongaFilter:

    def test_passes_when_all_conditions_met(self):
        r = _make(pbr=1.2, per=15.0, eps=500.0)
        assert 저평가_우량주.predicate(r) is True

    def test_fails_when_pbr_too_high(self):
        r = _make(pbr=2.0, per=15.0, eps=500.0)
        assert 저평가_우량주.predicate(r) is False

    def test_fails_when_per_too_high(self):
        r = _make(pbr=1.2, per=25.0, eps=500.0)
        assert 저평가_우량주.predicate(r) is False

    def test_fails_when_eps_zero(self):
        r = _make(pbr=1.2, per=15.0, eps=0.0)
        assert 저평가_우량주.predicate(r) is False

    def test_fails_when_eps_negative(self):
        r = _make(pbr=1.2, per=15.0, eps=-100.0)
        assert 저평가_우량주.predicate(r) is False

    def test_pbr_boundary_exactly_1_5_fails(self):
        # < 1.5, so 1.5 itself fails
        r = _make(pbr=1.5, per=15.0, eps=500.0)
        assert 저평가_우량주.predicate(r) is False

    def test_none_pbr_uses_fallback_99_and_fails(self):
        r = _make(pbr=1.2, per=15.0, eps=500.0)
        r = replace(r, pbr=None)
        assert 저평가_우량주.predicate(r) is False

    def test_none_per_uses_fallback_99_and_fails(self):
        r = _make(pbr=1.2, per=15.0, eps=500.0)
        r = replace(r, per=None)
        assert 저평가_우량주.predicate(r) is False

    def test_none_eps_uses_fallback_0_and_fails(self):
        r = _make(pbr=1.2, per=15.0, eps=500.0)
        r = replace(r, eps=None)
        assert 저평가_우량주.predicate(r) is False


# ── 성장주 ───────────────────────────────────────────────────────

class TestSeongJangFilter:

    def test_passes_when_all_conditions_met(self):
        r = _make(eps=500.0, per=25.0, volume_ratio_4w=1.5)
        assert 성장주.predicate(r) is True

    def test_fails_when_per_below_range(self):
        r = _make(eps=500.0, per=14.0, volume_ratio_4w=1.5)
        assert 성장주.predicate(r) is False

    def test_passes_at_per_boundary_15(self):
        r = _make(eps=500.0, per=15.0, volume_ratio_4w=1.5)
        assert 성장주.predicate(r) is True

    def test_passes_at_per_boundary_50(self):
        r = _make(eps=500.0, per=50.0, volume_ratio_4w=1.5)
        assert 성장주.predicate(r) is True

    def test_fails_when_per_above_range(self):
        r = _make(eps=500.0, per=51.0, volume_ratio_4w=1.5)
        assert 성장주.predicate(r) is False

    def test_fails_when_volume_below_threshold(self):
        r = _make(eps=500.0, per=25.0, volume_ratio_4w=0.9)
        assert 성장주.predicate(r) is False

    def test_passes_at_volume_boundary_1_0(self):
        r = _make(eps=500.0, per=25.0, volume_ratio_4w=1.0)
        assert 성장주.predicate(r) is True

    def test_fails_when_eps_negative(self):
        r = _make(eps=-100.0, per=25.0, volume_ratio_4w=1.5)
        assert 성장주.predicate(r) is False

    def test_none_volume_uses_fallback_0_and_fails(self):
        r = _make(eps=500.0, per=25.0, volume_ratio_4w=1.5)
        r = replace(r, volume_ratio_4w=None)
        assert 성장주.predicate(r) is False


# ── 배당주 ───────────────────────────────────────────────────────

class TestBaedangFilter:

    def test_passes_when_all_conditions_met(self):
        r = _make(dividend_yield=3.0, eps=500.0)
        assert 배당주.predicate(r) is True

    def test_fails_when_yield_below_threshold(self):
        r = _make(dividend_yield=2.4, eps=500.0)
        assert 배당주.predicate(r) is False

    def test_passes_at_boundary_2_5(self):
        r = _make(dividend_yield=2.5, eps=500.0)
        assert 배당주.predicate(r) is True

    def test_fails_when_eps_negative(self):
        r = _make(dividend_yield=3.0, eps=-100.0)
        assert 배당주.predicate(r) is False

    def test_fails_when_eps_zero(self):
        r = _make(dividend_yield=3.0, eps=0.0)
        assert 배당주.predicate(r) is False

    def test_none_dividend_yield_uses_fallback_0_and_fails(self):
        r = _make(dividend_yield=3.0, eps=500.0)
        r = replace(r, dividend_yield=None)
        assert 배당주.predicate(r) is False


# ── 가격건전성 ────────────────────────────────────────────────────

class TestGaGyeokGeonjeonsseong:

    def test_passes_at_boundary_0_8(self):
        r = _make(volume_ratio_4w=0.8)
        assert 가격건전성.predicate(r) is True

    def test_fails_below_threshold(self):
        r = _make(volume_ratio_4w=0.79)
        assert 가격건전성.predicate(r) is False

    def test_none_uses_fallback_0_and_fails(self):
        r = _make(volume_ratio_4w=1.0)
        r = replace(r, volume_ratio_4w=None)
        assert 가격건전성.predicate(r) is False


# ── apply_filters ────────────────────────────────────────────────

class TestApplyFilters:

    def _make_results(self):
        good = _make(pbr=1.0, per=15.0, eps=500.0, dividend_yield=3.0, volume_ratio_4w=1.2)
        no_value = _make(pbr=3.0, per=50.0, eps=0.0, dividend_yield=1.0, volume_ratio_4w=0.5)
        return [good, no_value]

    def test_empty_presets_returns_all(self):
        results = self._make_results()
        assert apply_filters(results, []) == results

    def test_intersection_returns_stocks_passing_all(self):
        results = self._make_results()
        out = apply_filters(results, [저평가_우량주, 가격건전성])
        assert len(out) == 1
        assert out[0].pbr == 1.0

    def test_union_returns_stocks_passing_any(self):
        good = _make(pbr=1.0, per=15.0, eps=500.0, dividend_yield=1.0, volume_ratio_4w=0.5)
        dividend_only = _make(pbr=3.0, per=50.0, eps=500.0, dividend_yield=3.0, volume_ratio_4w=0.5)
        nothing = _make(pbr=3.0, per=50.0, eps=0.0, dividend_yield=1.0, volume_ratio_4w=0.5)
        results = [good, dividend_only, nothing]
        out = apply_filters(results, [저평가_우량주, 배당주], combine="union")
        assert len(out) == 2

    def test_intersection_with_single_preset(self):
        results = self._make_results()
        out = apply_filters(results, [저평가_우량주])
        assert len(out) == 1


# ── filter_summary ───────────────────────────────────────────────

class TestFilterSummary:

    def test_returns_count_for_each_preset(self):
        good = _make(pbr=1.0, per=15.0, eps=500.0, dividend_yield=3.0, volume_ratio_4w=1.2)
        bad = _make(pbr=3.0, per=50.0, eps=0.0, dividend_yield=1.0, volume_ratio_4w=0.5)
        summary = filter_summary([good, bad])
        assert set(summary.keys()) == set(ALL_PRESETS.keys())
        assert summary["저평가_우량주"] == 1
        assert summary["배당주"] == 1

    def test_empty_results_returns_zero_counts(self):
        summary = filter_summary([])
        assert all(v == 0 for v in summary.values())

    def test_all_pass_returns_full_count(self):
        stock = _make(pbr=1.0, per=15.0, eps=500.0, dividend_yield=3.0, volume_ratio_4w=1.2)
        summary = filter_summary([stock, stock])
        assert summary["저평가_우량주"] == 2
        assert summary["성장주"] == 2
        assert summary["배당주"] == 2
        assert summary["가격건전성"] == 2
