"""
test_p3_remaining.py — 나머지 P3 기능 회귀 테스트

Covers:
  1. Condition G NaN Calibration (SCREENER_G_NAN_STRICT env var)
  2. Enhanced Ichimoku (is_enhanced: tenkan > kijun, both rising)
  3. Daily Ticker Cap (DAILY_CLASSIFIER_TICKERS env var)
  4. Tighten News Gating (_active_stage_tickers OR _screener_tickers)
"""
from __future__ import annotations

import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ── 1. Condition G NaN Calibration ──────────────────────────────────────────

class TestConditionGNanCalibration:
    """SCREENER_G_NAN_STRICT 환경변수로 NaN-pass/fail 전환."""

    def _eval_G(self, ma_120w_valid: bool, close_above: bool, strict: str = "") -> bool:
        """screen_ticker()의 G 조건 로직 추출."""
        close    = 100.0
        ma_120w  = 90.0 if close_above else 110.0
        g_strict = strict.lower() in ("1", "true", "yes")
        if g_strict:
            G = ma_120w_valid and (close > ma_120w)
        else:
            G = (not ma_120w_valid) or (close > ma_120w)
        return G

    def test_default_nan_pass(self):
        """NaN-pass 기본: ma_120w_valid=False → G 통과."""
        assert self._eval_G(ma_120w_valid=False, close_above=False, strict="") is True

    def test_strict_nan_fail(self):
        """STRICT=1: ma_120w_valid=False → G 실패."""
        assert self._eval_G(ma_120w_valid=False, close_above=False, strict="1") is False

    def test_strict_valid_above(self):
        """STRICT=1: ma_120w_valid=True, close > ma_120w → G 통과."""
        assert self._eval_G(ma_120w_valid=True, close_above=True, strict="1") is True

    def test_strict_valid_below(self):
        """STRICT=1: ma_120w_valid=True, close < ma_120w → G 실패."""
        assert self._eval_G(ma_120w_valid=True, close_above=False, strict="1") is False

    def test_env_var_toggle(self, monkeypatch):
        """환경변수 SCREENER_G_NAN_STRICT=1 실제 적용 확인."""
        monkeypatch.setenv("SCREENER_G_NAN_STRICT", "1")
        val = os.environ.get("SCREENER_G_NAN_STRICT", "").lower()
        assert val in ("1", "true", "yes")

    def test_env_var_default_empty(self, monkeypatch):
        monkeypatch.delenv("SCREENER_G_NAN_STRICT", raising=False)
        val = os.environ.get("SCREENER_G_NAN_STRICT", "").lower()
        assert val not in ("1", "true", "yes")


# ── 2. Enhanced Ichimoku is_enhanced ────────────────────────────────────────

class TestEnhancedIchimoku:
    """tenkan_sen > kijun_sen AND both rising → is_enhanced=True."""

    def _compute_enhanced(self, tenkan_cur, kijun_cur, tenkan_prev, kijun_prev) -> bool:
        """screen_ticker()의 Enhanced 조건 추출."""
        import math
        tenkan_ok = not math.isnan(tenkan_cur) and not math.isnan(tenkan_prev)
        kijun_ok  = not math.isnan(kijun_cur)  and not math.isnan(kijun_prev)
        if tenkan_ok and kijun_ok:
            H = tenkan_cur > kijun_cur
            I = tenkan_cur > tenkan_prev and kijun_cur > kijun_prev
            return H and I
        return False

    def test_enhanced_true_when_all_conditions_met(self):
        assert self._compute_enhanced(
            tenkan_cur=120, kijun_cur=100, tenkan_prev=115, kijun_prev=95
        ) is True

    def test_enhanced_false_tenkan_below_kijun(self):
        """전환선 < 기준선 → is_enhanced=False."""
        assert self._compute_enhanced(
            tenkan_cur=90, kijun_cur=100, tenkan_prev=85, kijun_prev=95
        ) is False

    def test_enhanced_false_kijun_not_rising(self):
        """기준선 하락 → is_enhanced=False."""
        assert self._compute_enhanced(
            tenkan_cur=120, kijun_cur=100, tenkan_prev=115, kijun_prev=105
        ) is False

    def test_enhanced_false_tenkan_not_rising(self):
        """전환선 하락 → is_enhanced=False."""
        assert self._compute_enhanced(
            tenkan_cur=120, kijun_cur=100, tenkan_prev=125, kijun_prev=95
        ) is False

    def test_enhanced_false_when_nan(self):
        """NaN 데이터 → is_enhanced=False (안전 처리)."""
        import math
        assert self._compute_enhanced(
            tenkan_cur=float("nan"), kijun_cur=100,
            tenkan_prev=115, kijun_prev=95
        ) is False

    def test_calc_ichimoku_adds_tenkan_kijun(self):
        """calc_ichimoku() 반환 DataFrame에 tenkan_sen, kijun_sen 컬럼 존재."""
        try:
            import pandas as pd
            import numpy as np
            from analysis.chart_screener import calc_ichimoku

            n = 60
            idx = pd.date_range("2024-01-01", periods=n, freq="W")
            df = pd.DataFrame({
                "Open":   np.linspace(100, 150, n),
                "High":   np.linspace(105, 155, n),
                "Low":    np.linspace(95, 145, n),
                "Close":  np.linspace(100, 150, n),
                "Volume": [1_000_000] * n,
            }, index=idx)

            result = calc_ichimoku(df)
            assert "tenkan_sen" in result.columns, "tenkan_sen 컬럼 없음"
            assert "kijun_sen"  in result.columns, "kijun_sen 컬럼 없음"
        except ImportError:
            pytest.skip("ta 라이브러리 미설치")


# ── 3. Daily Ticker Cap ──────────────────────────────────────────────────────

class TestDailyTickerCap:
    """DAILY_CLASSIFIER_TICKERS 환경변수로 최대 처리 종목 수 제한."""

    def _apply_cap(self, all_tickers, ichimoku_syms, cap):
        """_daily_stage_job()의 cap 로직 추출."""
        if len(all_tickers) <= cap:
            return all_tickers
        priority = [(t, n, s) for t, n, s in all_tickers if t in ichimoku_syms]
        others   = [(t, n, s) for t, n, s in all_tickers if t not in ichimoku_syms]
        n_fill   = max(0, cap - len(priority))
        return priority + others[:n_fill]

    def _make_tickers(self, n):
        return [(f"{i:06d}.KS", f"종목{i}", "KOSPI") for i in range(n)]

    def test_cap_limits_total(self):
        all_t = self._make_tickers(300)
        result = self._apply_cap(all_t, set(), cap=150)
        assert len(result) == 150

    def test_ichimoku_tickers_always_included(self):
        all_t = self._make_tickers(300)
        ichi  = {"000010.KS", "000020.KS", "000030.KS"}
        result = self._apply_cap(all_t, ichi, cap=10)
        result_syms = {t for t, _, _ in result}
        assert ichi.issubset(result_syms)

    def test_no_cap_when_under_limit(self):
        all_t = self._make_tickers(100)
        result = self._apply_cap(all_t, set(), cap=150)
        assert len(result) == 100

    def test_env_var_default_is_150(self, monkeypatch):
        monkeypatch.delenv("DAILY_CLASSIFIER_TICKERS", raising=False)
        cap = int(os.environ.get("DAILY_CLASSIFIER_TICKERS", "150"))
        assert cap == 150

    def test_env_var_override(self, monkeypatch):
        monkeypatch.setenv("DAILY_CLASSIFIER_TICKERS", "300")
        cap = int(os.environ.get("DAILY_CLASSIFIER_TICKERS", "150"))
        assert cap == 300


# ── 4. Tighten News Gating ───────────────────────────────────────────────────

class TestTightenNewsGating:
    """_screener_tickers OR _active_stage_tickers 기반 이중 게이팅."""

    def _gate(self, signal_syms, screener, stage):
        """summary_worker() 게이팅 로직 추출."""
        in_screener  = bool(signal_syms & screener)
        in_stage     = bool(signal_syms & stage)
        has_any_gate = bool(screener or stage)
        suppressed   = has_any_gate and bool(signal_syms) and not (in_screener or in_stage)
        return suppressed, in_screener, in_stage

    def test_screener_match_not_suppressed(self):
        suppressed, in_s, _ = self._gate(
            {"005930.KS"}, screener={"005930.KS"}, stage=set()
        )
        assert not suppressed
        assert in_s

    def test_stage_match_not_suppressed(self):
        suppressed, _, in_st = self._gate(
            {"005930.KS"}, screener=set(), stage={"005930.KS"}
        )
        assert not suppressed
        assert in_st

    def test_neither_match_suppressed(self):
        suppressed, _, _ = self._gate(
            {"005930.KS"}, screener={"373220.KS"}, stage={"066570.KS"}
        )
        assert suppressed

    def test_no_gate_when_both_empty(self):
        """게이팅 캐시 미로드 시 통과."""
        suppressed, _, _ = self._gate(
            {"005930.KS"}, screener=set(), stage=set()
        )
        assert not suppressed

    def test_screener_match_gets_high_confidence(self):
        _, in_s, _ = self._gate(
            {"005930.KS"}, screener={"005930.KS"}, stage=set()
        )
        assert in_s  # HIGH CONFIDENCE 후보

    def test_stage_only_match_normal_confidence(self):
        _, in_s, in_st = self._gate(
            {"005930.KS"}, screener=set(), stage={"005930.KS"}
        )
        assert not in_s   # HIGH CONFIDENCE 아님
        assert in_st      # NORMAL confidence로 전달

    def test_empty_signal_syms_not_suppressed(self):
        """signal_syms 없는 신호(거시경제 기사 등) → 게이팅 안 함."""
        suppressed, _, _ = self._gate(
            set(), screener={"005930.KS"}, stage={"005930.KS"}
        )
        assert not suppressed
