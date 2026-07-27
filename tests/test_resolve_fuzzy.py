"""
test_resolve_fuzzy.py — resolve_fuzzy() 및 signal_detector 심볼 해석 회귀 테스트

Covers:
  - TickerCache.resolve_fuzzy(): 근사 매칭 성공/실패, threshold, 캐시 미로드
  - _parse_signal_json(): 심볼 없는 티커에 대해 exact → fuzzy → miss 순 해석
  - _resolution_misses: 미해석 티커 카운터 증가 확인
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch


from core.ticker_cache import TickerCache


# ── TickerCache.resolve_fuzzy ────────────────────────────────────────────────

class TestResolveFuzzy:
    def _make_cache(self, entries: dict[str, str]) -> TickerCache:
        """name→symbol 매핑이 있는 캐시 반환."""
        cache = TickerCache()
        cache._by_name = entries
        cache._by_short = {}
        cache._by_sym = {}
        cache._loaded = True
        return cache

    def test_exact_name_resolves(self):
        cache = self._make_cache({"삼성전자": "005930.KS"})
        assert cache.resolve_fuzzy("삼성전자") == "005930.KS"

    def test_near_match_above_threshold(self):
        # "셀트리온헬스케어" vs "셀트리온헬스케어(주)" — LLM이 괄호를 생략하는 경우
        cache = self._make_cache({"셀트리온헬스케어(주)": "091990.KS"})
        result = cache.resolve_fuzzy("셀트리온헬스케어", threshold=0.82)
        assert result == "091990.KS"

    def test_false_positive_blocked_by_threshold(self):
        # 현대차(6) vs 현대차증권(8) → ratio ≈ 0.75 < 0.82
        cache = self._make_cache({"현대차증권": "001500.KS"})
        result = cache.resolve_fuzzy("현대차", threshold=0.82)
        assert result is None

    def test_returns_none_when_cache_empty(self):
        cache = TickerCache()  # not loaded
        assert cache.resolve_fuzzy("삼성전자") is None

    def test_returns_none_for_empty_name(self):
        cache = self._make_cache({"삼성전자": "005930.KS"})
        assert cache.resolve_fuzzy("") is None

    def test_custom_threshold_lower_matches_more(self):
        cache = self._make_cache({"현대차증권": "001500.KS"})
        # threshold=0.70 이면 ratio≈0.75 >= 0.70 → 매칭
        result = cache.resolve_fuzzy("현대차", threshold=0.70)
        assert result == "001500.KS"

    def test_best_match_returned_among_multiple(self):
        cache = self._make_cache({
            "LG에너지솔루션": "373220.KS",
            "LG화학": "051910.KS",
            "LG전자": "066570.KS",
        })
        # "LG에너지솔" → "LG에너지솔루션"이 가장 유사
        result = cache.resolve_fuzzy("LG에너지솔", threshold=0.70)
        assert result == "373220.KS"


# ── _parse_signal_json: 심볼 해석 통합 ──────────────────────────────────────

class TestParseSignalJsonSymbolResolution:
    """심볼 없는 티커에 대해 ticker_cache 해석이 동작하는지 확인."""

    def _make_raw(self, tickers) -> str:
        return json.dumps({
            "direction": "BUY",
            "strength": 3,
            "reason": "테스트",
            "tickers": tickers,
        })

    def test_name_without_symbol_resolved_via_exact_cache(self):
        from analysis.signal_detector import _parse_signal_json
        from reports.summarizer import Backend

        mock_cache = MagicMock()
        mock_cache.resolve.return_value = "005930.KS"
        mock_cache.resolve_fuzzy.return_value = None

        raw = self._make_raw([{"name": "삼성전자", "symbol": ""}])
        with patch("analysis.signal_detector._tc", mock_cache):
            sig = _parse_signal_json(raw, Backend.OLLAMA)

        assert sig.ticker_symbols.get("삼성전자") == "005930.KS"
        mock_cache.resolve.assert_called_once_with("삼성전자")

    def test_name_without_symbol_resolved_via_fuzzy_when_exact_fails(self):
        from analysis.signal_detector import _parse_signal_json
        from reports.summarizer import Backend

        mock_cache = MagicMock()
        mock_cache.resolve.return_value = None
        mock_cache.resolve_fuzzy.return_value = "091990.KS"

        raw = self._make_raw([{"name": "셀트리온헬스케어", "symbol": ""}])
        with patch("analysis.signal_detector._tc", mock_cache):
            sig = _parse_signal_json(raw, Backend.OLLAMA)

        assert sig.ticker_symbols.get("셀트리온헬스케어") == "091990.KS"

    def test_resolution_miss_recorded_when_both_fail(self):
        import data.market_data as market_data
        from analysis.signal_detector import _parse_signal_json
        from reports.summarizer import Backend

        market_data._resolution_misses.clear()
        mock_cache = MagicMock()
        mock_cache.resolve.return_value = None
        mock_cache.resolve_fuzzy.return_value = None

        raw = self._make_raw([{"name": "알수없는종목", "symbol": ""}])
        with patch("analysis.signal_detector._tc", mock_cache):
            sig = _parse_signal_json(raw, Backend.OLLAMA)

        assert market_data._resolution_misses["알수없는종목"] == 1
        assert "알수없는종목" not in sig.ticker_symbols

    def test_existing_symbol_not_overwritten(self):
        """LLM이 정확한 심볼을 제공한 경우 캐시를 건드리지 않음."""
        from analysis.signal_detector import _parse_signal_json
        from reports.summarizer import Backend

        mock_cache = MagicMock()
        raw = self._make_raw([{"name": "삼성전자", "symbol": "005930.KS"}])
        with patch("analysis.signal_detector._tc", mock_cache):
            sig = _parse_signal_json(raw, Backend.OLLAMA)

        assert sig.ticker_symbols["삼성전자"] == "005930.KS"
        mock_cache.resolve.assert_not_called()


# ── TradeSignal.confidence 기본값 ────────────────────────────────────────────

class TestTradeSignalConfidence:
    def test_default_confidence_is_normal(self):
        from analysis.signal_detector import TradeSignal
        from reports.summarizer import Backend
        sig = TradeSignal(
            direction="BUY", strength=3, reason="test",
            tickers=[], ticker_symbols={}, backend=Backend.OLLAMA, success=True,
        )
        assert sig.confidence == "NORMAL"

    def test_none_signal_confidence_is_normal(self):
        from analysis.signal_detector import NONE_SIGNAL
        assert NONE_SIGNAL.confidence == "NORMAL"
