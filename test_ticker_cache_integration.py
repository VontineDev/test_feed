"""
Integration tests for ticker_cache integration in market_data.py and volume_pattern.py.

Verifies:
- market_data.get_price_context(): cache hit resolves symbol before YFINANCE_MAP
- market_data.get_price_context(): cache miss falls through to YFINANCE_MAP
- market_data.get_price_context(): three name variants (raw, key, key_nsp) are tried
- volume_pattern.resolve_ticker(): cache hit returns correct (symbol, raw, "KR") tuple
- volume_pattern.resolve_ticker(): cache miss falls through to static maps unchanged
"""
from __future__ import annotations

from dataclasses import fields
from unittest.mock import patch, MagicMock

import market_data
from market_data import PriceContext


def _make_price_context(symbol: str, ticker: str) -> PriceContext:
    return PriceContext(
        ticker=ticker, symbol=symbol, source="yfinance",
        current=70000.0, change_pct=1.5, rsi=55.0,
        volume_ratio=1.2, week52_high=80000.0, week52_low=60000.0,
        volume_surge=False, success=True,
    )


# ── market_data.get_price_context() cache integration ───────────────────────

class TestMarketDataCacheIntegration:
    def test_cache_hit_used_before_yfinance_map(self, monkeypatch):
        """Cache hit should be returned without falling through to YFINANCE_MAP."""
        monkeypatch.setattr(market_data.ticker_cache, "resolve", lambda name: "005930.KS")
        monkeypatch.setattr(market_data, "YFINANCE_OK", True)

        expected = _make_price_context("005930.KS", "삼성전자")
        with patch.object(market_data, "_fetch_yfinance", return_value=expected) as mock_fetch:
            result = market_data.get_price_context(["삼성전자"])

        assert len(result) == 1
        assert result[0].symbol == "005930.KS"
        mock_fetch.assert_called_once_with("005930.KS", "삼성전자")

    def test_cache_miss_falls_through_to_yfinance_map(self, monkeypatch):
        """Cache miss should fall through to YFINANCE_MAP for known tickers."""
        monkeypatch.setattr(market_data.ticker_cache, "resolve", lambda name: None)
        monkeypatch.setattr(market_data, "YFINANCE_OK", True)

        expected = _make_price_context("^KS11", "코스피")
        with patch.object(market_data, "_fetch_yfinance", return_value=expected) as mock_fetch:
            result = market_data.get_price_context(["코스피"])

        assert len(result) == 1
        assert result[0].symbol == "^KS11"
        # YFINANCE_MAP["코스피"] == "^KS11" — confirms fallthrough path
        mock_fetch.assert_called_once_with("^KS11", "코스피")

    def test_cache_tries_three_name_variants(self, monkeypatch):
        """resolve() must be called with raw, key (lowercased), and key_nsp (no spaces)."""
        calls = []
        def spy_resolve(name):
            calls.append(name)
            return None

        monkeypatch.setattr(market_data.ticker_cache, "resolve", spy_resolve)
        monkeypatch.setattr(market_data, "YFINANCE_OK", False)

        market_data.get_price_context(["SK 하이닉스"])

        # raw="SK 하이닉스", key="sk 하이닉스", key_nsp="sk하이닉스"
        assert "SK 하이닉스" in calls
        assert "sk 하이닉스" in calls
        assert "sk하이닉스" in calls


# ── volume_pattern.resolve_ticker() cache integration ───────────────────────

class TestVolumPatternCacheIntegration:
    def test_cache_hit_ks_returns_kr_tuple(self, monkeypatch):
        """Cache hit with .KS suffix → returns (symbol, raw, 'KR') tuple."""
        import volume_pattern

        monkeypatch.setattr(volume_pattern.ticker_cache, "resolve", lambda name: "005930.KS")

        result = volume_pattern.resolve_ticker("삼성전자")
        assert result == ("005930.KS", "삼성전자", "KR")

    def test_cache_hit_kq_returns_kr_tuple(self, monkeypatch):
        """Cache hit with .KQ suffix → returns (symbol, raw, 'KR') tuple."""
        import volume_pattern

        monkeypatch.setattr(volume_pattern.ticker_cache, "resolve", lambda name: "086520.KQ")

        result = volume_pattern.resolve_ticker("에코프로비엠")
        assert result == ("086520.KQ", "에코프로비엠", "KR")

    def test_cache_miss_falls_through_to_static_map(self, monkeypatch):
        """Cache miss → static KR_KOSDAQ/KR_KOSPI maps used as before."""
        import volume_pattern

        monkeypatch.setattr(volume_pattern.ticker_cache, "resolve", lambda name: None)

        # "삼성전자" should be in the static KR_KOSPI map
        result = volume_pattern.resolve_ticker("삼성전자")
        # Returns from static map, not cache
        assert result[0].endswith(".KS")
        assert result[2] == "KR"

    def test_cache_unexpected_suffix_falls_through(self, monkeypatch):
        """Cache hit with unexpected suffix → falls through to static maps."""
        import volume_pattern

        # Return a symbol with an unexpected suffix (not .KS or .KQ)
        monkeypatch.setattr(volume_pattern.ticker_cache, "resolve", lambda name: "005930.XX")

        # Should NOT return the cache result — falls through to static map or US
        result = volume_pattern.resolve_ticker("삼성전자")
        assert result[0] != "005930.XX"
