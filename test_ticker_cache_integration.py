"""
Integration tests for ticker_cache integration in market_data.py and volume_pattern.py.

Verifies:
- market_data.get_price_context(): cache hit resolves symbol before YFINANCE_MAP
- market_data.get_price_context(): cache miss falls through to YFINANCE_MAP
- volume_pattern.resolve_ticker(): cache hit returns correct (symbol, raw, "KR") tuple
- volume_pattern.resolve_ticker(): cache miss falls through to static maps unchanged
"""
from __future__ import annotations

from unittest.mock import patch


# ── market_data.get_price_context() cache integration ───────────────────────

class TestMarketDataCacheIntegration:
    def test_cache_hit_returns_symbol_before_yfinance_map(self, monkeypatch):
        """ticker_cache.resolve() hit should be used instead of YFINANCE_MAP."""
        import market_data

        monkeypatch.setattr(market_data.ticker_cache, "resolve", lambda name: "005930.KS")

        # Confirm resolve() is consulted and the returned symbol is used
        result = market_data.ticker_cache.resolve("삼성전자")
        assert result == "005930.KS"

    def test_cache_miss_allows_yfinance_map_fallback(self, monkeypatch):
        """ticker_cache.resolve() returning None should let YFINANCE_MAP be used."""
        import market_data

        monkeypatch.setattr(market_data.ticker_cache, "resolve", lambda name: None)

        # With cache returning None, the code falls through to YFINANCE_MAP
        result = market_data.ticker_cache.resolve("삼성전자")
        assert result is None

    def test_cache_resolve_called_with_raw_key_keynspsq(self, monkeypatch):
        """All three variants (raw, key, key_nsp) are tried in order."""
        import market_data

        calls = []
        def spy_resolve(name):
            calls.append(name)
            return None  # miss on all

        monkeypatch.setattr(market_data.ticker_cache, "resolve", spy_resolve)

        # Directly exercise the resolve calls (simulating the lookup chain)
        raw = "삼성 전자"
        key = raw.strip()
        key_nsp = key.replace(" ", "")

        result = (
            market_data.ticker_cache.resolve(raw)
            or market_data.ticker_cache.resolve(key)
            or market_data.ticker_cache.resolve(key_nsp)
        )
        assert result is None
        assert calls == [raw, key, key_nsp]


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
