"""
Regression tests for krx_sync.py and ticker_cache.py.

Covers:
- _row_to_params: supported markets, KONEX exclusion, field mapping
- _derive_yfinance_symbol: KOSPI vs KOSDAQ suffix
- _parse_listed_at: valid/invalid/empty date strings
- TickerCache: safe before load, load, resolve, atomic reload
- sync_krx_listings: KRXOpenAPIClient mock, upsert flow, error paths
"""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from data.krx_sync import (
    _derive_yfinance_symbol,
    _parse_listed_at,
    _parse_listed_shares,
    _row_to_params,
    SUPPORTED_MARKETS,
)
from core.ticker_cache import TickerCache


# ── _derive_yfinance_symbol ──────────────────────────────────────────────────

class TestDeriveYfinanceSymbol:
    def test_kospi_gets_ks_suffix(self):
        assert _derive_yfinance_symbol("005930", "KOSPI") == "005930.KS"

    def test_kosdaq_gets_kq_suffix(self):
        assert _derive_yfinance_symbol("086520", "KOSDAQ") == "086520.KQ"

    def test_kosdaq_case_insensitive(self):
        assert _derive_yfinance_symbol("086520", "kosdaq") == "086520.KQ"

    def test_unknown_market_defaults_to_ks(self):
        # Unknown/empty market falls back to .KS (safe default for KOSPI-majority)
        assert _derive_yfinance_symbol("123456", "KONEX").endswith(".KS")


# ── _parse_listed_at ─────────────────────────────────────────────────────────

class TestParseListedAt:
    def test_valid_date(self):
        from datetime import date
        assert _parse_listed_at("20050112") == date(2005, 1, 12)

    def test_empty_string_returns_none(self):
        assert _parse_listed_at("") is None

    def test_invalid_format_returns_none(self):
        assert _parse_listed_at("2005-01-12") is None

    def test_non_numeric_returns_none(self):
        assert _parse_listed_at("abcdefgh") is None

    def test_invalid_calendar_date_returns_none(self):
        # Passes length/digit check but fails date(2000, 13, 40) → ValueError
        assert _parse_listed_at("20001340") is None


# ── _parse_listed_shares ─────────────────────────────────────────────────────

class TestParseListedShares:
    def test_plain_number(self):
        assert _parse_listed_shares("5969782550") == 5969782550

    def test_comma_separated(self):
        assert _parse_listed_shares("5,969,782,550") == 5969782550

    def test_empty_returns_none(self):
        assert _parse_listed_shares("") is None

    def test_non_numeric_returns_none(self):
        assert _parse_listed_shares("N/A") is None


# ── _row_to_params ───────────────────────────────────────────────────────────

class TestRowToParams:
    def _make_row(self, **overrides) -> dict:
        base = {
            "ISU_CD": "KR7005930003",
            "ISU_SRT_CD": "005930",
            "ISU_NM": "삼성전자",
            "ISU_ABBRV": "삼성전자",
            "ISU_ENG_NM": "Samsung Electronics Co., Ltd.",
            "LIST_DD": "19750611",
            "MKT_NM": "KOSPI",
            "SECUGRP_NM": "주권",
            "SECT_TP_NM": "대형주",
            "KIND_STKCERT_TP_NM": "보통주",
            "PAR_VAL": "100",
            "LIST_SHRS": "5969782550",
        }
        base.update(overrides)
        return base

    def test_valid_kospi_row(self):
        row = self._make_row()
        params = _row_to_params(row)
        assert params is not None
        isin, short, name_ko, name_ko_abbr, name_en, *rest, yf_symbol = params
        assert isin == "KR7005930003"
        assert short == "005930"
        assert name_ko == "삼성전자"
        assert yf_symbol == "005930.KS"

    def test_valid_kosdaq_row(self):
        row = self._make_row(
            ISU_CD="KR7086520006", ISU_SRT_CD="086520",
            ISU_NM="에코프로비엠", ISU_ABBRV="에코프로비엠", MKT_NM="KOSDAQ",
        )
        params = _row_to_params(row)
        assert params is not None
        assert params[-1] == "086520.KQ"

    def test_konex_row_is_excluded(self):
        row = self._make_row(MKT_NM="KONEX")
        assert _row_to_params(row) is None

    def test_missing_isin_returns_none(self):
        row = self._make_row(ISU_CD="")
        assert _row_to_params(row) is None

    def test_missing_short_code_returns_none(self):
        row = self._make_row(ISU_SRT_CD="")
        assert _row_to_params(row) is None

    def test_missing_name_ko_returns_none(self):
        row = self._make_row(ISU_NM="")
        assert _row_to_params(row) is None

    def test_name_ko_abbr_used_from_isu_abbrv(self):
        row = self._make_row(ISU_NM="LG에너지솔루", ISU_ABBRV="LG에너지솔루션")
        params = _row_to_params(row)
        assert params is not None
        # name_ko_abbr is 4th element (index 3)
        assert params[3] == "LG에너지솔루션"

    def test_supported_markets_only_kospi_kosdaq(self):
        assert "KOSPI" in SUPPORTED_MARKETS
        assert "KOSDAQ" in SUPPORTED_MARKETS
        assert "KONEX" not in SUPPORTED_MARKETS


# ── TickerCache ──────────────────────────────────────────────────────────────

class TestTickerCache:
    def test_resolve_returns_none_before_load(self):
        cache = TickerCache()
        assert cache.resolve("삼성전자") is None

    def test_loaded_is_false_before_load(self):
        cache = TickerCache()
        assert not cache.loaded

    @pytest.mark.asyncio
    async def test_load_populates_by_name(self):
        cache = TickerCache()
        mock_rows = [
            {
                "name_ko": "삼성전자",
                "name_ko_abbr": "삼성전자",
                "name_en": "Samsung Electronics Co., Ltd.",
                "short_code": "005930",
                "yfinance_symbol": "005930.KS",
            }
        ]
        pool = AsyncMock()
        pool.fetch = AsyncMock(return_value=mock_rows)
        await cache.load(pool)
        assert cache.loaded
        assert cache.resolve("삼성전자") == "005930.KS"
        assert cache.resolve("005930") == "005930.KS"
        assert cache.resolve("Samsung Electronics Co., Ltd.") == "005930.KS"

    @pytest.mark.asyncio
    async def test_load_is_idempotent(self):
        """Calling load() twice replaces the cache cleanly."""
        cache = TickerCache()
        row_v1 = [{"name_ko": "A", "name_ko_abbr": None, "name_en": None,
                   "short_code": "000001", "yfinance_symbol": "000001.KS"}]
        row_v2 = [{"name_ko": "B", "name_ko_abbr": None, "name_en": None,
                   "short_code": "000002", "yfinance_symbol": "000002.KQ"}]

        pool = AsyncMock()
        pool.fetch = AsyncMock(return_value=row_v1)
        await cache.load(pool)
        assert cache.resolve("A") == "000001.KS"

        pool.fetch = AsyncMock(return_value=row_v2)
        await cache.load(pool)
        assert cache.resolve("B") == "000002.KQ"
        assert cache.resolve("A") is None  # old entry gone

    @pytest.mark.asyncio
    async def test_resolve_prefers_name_ko_abbr_over_name_ko(self):
        """Both names map to the same symbol, but abbr is primary lookup key."""
        cache = TickerCache()
        row = [{
            "name_ko": "LG에너지솔루",       # truncated ISU_NM
            "name_ko_abbr": "LG에너지솔루션",  # full ISU_ABBRV
            "name_en": None,
            "short_code": "373220",
            "yfinance_symbol": "373220.KS",
        }]
        pool = AsyncMock()
        pool.fetch = AsyncMock(return_value=row)
        await cache.load(pool)
        assert cache.resolve("LG에너지솔루션") == "373220.KS"   # abbr works
        assert cache.resolve("LG에너지솔루") == "373220.KS"     # truncated name also works

    @pytest.mark.asyncio
    async def test_atomic_assignment_both_dicts_updated(self):
        """_by_name and _by_short are replaced in a single tuple unpack."""
        cache = TickerCache()
        row = [{"name_ko": "삼성전자", "name_ko_abbr": None, "name_en": None,
                "short_code": "005930", "yfinance_symbol": "005930.KS"}]
        pool = AsyncMock()
        pool.fetch = AsyncMock(return_value=row)
        await cache.load(pool)
        # Both lookups work after a single load
        assert cache.resolve("삼성전자") == "005930.KS"
        assert cache.resolve("005930") == "005930.KS"


# ── sync_krx_listings integration ───────────────────────────────────────────
# krx_sync.py now uses KRXOpenAPIClient (requests-based REST) rather than
# httpx scraping. Mocks target krx_openapi.KRXOpenAPIClient.

class TestSyncKrxListings:
    def _make_mock_pool(self):
        """Return (pool, conn) mocks wired for executemany/execute/fetchval."""
        mock_tx = MagicMock()
        mock_tx.__aenter__ = AsyncMock(return_value=None)
        mock_tx.__aexit__ = AsyncMock(return_value=False)

        mock_conn = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_conn.transaction = MagicMock(return_value=mock_tx)
        mock_conn.fetchval = AsyncMock(return_value="2025-01-01 00:00:00+00")
        mock_conn.execute = AsyncMock(return_value="DELETE 0")
        mock_conn.executemany = AsyncMock()

        mock_pool = AsyncMock()
        mock_pool.acquire = MagicMock(return_value=mock_conn)
        return mock_pool, mock_conn

    @pytest.mark.asyncio
    async def test_upsert_returns_row_count(self):
        """Valid KOSPI row → upsert called, count == 1."""
        from krx_sync import sync_krx_listings

        sample_row = {
            "ISU_CD": "KR7005930003", "ISU_SRT_CD": "005930",
            "ISU_NM": "삼성전자", "ISU_ABBRV": "삼성전자",
            "ISU_ENG_NM": "Samsung Electronics Co., Ltd.",
            "LIST_DD": "19750611",
        }
        mock_pool, mock_conn = self._make_mock_pool()

        with patch("krx_openapi.KRXOpenAPIClient") as MockClient:
            inst = MockClient.return_value
            inst.get_kospi_tickers.return_value = [sample_row]
            inst.get_kosdaq_tickers.return_value = []
            count = await sync_krx_listings(mock_pool)

        assert count == 1
        mock_conn.executemany.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_api_response_raises(self):
        """Both markets returning empty → ValueError mentioning KRX_OPENAPI_KEY."""
        from krx_sync import sync_krx_listings

        mock_pool = AsyncMock()

        with patch("krx_openapi.KRXOpenAPIClient") as MockClient:
            inst = MockClient.return_value
            inst.get_kospi_tickers.return_value = []
            inst.get_kosdaq_tickers.return_value = []
            with pytest.raises(ValueError, match="KRX_OPENAPI_KEY"):
                await sync_krx_listings(mock_pool)

        mock_pool.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_all_invalid_rows_raises_before_delete(self):
        """Rows missing required fields → ValueError before DB is touched."""
        from krx_sync import sync_krx_listings

        invalid_row = {"ISU_CD": "", "ISU_SRT_CD": "", "ISU_NM": ""}
        mock_pool = AsyncMock()

        with patch("krx_openapi.KRXOpenAPIClient") as MockClient:
            inst = MockClient.return_value
            inst.get_kospi_tickers.return_value = [invalid_row]
            inst.get_kosdaq_tickers.return_value = []
            with pytest.raises(ValueError, match="유효한 종목이 없음"):
                await sync_krx_listings(mock_pool)

        mock_pool.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_kospi_and_kosdaq_both_upserted(self):
        """Rows from KOSPI and KOSDAQ are both processed → count == 2."""
        from krx_sync import sync_krx_listings

        kospi_row  = {"ISU_CD": "KR7005930003", "ISU_SRT_CD": "005930", "ISU_NM": "삼성전자",  "LIST_DD": "19750611"}
        kosdaq_row = {"ISU_CD": "KR7086520006", "ISU_SRT_CD": "086520", "ISU_NM": "에코프로비엠", "LIST_DD": "20140425"}
        mock_pool, mock_conn = self._make_mock_pool()

        with patch("krx_openapi.KRXOpenAPIClient") as MockClient:
            inst = MockClient.return_value
            inst.get_kospi_tickers.return_value  = [kospi_row]
            inst.get_kosdaq_tickers.return_value = [kosdaq_row]
            count = await sync_krx_listings(mock_pool)

        assert count == 2
        # executemany called once with both rows in the params list
        args = mock_conn.executemany.call_args
        assert len(args[0][1]) == 2
