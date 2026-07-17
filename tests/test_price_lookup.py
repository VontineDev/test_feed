"""
test_price_lookup.py — 가격조회 2계통 특성화(characterization) 테스트

통합 전 회귀 스냅샷 (refactoring-roadmap.md Phase E 잔여 항목의 선행 작업):
  common._fetch_current_prices        — paper trading용. yfinance 배치(1d/1m)
                                        + 공유 TTL 캐시(5분), yfinance 형식 티커 전제.
  routers_portfolio._get_current_prices — 수동 포트폴리오용. aftermarket_snap 우선
                                        + yfinance 폴백, 캐시 없음, bare 티커 전제,
                                        KR(숫자 포함)/US(숫자 없음) 판별.

여기 기록된 동작은 "현재 그렇다"이지 "그래야 한다"가 아니다 — 통합 작업이
의도적으로 바꾸는 동작은 해당 테스트를 함께 갱신할 것. 특히 quirk 2건:
  - _fetch_current_prices의 캐시는 티커 목록과 무관하게 히트한다 (전역 스냅샷).
  - 단일 티커 응답(플랫 컬럼)은 티커 컬럼 매칭 실패로 빈 결과가 된다.
"""

from __future__ import annotations

import sys
import time
import types
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

# dashboard/backend/를 path에 추가해야 `from database import ...`가 동작함
_BACKEND = Path(__file__).parent.parent / "dashboard" / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))


# ── 공통 픽스처 ──────────────────────────────────────────────

def _make_pool(*fetch_returns):
    """asyncpg Pool mock (test_paper_analytics.py와 동일 패턴)."""
    conn = AsyncMock()
    if len(fetch_returns) == 1:
        conn.fetch = AsyncMock(return_value=fetch_returns[0])
    else:
        conn.fetch = AsyncMock(side_effect=list(fetch_returns))

    acq = AsyncMock()
    acq.__aenter__ = AsyncMock(return_value=conn)
    acq.__aexit__ = AsyncMock(return_value=False)

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acq)
    return pool, conn


def _row(**kwargs):
    class FakeRow(dict):
        def __iter__(self):
            return iter(self.values())
    return FakeRow(kwargs)


@pytest.fixture
def clean_price_cache():
    """common._POS_PRICE_CACHE 초기화 (dict 재대입 금지 — 참조 공유)."""
    import common
    common._POS_PRICE_CACHE["data"] = {}
    common._POS_PRICE_CACHE["expires"] = 0.0
    yield common._POS_PRICE_CACHE
    common._POS_PRICE_CACHE["data"] = {}
    common._POS_PRICE_CACHE["expires"] = 0.0


def _multiindex_hist(closes: dict[str, list[float]]) -> pd.DataFrame:
    """yf.download 배치 응답 형태(MultiIndex 컬럼)의 DataFrame."""
    data = {}
    for t, vals in closes.items():
        data[("Close", t)] = vals
        data[("Volume", t)] = [0] * len(vals)
    df = pd.DataFrame(data)
    df.columns = pd.MultiIndex.from_tuples(df.columns)
    return df


# ═════════════════════════════════════════════════════════════
# common._fetch_current_prices (paper trading 계열)
# ═════════════════════════════════════════════════════════════

class TestFetchCurrentPrices:

    @pytest.mark.asyncio
    async def test_empty_tickers_returns_empty(self, clean_price_cache):
        from common import _fetch_current_prices
        assert await _fetch_current_prices([]) == {}
        assert clean_price_cache["data"] == {}  # 캐시 미기록

    @pytest.mark.asyncio
    async def test_batch_fetch_takes_last_close_per_ticker(self, clean_price_cache):
        """MultiIndex 배치 응답 → 티커별 마지막 비-NaN Close, float 변환."""
        from common import _fetch_current_prices
        hist = _multiindex_hist({
            "005930.KS": [70000.0, 71000.0],
            "000660.KS": [150000.0, float("nan")],  # 마지막이 NaN → 직전 값
        })
        with patch("yfinance.download", MagicMock(return_value=hist)):
            prices = await _fetch_current_prices(["005930.KS", "000660.KS"])
        assert prices == {"005930.KS": 71000.0, "000660.KS": 150000.0}
        assert all(isinstance(v, float) for v in prices.values())

    @pytest.mark.asyncio
    async def test_missing_ticker_omitted(self, clean_price_cache):
        """응답에 없는 티커는 결과에서 제외 (예외 없이 skip)."""
        from common import _fetch_current_prices
        hist = _multiindex_hist({"005930.KS": [71000.0]})
        with patch("yfinance.download", MagicMock(return_value=hist)):
            prices = await _fetch_current_prices(["005930.KS", "999999.KQ"])
        assert prices == {"005930.KS": 71000.0}

    @pytest.mark.asyncio
    async def test_cache_write_and_ttl(self, clean_price_cache):
        """update_cache=True(기본): 결과와 만료시각(now+300s)을 공유 캐시에 기록."""
        from common import _fetch_current_prices
        hist = _multiindex_hist({"005930.KS": [71000.0]})
        before = time.time()
        with patch("yfinance.download", MagicMock(return_value=hist)):
            await _fetch_current_prices(["005930.KS"])
        assert clean_price_cache["data"] == {"005930.KS": 71000.0}
        assert before + 290 < clean_price_cache["expires"] <= time.time() + 300

    @pytest.mark.asyncio
    async def test_cache_hit_ignores_requested_tickers(self, clean_price_cache):
        """[quirk] 캐시는 티커 목록과 무관한 전역 스냅샷 — 유효하면 요청 티커가
        달라도 그대로 반환하고 yfinance를 호출하지 않는다."""
        from common import _fetch_current_prices
        clean_price_cache["data"] = {"CACHED.KS": 123.0}
        clean_price_cache["expires"] = time.time() + 100
        dl = MagicMock()
        with patch("yfinance.download", dl):
            prices = await _fetch_current_prices(["005930.KS"])
        assert prices == {"CACHED.KS": 123.0}
        dl.assert_not_called()

    @pytest.mark.asyncio
    async def test_update_cache_false_skips_cache_write(self, clean_price_cache):
        """update_cache=False: 단일 종목 조회가 공유 캐시를 오염시키지 않는다."""
        from common import _fetch_current_prices
        hist = _multiindex_hist({"005930.KS": [71000.0]})
        with patch("yfinance.download", MagicMock(return_value=hist)):
            prices = await _fetch_current_prices(["005930.KS"], update_cache=False)
        assert prices == {"005930.KS": 71000.0}
        assert clean_price_cache["data"] == {}
        assert clean_price_cache["expires"] == 0.0

    @pytest.mark.asyncio
    async def test_expired_cache_refetches(self, clean_price_cache):
        """만료된 캐시는 무시하고 재조회한다."""
        from common import _fetch_current_prices
        clean_price_cache["data"] = {"STALE.KS": 1.0}
        clean_price_cache["expires"] = time.time() - 1
        hist = _multiindex_hist({"005930.KS": [71000.0]})
        with patch("yfinance.download", MagicMock(return_value=hist)):
            prices = await _fetch_current_prices(["005930.KS"])
        assert prices == {"005930.KS": 71000.0}

    @pytest.mark.asyncio
    async def test_yfinance_error_returns_empty(self, clean_price_cache):
        """yfinance 예외는 삼키고 빈 dict 반환 (요청은 죽지 않음)."""
        from common import _fetch_current_prices
        with patch("yfinance.download", MagicMock(side_effect=RuntimeError("boom"))):
            prices = await _fetch_current_prices(["005930.KS"])
        assert prices == {}

    @pytest.mark.asyncio
    async def test_timeout_returns_empty(self, clean_price_cache):
        """외부풀 타임아웃(20s) → 빈 dict 반환."""
        import asyncio
        import common
        with patch.object(common, "_ext_thread",
                          AsyncMock(side_effect=asyncio.TimeoutError)):
            prices = await common._fetch_current_prices(["005930.KS"])
        assert prices == {}

    @pytest.mark.asyncio
    async def test_flat_columns_yield_empty(self, clean_price_cache):
        """[quirk] 플랫 컬럼(OHLCV) 응답 — 단일 티커 형태 — 은 티커 컬럼
        매칭에 실패해 빈 결과가 된다 (현재 동작 기록)."""
        from common import _fetch_current_prices
        hist = pd.DataFrame({"Open": [1.0], "Close": [71000.0]})
        with patch("yfinance.download", MagicMock(return_value=hist)):
            prices = await _fetch_current_prices(["005930.KS"])
        assert prices == {}


# ═════════════════════════════════════════════════════════════
# routers_portfolio._get_current_prices (수동 포트폴리오 계열)
# ═════════════════════════════════════════════════════════════

def _fake_ticker_factory(fast_infos: dict):
    """yf.Ticker mock: {symbol: SimpleNamespace(...)} — 미등록 심볼은 예외."""
    calls: list[str] = []

    def _factory(symbol: str):
        calls.append(symbol)
        if symbol not in fast_infos:
            raise ValueError(f"unknown symbol {symbol}")
        return types.SimpleNamespace(fast_info=fast_infos[symbol])

    _factory.calls = calls
    return _factory


def _fi(last_price=None, regular_market_price=None):
    return types.SimpleNamespace(
        last_price=last_price, regular_market_price=regular_market_price
    )


class TestGetCurrentPrices:

    @pytest.mark.asyncio
    async def test_empty_tickers_returns_empty(self):
        from routers_portfolio import _get_current_prices
        pool, _ = _make_pool([])
        assert await _get_current_prices(pool, []) == {}
        pool.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_kr_ticker_from_aftermarket_snap(self):
        """숫자 포함 티커는 aftermarket_snap 우선 — DB 히트 시 yfinance 미호출."""
        from routers_portfolio import _get_current_prices
        pool, conn = _make_pool([_row(ticker="005930", reg_close=71000)])
        tk = _fake_ticker_factory({})
        with patch("yfinance.Ticker", tk):
            prices = await _get_current_prices(pool, ["005930"])
        assert prices == {"005930": 71000.0}
        assert isinstance(prices["005930"], float)
        assert tk.calls == []
        # DB 쿼리에 KR 티커만 전달됐는지
        assert conn.fetch.call_args.args[1] == ["005930"]

    @pytest.mark.asyncio
    async def test_us_ticker_skips_db_uses_yfinance(self):
        """숫자 없는 티커 = US 판별 — DB를 아예 조회하지 않고 yfinance 직행."""
        from routers_portfolio import _get_current_prices
        pool, _ = _make_pool([])
        tk = _fake_ticker_factory({"AAPL": _fi(last_price=200.5)})
        with patch("yfinance.Ticker", tk):
            prices = await _get_current_prices(pool, ["AAPL"])
        assert prices == {"AAPL": 200.5}
        pool.acquire.assert_not_called()
        assert tk.calls == ["AAPL"]

    @pytest.mark.asyncio
    async def test_kr_db_miss_falls_back_ks_suffix(self):
        """DB 미수록 KR 티커는 .KS 먼저 시도, bare 티커로 키잉."""
        from routers_portfolio import _get_current_prices
        pool, _ = _make_pool([])
        tk = _fake_ticker_factory({"005930.KS": _fi(last_price=71000.0)})
        with patch("yfinance.Ticker", tk):
            prices = await _get_current_prices(pool, ["005930"])
        assert prices == {"005930": 71000.0}
        assert tk.calls == ["005930.KS"]  # .KQ까지 안 감

    @pytest.mark.asyncio
    async def test_kr_ks_fails_then_kq(self):
        """.KS 실패(예외) 시 .KQ로 폴백."""
        from routers_portfolio import _get_current_prices
        pool, _ = _make_pool([])
        tk = _fake_ticker_factory({"035720.KQ": _fi(last_price=45000.0)})
        with patch("yfinance.Ticker", tk):
            prices = await _get_current_prices(pool, ["035720"])
        assert prices == {"035720": 45000.0}
        assert tk.calls == ["035720.KS", "035720.KQ"]

    @pytest.mark.asyncio
    async def test_last_price_none_uses_regular_market_price(self):
        """fast_info.last_price 없으면 regular_market_price 사용."""
        from routers_portfolio import _get_current_prices
        pool, _ = _make_pool([])
        tk = _fake_ticker_factory({"AAPL": _fi(regular_market_price=199.0)})
        with patch("yfinance.Ticker", tk):
            prices = await _get_current_prices(pool, ["AAPL"])
        assert prices == {"AAPL": 199.0}

    @pytest.mark.asyncio
    async def test_mixed_db_hit_plus_yf_fallback(self):
        """KR 2종목 중 1개만 DB 히트 → 나머지만 yfinance 폴백."""
        from routers_portfolio import _get_current_prices
        pool, _ = _make_pool([_row(ticker="005930", reg_close=71000)])
        tk = _fake_ticker_factory({"000660.KS": _fi(last_price=150000.0)})
        with patch("yfinance.Ticker", tk):
            prices = await _get_current_prices(pool, ["005930", "000660"])
        assert prices == {"005930": 71000.0, "000660": 150000.0}
        assert tk.calls == ["000660.KS"]

    @pytest.mark.asyncio
    async def test_yf_fallback_failure_returns_db_partial(self):
        """폴백 스레드 자체가 실패해도 DB에서 얻은 부분 결과는 반환."""
        import routers_portfolio as mod
        pool, _ = _make_pool([_row(ticker="005930", reg_close=71000)])
        with patch.object(mod, "_ext_thread",
                          AsyncMock(side_effect=RuntimeError("pool down"))):
            prices = await mod._get_current_prices(pool, ["005930", "000660"])
        assert prices == {"005930": 71000.0}

    @pytest.mark.asyncio
    async def test_all_sources_fail_returns_empty(self):
        """DB 미수록 + yfinance 전 심볼 실패 → 빈 dict (예외 전파 없음)."""
        from routers_portfolio import _get_current_prices
        pool, _ = _make_pool([])
        tk = _fake_ticker_factory({})  # 모든 심볼 예외
        with patch("yfinance.Ticker", tk):
            prices = await _get_current_prices(pool, ["005930", "AAPL"])
        assert prices == {}

    @pytest.mark.asyncio
    async def test_null_reg_close_row_ignored(self):
        """reg_close가 falsy(0/None)인 행은 무시하고 폴백 대상에 포함."""
        from routers_portfolio import _get_current_prices
        pool, _ = _make_pool([_row(ticker="005930", reg_close=0)])
        tk = _fake_ticker_factory({"005930.KS": _fi(last_price=71000.0)})
        with patch("yfinance.Ticker", tk):
            prices = await _get_current_prices(pool, ["005930"])
        assert prices == {"005930": 71000.0}
        assert tk.calls == ["005930.KS"]
