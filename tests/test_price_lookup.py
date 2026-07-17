"""
test_price_lookup.py — 통합 가격조회(common.fetch_current_prices) 테스트

2026-07-17 통합: common._fetch_current_prices(paper)와
routers_portfolio._get_current_prices(포트폴리오)가 단일 구현
common.fetch_current_prices(tickers, *, pool=None, use_cache=True)로 합쳐짐.
두 기존 이름은 얇은 위임 wrapper로 유지 (호출부/패치 타깃 보존).

통합하면서 고친 quirk 2건 (이전 특성화 테스트가 기록했던 동작):
  - 캐시가 티커 목록과 무관한 전역 스냅샷 → 티커별 (price, expires) 엔트리
  - 단일 티커 플랫 컬럼 응답이 빈 결과 → Close 컬럼을 티커로 정규화해 반환
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


@pytest.fixture(autouse=True)
def price_cache():
    """티커별 공유 캐시 초기화 (모든 테스트 — dict 재대입 금지, clear만)."""
    import common
    common._POS_PRICE_CACHE.clear()
    yield common._POS_PRICE_CACHE
    common._POS_PRICE_CACHE.clear()


def _multiindex_hist(closes: dict[str, list[float]]) -> pd.DataFrame:
    """yf.download 배치 응답 형태(MultiIndex 컬럼)의 DataFrame."""
    data = {}
    for t, vals in closes.items():
        data[("Close", t)] = vals
        data[("Volume", t)] = [0] * len(vals)
    df = pd.DataFrame(data)
    df.columns = pd.MultiIndex.from_tuples(df.columns)
    return df


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


# ═════════════════════════════════════════════════════════════
# yfinance 배치 경로 (yfinance 형식 티커 — 구 _fetch_current_prices 계열)
# ═════════════════════════════════════════════════════════════

class TestBatchPath:

    @pytest.mark.asyncio
    async def test_empty_tickers_returns_empty(self, price_cache):
        from common import fetch_current_prices
        assert await fetch_current_prices([]) == {}
        assert price_cache == {}

    @pytest.mark.asyncio
    async def test_batch_fetch_takes_last_close_per_ticker(self):
        """MultiIndex 배치 응답 → 티커별 마지막 비-NaN Close, float 변환."""
        from common import fetch_current_prices
        hist = _multiindex_hist({
            "005930.KS": [70000.0, 71000.0],
            "000660.KS": [150000.0, float("nan")],  # 마지막이 NaN → 직전 값
        })
        with patch("yfinance.download", MagicMock(return_value=hist)):
            prices = await fetch_current_prices(["005930.KS", "000660.KS"])
        assert prices == {"005930.KS": 71000.0, "000660.KS": 150000.0}
        assert all(isinstance(v, float) for v in prices.values())

    @pytest.mark.asyncio
    async def test_missing_ticker_omitted(self):
        """응답에 없는 티커는 결과에서 제외 (예외 없이 skip)."""
        from common import fetch_current_prices
        hist = _multiindex_hist({"005930.KS": [71000.0]})
        with patch("yfinance.download", MagicMock(return_value=hist)):
            prices = await fetch_current_prices(["005930.KS", "999999.KQ"])
        assert prices == {"005930.KS": 71000.0}

    @pytest.mark.asyncio
    async def test_flat_columns_single_ticker_normalized(self):
        """[quirk 수정] 단일 티커 플랫 컬럼(OHLCV) 응답도 Close를 반환한다.
        (통합 전에는 티커 컬럼 매칭 실패로 빈 결과였음.)"""
        from common import fetch_current_prices
        hist = pd.DataFrame({"Open": [70500.0], "Close": [71000.0]})
        with patch("yfinance.download", MagicMock(return_value=hist)):
            prices = await fetch_current_prices(["005930.KS"])
        assert prices == {"005930.KS": 71000.0}

    @pytest.mark.asyncio
    async def test_yfinance_error_returns_empty(self):
        """yfinance 예외는 삼키고 빈 dict 반환 (요청은 죽지 않음)."""
        from common import fetch_current_prices
        with patch("yfinance.download", MagicMock(side_effect=RuntimeError("boom"))):
            prices = await fetch_current_prices(["005930.KS"])
        assert prices == {}

    @pytest.mark.asyncio
    async def test_timeout_returns_empty(self):
        """외부풀 타임아웃(20s) → 빈 dict 반환."""
        import asyncio
        import common
        with patch.object(common, "_ext_thread",
                          AsyncMock(side_effect=asyncio.TimeoutError)):
            prices = await common.fetch_current_prices(["005930.KS"])
        assert prices == {}


# ═════════════════════════════════════════════════════════════
# 티커별 캐시 (quirk 수정: 전역 스냅샷 → per-ticker 엔트리)
# ═════════════════════════════════════════════════════════════

class TestPerTickerCache:

    @pytest.mark.asyncio
    async def test_cache_write_per_ticker_with_ttl(self, price_cache):
        """조회 결과는 티커별 (price, expires=now+300s) 엔트리로 기록."""
        from common import fetch_current_prices
        hist = _multiindex_hist({"005930.KS": [71000.0]})
        before = time.time()
        with patch("yfinance.download", MagicMock(return_value=hist)):
            await fetch_current_prices(["005930.KS"])
        price, expires = price_cache["005930.KS"]
        assert price == 71000.0
        assert before + 290 < expires <= time.time() + 300

    @pytest.mark.asyncio
    async def test_cache_hit_skips_fetch(self, price_cache):
        """[quirk 수정] 캐시는 요청한 티커에 대해서만 히트한다."""
        from common import fetch_current_prices
        price_cache["005930.KS"] = (71000.0, time.time() + 100)
        dl = MagicMock()
        with patch("yfinance.download", dl):
            prices = await fetch_current_prices(["005930.KS"])
        assert prices == {"005930.KS": 71000.0}
        dl.assert_not_called()

    @pytest.mark.asyncio
    async def test_partial_cache_fetches_only_missing(self, price_cache):
        """[quirk 수정] 일부만 캐시 유효 → 나머지 티커만 조회한다.
        (통합 전에는 캐시가 유효하면 다른 티커 요청에도 캐시 전체를 반환.)"""
        from common import fetch_current_prices
        price_cache["005930.KS"] = (71000.0, time.time() + 100)
        hist = _multiindex_hist({"000660.KS": [150000.0]})
        dl = MagicMock(return_value=hist)
        with patch("yfinance.download", dl):
            prices = await fetch_current_prices(["005930.KS", "000660.KS"])
        assert prices == {"005930.KS": 71000.0, "000660.KS": 150000.0}
        # 다운로드는 캐시 미스 티커만
        assert dl.call_args.args[0] == ["000660.KS"]

    @pytest.mark.asyncio
    async def test_expired_entry_refetches(self, price_cache):
        """만료된 엔트리는 무시하고 재조회한다."""
        from common import fetch_current_prices
        price_cache["005930.KS"] = (1.0, time.time() - 1)
        hist = _multiindex_hist({"005930.KS": [71000.0]})
        with patch("yfinance.download", MagicMock(return_value=hist)):
            prices = await fetch_current_prices(["005930.KS"])
        assert prices == {"005930.KS": 71000.0}

    @pytest.mark.asyncio
    async def test_use_cache_false_bypasses_read_and_write(self, price_cache):
        """use_cache=False: 캐시를 읽지도 쓰지도 않는다 (항상 신선)."""
        from common import fetch_current_prices
        price_cache["005930.KS"] = (1.0, time.time() + 100)  # 유효한 엔트리 무시돼야 함
        hist = _multiindex_hist({"005930.KS": [71000.0]})
        with patch("yfinance.download", MagicMock(return_value=hist)):
            prices = await fetch_current_prices(["005930.KS"], use_cache=False)
        assert prices == {"005930.KS": 71000.0}
        assert price_cache["005930.KS"] == (1.0, pytest.approx(price_cache["005930.KS"][1]))

    @pytest.mark.asyncio
    async def test_legacy_wrapper_update_cache_false(self, price_cache):
        """(호환) _fetch_current_prices(update_cache=False) → use_cache=False 매핑."""
        from common import _fetch_current_prices
        hist = _multiindex_hist({"005930.KS": [71000.0]})
        with patch("yfinance.download", MagicMock(return_value=hist)):
            prices = await _fetch_current_prices(["005930.KS"], update_cache=False)
        assert prices == {"005930.KS": 71000.0}
        assert price_cache == {}


# ═════════════════════════════════════════════════════════════
# bare 티커 경로 (aftermarket_snap + fast_info — 구 _get_current_prices 계열)
# ═════════════════════════════════════════════════════════════

class TestBareTickerPath:

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
        """폴백 스레드 자체가 실패해도 DB에서 얻은 부분 결과는 반환.
        (통합으로 구현이 common으로 이동 — 패치 타깃도 common._ext_thread.)"""
        import common
        from routers_portfolio import _get_current_prices
        pool, _ = _make_pool([_row(ticker="005930", reg_close=71000)])
        with patch.object(common, "_ext_thread",
                          AsyncMock(side_effect=RuntimeError("pool down"))):
            prices = await _get_current_prices(pool, ["005930", "000660"])
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

    @pytest.mark.asyncio
    async def test_portfolio_now_cached(self, price_cache):
        """(통합 신규 동작) 포트폴리오 경로도 티커별 5분 캐시 적용 —
        두 번째 호출은 DB/yfinance를 다시 치지 않는다."""
        from routers_portfolio import _get_current_prices
        pool1, conn1 = _make_pool([_row(ticker="005930", reg_close=71000)])
        with patch("yfinance.Ticker", _fake_ticker_factory({})):
            first = await _get_current_prices(pool1, ["005930"])
        assert first == {"005930": 71000.0}

        pool2, _ = _make_pool([])
        with patch("yfinance.Ticker", _fake_ticker_factory({})):
            second = await _get_current_prices(pool2, ["005930"])
        assert second == {"005930": 71000.0}
        pool2.acquire.assert_not_called()


# ═════════════════════════════════════════════════════════════
# 혼합 형식 (통합 함수 직접 호출)
# ═════════════════════════════════════════════════════════════

class TestMixedFormats:

    @pytest.mark.asyncio
    async def test_three_paths_in_one_call(self):
        """bare KR(DB) + yfinance 형식(배치) + US(fast_info)를 한 호출로."""
        from common import fetch_current_prices
        pool, _ = _make_pool([_row(ticker="005930", reg_close=71000)])
        hist = _multiindex_hist({"000660.KS": [150000.0]})
        tk = _fake_ticker_factory({"AAPL": _fi(last_price=200.5)})
        with (
            patch("yfinance.download", MagicMock(return_value=hist)),
            patch("yfinance.Ticker", tk),
        ):
            prices = await fetch_current_prices(
                ["005930", "000660.KS", "AAPL"], pool=pool
            )
        assert prices == {
            "005930": 71000.0,
            "000660.KS": 150000.0,
            "AAPL": 200.5,
        }

    @pytest.mark.asyncio
    async def test_no_pool_bare_kr_goes_straight_to_fast_info(self):
        """pool 미제공 시 bare KR도 DB 없이 fast_info 폴백만 탄다."""
        from common import fetch_current_prices
        tk = _fake_ticker_factory({"005930.KS": _fi(last_price=71000.0)})
        with patch("yfinance.Ticker", tk):
            prices = await fetch_current_prices(["005930"])
        assert prices == {"005930": 71000.0}

    @pytest.mark.asyncio
    async def test_duplicate_tickers_deduped(self):
        """중복 티커는 1회만 조회한다."""
        from common import fetch_current_prices
        hist = _multiindex_hist({"005930.KS": [71000.0]})
        dl = MagicMock(return_value=hist)
        with patch("yfinance.download", dl):
            prices = await fetch_current_prices(["005930.KS", "005930.KS"])
        assert prices == {"005930.KS": 71000.0}
        assert dl.call_args.args[0] == ["005930.KS"]