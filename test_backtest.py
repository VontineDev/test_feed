"""
test_backtest.py  —  백테스팅 모듈 단위 테스트
────────────────────────────────────────────────────────────
pytest 기반, DB 불필요 (asyncpg pool mock 사용).

실행:
    pytest test_backtest.py -v
"""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ── backtest 모듈 import ──────────────────────────────────────
from backtest import _esc, calculate_metrics, backtest_report_telegram, cross_analyze_historical


# ═════════════════════════════════════════════════════════════
# _esc() — MarkdownV2 이스케이프
# ═════════════════════════════════════════════════════════════

class TestEsc:
    def test_korean_passthrough(self):
        assert _esc("삼성전자") == "삼성전자"

    def test_ampersand_escaped(self):
        assert _esc("S&P500") == "S\\&P500"

    def test_parens_and_ampersand(self):
        assert _esc("(S&P500)") == "\\(S\\&P500\\)"

    def test_dash_and_dot(self):
        assert _esc("-0.5%") == "\\-0\\.5%"

    def test_plus_and_dot(self):
        assert _esc("68% → +1.2%") == "68% → \\+1\\.2%"

    def test_empty_string(self):
        assert _esc("") == ""

    def test_no_special_chars(self):
        assert _esc("hello world") == "hello world"


# ═════════════════════════════════════════════════════════════
# calculate_metrics() — pool mock 헬퍼
# ═════════════════════════════════════════════════════════════

def _make_pool(rows: list[dict]):
    """asyncpg pool mock — first conn.fetch() returns rows, second (baseline) returns []."""
    metrics_result = [
        {
            "verdict": r["verdict"],
            "direction": r["direction"],
            "score": r.get("score", 5),
            "checkpoint": r.get("checkpoint", "1d"),
            "return_pct": r["return_pct"],
        }
        for r in rows
    ]
    conn = AsyncMock()
    conn.fetch = AsyncMock(side_effect=[metrics_result, []])
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=conn),
        __aexit__=AsyncMock(return_value=False),
    ))
    return pool


# ═════════════════════════════════════════════════════════════
# calculate_metrics() — 적중률 케이스
# ═════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_calculate_metrics_hit_rates():
    """BUY/SELL/FILTER 방향 적중률 검증."""
    rows = [
        # BUY CONFIRM: hit (ret > 0)
        {"verdict": "CONFIRM", "direction": "BUY",  "return_pct": 1.5,  "checkpoint": "1d"},
        # BUY CONFIRM: miss (ret < 0)
        {"verdict": "CONFIRM", "direction": "BUY",  "return_pct": -0.5, "checkpoint": "1d"},
        # SELL CONFIRM: hit (ret < 0)
        {"verdict": "CONFIRM", "direction": "SELL", "return_pct": -1.0, "checkpoint": "1d"},
        # SELL CONFIRM: miss (ret > 0)
        {"verdict": "CONFIRM", "direction": "SELL", "return_pct": 0.5,  "checkpoint": "1d"},
        # FILTER BUY: hit — 손실 방어 (ret <= 0)
        {"verdict": "FILTER",  "direction": "BUY",  "return_pct": -1.0, "checkpoint": "1d"},
        # FILTER BUY: miss — 상승 방어 실패 (ret > 0)
        {"verdict": "FILTER",  "direction": "BUY",  "return_pct": 0.5,  "checkpoint": "1d"},
    ]

    pool = _make_pool(rows)
    metrics = await calculate_metrics(pool)

    assert metrics["rows"] == 6
    by_vc = metrics["by_verdict_checkpoint"]

    confirm_1d = by_vc[("CONFIRM", "1d")]
    assert confirm_1d["count"] == 4
    assert confirm_1d["hit_rate"] == 50.0  # 2 hits out of 4

    filter_1d = by_vc[("FILTER", "1d")]
    assert filter_1d["count"] == 2
    assert filter_1d["hit_rate"] == 50.0  # 1 hit out of 2


@pytest.mark.asyncio
async def test_calculate_metrics_empty():
    """빈 DB → {'message': '데이터 없음', 'rows': 0}."""
    pool = _make_pool([])
    metrics = await calculate_metrics(pool)
    assert metrics == {"message": "데이터 없음", "rows": 0}


@pytest.mark.asyncio
async def test_calculate_metrics_nan_filtered():
    """NaN return_pct 행은 집계에서 제외 — ZeroDivisionError 없어야 함."""
    rows = [
        {"verdict": "CONFIRM", "direction": "BUY",  "return_pct": float("nan"), "checkpoint": "1d"},
        {"verdict": "CONFIRM", "direction": "BUY",  "return_pct": float("nan"), "checkpoint": "1d"},
    ]
    pool = _make_pool(rows)
    # NaN만 있으므로 유효 행 0 — 빈 결과 또는 hit_rate=0 반환 (ZeroDivisionError 없어야 함)
    metrics = await calculate_metrics(pool)
    # rows 필드는 DB에서 반환된 원본 2건이지만 유효 집계는 없어야 함
    by_vc = metrics.get("by_verdict_checkpoint", {})
    if ("CONFIRM", "1d") in by_vc:
        assert by_vc[("CONFIRM", "1d")]["count"] == 0


@pytest.mark.asyncio
async def test_watch_hit_rate_is_none():
    """WATCH 방향은 hit_rate = None (방향성 없는 모니터링 신호)."""
    rows = [
        {"verdict": "CONFIRM", "direction": "WATCH", "return_pct": 2.0,  "checkpoint": "1d"},
        {"verdict": "CONFIRM", "direction": "WATCH", "return_pct": -1.5, "checkpoint": "1d"},
        {"verdict": "CONFIRM", "direction": "WATCH", "return_pct": 0.3,  "checkpoint": "1d"},
    ]
    pool = _make_pool(rows)
    metrics = await calculate_metrics(pool)
    by_vc = metrics.get("by_verdict_checkpoint", {})
    assert ("CONFIRM", "1d") in by_vc
    assert by_vc[("CONFIRM", "1d")]["hit_rate"] is None


@pytest.mark.asyncio
async def test_calculate_metrics_mixed_watch_directional():
    """WATCH + BUY 혼합: hit_rate는 BUY 행만으로 계산."""
    rows = [
        # BUY hit
        {"verdict": "CONFIRM", "direction": "BUY",   "return_pct": 1.0, "checkpoint": "1d"},
        # BUY miss
        {"verdict": "CONFIRM", "direction": "BUY",   "return_pct": -0.5, "checkpoint": "1d"},
        # WATCH — 계산 제외
        {"verdict": "CONFIRM", "direction": "WATCH",  "return_pct": 2.0, "checkpoint": "1d"},
    ]
    pool = _make_pool(rows)
    metrics = await calculate_metrics(pool)
    confirm_1d = metrics["by_verdict_checkpoint"][("CONFIRM", "1d")]
    # 3 total rows, but only 2 directional (BUY) — hit_rate = 1/2 = 50%
    assert confirm_1d["count"] == 3
    assert confirm_1d["hit_rate"] == 50.0


# ═════════════════════════════════════════════════════════════
# TestMarketBaseline — two-query calculate_metrics()
# ═════════════════════════════════════════════════════════════

def _make_pool_two_queries(metrics_rows: list[dict], baseline_rows: list[dict]):
    """
    asyncpg pool mock where conn.fetch() returns different results for each call:
    first call → metrics_rows, second call → baseline_rows.
    """
    metrics_result = [
        {
            "verdict": r["verdict"],
            "direction": r["direction"],
            "score": r.get("score", 5),
            "checkpoint": r.get("checkpoint", "1d"),
            "return_pct": r["return_pct"],
        }
        for r in metrics_rows
    ]
    baseline_result = [
        {
            "direction": r["direction"],
            "up_count": r["up_count"],
            "down_count": r["down_count"],
            "total": r["total"],
        }
        for r in baseline_rows
    ]
    conn = AsyncMock()
    conn.fetch = AsyncMock(side_effect=[metrics_result, baseline_result])
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=conn),
        __aexit__=AsyncMock(return_value=False),
    ))
    return pool


class TestMarketBaseline:
    @pytest.mark.asyncio
    async def test_baseline_buy_sell_computed(self):
        """BUY baseline = up_count/total, SELL baseline = down_count/total."""
        metrics_rows = [
            {"verdict": "CONFIRM", "direction": "BUY", "return_pct": 1.0, "checkpoint": "1d"},
        ]
        baseline_rows = [
            {"direction": "BUY",  "up_count": 54, "down_count": 46, "total": 100},
            {"direction": "SELL", "up_count": 48, "down_count": 52, "total": 100},
        ]
        pool = _make_pool_two_queries(metrics_rows, baseline_rows)
        metrics = await calculate_metrics(pool)

        assert metrics["market_baseline"] is not None
        assert metrics["market_baseline"]["BUY"] == 54.0
        assert metrics["market_baseline"]["SELL"] == 52.0

    @pytest.mark.asyncio
    async def test_baseline_none_when_no_data(self):
        """Empty baseline query → market_baseline is None."""
        metrics_rows = [
            {"verdict": "CONFIRM", "direction": "BUY", "return_pct": 1.0, "checkpoint": "1d"},
        ]
        pool = _make_pool_two_queries(metrics_rows, [])
        metrics = await calculate_metrics(pool)

        assert metrics["market_baseline"] is None

    @pytest.mark.asyncio
    async def test_baseline_zero_total_no_division_error(self):
        """total=0 in baseline row should not raise ZeroDivisionError."""
        metrics_rows = [
            {"verdict": "CONFIRM", "direction": "BUY", "return_pct": 1.0, "checkpoint": "1d"},
        ]
        baseline_rows = [
            {"direction": "BUY", "up_count": 0, "down_count": 0, "total": 0},
        ]
        pool = _make_pool_two_queries(metrics_rows, baseline_rows)
        metrics = await calculate_metrics(pool)
        # total=0 row is skipped — market_baseline may be None or {}
        bl = metrics["market_baseline"]
        assert bl is None or "BUY" not in bl

    @pytest.mark.asyncio
    async def test_baseline_in_market_baseline_key(self):
        """market_baseline is a top-level key on the metrics dict."""
        metrics_rows = [
            {"verdict": "CONFIRM", "direction": "BUY", "return_pct": 1.0, "checkpoint": "1d"},
        ]
        baseline_rows = [
            {"direction": "BUY", "up_count": 60, "down_count": 40, "total": 100},
        ]
        pool = _make_pool_two_queries(metrics_rows, baseline_rows)
        metrics = await calculate_metrics(pool)

        assert "market_baseline" in metrics
        assert isinstance(metrics["market_baseline"], dict)
        assert metrics["market_baseline"]["BUY"] == 60.0


# ═════════════════════════════════════════════════════════════
# TestBaselineTelegramEsc — "랜덤 기준선" line in MarkdownV2
# ═════════════════════════════════════════════════════════════

class TestBaselineTelegramEsc:
    def test_baseline_percent_escaped(self):
        """Percentage values in baseline line must have '.' escaped as '\\.'."""
        # 54.1% → 54\\.1% in MarkdownV2
        result = _esc("54.1%")
        assert "\\." in result
        assert result == "54\\.1%"

    def test_baseline_line_format(self):
        """랜덤 기준선 line contains escaped percent values for BUY and SELL."""
        buy = _esc("54.1%")
        sell = _esc("48.3%")
        line = f"랜덤 기준선: {buy} \\(BUY\\) / {sell} \\(SELL\\)"
        assert "54\\.1%" in line
        assert "48\\.3%" in line
        assert "\\(BUY\\)" in line
        assert "\\(SELL\\)" in line


# ═════════════════════════════════════════════════════════════
# TestBuildPriceContextCache — isocalendar cache in backfill_historical
# ═════════════════════════════════════════════════════════════

class TestBuildPriceContextCache:
    @pytest.mark.asyncio
    async def test_same_symbol_week_not_refetched(self):
        """Second call with same (symbol, iso_week) does NOT re-fetch from yfinance."""
        from datetime import datetime, timezone
        from unittest.mock import patch
        from market_data import PriceContext

        mock_ctx = PriceContext(
            ticker="삼성전자", symbol="005930.KS", source="yfinance",
            current=70000.0, change_pct=0.5,
            rsi=50.0, volume_ratio=1.0,
            week52_high=80000.0, week52_low=60000.0,
            volume_surge=False, success=True,
        )

        as_of = datetime(2026, 4, 7, tzinfo=timezone.utc)  # ISO week 15
        cache: dict = {}

        with patch("backtest._build_price_context_historical", return_value=mock_ctx) as mock_build:
            # First call: cache miss — should call _build_price_context_historical
            await cross_analyze_historical(
                direction="BUY", strength=3,
                tickers=["삼성전자"], ticker_symbols={"삼성전자": "005930.KS"},
                as_of_date=as_of, _ctx_cache=cache,
            )
            assert mock_build.call_count == 1

            # Second call: same symbol, same ISO week — should NOT call again
            await cross_analyze_historical(
                direction="BUY", strength=3,
                tickers=["삼성전자"], ticker_symbols={"삼성전자": "005930.KS"},
                as_of_date=as_of, _ctx_cache=cache,
            )
            assert mock_build.call_count == 1  # still 1, not 2

    @pytest.mark.asyncio
    async def test_different_week_refetches(self):
        """Different ISO week for same symbol fetches independently."""
        from datetime import datetime, timezone
        from unittest.mock import patch
        from market_data import PriceContext

        mock_ctx = PriceContext(
            ticker="삼성전자", symbol="005930.KS", source="yfinance",
            current=70000.0, change_pct=0.5,
            rsi=50.0, volume_ratio=1.0,
            week52_high=80000.0, week52_low=60000.0,
            volume_surge=False, success=True,
        )

        week1 = datetime(2026, 4, 7, tzinfo=timezone.utc)   # ISO week 15
        week2 = datetime(2026, 4, 14, tzinfo=timezone.utc)  # ISO week 16
        cache: dict = {}

        with patch("backtest._build_price_context_historical", return_value=mock_ctx) as mock_build:
            await cross_analyze_historical(
                direction="BUY", strength=3,
                tickers=["삼성전자"], ticker_symbols={"삼성전자": "005930.KS"},
                as_of_date=week1, _ctx_cache=cache,
            )
            await cross_analyze_historical(
                direction="BUY", strength=3,
                tickers=["삼성전자"], ticker_symbols={"삼성전자": "005930.KS"},
                as_of_date=week2, _ctx_cache=cache,
            )
            assert mock_build.call_count == 2  # different week → two fetches
