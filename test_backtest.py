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
from backtest import _esc, calculate_metrics


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
    """asyncpg pool mock — conn.fetch() 가 rows를 반환."""
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=[
        {
            "verdict": r["verdict"],
            "direction": r["direction"],
            "score": r.get("score", 5),
            "checkpoint": r.get("checkpoint", "1d"),
            "return_pct": r["return_pct"],
        }
        for r in rows
    ])
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
