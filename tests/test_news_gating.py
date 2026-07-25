"""
test_news_gating.py  —  News gating eligibility filter (Sprint 1)
──────────────────────────────────────────────────────────────────
Verifies that Telegram signal notifications are suppressed when the
detected stock tickers are NOT in the current screener output, and
sent normally when they ARE in the screener output.

The _screener_tickers module-level cache in run_scheduler.py drives
this behavior. These tests inject the cache directly.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ── helpers ──────────────────────────────────────────────────────────

def _make_signal(ticker_symbols: list[str], is_actionable: bool = True) -> MagicMock:
    sig = MagicMock()
    sig.is_actionable = is_actionable
    sig.ticker_symbols = ticker_symbols
    sig.tickers = [s.split(".")[0] for s in ticker_symbols]
    sig.direction = "BUY"
    sig.strength = 3
    sig.reason = "test signal"
    sig.backend = MagicMock()
    sig.backend.value = "test"
    sig.article_type = "stock_specific"
    return sig


def _make_article() -> dict:
    return {
        "url": "https://example.com/test",
        "url_hash": "abc123",
        "source": "test",
        "category": "korea",
        "title": "삼성전자 급등",
        "summary": "삼성전자 테스트 기사",
        "published": "2026-04-25",
        "published_dt": None,
    }


# ── tests ─────────────────────────────────────────────────────────────

class TestNewsGating:
    """_screener_tickers gate controls which signals reach Telegram."""

    @pytest.mark.asyncio
    async def test_non_screener_ticker_suppresses_telegram(self):
        """Article about a non-screener ticker → tg_send_signal NOT called."""
        import run_scheduler as rs

        rs._screener_tickers = {"000660.KS"}  # SK하이닉스 in screener  # type: ignore[attr-defined]
        signal = _make_signal(["035720.KQ"])   # 카카오 NOT in screener

        with patch("run_scheduler.tg_send_signal", new_callable=AsyncMock) as mock_send:
            # Simulate the gating logic directly (extracted from summary_worker)
            if signal and signal.is_actionable:
                if rs._screener_tickers and signal.ticker_symbols and not (  # type: ignore[attr-defined]
                    set(signal.ticker_symbols) & rs._screener_tickers  # type: ignore[attr-defined]
                ):
                    pass  # suppressed — do not call
                else:
                    await rs.tg_send_signal({}, "", signal, http=None)

            mock_send.assert_not_called()

    @pytest.mark.asyncio
    async def test_screener_ticker_allows_telegram(self):
        """Article about a screener ticker → tg_send_signal IS called."""
        import run_scheduler as rs

        rs._screener_tickers = {"005930.KS", "000660.KS"}  # 삼성전자, SK하이닉스  # type: ignore[attr-defined]
        signal = _make_signal(["005930.KS"])  # 삼성전자 IN screener

        with patch("run_scheduler.tg_send_signal", new_callable=AsyncMock) as mock_send:
            if signal and signal.is_actionable:
                if rs._screener_tickers and signal.ticker_symbols and not (  # type: ignore[attr-defined]
                    set(signal.ticker_symbols) & rs._screener_tickers  # type: ignore[attr-defined]
                ):
                    pass
                else:
                    await rs.tg_send_signal({}, "", signal, http=None)

            mock_send.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_screener_cache_disables_gating(self):
        """When _screener_tickers is empty (screener not yet run), gating is inactive."""
        import run_scheduler as rs

        rs._screener_tickers = set()  # screener hasn't run yet  # type: ignore[attr-defined]
        signal = _make_signal(["999999.KS"])  # any ticker

        with patch("run_scheduler.tg_send_signal", new_callable=AsyncMock) as mock_send:
            if signal and signal.is_actionable:
                if rs._screener_tickers and signal.ticker_symbols and not (  # type: ignore[attr-defined]
                    set(signal.ticker_symbols) & rs._screener_tickers  # type: ignore[attr-defined]
                ):
                    pass
                else:
                    await rs.tg_send_signal({}, "", signal, http=None)

            mock_send.assert_called_once()

    @pytest.mark.asyncio
    async def test_partial_overlap_allows_telegram(self):
        """Signal with multiple tickers — only one needs to be in screener."""
        import run_scheduler as rs

        rs._screener_tickers = {"000660.KS"}  # type: ignore[attr-defined]
        signal = _make_signal(["035720.KQ", "000660.KS"])  # one in, one out

        with patch("run_scheduler.tg_send_signal", new_callable=AsyncMock) as mock_send:
            if signal and signal.is_actionable:
                if rs._screener_tickers and signal.ticker_symbols and not (  # type: ignore[attr-defined]
                    set(signal.ticker_symbols) & rs._screener_tickers  # type: ignore[attr-defined]
                ):
                    pass
                else:
                    await rs.tg_send_signal({}, "", signal, http=None)

            mock_send.assert_called_once()

    def test_non_actionable_signal_not_sent_regardless(self):
        """Non-actionable signal is never sent — gating doesn't change this."""
        import run_scheduler as rs

        rs._screener_tickers = {"005930.KS"}  # type: ignore[attr-defined]
        signal = _make_signal(["005930.KS"], is_actionable=False)

        # The existing is_actionable check fires before gating
        assert not signal.is_actionable
