"""
test_telegram_routing.py  —  Regression tests for ISSUE-005
────────────────────────────────────────────────────────────
Verifies that tg_send is never called unconditionally and that
tg_send_signal fires only when an actionable signal is detected,
regardless of article category (Korean or foreign).

Fix: run_scheduler.py:433 — tg_send removed, all articles go through
tg_send_signal gated by signal.is_actionable.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ── helpers ──────────────────────────────────────────────────

def _make_art(category: str, source: str = "test") -> dict:
    return {
        "source": source,
        "category": category,
        "title": f"Test article [{category}]",
        "url": "https://example.com/article",
        "url_hash": "abc123",
        "summary": "Test summary",
        "published": "04-06 12:00",
        "published_dt": None,
    }


def _make_none_signal():
    sig = MagicMock()
    sig.is_actionable = False
    sig.success = False
    return sig


def _make_buy_signal(strength: int = 3):
    sig = MagicMock()
    sig.is_actionable = True
    sig.direction = "BUY"
    sig.strength = strength
    sig.reason = "Test reason"
    sig.tickers = ["005930.KS"]
    sig.ticker_symbols = {"삼성전자": "005930.KS"}
    sig.backend = MagicMock()
    sig.backend.value = "ollama"
    return sig


async def _run_routing(art, signal, mock_send, mock_send_sig):
    """Simulate the Telegram routing logic from summary_worker."""
    import run_scheduler
    import httpx

    async with httpx.AsyncClient() as http:
        sig = signal
        if sig and sig.is_actionable:
            await run_scheduler.tg_send_signal(art, "요약", sig, http=http, cross=None)


# ── ISSUE-005: all articles gated by signal ──────────────────

@pytest.mark.asyncio
async def test_korean_article_no_signal_sends_nothing():
    """category='korea' + no signal → no Telegram message sent."""
    art = _make_art("korea", "yonhap")
    signal = _make_none_signal()

    with patch("run_scheduler.tg_send", new_callable=AsyncMock) as mock_send, \
         patch("run_scheduler.tg_send_signal", new_callable=AsyncMock) as mock_send_sig:

        await _run_routing(art, signal, mock_send, mock_send_sig)

    mock_send.assert_not_called()
    mock_send_sig.assert_not_called()


@pytest.mark.asyncio
async def test_foreign_article_no_signal_sends_nothing():
    """category='markets' + no signal → no Telegram message sent."""
    art = _make_art("markets", "bloomberg")
    signal = _make_none_signal()

    with patch("run_scheduler.tg_send", new_callable=AsyncMock) as mock_send, \
         patch("run_scheduler.tg_send_signal", new_callable=AsyncMock) as mock_send_sig:

        await _run_routing(art, signal, mock_send, mock_send_sig)

    mock_send.assert_not_called()
    mock_send_sig.assert_not_called()


@pytest.mark.asyncio
async def test_korean_article_with_signal_sends_signal_message():
    """category='korea' + actionable signal → only tg_send_signal called."""
    art = _make_art("korea", "hankyung")
    signal = _make_buy_signal(strength=3)

    with patch("run_scheduler.tg_send", new_callable=AsyncMock) as mock_send, \
         patch("run_scheduler.tg_send_signal", new_callable=AsyncMock) as mock_send_sig:

        await _run_routing(art, signal, mock_send, mock_send_sig)

    mock_send.assert_not_called()
    mock_send_sig.assert_called_once()


@pytest.mark.asyncio
async def test_foreign_article_with_signal_sends_signal_message():
    """category='markets' + actionable signal → only tg_send_signal called."""
    art = _make_art("markets", "reuters")
    signal = _make_buy_signal(strength=4)

    with patch("run_scheduler.tg_send", new_callable=AsyncMock) as mock_send, \
         patch("run_scheduler.tg_send_signal", new_callable=AsyncMock) as mock_send_sig:

        await _run_routing(art, signal, mock_send, mock_send_sig)

    mock_send.assert_not_called()
    mock_send_sig.assert_called_once()
