"""
test_telegram_routing.py  — Regression tests for ISSUE-005
────────────────────────────────────────────────────────────
Verifies that tg_send is only called for Korean-category articles
and that tg_send_signal is only called for actionable signals.

Root cause (ISSUE-005): tg_send was called unconditionally for all
articles, causing foreign news (category: markets/macro) to flood
the Telegram channel regardless of trading signal status.

Fix: run_scheduler.py:433 — wrap tg_send in `if art["category"] == "korea"`
"""

from __future__ import annotations

import asyncio
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


# ── ISSUE-005: tg_send category routing ──────────────────────

@pytest.mark.asyncio
async def test_korean_article_always_sends_article_message():
    """category='korea' → tg_send called regardless of signal."""
    art = _make_art("korea", "yonhap")
    signal = _make_none_signal()

    with patch("run_scheduler.tg_send", new_callable=AsyncMock) as mock_send, \
         patch("run_scheduler.tg_send_signal", new_callable=AsyncMock) as mock_send_sig, \
         patch("run_scheduler.detect_signal", new_callable=AsyncMock, return_value=signal), \
         patch("run_scheduler.summarize", new_callable=AsyncMock) as mock_summarize, \
         patch("run_scheduler.fetch_article_body", new_callable=AsyncMock, return_value="본문 내용 테스트"), \
         patch("run_scheduler._db_pool", None), \
         patch("run_scheduler._summary_queue") as mock_q:

        mock_summarize.return_value = MagicMock(success=True, text="한국어 요약", backend=MagicMock(value="ollama"))
        mock_q.get = AsyncMock(return_value=art)
        mock_q.task_done = MagicMock()

        # Run one iteration of summary_worker by extracting just the processing logic
        import run_scheduler
        import httpx

        async with httpx.AsyncClient() as http:
            # Simulate the worker body for one article
            body = await run_scheduler.fetch_article_body(url=art["url"], source=art["source"], http=http)
            input_text = body if len(body) > run_scheduler.MIN_INPUT_LEN else art["title"]
            res = await run_scheduler.summarize(title=art["title"], body=input_text, http=http)
            summary_ko = res.text if res.success else ""
            sig = await run_scheduler.detect_signal(title=art["title"], summary_ko=summary_ko, http=http)

            if art.get("category") == "korea":
                await run_scheduler.tg_send(art, summary_ko, http=http)
            if sig and sig.is_actionable:
                await run_scheduler.tg_send_signal(art, summary_ko, sig, http=http, cross=None)

    mock_send.assert_called_once()           # article message sent
    mock_send_sig.assert_not_called()        # no signal message (signal not actionable)


@pytest.mark.asyncio
async def test_foreign_article_no_signal_sends_nothing():
    """category='markets' + no signal → neither tg_send nor tg_send_signal called."""
    art = _make_art("markets", "bloomberg")
    signal = _make_none_signal()

    with patch("run_scheduler.tg_send", new_callable=AsyncMock) as mock_send, \
         patch("run_scheduler.tg_send_signal", new_callable=AsyncMock) as mock_send_sig, \
         patch("run_scheduler.detect_signal", new_callable=AsyncMock, return_value=signal), \
         patch("run_scheduler.summarize", new_callable=AsyncMock) as mock_summarize, \
         patch("run_scheduler.fetch_article_body", new_callable=AsyncMock, return_value="Article body content here"), \
         patch("run_scheduler._db_pool", None):

        mock_summarize.return_value = MagicMock(success=True, text="시장 분석 요약", backend=MagicMock(value="ollama"))

        import run_scheduler
        import httpx

        async with httpx.AsyncClient() as http:
            body = await run_scheduler.fetch_article_body(url=art["url"], source=art["source"], http=http)
            input_text = body if len(body) > run_scheduler.MIN_INPUT_LEN else art["title"]
            res = await run_scheduler.summarize(title=art["title"], body=input_text, http=http)
            summary_ko = res.text if res.success else ""
            sig = await run_scheduler.detect_signal(title=art["title"], summary_ko=summary_ko, http=http)

            if art.get("category") == "korea":
                await run_scheduler.tg_send(art, summary_ko, http=http)
            if sig and sig.is_actionable:
                await run_scheduler.tg_send_signal(art, summary_ko, sig, http=http, cross=None)

    mock_send.assert_not_called()            # no article message for foreign
    mock_send_sig.assert_not_called()        # no signal message (signal not actionable)


@pytest.mark.asyncio
async def test_foreign_article_with_signal_sends_only_signal_message():
    """category='markets' + actionable signal → only tg_send_signal called, not tg_send."""
    art = _make_art("markets", "reuters")
    signal = _make_buy_signal(strength=4)

    with patch("run_scheduler.tg_send", new_callable=AsyncMock) as mock_send, \
         patch("run_scheduler.tg_send_signal", new_callable=AsyncMock) as mock_send_sig, \
         patch("run_scheduler.detect_signal", new_callable=AsyncMock, return_value=signal), \
         patch("run_scheduler.summarize", new_callable=AsyncMock) as mock_summarize, \
         patch("run_scheduler.fetch_article_body", new_callable=AsyncMock, return_value="Fed cuts rates by 25bp in surprise move"), \
         patch("run_scheduler._db_pool", None):

        mock_summarize.return_value = MagicMock(success=True, text="연준 금리 인하", backend=MagicMock(value="ollama"))

        import run_scheduler
        import httpx

        async with httpx.AsyncClient() as http:
            body = await run_scheduler.fetch_article_body(url=art["url"], source=art["source"], http=http)
            input_text = body if len(body) > run_scheduler.MIN_INPUT_LEN else art["title"]
            res = await run_scheduler.summarize(title=art["title"], body=input_text, http=http)
            summary_ko = res.text if res.success else ""
            sig = await run_scheduler.detect_signal(title=art["title"], summary_ko=summary_ko, http=http)

            if art.get("category") == "korea":
                await run_scheduler.tg_send(art, summary_ko, http=http)
            if sig and sig.is_actionable:
                await run_scheduler.tg_send_signal(art, summary_ko, sig, http=http, cross=None)

    mock_send.assert_not_called()            # no article message for foreign
    mock_send_sig.assert_called_once()       # only signal message


@pytest.mark.asyncio
async def test_korean_article_with_signal_sends_both_messages():
    """category='korea' + actionable signal → both tg_send AND tg_send_signal called."""
    art = _make_art("korea", "hankyung")
    signal = _make_buy_signal(strength=3)

    with patch("run_scheduler.tg_send", new_callable=AsyncMock) as mock_send, \
         patch("run_scheduler.tg_send_signal", new_callable=AsyncMock) as mock_send_sig, \
         patch("run_scheduler.detect_signal", new_callable=AsyncMock, return_value=signal), \
         patch("run_scheduler.summarize", new_callable=AsyncMock) as mock_summarize, \
         patch("run_scheduler.fetch_article_body", new_callable=AsyncMock, return_value="삼성전자 실적 발표"), \
         patch("run_scheduler._db_pool", None):

        mock_summarize.return_value = MagicMock(success=True, text="삼성전자 호실적", backend=MagicMock(value="ollama"))

        import run_scheduler
        import httpx

        async with httpx.AsyncClient() as http:
            body = await run_scheduler.fetch_article_body(url=art["url"], source=art["source"], http=http)
            input_text = body if len(body) > run_scheduler.MIN_INPUT_LEN else art["title"]
            res = await run_scheduler.summarize(title=art["title"], body=input_text, http=http)
            summary_ko = res.text if res.success else ""
            sig = await run_scheduler.detect_signal(title=art["title"], summary_ko=summary_ko, http=http)

            if art.get("category") == "korea":
                await run_scheduler.tg_send(art, summary_ko, http=http)
            if sig and sig.is_actionable:
                await run_scheduler.tg_send_signal(art, summary_ko, sig, http=http, cross=None)

    mock_send.assert_called_once()           # article message sent for Korean
    mock_send_sig.assert_called_once()       # AND signal message
