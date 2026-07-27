"""
test_article_type.py  —  Article type classification feature tests
────────────────────────────────────────────────────────────────────
Covers:
  - TradeSignal.article_type default value
  - _parse_signal_json(): valid type, unknown type → "other", missing → "other",
    case-insensitive normalisation, exception path → NONE_SIGNAL.article_type
  - save_signal() / fetch_latest_signals(): article_type persisted and retrieved
  - send_signal(): badge prepended for non-"other" types, suppressed for "other",
    all 7 non-other badge values correct
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ── signal_detector tests ─────────────────────────────────────────────────────

class TestTradeSignalDefault:
    """TradeSignal.article_type defaults to 'other'."""

    def test_default_article_type(self):
        from analysis.signal_detector import TradeSignal
        from reports.summarizer import Backend

        sig = TradeSignal(
            direction="BUY", strength=3, reason="테스트",
            tickers=[], ticker_symbols={},
            backend=Backend.OLLAMA, success=True,
        )
        assert sig.article_type == "other"

    def test_explicit_article_type(self):
        from analysis.signal_detector import TradeSignal
        from reports.summarizer import Backend

        sig = TradeSignal(
            direction="BUY", strength=3, reason="테스트",
            tickers=[], ticker_symbols={},
            backend=Backend.OLLAMA, success=True,
            article_type="earnings",
        )
        assert sig.article_type == "earnings"

    def test_none_signal_article_type(self):
        from analysis.signal_detector import NONE_SIGNAL
        assert NONE_SIGNAL.article_type == "other"


class TestParseSignalJsonArticleType:
    """_parse_signal_json() correctly extracts and validates article_type."""

    def _make_raw(self, **overrides) -> str:
        data = {
            "direction": "BUY",
            "strength": 3,
            "reason": "테스트 근거",
            "tickers": [],
            "article_type": "earnings",
        }
        data.update(overrides)
        return json.dumps(data)

    def test_valid_type_returned(self):
        from analysis.signal_detector import _parse_signal_json
        from reports.summarizer import Backend

        for t in ("earnings", "ma", "management", "analyst",
                  "regulatory", "product", "macro", "other"):
            raw = self._make_raw(article_type=t)
            sig = _parse_signal_json(raw, Backend.OLLAMA)
            assert sig.article_type == t, f"expected {t}, got {sig.article_type}"

    def test_unknown_type_maps_to_other(self):
        from analysis.signal_detector import _parse_signal_json
        from reports.summarizer import Backend

        raw = self._make_raw(article_type="politics")
        sig = _parse_signal_json(raw, Backend.OLLAMA)
        assert sig.article_type == "other"

    def test_missing_type_defaults_to_other(self):
        from analysis.signal_detector import _parse_signal_json
        from reports.summarizer import Backend

        data = {"direction": "BUY", "strength": 3, "reason": "테스트", "tickers": []}
        sig = _parse_signal_json(json.dumps(data), Backend.OLLAMA)
        assert sig.article_type == "other"

    def test_case_insensitive_normalisation(self):
        from analysis.signal_detector import _parse_signal_json
        from reports.summarizer import Backend

        for raw_type, expected in [("EARNINGS", "earnings"), ("MA", "ma"), ("Macro", "macro")]:
            raw = self._make_raw(article_type=raw_type)
            sig = _parse_signal_json(raw, Backend.OLLAMA)
            assert sig.article_type == expected, f"{raw_type!r} should map to {expected!r}"

    def test_parse_failure_returns_other(self):
        from analysis.signal_detector import _parse_signal_json
        from reports.summarizer import Backend

        sig = _parse_signal_json("this is not json at all", Backend.OLLAMA)
        assert sig.article_type == "other"
        assert not sig.success


# ── DB tests ──────────────────────────────────────────────────────────────────

class TestSaveSignalArticleType:
    """save_signal() passes article_type to the DB INSERT."""

    @pytest.mark.asyncio
    async def test_article_type_included_in_insert(self):
        """save_signal() includes article_type in the INSERT query."""
        from core.db import save_signal

        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value={"id": 42})
        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_conn),
            __aexit__=AsyncMock(return_value=False),
        ))

        result = await save_signal(
            mock_pool,
            article_id=1,
            direction="BUY",
            strength=3,
            reason="test",
            tickers=[],
            llm_backend="ollama",
            article_type="earnings",
        )

        assert result == 42
        call_args = mock_conn.fetchrow.call_args
        sql = call_args[0][0]
        # article_type column and placeholder both present
        assert "article_type" in sql
        assert "$9" in sql
        # value passed
        assert "earnings" in call_args[0]

    @pytest.mark.asyncio
    async def test_default_article_type_is_other(self):
        """save_signal() defaults article_type to 'other' when omitted."""
        from core.db import save_signal

        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value={"id": 1})
        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_conn),
            __aexit__=AsyncMock(return_value=False),
        ))

        await save_signal(
            mock_pool,
            article_id=1,
            direction="BUY",
            strength=3,
            reason="test",
            tickers=[],
            llm_backend="ollama",
        )

        call_args = mock_conn.fetchrow.call_args
        assert "other" in call_args[0]


class TestFetchLatestSignalsArticleType:
    """fetch_latest_signals() SELECTs article_type from trade_signals."""

    @pytest.mark.asyncio
    async def test_article_type_in_select(self):
        from core.db import fetch_latest_signals

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_conn),
            __aexit__=AsyncMock(return_value=False),
        ))

        await fetch_latest_signals(mock_pool)

        call_args = mock_conn.fetch.call_args
        sql = call_args[0][0]
        assert "article_type" in sql

    @pytest.mark.asyncio
    async def test_article_type_in_select_with_direction_filter(self):
        from core.db import fetch_latest_signals

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_conn),
            __aexit__=AsyncMock(return_value=False),
        ))

        await fetch_latest_signals(mock_pool, direction="BUY")

        call_args = mock_conn.fetch.call_args
        sql = call_args[0][0]
        assert "article_type" in sql


# ── Telegram badge tests ──────────────────────────────────────────────────────

@dataclass
class _FakeSignal:
    direction: str = "BUY"
    strength: int = 3
    reason: str = "테스트 근거"
    tickers: Optional[list] = None
    ticker_symbols: Optional[dict] = None
    backend: str = "ollama"
    success: bool = True
    article_type: str = "other"

    def __post_init__(self):
        if self.tickers is None:
            self.tickers = []
        if self.ticker_symbols is None:
            self.ticker_symbols = {}

    @property
    def is_actionable(self):
        return True


_FAKE_ART = {
    "source": "reuters",
    "title": "Test Article",
    "url": "https://example.com",
    "published": "04-16 10:00",
}


async def _capture_signal_message(signal) -> str:
    """Run send_signal and return the captured message text."""
    captured = {}

    async def _fake_post(http, token, chat_id, text, label=""):
        captured["text"] = text
        return True

    with patch("telegram.telegram_notify._get_token", return_value="tok"), \
         patch("telegram.telegram_notify._get_chat_id", return_value="123"), \
         patch("telegram.telegram_notify._get_channel_id", return_value=""), \
         patch("telegram.telegram_notify._post_message", side_effect=_fake_post):
        from telegram.telegram_notify import send_signal
        await send_signal(_FAKE_ART, "테스트 요약", signal)

    return captured.get("text", "")


class TestSendSignalBadge:
    """send_signal() prepends type badge for non-'other' article types."""

    @pytest.mark.asyncio
    async def test_non_other_type_gets_badge(self):
        """earnings type → 📊 appears at the start of the message."""
        sig = _FakeSignal(article_type="earnings")
        msg = await _capture_signal_message(sig)
        assert "📊" in msg

    @pytest.mark.asyncio
    async def test_other_type_no_badge(self):
        """article_type='other' → none of the type badges appear as prefix."""
        sig = _FakeSignal(article_type="other")
        msg = await _capture_signal_message(sig)
        # 🤝🔍⚖️🚀🌐👤📊 as prefixes should not appear
        # (🟢/🔴/🟡 are direction icons — those are fine)
        # Simplest check: message starts with a direction icon, not a type badge
        first_line = msg.split("\n")[0]
        for badge in ("🤝", "🔍", "⚖️", "🚀", "🌐", "👤"):
            assert badge not in first_line[:4], f"Badge {badge} should not prefix 'other' message"

    @pytest.mark.asyncio
    async def test_all_non_other_types_have_badges(self):
        """Every non-'other' type has a badge defined and it appears in the message."""
        from telegram.telegram_notify import TYPE_BADGE
        non_other = {k: v for k, v in TYPE_BADGE.items() if k != "other"}
        for t, badge in non_other.items():
            sig = _FakeSignal(article_type=t)
            msg = await _capture_signal_message(sig)
            assert badge in msg, f"Badge {badge!r} for type {t!r} not found in message"

    @pytest.mark.asyncio
    async def test_seven_non_other_types_defined(self):
        """TYPE_BADGE has exactly 7 non-'other' entries."""
        from telegram.telegram_notify import TYPE_BADGE
        non_other = [k for k in TYPE_BADGE if k != "other"]
        assert len(non_other) == 7

    @pytest.mark.asyncio
    async def test_esc_function_escapes_backtick(self):
        """send_signal local esc() must escape backticks (regression: ISSUE-002 pattern)."""
        sig = _FakeSignal(reason="테스트`근거")
        msg = await _capture_signal_message(sig)
        assert "테스트\\`근거" in msg, "Backtick in reason not escaped"
