"""
test_screener_telegram.regression-1.py  —  Regression tests for ISSUE-001/002
────────────────────────────────────────────────────────────
ISSUE-001: esc(r.ticker) inside MarkdownV2 code spans escaped '.' in tickers
           like '005930.KS' to '005930\\.KS', rendering a literal backslash.
           Fix: esc_code() escapes only backtick and backslash inside code spans.

ISSUE-002: local esc() in send_weekly_screener was missing backtick from escape
           list, inconsistent with the rest of telegram_notify.py.

Found by /qa on 2026-04-16
Report: .gstack/qa-reports/qa-report-test_feed-2026-04-16.md
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, patch

import pytest


@dataclass
class _FakeResult:
    ticker: str
    name: str
    close: float
    ma_20w: float
    ma_60w: float
    cloud_top: float
    is_enhanced: bool
    has_gapjum: bool
    screened_at: str
    week_of: str


def _make_result(ticker="005930.KS", name="삼성전자", has_gapjum=False):
    return _FakeResult(
        ticker=ticker,
        name=name,
        close=75000.0,
        ma_20w=70000.0,
        ma_60w=65000.0,
        cloud_top=72000.0,
        is_enhanced=False,
        has_gapjum=has_gapjum,
        screened_at="2026-04-16T00:00:00+00:00",
        week_of="2026-W16",
    )


async def _capture_message(results):
    """Run send_weekly_screener and return the captured message text."""
    captured = {}

    async def _fake_post(http, token, chat_id, text, label=""):
        captured["text"] = text
        return True

    with patch("telegram_notify._get_token", return_value="tok"), \
         patch("telegram_notify._get_chat_id", return_value="123"), \
         patch("telegram_notify._get_channel_id", return_value=""), \
         patch("telegram_notify._post_message", side_effect=_fake_post), \
         patch("chart_screener.current_week_of", return_value="2026-W16"):
        from telegram_notify import send_weekly_screener
        await send_weekly_screener(results)

    return captured.get("text", "")


class TestIssue001TickerCodeSpan:
    """Regression: ISSUE-001 — dot in ticker must not be backslash-escaped inside code span."""

    @pytest.mark.asyncio
    async def test_kospi_ticker_dot_not_escaped_in_code_span(self):
        """005930.KS should appear as `005930.KS`, not `005930\\.KS`."""
        results = [_make_result(ticker="005930.KS")]
        msg = await _capture_message(results)
        # The ticker must appear verbatim inside the code span
        assert "`005930.KS`" in msg, (
            f"Ticker dot was over-escaped. Got: {msg!r}"
        )
        assert r"`005930\.KS`" not in msg, (
            f"Ticker dot should not be backslash-escaped inside code span. Got: {msg!r}"
        )

    @pytest.mark.asyncio
    async def test_kosdaq_ticker_dot_not_escaped(self):
        """035720.KQ — KOSDAQ tickers have dots too."""
        results = [_make_result(ticker="035720.KQ", name="카카오")]
        msg = await _capture_message(results)
        assert "`035720.KQ`" in msg
        assert r"`035720\.KQ`" not in msg

    @pytest.mark.asyncio
    async def test_ticker_with_no_special_chars_unchanged(self):
        """Plain alphanumeric ticker passes through unchanged."""
        results = [_make_result(ticker="AAPL", name="Apple")]
        msg = await _capture_message(results)
        assert "`AAPL`" in msg


class TestIssue002EscFunctionConsistency:
    """Regression: ISSUE-002 — esc() in send_weekly_screener must escape backtick."""

    @pytest.mark.asyncio
    async def test_backtick_in_stock_name_is_escaped(self):
        """Backtick in a stock name should be escaped to avoid breaking MarkdownV2."""
        results = [_make_result(name="테스트`종목")]
        msg = await _capture_message(results)
        # The backtick in name must be escaped: `테스트\`종목`
        assert "테스트\\`종목" in msg, (
            f"Backtick in name not escaped. Got: {msg!r}"
        )


class TestEmptyResults:
    """send_weekly_screener with no results still sends a message."""

    @pytest.mark.asyncio
    async def test_empty_results_sends_no_candidates_message(self):
        msg = await _capture_message([])
        assert "통과 종목 없음" in msg or "없음" in msg

    @pytest.mark.asyncio
    async def test_empty_results_includes_week(self):
        msg = await _capture_message([])
        assert "2026-W16" in msg or "W16" in msg


class TestSortingAndTruncation:
    """has_gapjum=True stocks sorted first; > 20 results triggers truncation note."""

    @pytest.mark.asyncio
    async def test_gapjum_star_appears_for_flagged_stocks(self):
        results = [_make_result(ticker="005930.KS", has_gapjum=True)]
        msg = await _capture_message(results)
        assert "★" in msg

    @pytest.mark.asyncio
    async def test_over_twenty_results_shows_overflow_note(self):
        results = [_make_result(ticker=f"{i:06d}.KS", name=f"종목{i}") for i in range(25)]
        msg = await _capture_message(results)
        assert "외" in msg and "종목" in msg
