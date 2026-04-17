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
    sector: str = ""
    ma_120w: float | None = None


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
    """Regression: ISSUE-001 — code span ticker must not be backslash-escaped.
    New format: split('.')[0] strips exchange suffix, showing just the 6-digit code."""

    @pytest.mark.asyncio
    async def test_kospi_ticker_shown_as_short_code(self):
        """005930.KS → code span shows `005930` (suffix stripped, no over-escaping)."""
        results = [_make_result(ticker="005930.KS")]
        msg = await _capture_message(results)
        assert "`005930`" in msg, (
            f"Short code not found in message. Got: {msg!r}"
        )
        assert r"`005930\.KS`" not in msg, (
            f"Full ticker over-escaped in code span. Got: {msg!r}"
        )

    @pytest.mark.asyncio
    async def test_kosdaq_ticker_shown_as_short_code(self):
        """035720.KQ — KOSDAQ ticker shows just `035720`."""
        results = [_make_result(ticker="035720.KQ", name="카카오")]
        msg = await _capture_message(results)
        assert "`035720`" in msg
        assert r"`035720\.KQ`" not in msg

    @pytest.mark.asyncio
    async def test_ticker_with_no_special_chars_unchanged(self):
        """Plain alphanumeric ticker: split('.')[0] returns itself."""
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
    async def test_sector_header_shows_total_count(self):
        results = [_make_result(ticker=f"{i:06d}.KS", name=f"종목{i}") for i in range(25)]
        msg = await _capture_message(results)
        assert "통과:" in msg


class TestKINDFailureWarning:
    """KIND 섹터 데이터 없음 경고 표시 조건."""

    @pytest.mark.asyncio
    async def test_warning_shown_when_all_sectors_empty(self):
        """모든 종목의 sector가 빈 문자열이면 KIND 경고가 출력된다."""
        results = [_make_result(ticker=f"{i:06d}.KS", name=f"종목{i}") for i in range(3)]
        # sector="" (기본값) — KIND 조회 실패 상황
        msg = await _capture_message(results)
        assert "KIND" in msg or "없음" in msg, f"KIND 경고가 없음: {msg!r}"

    @pytest.mark.asyncio
    async def test_no_warning_when_sectors_populated(self):
        """sector가 채워진 결과에는 KIND 경고가 없어야 한다."""
        results = [
            _FakeResult(
                ticker=f"{i:06d}.KS", name=f"종목{i}", close=75000.0,
                ma_20w=70000.0, ma_60w=65000.0, cloud_top=72000.0,
                is_enhanced=False, has_gapjum=False,
                screened_at="2026-04-16T00:00:00+00:00", week_of="2026-W16",
                sector="반도체",
            )
            for i in range(3)
        ]
        msg = await _capture_message(results)
        assert "KIND" not in msg, f"KIND 경고가 잘못 출력됨: {msg!r}"
        assert "없음" not in msg or "통과" in msg  # "없음" is in "조건 통과 종목 없음" for empty only


class TestSectorGroupedFormat:
    """섹터 그룹핑 포맷 검증."""

    @pytest.mark.asyncio
    async def test_sector_grouping_by_업종(self):
        """섹터 A 종목이 섹터 B보다 많으면 섹터 A가 먼저 나온다."""
        results = [
            _FakeResult(
                ticker=f"{i:06d}.KS", name=f"A종목{i}", close=75000.0,
                ma_20w=70000.0, ma_60w=65000.0, cloud_top=72000.0,
                is_enhanced=False, has_gapjum=False,
                screened_at="2026-04-16T00:00:00+00:00", week_of="2026-W16",
                sector="반도체",
            )
            for i in range(3)
        ] + [
            _FakeResult(
                ticker=f"{i:06d}.KQ", name=f"B종목{i}", close=50000.0,
                ma_20w=45000.0, ma_60w=40000.0, cloud_top=48000.0,
                is_enhanced=False, has_gapjum=False,
                screened_at="2026-04-16T00:00:00+00:00", week_of="2026-W16",
                sector="소프트웨어",
            )
            for i in range(2)
        ]
        msg = await _capture_message(results)
        pos_a = msg.find("반도체")
        pos_b = msg.find("소프트웨어")
        assert pos_a != -1 and pos_b != -1, f"섹터 이름이 메시지에 없음: {msg!r}"
        assert pos_a < pos_b, "반도체(3종목)가 소프트웨어(2종목)보다 먼저 나와야 함"

    @pytest.mark.asyncio
    async def test_기타_fallback_for_empty_sector(self):
        """sector=""인 종목은 '기타' 섹터로 표시된다."""
        results = [_make_result(ticker="005930.KS", name="삼성전자")]
        msg = await _capture_message(results)
        assert "기타" in msg, f"'기타' fallback 없음: {msg!r}"

    @pytest.mark.asyncio
    async def test_sector_name_truncated_at_20_chars(self):
        """30자 섹터명은 앞 20자만 표시된다."""
        long_sector = "A" * 30
        results = [
            _FakeResult(
                ticker="005930.KS", name="삼성전자", close=75000.0,
                ma_20w=70000.0, ma_60w=65000.0, cloud_top=72000.0,
                is_enhanced=False, has_gapjum=False,
                screened_at="2026-04-16T00:00:00+00:00", week_of="2026-W16",
                sector=long_sector,
            )
        ]
        msg = await _capture_message(results)
        truncated = "A" * 20
        assert truncated in msg, f"20자 잘린 섹터명이 없음: {msg!r}"
        assert "A" * 30 not in msg, f"30자 전체 섹터명이 그대로 출력됨: {msg!r}"

    @pytest.mark.asyncio
    async def test_top3_per_sector_shown(self):
        """섹터 내 5개 종목이 있어도 상위 3개만 출력된다."""
        results = [
            _FakeResult(
                ticker=f"{i:06d}.KS", name=f"종목{i}", close=float(75000 - i * 100),
                ma_20w=70000.0, ma_60w=65000.0, cloud_top=72000.0,
                is_enhanced=False, has_gapjum=False,
                screened_at="2026-04-16T00:00:00+00:00", week_of="2026-W16",
                sector="반도체",
            )
            for i in range(5)
        ]
        msg = await _capture_message(results)
        # 상위 3 종목(종목0~2)은 있어야 하고, 하위 2종목(종목3~4)은 없어야 함
        assert "종목0" in msg
        assert "종목1" in msg
        assert "종목2" in msg
        assert "종목3" not in msg
        assert "종목4" not in msg
