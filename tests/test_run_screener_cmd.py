"""
test_run_screener_cmd.py  —  /run_screener 봇 명령어 단위 테스트

옛 test_scan_cmd.py는 제거된 _handle_scan을 대상으로 했다. 락 체크 +
실전 스크리닝 실행 + 저장 + invoker 전용 발송 동작은 이제
_handle_run_screener(가드) + _run_screener_task(실행)로 나뉘어 있다.
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch
import asyncio

import pytest


@dataclass
class _FakeResult:
    ticker: str
    name: str
    close: float
    ma_20w: float
    ma_60w: float
    cloud_top: float
    is_enhanced: bool = False
    has_gapjum: bool = False
    screened_at: str = ""
    week_of: str = "2026-W16"
    sector: str = "반도체"
    ma_120w: float | None = None


_SAMPLE = [_FakeResult("005930.KS", "삼성전자", 80000, 75000, 70000, 79000)]


class TestHandleRunScreener:

    @pytest.mark.asyncio
    async def test_no_pool_sends_error(self):
        """/run_screener pool=None 시 오류 메시지 전송."""
        import telegram.telegram_bot as telegram_bot
        telegram_bot._scan_lock = asyncio.Lock()

        mock_notify = AsyncMock()
        with patch("telegram.telegram_bot._send_plain", mock_notify):
            from telegram.telegram_bot import _handle_run_screener
            await _handle_run_screener(AsyncMock(), "CHAT_X", None)

        sent_texts = [c.args[2] for c in mock_notify.call_args_list]
        assert any("DB 미연결" in t for t in sent_texts)

    @pytest.mark.asyncio
    async def test_locked_returns_busy_message(self):
        """/run_screener 실행 중 재호출 시 '이미 실행 중' 메시지 전송."""
        import telegram.telegram_bot as telegram_bot
        telegram_bot._scan_lock = asyncio.Lock()

        mock_notify = AsyncMock()
        with patch("telegram.telegram_bot._send_plain", mock_notify):
            from telegram.telegram_bot import _handle_run_screener
            async with telegram_bot._scan_lock:
                await _handle_run_screener(AsyncMock(), "SECOND_USER", AsyncMock())

        sent_texts = [c.args[2] for c in mock_notify.call_args_list]
        assert any("이미 실행 중" in t for t in sent_texts), (
            f"'이미 실행 중' 메시지가 없습니다. 실제 전송: {sent_texts}"
        )


class TestRunScreenerTask:

    @pytest.mark.asyncio
    async def test_runs_live_screen_and_saves(self):
        """_run_screener_task는 run_weekly_screen()을 실행하고 DB에 저장해야 합니다."""
        import telegram.telegram_bot as telegram_bot
        telegram_bot._scan_lock = asyncio.Lock()

        mock_screen = MagicMock(return_value=_SAMPLE)
        mock_save = AsyncMock(return_value=1)
        mock_send = AsyncMock(return_value=True)

        with (
            patch("analysis.chart_screener.run_weekly_screen", mock_screen),
            patch("core.db.save_chart_signals", mock_save),
            patch("telegram.telegram_notify.send_weekly_screener", mock_send),
        ):
            from telegram.telegram_bot import _run_screener_task
            await _run_screener_task(AsyncMock(), "CHAT_123", AsyncMock())

        mock_screen.assert_called_once()
        mock_save.assert_called_once()

    @pytest.mark.asyncio
    async def test_sends_only_to_invoker(self):
        """_run_screener_task 결과는 명령어 발신자에게만 전송 (채널 브로드캐스트 없음)."""
        import telegram.telegram_bot as telegram_bot
        telegram_bot._scan_lock = asyncio.Lock()

        mock_send = AsyncMock(return_value=True)

        with (
            patch("analysis.chart_screener.run_weekly_screen", MagicMock(return_value=_SAMPLE)),
            patch("core.db.save_chart_signals", AsyncMock(return_value=1)),
            patch("telegram.telegram_notify.send_weekly_screener", mock_send),
        ):
            from telegram.telegram_bot import _run_screener_task
            await _run_screener_task(AsyncMock(), "INVOKER_999", AsyncMock())

        mock_send.assert_called_once()
        kwargs = mock_send.call_args.kwargs
        assert kwargs.get("target_chat_id") == "INVOKER_999"
