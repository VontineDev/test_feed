"""
test_screener_telegram_regression_2.py  —  Regression test for ISSUE-003
────────────────────────────────────────────────────────────
ISSUE-003: /screener bot command called send_weekly_screener() without
           target_chat_id, causing channel broadcast at any time a user
           sent /screener — not only on the scheduled Sunday 20:30 KST run.

Fix: send_weekly_screener() accepts target_chat_id; when set, sends only
     to that chat and skips channel broadcast.
     _handle_screener() now passes chat_id as target_chat_id.

Found by /investigate on 2026-04-18
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, patch, call

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
    week_of: str = "2026-W15"
    sector: str = "반도체"
    ma_120w: float | None = None
    close_history: list = None  # noqa: RUF009 — stub class only, never passed to _sparkline
    per: float | None = None
    pbr: float | None = None
    eps: float | None = None
    dividend_yield: float | None = None
    foreign_rate: float | None = None
    volume_ratio_4w: float | None = None
    high_w: float | None = None
    volume_w: int | None = None
    foreign_net_buy: float | None = None
    inst_net_buy: float | None = None


_SAMPLE = [
    _FakeResult("005930.KS", "삼성전자", 80000, 75000, 70000, 79000),
]


class TestSendWeeklyScreenerTargetChatId:
    """target_chat_id가 지정되면 채널 브로드캐스트를 건너뜀 (ISSUE-003 회귀)."""

    @pytest.mark.asyncio
    async def test_target_chat_id_skips_channel_broadcast(self):
        """
        Regression: ISSUE-003 — /screener 명령어가 채널에 브로드캐스트되는 문제
        target_chat_id 지정 시 채널 발송 없이 해당 chat_id에만 전송해야 함.
        """
        from telegram.telegram_notify import send_weekly_screener

        mock_post = AsyncMock(return_value=True)
        with (
            patch("telegram.telegram_notify._get_token", return_value="tok"),
            patch("telegram.telegram_notify._get_chat_id", return_value="DEFAULT_DM"),
            patch("telegram.telegram_notify._get_channel_id", return_value="CHANNEL_ID"),
            patch("telegram.telegram_notify._post_message", mock_post),
        ):
            await send_weekly_screener(_SAMPLE, target_chat_id="USER_123")

        # 반드시 USER_123에만 전송, 채널에는 전송 안 됨
        called_targets = [c.args[2] for c in mock_post.call_args_list]
        assert "USER_123" in called_targets, "target_chat_id로 전송해야 합니다"
        assert "CHANNEL_ID" not in called_targets, "채널에 전송하면 안 됩니다"
        assert "DEFAULT_DM" not in called_targets, "env var DM에 전송하면 안 됩니다"

    @pytest.mark.asyncio
    async def test_no_target_chat_id_broadcasts_to_channel(self):
        """target_chat_id 미지정 시 기존대로 DM + 채널 브로드캐스트."""
        from telegram.telegram_notify import send_weekly_screener

        mock_post = AsyncMock(return_value=True)
        with (
            patch("telegram.telegram_notify._get_token", return_value="tok"),
            patch("telegram.telegram_notify._get_chat_id", return_value="DEFAULT_DM"),
            patch("telegram.telegram_notify._get_channel_id", return_value="CHANNEL_ID"),
            patch("telegram.telegram_notify._post_message", mock_post),
        ):
            await send_weekly_screener(_SAMPLE)

        called_targets = [c.args[2] for c in mock_post.call_args_list]
        assert "DEFAULT_DM" in called_targets, "DM에 전송해야 합니다"
        assert "CHANNEL_ID" in called_targets, "채널에도 전송해야 합니다"

    @pytest.mark.asyncio
    async def test_target_chat_id_uses_specified_not_env_dm(self):
        """target_chat_id 지정 시 env var TELEGRAM_CHAT_ID 대신 해당 ID에 전송."""
        from telegram.telegram_notify import send_weekly_screener

        mock_post = AsyncMock(return_value=True)
        with (
            patch("telegram.telegram_notify._get_token", return_value="tok"),
            patch("telegram.telegram_notify._get_chat_id", return_value="DEFAULT_DM"),
            patch("telegram.telegram_notify._get_channel_id", return_value=""),
            patch("telegram.telegram_notify._post_message", mock_post),
        ):
            await send_weekly_screener(_SAMPLE, target_chat_id="CMD_USER")

        called_targets = [c.args[2] for c in mock_post.call_args_list]
        assert "CMD_USER" in called_targets
        assert "DEFAULT_DM" not in called_targets


class TestHandleScreenerPassesChatId:
    """_handle_screener가 send_weekly_screener에 chat_id를 전달하는지 검증 (ISSUE-003)."""

    @pytest.mark.asyncio
    async def test_handle_screener_passes_invoker_chat_id(self):
        """
        Regression: ISSUE-003 — _handle_screener가 명령어 발송자의 chat_id를
        target_chat_id로 전달해야 합니다.
        """
        from telegram.telegram_bot import _handle_screener
        from analysis.chart_screener import ScreenResult

        fake_row = {
            "ticker": "005930.KS",
            "name": "삼성전자",
            "close": 80000,
            "ma_20w": 75000,
            "ma_60w": 70000,
            "cloud_top": 79000,
            "is_enhanced": False,
            "has_gapjum": False,
            "screened_at": None,
            "week_of": "2026-W15",
            "sector": "반도체",
            "ma_120w": None,
        }

        mock_send = AsyncMock(return_value=True)
        with (
            patch("core.db.load_chart_signals_latest", AsyncMock(return_value=("2026-W15", [fake_row]))),
            patch("telegram.telegram_notify.send_weekly_screener", mock_send),
        ):
            http_mock = AsyncMock()
            pool_mock = AsyncMock()
            await _handle_screener(http_mock, "INVOKER_CHAT", pool_mock)

        mock_send.assert_called_once()
        kwargs = mock_send.call_args.kwargs
        assert kwargs.get("target_chat_id") == "INVOKER_CHAT", (
            "send_weekly_screener에 target_chat_id='INVOKER_CHAT'이 전달돼야 합니다"
        )
