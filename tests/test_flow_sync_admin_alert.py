"""
test_flow_sync_admin_alert.py — send_admin_alert() and daily_flow_sync_job()
failure-alert wiring.

Covers the gap found in /plan-eng-review: daily_flow_sync_job depended
silently on data.krx.co.kr connectivity (Tor Browser must be running) with
no operator-visible signal on failure. send_admin_alert() + the
_alert_flow_sync_failure() wiring in jobs/infra_jobs.py close that gap.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest


# ── send_admin_alert() ──────────────────────────────────────────────────────

class TestSendAdminAlert:
    @pytest.mark.asyncio
    async def test_returns_false_when_token_missing(self, monkeypatch):
        monkeypatch.delenv("TELEGRAM_TOKEN", raising=False)
        from telegram.telegram_notify import send_admin_alert
        result = await send_admin_alert("test failure")
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_chat_id_missing(self, monkeypatch):
        monkeypatch.setenv("TELEGRAM_TOKEN", "fake-token")
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
        from telegram.telegram_notify import send_admin_alert
        result = await send_admin_alert("test failure")
        assert result is False

    @pytest.mark.asyncio
    async def test_sends_to_chat_id_not_channel(self, monkeypatch):
        """Admin alerts are a DM to TELEGRAM_CHAT_ID, never the public channel."""
        monkeypatch.setenv("TELEGRAM_TOKEN", "fake-token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "111")
        monkeypatch.setenv("TELEGRAM_CHANNEL_ID", "-999")  # must be ignored

        import telegram.telegram_notify as tn
        with patch.object(tn, "_post_message", new=AsyncMock(return_value=True)) as mock_post:
            result = await tn.send_admin_alert("daily_flow_sync_job failed")

        assert result is True
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert call_args[0][2] == "111"  # chat_id positional arg, not channel

    @pytest.mark.asyncio
    async def test_escapes_markdown_special_chars(self, monkeypatch):
        monkeypatch.setenv("TELEGRAM_TOKEN", "fake-token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "111")

        import telegram.telegram_notify as tn
        with patch.object(tn, "_post_message", new=AsyncMock(return_value=True)) as mock_post:
            await tn.send_admin_alert("exit code [1] (failed)")

        sent_text = mock_post.call_args[0][3]
        assert "\\[1\\]" in sent_text
        assert "\\(failed\\)" in sent_text

    @pytest.mark.asyncio
    async def test_post_message_exception_does_not_raise(self, monkeypatch):
        """Alert delivery failing must not raise — callers treat this as best-effort."""
        monkeypatch.setenv("TELEGRAM_TOKEN", "fake-token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "111")

        import telegram.telegram_notify as tn
        with patch.object(tn, "_post_message", new=AsyncMock(side_effect=RuntimeError("boom"))):
            with pytest.raises(RuntimeError):
                await tn.send_admin_alert("test")
        # Note: send_admin_alert itself does not swallow _post_message errors —
        # the caller (_alert_flow_sync_failure) is responsible for that. See
        # TestAlertFlowSyncFailure below.


# ── jobs/infra_jobs.py:_alert_flow_sync_failure() ───────────────────────────

class TestAlertFlowSyncFailure:
    @pytest.mark.asyncio
    async def test_calls_send_admin_alert_with_reason(self):
        import jobs.infra_jobs as ij
        with patch(
            "telegram.telegram_notify.send_admin_alert", new=AsyncMock(return_value=True)
        ) as mock_alert:
            await ij._alert_flow_sync_failure("daily_flow_sync_job 비정상 종료 (exit=1)")

        mock_alert.assert_called_once()
        sent_text = mock_alert.call_args[0][0]
        assert "exit=1" in sent_text

    @pytest.mark.asyncio
    async def test_swallows_exception_from_send_admin_alert(self):
        """A broken Telegram alert path must not crash the scheduler job."""
        import jobs.infra_jobs as ij
        with patch(
            "telegram.telegram_notify.send_admin_alert",
            new=AsyncMock(side_effect=RuntimeError("network down")),
        ):
            await ij._alert_flow_sync_failure("test reason")  # must not raise
