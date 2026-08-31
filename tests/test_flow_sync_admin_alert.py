"""
test_flow_sync_admin_alert.py — send_admin_alert() and daily_flow_sync_job()
failure-alert wiring.

Covers the gap found in /plan-eng-review: daily_flow_sync_job depended
silently on data.krx.co.kr connectivity (Tor Browser must be running) with
no operator-visible signal on failure. send_admin_alert() + the
_alert_flow_sync_failure() wiring in jobs/infra_jobs.py close that gap.
"""
from __future__ import annotations

import asyncio
import logging
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


# ── jobs/infra_jobs.py:daily_flow_sync_job() 중복 실행 방지 락 ──────────────
#
# 2026-08-31: cron(평일 18:00 KST)·대시보드 트리거(scheduler_triggers 폴링)·
# 텔레그램(/run_flow, /run_all) 3개의 독립 진입 경로가 daily_flow_sync_job()을
# 호출할 수 있는데, 텔레그램 경로에만 자체 락(bot._flow_lock)이 있어 나머지
# 둘과 조율이 안 됐다 — cron이 아직 실행 중인데 /run_all이 같은
# krx_flow_sync.py 서브프로세스를 하나 더 띄워 ~9분간 동시 실행되는 사고가
# 났다. 락을 호출부가 아니라 실제 자원을 쓰는 함수 자체(flow_sync_lock)에
# 둬서 세 경로 전부가 자동으로 보호받도록 고쳤다.

class TestFlowSyncLock:
    @pytest.mark.asyncio
    async def test_second_call_skips_without_spawning_subprocess_while_first_running(self):
        """이미 실행 중이면(락 보유 중) 새 호출은 서브프로세스를 아예 안 띄우고
        즉시 리턴한다 — 대기하지 않음(cron이 텔레그램 실행을 몇 시간씩
        기다리게 두면 안 되므로)."""
        import jobs.infra_jobs as ij

        async def _hold_lock_forever():
            async with ij.flow_sync_lock:
                await asyncio.sleep(10)

        holder = asyncio.create_task(_hold_lock_forever())
        await asyncio.sleep(0)  # holder가 락을 잡을 기회를 준다
        try:
            assert ij.flow_sync_lock.locked()
            with patch("asyncio.create_subprocess_exec", new=AsyncMock()) as mock_spawn:
                await ij.daily_flow_sync_job()
            mock_spawn.assert_not_called()
        finally:
            holder.cancel()
            with pytest.raises(asyncio.CancelledError):
                await holder

    def test_telegram_bot_flow_lock_is_the_same_object(self):
        """telegram_bot._flow_lock이 jobs.infra_jobs.flow_sync_lock을 그대로
        re-export한 것인지 확인 — 별개 락 객체로 다시 갈라지면 이번에 고친
        보호가 텔레그램 경로에서만 조용히 무력화된다."""
        import jobs.infra_jobs as ij
        import telegram.telegram_bot as bot
        assert bot._flow_lock is ij.flow_sync_lock


# ── jobs/infra_jobs.py:_relog_subprocess_line() ─────────────────────────────
#
# krx_flow_sync.py subprocess output was previously re-logged at DEBUG
# unconditionally (2026-07-14 investigation), which the INFO-level scheduler
# log silently dropped — hiding WARNING/ERROR signals like session expiry
# from the operator. _relog_subprocess_line() preserves the original level.

class TestRelogSubprocessLine:
    def test_warning_line_relogs_as_warning(self, caplog):
        import jobs.infra_jobs as ij
        with caplog.at_level(logging.DEBUG, logger="jobs.infra_jobs"):
            ij._relog_subprocess_line(
                "17:32:16 [WARNING] [krx-direct] 인증 필요(세션 만료 의심, status=400)"
            )
        assert len(caplog.records) == 1
        assert caplog.records[0].levelno == logging.WARNING

    def test_error_line_relogs_as_error(self, caplog):
        import jobs.infra_jobs as ij
        with caplog.at_level(logging.DEBUG, logger="jobs.infra_jobs"):
            ij._relog_subprocess_line("00:07:00 [ERROR] [flow] 세션 갱신 대기 포기")
        assert caplog.records[0].levelno == logging.ERROR

    def test_info_line_relogs_as_info(self, caplog):
        import jobs.infra_jobs as ij
        with caplog.at_level(logging.DEBUG, logger="jobs.infra_jobs"):
            ij._relog_subprocess_line("18:00:05 [INFO] [flow] 완료 — 총 저장: 803건")
        assert caplog.records[0].levelno == logging.INFO

    def test_line_without_level_tag_defaults_to_info(self, caplog):
        """A stray line with no [LEVEL] marker (e.g. a bare traceback line)
        must not be silently dropped back to DEBUG."""
        import jobs.infra_jobs as ij
        with caplog.at_level(logging.DEBUG, logger="jobs.infra_jobs"):
            ij._relog_subprocess_line("Traceback (most recent call last):")
        assert caplog.records[0].levelno == logging.INFO
