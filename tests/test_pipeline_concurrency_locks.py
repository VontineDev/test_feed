"""stage/screener/youtube 파이프라인의 중복 실행 방지 락 테스트.

2026-08-31: daily_flow_sync_job()에서 cron(평일)·대시보드 트리거
(scheduler_triggers 폴링)·텔레그램(/run_X, /run_all) 3개의 독립 진입
경로가 서로 몰라 동시 실행되는 사고가 났다(tests/test_flow_sync_admin_alert.py
::TestFlowSyncLock 참고). 같은 3-경로 구조를 가진 stage/screener/youtube도
동일한 패턴(자원을 실제로 쓰는 함수 자체에 락)으로 예방적으로 고쳤다 —
이 파일은 그 세 군데를 커버한다.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest


# ── jobs/stage_job.py:daily_stage_job() ─────────────────────────────────────

class TestStageJobLock:
    @pytest.mark.asyncio
    async def test_second_call_skips_impl_while_first_running(self):
        import jobs.stage_job as sj

        async def _hold_lock_forever():
            async with sj.stage_job_lock:
                await asyncio.sleep(10)

        holder = asyncio.create_task(_hold_lock_forever())
        await asyncio.sleep(0)
        try:
            assert sj.stage_job_lock.locked()
            with patch("jobs.stage_job._daily_stage_job_impl", new=AsyncMock()) as mock_impl:
                result = await sj.daily_stage_job(db_pool=AsyncMock())
            mock_impl.assert_not_called()
            assert result == set()
        finally:
            holder.cancel()
            with pytest.raises(asyncio.CancelledError):
                await holder

    def test_telegram_bot_stage_lock_is_the_same_object(self):
        import jobs.stage_job as sj
        import telegram.telegram_bot as bot
        assert bot._stage_lock is sj.stage_job_lock


# ── jobs/screener_job.py:weekly_screener_job() ──────────────────────────────

class TestScreenerJobLock:
    @pytest.mark.asyncio
    async def test_second_call_skips_impl_while_first_running(self):
        import jobs.screener_job as scj

        async def _hold_lock_forever():
            async with scj.screener_lock:
                await asyncio.sleep(10)

        holder = asyncio.create_task(_hold_lock_forever())
        await asyncio.sleep(0)
        try:
            assert scj.screener_lock.locked()
            with patch("jobs.screener_job._weekly_screener_job_impl", new=AsyncMock()) as mock_impl:
                result = await scj.weekly_screener_job(db_pool=AsyncMock())
            mock_impl.assert_not_called()
            assert result == set()
        finally:
            holder.cancel()
            with pytest.raises(asyncio.CancelledError):
                await holder

    def test_telegram_bot_scan_lock_is_the_same_object(self):
        """bot._scan_lock은 텔레그램 /run_screener의 자체 재구현 경로용으로
        계속 별도 async with로 감싸 쓰이지만(run_weekly_screen()을 직접
        부르므로 재귀 아님, 데드락 위험 없음), cron/대시보드 경로와 상호
        배제되려면 같은 객체여야 한다."""
        import jobs.screener_job as scj
        import telegram.telegram_bot as bot
        assert bot._scan_lock is scj.screener_lock


# ── jobs/infra_jobs.py:youtube_narrative_sync_job() ─────────────────────────

class TestYoutubeSyncLock:
    @pytest.mark.asyncio
    async def test_second_call_skips_without_running_sync_while_first_running(self):
        import jobs.infra_jobs as ij

        async def _hold_lock_forever():
            async with ij.youtube_sync_lock:
                await asyncio.sleep(10)

        holder = asyncio.create_task(_hold_lock_forever())
        await asyncio.sleep(0)
        try:
            assert ij.youtube_sync_lock.locked()
            with patch("data.youtube_narrative_sync.run_sync", new=AsyncMock()) as mock_run:
                await ij.youtube_narrative_sync_job()
            mock_run.assert_not_called()
        finally:
            holder.cancel()
            with pytest.raises(asyncio.CancelledError):
                await holder

    def test_telegram_bot_youtube_lock_is_the_same_object(self):
        import jobs.infra_jobs as ij
        import telegram.telegram_bot as bot
        assert bot._youtube_lock is ij.youtube_sync_lock
