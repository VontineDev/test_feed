"""jobs/scheduler_jobs.py — 상태 갱신·오케스트레이션 잡.

scheduler_wrappers.py(순수 위임)와 달리 여기 잡들은 공유 상태
(jobs/scheduler_state.py)를 갱신하거나 다른 잡을 체이닝한다.
공유 state 모듈 도입으로 전역 재바인딩 제약이 풀려 run_scheduler.py에서
이동 (Phase G 잔여). run_scheduler.py가 top-level import로 재수출한다.
"""

from __future__ import annotations

import logging

from jobs import scheduler_state as state
from jobs.scheduler_wrappers import (
    _dart_screened_sync_job,
    _paper_eod_sampler_job,
    _youtube_narrative_sync_job,
    _youtube_attention_score_job,
    _daily_flow_sync_job,
)

logger = logging.getLogger(__name__)


async def _daily_stage_job() -> None:
    """일봉 3단계 분류기 — jobs/stage_job.py 위임."""
    from jobs.stage_job import daily_stage_job as _impl
    state.active_stage_tickers = await _impl(state.db_pool)
    await _dart_screened_sync_job()


async def _weekly_screener_job():
    if not state.db_pool:
        logger.warning("[차트스크리너] DB 풀 없음 — 스크리닝 건너뜀")
        return
    from jobs.screener_job import weekly_screener_job
    state.screener_tickers = await weekly_screener_job(state.db_pool)
    # 스크리닝 완료 후 신규 종목 DART 분석 자동 실행
    await _dart_screened_sync_job()


# ── 대시보드 → 스케줄러 트리거 폴러 ─────────────────────────
# dashboard POST /api/scheduler/trigger → scheduler_triggers INSERT
# 이 잡이 30초마다 pending 행을 1개씩 꺼내 실행하고 status='done'으로 갱신.
# FOR UPDATE SKIP LOCKED: 동시 실행 방지 (max_instances=1로도 충분하나 DB 레벨 보장)
async def _trigger_watcher_job():
    if not state.db_pool:
        return
    try:
        async with state.db_pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT id, job_name FROM scheduler_triggers"
                    " WHERE status = 'pending'"
                    " ORDER BY requested_at ASC LIMIT 1"
                    " FOR UPDATE SKIP LOCKED"
                )
                if not row:
                    return
                trig_id = row["id"]
                job_name = row["job_name"]
                await conn.execute(
                    "UPDATE scheduler_triggers"
                    " SET status='running', executed_at=NOW()"
                    " WHERE id=$1", trig_id
                )
        logger.info("[trigger] 대시보드 요청 잡 실행: %s", job_name)
        try:
            if job_name == "stage":
                await _daily_stage_job()
            elif job_name == "screener":
                await _weekly_screener_job()
            elif job_name == "dart_screened":
                await _dart_screened_sync_job()
            elif job_name == "paper_sample":
                await _paper_eod_sampler_job()
            elif job_name == "youtube":
                await _youtube_narrative_sync_job()
                await _youtube_attention_score_job()
            elif job_name == "flow":
                await _daily_flow_sync_job()
            else:
                logger.warning("[trigger] 알 수 없는 잡: %s", job_name)
        finally:
            async with state.db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE scheduler_triggers SET status='done' WHERE id=$1",
                    trig_id
                )
    except Exception as e:
        logger.warning("[trigger] 폴링 실패: %s", e)
