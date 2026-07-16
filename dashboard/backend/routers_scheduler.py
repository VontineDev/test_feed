"""
dashboard/backend/routers_scheduler.py

  POST /api/scheduler/trigger — 스케줄러 잡 수동 트리거 (admin)
  GET  /api/scheduler/status  — 최근 트리거 이력
  GET  /api/scheduler/stream  — SSE 스케줄러 상태 스트림

의존 방향: routers_* → common/database/core.*/data.* 만 허용 (main import 금지).
_SSE_CONNECTIONS는 common의 dict를 in-place로만 변경 — 재대입 금지 (health가 참조 공유).
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncGenerator

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from database import get_pool
from common import _SSE_CONNECTIONS

logger = logging.getLogger(__name__)

router = APIRouter()


# ── POST /api/scheduler/trigger ───────────────────────────────
_VALID_JOBS = {"stage", "screener", "paper_sample", "dart_screened", "youtube", "flow"}


class TriggerBody(BaseModel):
    job: str


@router.post("/api/scheduler/trigger")
async def trigger_job(request: Request, body: TriggerBody):
    if getattr(request.state, "role", "admin") != "admin":
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다")
    if body.job not in _VALID_JOBS:
        raise HTTPException(status_code=400, detail=f"unknown job: {body.job}")
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            # 이미 pending/running 중인 동일 잡 중복 방지
            existing = await conn.fetchval(
                "SELECT id FROM scheduler_triggers"
                " WHERE job_name=$1 AND status IN ('pending','running') LIMIT 1",
                body.job,
            )
            if existing:
                return {"status": "already_queued", "job": body.job}
            trig_id = await conn.fetchval(
                "INSERT INTO scheduler_triggers (job_name) VALUES ($1) RETURNING id",
                body.job,
            )
        return {"status": "queued", "job": body.job, "id": trig_id}
    except Exception as e:
        logger.error("[trigger] INSERT 실패: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ── GET /api/scheduler/status ─────────────────────────────────
@router.get("/api/scheduler/status")
async def scheduler_status():
    """최근 10개 트리거 이력 반환 (대시보드 상태 표시용)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, job_name, requested_at, executed_at, status
            FROM   scheduler_triggers
            ORDER  BY requested_at DESC LIMIT 10
            """,
        )
    return {"data": [dict(r) for r in rows]}


# ── GET /api/scheduler/stream  (SSE) ──────────────────────────
async def _scheduler_stream_generator(request: Request) -> AsyncGenerator[str, None]:
    pool = await get_pool()
    last_payload: str = ""

    async def _fetch() -> str:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, job_name, requested_at, executed_at, status
                FROM   scheduler_triggers
                ORDER  BY requested_at DESC LIMIT 10
                """
            )
        return json.dumps([dict(r) for r in rows], default=str)

    # 초기 전송
    try:
        last_payload = await _fetch()
        yield f"data: {last_payload}\n\n"
    except Exception as e:
        logger.warning("[scheduler-sse] 초기 조회 실패: %s", e)

    # 3초마다 변경 시에만 push
    while True:
        try:
            if await request.is_disconnected():
                break
        except Exception:
            break
        await asyncio.sleep(10)
        try:
            payload = await _fetch()
            if payload != last_payload:
                last_payload = payload
                yield f"data: {payload}\n\n"
        except Exception as e:
            logger.warning("[scheduler-sse] 조회 실패: %s", e)


@router.get("/api/scheduler/stream")
async def scheduler_stream(request: Request):
    _SSE_CONNECTIONS["scheduler"] += 1
    try:
        return StreamingResponse(
            _scheduler_stream_generator(request),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )
    finally:
        _SSE_CONNECTIONS["scheduler"] = max(0, _SSE_CONNECTIONS["scheduler"] - 1)
