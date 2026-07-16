"""
dashboard/backend/routers_signals.py

  GET /api/signals/stream — SSE 신호 라이브 피드 (15초 폴링)

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

from database import get_pool
from common import _SSE_CONNECTIONS

logger = logging.getLogger(__name__)

router = APIRouter()


# ── GET /api/signals/stream  (SSE) ────────────────────────────
async def _signal_generator(request: Request) -> AsyncGenerator[str, None]:
    pool = await get_pool()
    last_id: int = 0

    # 초기 20건 전송
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT s.id, s.direction, s.strength, s.tickers,
                       s.detected_at, s.article_type,
                       a.title_en, a.summary_ko
                FROM   trade_signals s
                JOIN   news_articles a ON a.id = s.article_id
                ORDER  BY s.detected_at DESC LIMIT 20
                """
            )
        if rows:
            last_id = rows[0]["id"]
            payload = [_signal_to_dict(r) for r in rows]
            yield f"data: {json.dumps(payload, default=str)}\n\n"
    except Exception as e:
        logger.warning("[sse] 초기 신호 조회 실패: %s", e)

    # 15초 폴링
    while True:
        try:
            if await request.is_disconnected():
                break
        except Exception:
            break
        await asyncio.sleep(15)
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT s.id, s.direction, s.strength, s.tickers,
                           s.detected_at, s.article_type,
                           a.title_en, a.summary_ko
                    FROM   trade_signals s
                    JOIN   news_articles a ON a.id = s.article_id
                    WHERE  s.id > $1
                    ORDER  BY s.detected_at DESC LIMIT 10
                    """,
                    last_id,
                )
            if rows:
                last_id = rows[0]["id"]
                payload = [_signal_to_dict(r) for r in rows]
                yield f"data: {json.dumps(payload, default=str)}\n\n"
        except Exception as e:
            logger.warning("[sse] 폴링 실패: %s", e)


def _signal_to_dict(r) -> dict:
    d = dict(r)
    d["detected_at"] = d["detected_at"].isoformat() if d.get("detected_at") else None
    if d.get("tickers") and not isinstance(d["tickers"], list):
        d["tickers"] = list(d["tickers"])
    return d


@router.get("/api/signals/stream")
async def signals_stream(request: Request):
    _SSE_CONNECTIONS["signals"] += 1
    try:
        return StreamingResponse(
            _signal_generator(request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )
    finally:
        _SSE_CONNECTIONS["signals"] = max(0, _SSE_CONNECTIONS["signals"] - 1)
