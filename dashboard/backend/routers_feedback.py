"""
dashboard/backend/routers_feedback.py
피드백 전송 + 인증 역할 조회 라우터.

  POST /api/feedback  — 피드백 텍스트+스크린샷 → Telegram 전송
  GET  /api/auth/me   — 현재 로그인 사용자의 역할 반환

의존 방향: routers_* → common/database/core.*/data.* 만 허용 (main import 금지).
"""
from __future__ import annotations

import base64
import logging
import os

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()


# ── POST /api/feedback ───────────────────────────────────────
class FeedbackBody(BaseModel):
    text: str
    screenshot: str | None = None  # base64 JPEG


@router.post("/api/feedback")
async def post_feedback(request: Request, body: FeedbackBody):
    token = os.environ.get("TELEGRAM_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        raise HTTPException(status_code=503, detail="Telegram 미설정")

    role = getattr(request.state, "role", "admin")
    caption = f"[피드백] ({role})\n{body.text[:900]}"

    import httpx
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            if body.screenshot:
                img_bytes = base64.b64decode(body.screenshot)
                r = await client.post(
                    f"https://api.telegram.org/bot{token}/sendPhoto",
                    data={"chat_id": chat_id, "caption": caption},
                    files={"photo": ("screenshot.jpg", img_bytes, "image/jpeg")},
                )
            else:
                r = await client.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": caption},
                )
        if r.status_code != 200:
            logger.error("[feedback] Telegram 전송 실패: %s", r.text)
            raise HTTPException(status_code=502, detail="Telegram 전송 실패")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[feedback] 전송 오류: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    return {"status": "sent"}


# ── GET /api/auth/me ──────────────────────────────────────────
@router.get("/api/auth/me")
async def auth_me(request: Request):
    """현재 로그인 사용자의 역할 반환. 프론트엔드 역할 기반 UI 분기용."""
    return {"role": getattr(request.state, "role", "user")}
