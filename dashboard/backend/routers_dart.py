"""
dashboard/backend/routers_dart.py
DART 재무 요약 라우터.

  GET /api/dart/summary/{ticker} — DART 최신 재무요약

의존 방향: routers_* → common/database/core.*/data.* 만 허용 (main import 금지).
"""
from __future__ import annotations

import json
import logging

from fastapi import APIRouter

from database import get_pool

logger = logging.getLogger(__name__)

router = APIRouter()


# ── GET /api/dart/summary/{ticker} ───────────────────────────
@router.get("/api/dart/summary/{ticker}")
async def get_dart_summary(ticker: str):
    """DART 재무 현황 — 최신 보고서 기준 매출/영업이익/사업부문.

    ticker: yfinance 형식 (005930.KS, 005930.KQ)
    dart_companies.stock_code(6자리)와 매핑 후 가장 최근 추출 결과 반환.
    응답: {data: {corp_name, period, report_type, extracted_at, revenue, segments}}
    """
    stock_code = ticker.split(".")[0]
    if not stock_code.isdigit() or len(stock_code) != 6:
        return {"data": None}

    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT de.corp_name, de.period, de.report_type, de.extracted_at,
                       de.revenue_json, de.segments_json
                FROM   dart_extractions de
                JOIN   dart_companies dc ON dc.corp_name = de.corp_name
                WHERE  dc.stock_code = $1
                  AND  de.revenue_json IS NOT NULL
                ORDER  BY de.period DESC
                LIMIT  1
                """,
                stock_code,
            )
    except Exception as e:
        logger.warning("[dart/summary] DB 조회 실패 (%s): %s", ticker, e)
        return {"data": None}

    if not row:
        return {"data": None}

    def _parse(v):
        if v is None:
            return None
        if isinstance(v, (dict, list)):
            return v
        try:
            return json.loads(v)
        except Exception:
            return None

    return {
        "data": {
            "corp_name":   row["corp_name"],
            "period":      row["period"],
            "report_type": row["report_type"],
            "extracted_at": str(row["extracted_at"]) if row["extracted_at"] else None,
            "revenue":     _parse(row["revenue_json"]),
            "segments":    _parse(row["segments_json"]),
        }
    }
