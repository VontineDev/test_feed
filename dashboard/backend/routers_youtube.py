"""
dashboard/backend/routers_youtube.py
유튜브 내러티브 스크리너 라우터.

  GET /api/youtube/screener — attention_score 상위 종목 + stage/스크리너 교차

의존 방향: routers_* → common/database/core.*/data.* 만 허용 (main import 금지).
"""
from __future__ import annotations

import logging

from fastapi import APIRouter

from database import get_pool

logger = logging.getLogger(__name__)

router = APIRouter()


# ── GET /api/youtube/screener ────────────────────────────────────
@router.get("/api/youtube/screener")
async def get_youtube_screener():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            WITH yt AS (
                SELECT ticker, attention_score,
                       NTILE(5) OVER (ORDER BY attention_score) AS q
                FROM youtube_attention_scores
                WHERE window_end = (SELECT MAX(window_end) FROM youtube_attention_scores)
                  AND attention_score > 0
            ),
            sc AS (
                SELECT DISTINCT ON (ticker)
                       SPLIT_PART(ticker, '.', 1) AS t, stage
                FROM stage_classifications
                ORDER BY ticker, classified_date DESC
            ),
            cs AS (
                SELECT SPLIT_PART(ticker, '.', 1) AS t,
                       is_enhanced, has_gapjum, close, sector, name
                FROM chart_signals
                WHERE week_of = (SELECT MAX(week_of) FROM chart_signals)
            ),
            tn AS (
                SELECT SPLIT_PART(ticker, '.', 1) AS t, name_ko
                FROM ticker_names
            ),
            mr AS (
                SELECT DISTINCT ON (ticker) ticker, stock_name_raw
                FROM youtube_mention_raw
                ORDER BY ticker, created_at DESC
            )
            SELECT
                yt.ticker,
                COALESCE(tn.name_ko, cs.name, mr.stock_name_raw, yt.ticker) AS name,
                yt.attention_score,
                yt.q AS attention_q,
                sc.stage,
                cs.is_enhanced,
                cs.has_gapjum,
                cs.sector,
                cs.close
            FROM yt
            LEFT JOIN sc ON sc.t = yt.ticker
            LEFT JOIN cs ON cs.t = yt.ticker
            LEFT JOIN tn ON tn.t = yt.ticker
            LEFT JOIN mr ON mr.ticker = yt.ticker
            ORDER BY yt.attention_score DESC, yt.ticker
        """)

    items = []
    for r in rows:
        items.append({
            "ticker":          r["ticker"],
            "name":            r["name"] or r["ticker"],
            "attention_score": float(r["attention_score"]),
            "attention_q":     int(r["attention_q"]),
            "stage":           int(r["stage"]) if r["stage"] is not None else None,
            "is_enhanced":     r["is_enhanced"],
            "has_gapjum":      r["has_gapjum"],
            "sector":          r["sector"],
            "close":           float(r["close"]) if r["close"] else None,
        })

    stage2_plus  = sum(1 for i in items if i["stage"] is not None and i["stage"] >= 2)
    in_screener  = sum(1 for i in items if i["is_enhanced"] or i["has_gapjum"])
    narrative_q  = sum(1 for i in items if i["attention_q"] in (2, 3, 4))
    triple_combo = sum(
        1 for i in items
        if (i["stage"] is not None and i["stage"] >= 2)
        and (i["is_enhanced"] or i["has_gapjum"])
        and i["attention_q"] in (2, 3, 4)
    )

    return {
        "data": {
            "total":        len(items),
            "stage2_plus":  stage2_plus,
            "in_screener":  in_screener,
            "narrative_q":  narrative_q,
            "triple_combo": triple_combo,
            "items":        items,
        }
    }
