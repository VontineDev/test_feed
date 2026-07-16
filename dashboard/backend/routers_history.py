"""
dashboard/backend/routers_history.py
이력 트래킹 라우터.

  GET /api/history/stage            — 기간별 Stage 분류 집계
  GET /api/history/screener         — 기간별 스크리너 집계
  GET /api/history/ticker/{ticker}  — 종목별 Stage+스크리너 이력

의존 방향: routers_* → common/database/core.*/data.* 만 허용 (main import 금지).
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

from fastapi import APIRouter, HTTPException

from database import get_pool

logger = logging.getLogger(__name__)

router = APIRouter()

_HISTORY_DEFAULT_DAYS = 14   # 기본 조회 기간 (일)
_HISTORY_MAX_DAYS     = 365  # 최대 조회 범위 — 초과 시 422


def _date_to_week(d: date) -> str:
    """date → ISO 주차 문자열 (예: 2026-W20)"""
    return d.strftime("%G-W%V")


def _parse_date(s: str | None, default: date) -> date:
    if s is None:
        return default
    try:
        return date.fromisoformat(s)
    except ValueError:
        return default


@router.get("/api/history/stage")
async def get_stage_history(
    start: str | None = None,
    end: str | None = None,
    stage: int | None = None,
):
    if stage is not None and stage not in (1, 2, 3):
        raise HTTPException(status_code=422, detail="stage must be 1, 2, or 3")
    today = date.today()
    start_date = _parse_date(start, today - timedelta(days=_HISTORY_DEFAULT_DAYS))
    end_date   = _parse_date(end, today)
    if start_date > end_date:
        start_date = end_date
    if (end_date - start_date).days > _HISTORY_MAX_DAYS:
        raise HTTPException(status_code=422, detail=f"조회 범위는 최대 {_HISTORY_MAX_DAYS}일입니다")

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            # per-stage subquery: GROUP BY에 COALESCE 전체 expression 반복 (alias 금지)
            # LATERAL로 latest_stage 조회 — idx_stage_class_ticker 사용
            _SUB = """
                SELECT agg.ticker, agg.name, agg.appearance_count,
                       agg.first_seen, agg.last_seen, agg.any_peakout,
                       {stage_val} AS stage_queried, latest.stage AS latest_stage
                FROM (
                    SELECT sc.ticker,
                           COALESCE(tn.name_ko, cs.name, SPLIT_PART(sc.ticker, '.', 1)) AS name,
                           COUNT(*) AS appearance_count,
                           MIN(sc.classified_date) AS first_seen,
                           MAX(sc.classified_date) AS last_seen,
                           BOOL_OR(sc.peakout_flag) AS any_peakout
                    FROM stage_classifications sc
                    LEFT JOIN ticker_names tn ON tn.ticker = sc.ticker
                    LEFT JOIN LATERAL (
                        SELECT name FROM chart_signals
                        WHERE ticker = sc.ticker
                        ORDER BY screened_at DESC LIMIT 1
                    ) cs ON TRUE
                    WHERE sc.classified_date BETWEEN $1 AND $2
                      AND sc.stage = {stage_val}
                    GROUP BY sc.ticker,
                             COALESCE(tn.name_ko, cs.name, SPLIT_PART(sc.ticker, '.', 1))
                    ORDER BY COUNT(*) DESC
                    LIMIT 50
                ) agg
                LEFT JOIN LATERAL (
                    SELECT stage FROM stage_classifications
                    WHERE ticker = agg.ticker
                    ORDER BY classified_date DESC LIMIT 1
                ) latest ON TRUE
            """

            if stage is not None:
                q = _SUB.format(stage_val=stage) + " ORDER BY appearance_count DESC"
                rows = await conn.fetch(q, start_date, end_date)
            else:
                # UNION ALL에서 $1/$2는 전체 쿼리에 걸쳐 동일 슬롯 → 한 번만 전달
                union_q = (
                    _SUB.format(stage_val=1) +
                    " UNION ALL " +
                    _SUB.format(stage_val=2) +
                    " UNION ALL " +
                    _SUB.format(stage_val=3) +
                    " ORDER BY stage_queried, appearance_count DESC"
                )
                rows = await conn.fetch(union_q, start_date, end_date)

        items = [
            {
                "ticker": r["ticker"],
                "name": r["name"] or r["ticker"],
                "appearance_count": r["appearance_count"],
                "first_seen": str(r["first_seen"]) if r["first_seen"] else None,
                "last_seen": str(r["last_seen"]) if r["last_seen"] else None,
                "any_peakout": bool(r["any_peakout"]),
                "stage_queried": r["stage_queried"],
                "latest_stage": r["latest_stage"],
            }
            for r in rows
        ]
        return {"data": {"start": str(start_date), "end": str(end_date),
                         "stage_filter": stage, "items": items}}
    except Exception as e:
        logger.error("[history/stage] 조회 실패: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/history/screener")
async def get_screener_history(
    start: str | None = None,
    end: str | None = None,
):
    today = date.today()
    start_date = _parse_date(start, today - timedelta(days=_HISTORY_DEFAULT_DAYS))
    end_date   = _parse_date(end, today)
    if start_date > end_date:
        start_date = end_date
    if (end_date - start_date).days > _HISTORY_MAX_DAYS:
        raise HTTPException(status_code=422, detail=f"조회 범위는 최대 {_HISTORY_MAX_DAYS}일입니다")
    start_week = _date_to_week(start_date)
    end_week   = _date_to_week(end_date)

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT ticker, MAX(name) AS name, COUNT(*) AS week_count,
                       MIN(week_of) AS first_week, MAX(week_of) AS last_week,
                       BOOL_OR(is_enhanced) AS any_enhanced,
                       BOOL_OR(has_gapjum) AS any_gapjum
                FROM chart_signals
                WHERE week_of BETWEEN $1 AND $2
                GROUP BY ticker
                ORDER BY week_count DESC
                LIMIT 100
                """,
                start_week, end_week,
            )

        items = [
            {
                "ticker": r["ticker"],
                "name": r["name"] or r["ticker"],
                "week_count": r["week_count"],
                "first_week": r["first_week"],
                "last_week": r["last_week"],
                "any_enhanced": bool(r["any_enhanced"]),
                "any_gapjum": bool(r["any_gapjum"]),
            }
            for r in rows
        ]
        return {"data": {"start": str(start_date), "end": str(end_date),
                         "start_week": start_week, "end_week": end_week,
                         "items": items}}
    except Exception as e:
        logger.error("[history/screener] 조회 실패: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/history/ticker/{ticker}")
async def get_ticker_history(
    ticker: str,
    start: str | None = None,
    end: str | None = None,
):
    today = date.today()
    start_date = _parse_date(start, today - timedelta(days=_HISTORY_DEFAULT_DAYS))
    end_date   = _parse_date(end, today)
    if (end_date - start_date).days > _HISTORY_MAX_DAYS:
        raise HTTPException(status_code=422, detail=f"조회 범위는 최대 {_HISTORY_MAX_DAYS}일입니다")
    if start_date > end_date:
        start_date = end_date
    start_week = _date_to_week(start_date)
    end_week   = _date_to_week(end_date)

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            stage_rows = await conn.fetch(
                """
                SELECT classified_date, stage, peakout_flag, s1_high, s1_txamt
                FROM stage_classifications
                WHERE ticker = $1
                  AND classified_date BETWEEN $2 AND $3
                ORDER BY classified_date DESC
                """,
                ticker, start_date, end_date,
            )
            screener_rows = await conn.fetch(
                """
                SELECT week_of, is_enhanced, has_gapjum, close
                FROM chart_signals
                WHERE ticker = $1
                  AND week_of BETWEEN $2 AND $3
                ORDER BY week_of DESC
                """,
                ticker, start_week, end_week,
            )

        stage_history = [
            {
                "classified_date": str(r["classified_date"]),
                "stage": r["stage"],
                "peakout_flag": bool(r["peakout_flag"]),
                "s1_high": float(r["s1_high"]) if r["s1_high"] else None,
                "s1_txamt": r["s1_txamt"],
            }
            for r in stage_rows
        ]
        screener_history = [
            {
                "week_of": r["week_of"],
                "is_enhanced": bool(r["is_enhanced"]),
                "has_gapjum": bool(r["has_gapjum"]),
                "close": float(r["close"]) if r["close"] else None,
            }
            for r in screener_rows
        ]
        return {"data": {"ticker": ticker,
                         "start": str(start_date), "end": str(end_date),
                         "stage_history": stage_history,
                         "screener_history": screener_history}}
    except Exception as e:
        logger.error("[history/ticker] 조회 실패: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
