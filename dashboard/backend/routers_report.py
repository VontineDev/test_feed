"""
dashboard/backend/routers_report.py
리포트 라우터.

  GET /api/report/stage            — Stage 분류 결과 (최신일)
  GET /api/report/screener         — 차트 스크리너 결과 (최신주)
  GET /api/report/pipeline-status  — 파이프라인 최신 날짜/상태
  GET /api/report/unified          — stage+screener+youtube 통합 스크리너
  GET /api/report/paper            — 모의투자 포지션 리포트

의존 방향: routers_* → common/database/core.*/data.* 만 허용 (main import 금지).
"""
from __future__ import annotations

import logging
from datetime import date

from fastapi import APIRouter

from database import get_pool
from common import _fetch_current_prices, _NAME_RESOLUTION_JOIN
from report_queries import (
    _UNIFIED_TODAY_SQL, _UNIFIED_HISTORY_SQL, _AS_OF_SQL, _AS_OF_HISTORY_SQL,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ── GET /api/report/stage ────────────────────────────────────
@router.get("/api/report/stage")
async def get_stage_report():
    pool = await get_pool()
    async with pool.acquire() as conn:
        latest_date = await conn.fetchval(
            "SELECT MAX(classified_date) FROM stage_classifications"
        )
        if not latest_date:
            return {"data": None}

        summary = await conn.fetch(
            """
            SELECT stage,
                   COUNT(*)                                              AS count,
                   SUM(CASE WHEN peakout_flag THEN 1 ELSE 0 END)       AS peakout
            FROM   stage_classifications
            WHERE  classified_date = $1
            GROUP  BY stage ORDER BY stage
            """, latest_date,
        )

        _STAGE_QUERY = """
            SELECT sc.ticker,
                   COALESCE(tn.name_ko, k.name_ko,
                            cs.name,
                            SPLIT_PART(sc.ticker, '.', 1)) AS name,
                   COALESCE(k.sector, cs.sector)           AS sector,
                   sc.s1_high, sc.s1_volume, sc.peakout_flag
            FROM   stage_classifications sc
            LEFT JOIN ticker_names tn ON tn.ticker = sc.ticker
            LEFT JOIN krx_listings k  ON k.yfinance_symbol = sc.ticker
            LEFT JOIN LATERAL (
                SELECT name, sector FROM chart_signals
                WHERE  ticker = sc.ticker
                ORDER  BY screened_at DESC LIMIT 1
            ) cs ON TRUE
            WHERE  sc.classified_date = $1 AND sc.stage = $2
            ORDER  BY sc.s1_volume DESC NULLS LAST
            LIMIT  50
        """
        stage1 = await conn.fetch(_STAGE_QUERY, latest_date, 1)
        stage2 = await conn.fetch(_STAGE_QUERY, latest_date, 2)
        stage3 = await conn.fetch(_STAGE_QUERY, latest_date, 3)

    def _fmt_stage_rows(rows) -> list:
        return [
            {
                "ticker": r["ticker"],
                "name": r["name"],
                "sector": r["sector"],
                "s1_high": float(r["s1_high"]) if r["s1_high"] else None,
                "s1_volume": r["s1_volume"],
                "peakout_flag": r["peakout_flag"],
            }
            for r in rows
        ]

    return {
        "data": {
            "date": str(latest_date),
            "summary": [{"stage": r["stage"], "count": r["count"], "peakout": r["peakout"]} for r in summary],
            "stage1": _fmt_stage_rows(stage1),
            "stage2": _fmt_stage_rows(stage2),
            "stage3": _fmt_stage_rows(stage3),
        }
    }


# ── GET /api/report/screener ──────────────────────────────────
@router.get("/api/report/screener")
async def get_screener_report():
    pool = await get_pool()
    async with pool.acquire() as conn:
        latest_week = await conn.fetchval(
            "SELECT MAX(week_of) FROM chart_signals"
        )
        if not latest_week:
            return {"data": None}

        rows = await conn.fetch(
            """
            WITH latest_yt AS (
                SELECT ticker,
                       attention_score,
                       NTILE(5) OVER (ORDER BY attention_score) AS attention_q
                FROM   youtube_attention_scores
                WHERE  window_end = (SELECT MAX(window_end) FROM youtube_attention_scores)
                  AND  attention_score > 0
            )
            SELECT cs.ticker, cs.name, cs.close, cs.ma_20w, cs.ma_60w, cs.ma_120w,
                   cs.cloud_top, cs.is_enhanced, cs.has_gapjum, cs.sector, cs.screened_at,
                   yt.attention_score, yt.attention_q
            FROM   chart_signals cs
            LEFT JOIN latest_yt yt ON yt.ticker = cs.ticker
            WHERE  cs.week_of = $1
            ORDER  BY cs.is_enhanced DESC, cs.has_gapjum DESC, cs.close DESC
            """, latest_week,
        )

    items = []
    for r in rows:
        items.append({
            "ticker": r["ticker"],
            "name": r["name"] or r["ticker"],
            "close": float(r["close"]) if r["close"] else None,
            "ma_20w": float(r["ma_20w"]) if r["ma_20w"] else None,
            "ma_60w": float(r["ma_60w"]) if r["ma_60w"] else None,
            "ma_120w": float(r["ma_120w"]) if r["ma_120w"] else None,
            "cloud_top": float(r["cloud_top"]) if r["cloud_top"] else None,
            "is_enhanced": r["is_enhanced"],
            "has_gapjum": r["has_gapjum"],
            "sector": r["sector"],
            "attention_score": float(r["attention_score"]) if r["attention_score"] else None,
            "attention_q": int(r["attention_q"]) if r["attention_q"] else None,
        })

    enhanced = sum(1 for i in items if i["is_enhanced"])
    gapjum   = sum(1 for i in items if i["has_gapjum"])

    return {
        "data": {
            "week": latest_week,
            "total": len(items),
            "enhanced": enhanced,
            "gapjum": gapjum,
            "items": items,
        }
    }


@router.get("/api/report/pipeline-status")
async def get_pipeline_status():
    """수급·스테이지·스크리너·유튜브 파이프라인 최신 날짜 및 상태 반환."""
    from datetime import date as _date
    today = _date.today()

    def _status(last: _date | None, daily: bool) -> str:
        if last is None:
            return "error"
        gap = (today - last).days
        return ("ok" if gap <= (1 if daily else 7)
                else "warn" if gap <= (3 if daily else 14)
                else "error")

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                (SELECT MAX(trade_date)        FROM daily_flow)               AS flow_date,
                (SELECT COUNT(*) FROM daily_flow
                 WHERE trade_date = (SELECT MAX(trade_date) FROM daily_flow)) AS flow_tickers,
                (SELECT MAX(classified_date)   FROM stage_classifications)    AS stage_date,
                (SELECT MAX(screened_at)::date FROM chart_signals)            AS screener_date,
                (SELECT MAX(window_end)        FROM youtube_attention_scores) AS youtube_date
        """)

    flow_d     = row["flow_date"]
    stage_d    = row["stage_date"]
    screener_d = row["screener_date"]
    youtube_d  = row["youtube_date"]

    return {
        "flow":     {"date": str(flow_d)     if flow_d     else None, "tickers": row["flow_tickers"], "status": _status(flow_d,     daily=True)},
        "stage":    {"date": str(stage_d)    if stage_d    else None, "status": _status(stage_d,    daily=False)},
        "screener": {"date": str(screener_d) if screener_d else None, "status": _status(screener_d, daily=False)},
        "youtube":  {"date": str(youtube_d)  if youtube_d  else None, "status": _status(youtube_d,  daily=True)},
    }


@router.get("/api/report/unified")
async def get_unified_screener(
    start: date | None = None,
    end:   date | None = None,
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        if start is None or end is None:
            rows = await conn.fetch(_UNIFIED_TODAY_SQL)
            as_of_row = await conn.fetchrow(_AS_OF_SQL)
        else:
            rows = await conn.fetch(_UNIFIED_HISTORY_SQL, start, end)
            as_of_row = await conn.fetchrow(_AS_OF_HISTORY_SQL, start, end)

    as_of = {
        "stage":     str(as_of_row["stage_date"])[:10]     if as_of_row["stage_date"]     else None,
        "screener":  str(as_of_row["screener_date"])[:10]  if as_of_row["screener_date"]  else None,
        "narrative": str(as_of_row["narrative_date"])[:10] if as_of_row["narrative_date"] else None,
    }

    items = []
    for r in rows:
        items.append({
            "ticker":        r["ticker"],
            "name":          r["name"] or r["ticker"],
            "attention_score": float(r["attention_score"]) if r["attention_score"] is not None else None,
            "attention_q":   int(r["attention_q"]) if r["attention_q"] is not None else None,
            "stage":         int(r["stage"]) if r["stage"] is not None else None,
            "s1_high":       float(r["s1_high"]) if r["s1_high"] is not None else None,
            "s1_volume":     int(r["s1_volume"]) if r["s1_volume"] is not None else None,
            "peakout_flag":  bool(r["peakout_flag"]) if r["peakout_flag"] is not None else False,
            "is_enhanced":   bool(r["is_enhanced"]) if r["is_enhanced"] is not None else False,
            "has_gapjum":    bool(r["has_gapjum"]) if r["has_gapjum"] is not None else False,
            "sector":        r["sector"],
            "close":         float(r["close"]) if r["close"] is not None else None,
            "ma_20w":        float(r["ma_20w"]) if r["ma_20w"] is not None else None,
            "cloud_top":     float(r["cloud_top"]) if r["cloud_top"] is not None else None,
        })

    stage1       = sum(1 for i in items if i["stage"] == 1)
    stage2       = sum(1 for i in items if i["stage"] == 2)
    in_screener  = sum(1 for i in items if i["is_enhanced"] or i["has_gapjum"])
    narrative_q  = sum(1 for i in items if i["attention_q"] in (2, 3, 4))
    triple_combo = sum(
        1 for i in items
        if (i["stage"] in (1, 2))
        and (i["is_enhanced"] or i["has_gapjum"])
        and (i["attention_q"] in (2, 3, 4))
    )

    return {
        "data": {
            "total":        len(items),
            "stage1":       stage1,
            "stage2":       stage2,
            "in_screener":  in_screener,
            "narrative_q":  narrative_q,
            "triple_combo": triple_combo,
            "items":        items,
            "as_of":        as_of,
        }
    }


# ── GET /api/report/paper ─────────────────────────────────────
@router.get("/api/report/paper")
async def get_paper_report():
    pool = await get_pool()
    async with pool.acquire() as conn:
        # 모델별 상태 요약
        model_summary = await conn.fetch(
            """
            SELECT model, status, COUNT(*) AS count,
                   AVG(blended_return) FILTER (WHERE blended_return IS NOT NULL) AS avg_return
            FROM   paper_positions
            GROUP  BY model, status
            ORDER  BY model, status
            """
        )

        # 모델별 승률·실현누적 (청산 포지션 기준)
        model_stats = await conn.fetch(
            """
            SELECT model,
                   COUNT(*) FILTER (WHERE blended_return > 0) AS n_wins,
                   SUM(blended_return)                         AS total_realized
            FROM   paper_positions
            WHERE  status = 'closed' AND blended_return IS NOT NULL
            GROUP  BY model
            """
        )

        # 최근 청산 이력 (30건)
        closed = await conn.fetch(
            f"""
            SELECT p.ticker,
                   COALESCE(tn.name_ko, k.name_ko,
                            cs.name, SPLIT_PART(p.ticker, '.', 1)) AS name,
                   p.model, p.signal_date, p.entry_actual,
                   p.exit_date, p.exit_price, p.exit_type,
                   p.blended_return, p.tp1_date
            FROM   paper_positions p
            {_NAME_RESOLUTION_JOIN}
            WHERE  p.status = 'closed' AND p.blended_return IS NOT NULL
            ORDER  BY p.exit_date DESC NULLS LAST, p.id DESC
            LIMIT  30
            """
        )

        # 현재 오픈 포지션
        open_pos = await conn.fetch(
            f"""
            SELECT p.ticker,
                   COALESCE(tn.name_ko, k.name_ko,
                            cs.name, SPLIT_PART(p.ticker, '.', 1)) AS name,
                   p.model, p.signal_date, p.entry_actual, p.qty, p.status
            FROM   paper_positions p
            {_NAME_RESOLUTION_JOIN}
            WHERE  p.status IN ('open', 'pending')
            ORDER  BY p.signal_date DESC
            """
        )

    # 오픈 포지션 현재가 yfinance로 조회
    open_tickers = list({r["ticker"] for r in open_pos})
    open_prices = await _fetch_current_prices(open_tickers) if open_tickers else {}

    def _fmt_pos(r) -> dict:
        d = dict(r)
        for k in ("entry_actual", "exit_price", "blended_return"):
            if d.get(k) is not None:
                d[k] = float(d[k])
        for k in ("signal_date", "exit_date", "tp1_date"):
            if d.get(k) is not None:
                d[k] = str(d[k])
        # 오픈 포지션이면 yfinance 캐시, 청산이면 None
        curr = open_prices.get(d["ticker"]) if d.get("status") in ("open", "pending") else None
        d["current_price"] = curr
        entry = d.get("entry_actual")
        d["unrealized_pct"] = (
            round((curr / entry - 1) * 100, 2)
            if curr and entry else None
        )
        return d

    # 모델별 요약 구조화
    models: dict = {}
    for r in model_summary:
        m = r["model"]
        if m not in models:
            models[m] = {}
        models[m][r["status"]] = {
            "count": r["count"],
            "avg_return": round(float(r["avg_return"]) * 100, 2) if r["avg_return"] else None,
        }
    for r in model_stats:
        m = r["model"]
        if m not in models:
            continue
        n_closed = models[m].get("closed", {}).get("count", 0)
        models[m]["win_rate"] = round(r["n_wins"] / n_closed, 3) if n_closed else None
        models[m]["total_realized"] = (
            round(float(r["total_realized"]) * 100, 2) if r["total_realized"] is not None else None
        )

    return {
        "data": {
            "model_summary": models,
            "open": [_fmt_pos(r) for r in open_pos],
            "closed": [_fmt_pos(r) for r in closed],
        }
    }
