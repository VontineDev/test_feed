"""
dashboard/backend/routers_heatmap.py
히트맵 + 포지션 라우터.

  GET /api/heatmap    — Stage 색상 히트맵 데이터 (5분 캐시, SWR)
  GET /api/positions  — paper_positions 미실현 수익률

의존 방향: routers_* → common/market_snap/database 만 허용 (main import 금지).
_HEATMAP_CACHE는 common 소유 dict — in-place 변경만, 재대입 금지.
"""
from __future__ import annotations

import asyncio
import logging
import time as _time_module
from datetime import date

from fastapi import APIRouter, HTTPException

from database import get_pool
from common import (
    _HEATMAP_CACHE,
    _HEATMAP_LOCK,
    _NAME_RESOLUTION_JOIN,
    _bg_refresh,
    _cache_is_valid,
    _compute_cache_ttl,
    _ext_thread,
    _fetch_current_prices,
    _is_market_open,
    _is_nxt_open,
)
from market_snap import (
    _fetch_aftermarket_snap_top_async,
    _fetch_daily_snap_top_async,
    _fetch_nxt_live,
    _fetch_top_kiwoom,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ── 히트맵 데이터 빌드 (Kiwoom 거래대금 Top 50 + Stage 오버레이) ──
# TODO [엣지 1] 평일 08:00~09:00 프리마켓 케이스:
#   장전 단일가(08:00~09:00) + 전일 합산 표시가 필요하지만
#   Kiwoom 장전 단일가 API(코드 미확인)가 없어 미구현.
#   확인 후 별도 케이스(_is_premarket()) 분기 추가 필요.
#
# TODO [엣지 2] 주말 08:00~09:00:
#   위 프리마켓 케이스 구현 시 weekday < 5 체크 반드시 포함할 것.
#   현재는 _is_market_open()이 weekday >= 5 → False 반환하므로 장마감 경로로 처리됨.
#
# TODO [엣지 5] ka10032의 장전 단일가(08:00~09:00) 포함 여부:
#   09:00 직전/직후 trde_prica 실측으로 확인 필요.
#   포함되면 "08시부터" 자연 충족, 미포함이면 별도 장전 API 보완 필요.
#
# TODO [엣지 8] 15:30~15:40 NXT 미시작 공백:
#   정규장 마감(15:30) 직후 NXT 시간외는 15:40 시작.
#   이 10분간은 _is_market_open()=False이지만 aftermarket_snap에
#   오늘 데이터가 없음 → MAX(trade_date)가 어제 데이터로 표시됨.
#   16:05 수집 잡이 완료되기 전까지 동일 현상 지속.
#   개선 방법: 15:30~16:05 구간에 ka10032 frozen 스냅샷 별도 캐시 유지.
#
# TODO [엣지 3·12] 종목 구성 불연속:
#   장마감 → 장전 전환(07:59→08:00) 시 daily_market_snap top100(장중) vs
#   daily_market_snap 전날 데이터가 그대로 유지되므로 연속성 개선됨.
#   단, 스냅샷 수집(16:10) 전 15:30~16:10 구간은 어제 데이터로 표시.
async def _build_heatmap_data() -> dict:
    """{"items": list[dict], "fetched_at": str|None, "is_nxt": bool} 반환.
    fetched_at: 장마감 시 trade_date(YYYY-MM-DD), 장중/NXT 시 None.

    데이터 소스 우선순위:
      NXT 시간외 (15:40~16:05): ka10098 실시간
      장 마감 1순위: daily_market_snap — ka10032 top100, KRX+NXT 합산, 전 종목 커버
      장 마감 2순위: aftermarket_snap  — NXT 거래 종목만, 폴백
    """
    pool = await get_pool()

    # NXT 시간외 (15:40~16:05): ka10098 실시간
    if _is_nxt_open():
        try:
            nxt_data = await _ext_thread(_fetch_nxt_live, 50, timeout=15.0)
            nxt_items = nxt_data.get("items", [])
        except Exception as e:
            logger.warning("[heatmap] NXT 조회 실패, 스냅샷 폴백: %s", e)
            nxt_items = []
        if nxt_items:
            tickers = [it["ticker"] for it in nxt_items]
            async with pool.acquire() as conn:
                stage_rows = await conn.fetch(
                    """
                    SELECT ticker, stage FROM stage_classifications
                    WHERE classified_date = (SELECT MAX(classified_date) FROM stage_classifications)
                      AND ticker = ANY($1::text[])
                    """,
                    tickers,
                )
                name_rows = await conn.fetch(
                    "SELECT ticker, name_ko FROM ticker_names WHERE ticker = ANY($1::text[])",
                    tickers,
                )
            stage_map = {r["ticker"]: r["stage"] for r in stage_rows}
            name_map  = {r["ticker"]: r["name_ko"] for r in name_rows}
            items = [
                {
                    "ticker":     it["ticker"],
                    "name":       name_map.get(it["ticker"]) or it["name"],
                    "stage":      stage_map.get(it["ticker"]),
                    "amount":     it["amount"],
                    "change_pct": it["change_pct"],
                    "market":     it.get("market", ""),
                    "is_nxt":     True,
                }
                for it in nxt_items
            ]
            return {"items": items, "fetched_at": None, "is_nxt": True}
        # NXT 데이터 없으면 아래 스냅샷 경로로 폴백

    # 장 마감 시: daily_market_snap 우선, aftermarket_snap 폴백
    if not _is_market_open():
        snap_data = await _fetch_daily_snap_top_async(50)
        if not snap_data or not snap_data.get("items"):
            snap_data = await _fetch_aftermarket_snap_top_async(50)
        if snap_data and snap_data.get("items"):
            tickers = [it["ticker"] for it in snap_data["items"]]
            async with pool.acquire() as conn:
                stage_rows = await conn.fetch(
                    """
                    SELECT ticker, stage FROM stage_classifications
                    WHERE classified_date = (SELECT MAX(classified_date) FROM stage_classifications)
                      AND ticker = ANY($1::text[])
                    """,
                    tickers,
                )
            stage_map = {r["ticker"]: r["stage"] for r in stage_rows}
            items = [
                {
                    "ticker":        it["ticker"],
                    "name":          it["name"],
                    "stage":         stage_map.get(it["ticker"]),
                    "amount":        it["amount"],
                    "change_pct":    it["change_pct"],
                    "market":        it.get("market", ""),
                    "is_aftermarket": True,
                }
                for it in snap_data["items"]
            ]
            return {"items": items, "fetched_at": snap_data.get("fetched_at")}

    # 1. Kiwoom top 50 조회 (15초 타임아웃)
    try:
        top_data = await _ext_thread(_fetch_top_kiwoom, 50, timeout=15.0)
        kiwoom_items = top_data.get("items", [])
    except (asyncio.TimeoutError, Exception) as e:
        logger.warning("[heatmap] Kiwoom 조회 실패, Stage 분류 폴백: %s", e)
        kiwoom_items = []

    today = date.today()

    if kiwoom_items:
        tickers = [i["ticker"] for i in kiwoom_items]
        async with pool.acquire() as conn:
            stage_rows = await conn.fetch(
                """
                SELECT ticker, stage FROM stage_classifications
                WHERE classified_date = $1 AND ticker = ANY($2::text[])
                """,
                today, tickers,
            )
        stage_map = {r["ticker"]: r["stage"] for r in stage_rows}
        items = [
            {
                "ticker":     it["ticker"],
                "name":       it["name"],
                "stage":      stage_map.get(it["ticker"]),
                "amount":     it["amount"],
                "change_pct": it["change_pct"],
                "market":     it.get("market", ""),
            }
            for it in kiwoom_items
        ]
        return {"items": items, "fetched_at": None}

    # ── Stage 분류 폴백 (Kiwoom 미응답 시) ──────────────────────
    logger.info("[heatmap] Stage 분류 데이터로 폴백")
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT sc.ticker, sc.stage, sc.s1_high, sc.s1_volume,
                   COALESCE(tn.name_ko, k.name_ko, SPLIT_PART(sc.ticker, '.', 1)) AS name,
                   CASE WHEN sc.ticker LIKE '%.KS' THEN 'KOSPI'
                        WHEN sc.ticker LIKE '%.KQ' THEN 'KOSDAQ'
                        ELSE '' END AS market
            FROM stage_classifications sc
            LEFT JOIN ticker_names tn ON tn.ticker = sc.ticker
            LEFT JOIN krx_listings k  ON k.yfinance_symbol = sc.ticker
            WHERE sc.classified_date = $1
            """,
            today,
        )
    result = []
    for r in rows:
        s1_high = float(r["s1_high"]) if r["s1_high"] else 0.0
        s1_vol  = float(r["s1_volume"]) if r["s1_volume"] else 0.0
        amount  = s1_high * s1_vol if s1_high and s1_vol else 1.0
        result.append({
            "ticker":     r["ticker"],
            "name":       r["name"],
            "stage":      r["stage"],
            "amount":     amount,
            "change_pct": 0.0,
            "market":     r["market"] or "",
        })
    items = sorted(result, key=lambda x: x["amount"], reverse=True)
    return {"items": items, "fetched_at": None}


# ── GET /api/heatmap ──────────────────────────────────────────
def _heatmap_response(cache_data: dict, cached: bool, stale: bool = False) -> dict:
    items = cache_data.get("items") or []
    fetched_at = cache_data.get("fetched_at")
    is_aftermarket = bool(items and items[0].get("is_aftermarket"))
    r: dict = {"data": items, "cached": cached, "is_aftermarket": is_aftermarket}
    if fetched_at:
        r["fetched_at"] = fetched_at   # YYYY-MM-DD (장마감 trade_date)
    if stale:
        r["stale"] = True
    return r


@router.get("/api/heatmap")
async def get_heatmap():
    if _cache_is_valid(_HEATMAP_CACHE):
        return _heatmap_response(_HEATMAP_CACHE["data"], cached=True)
    if _HEATMAP_CACHE["data"]:
        # stale 또는 market_open/is_nxt 상태 전환 — 즉시 반환하고 백그라운드에서 갱신
        if not _HEATMAP_LOCK.locked():
            asyncio.create_task(_bg_refresh(
                _HEATMAP_CACHE, _HEATMAP_LOCK, _build_heatmap_data, _compute_cache_ttl, "heatmap"
            ))
        return _heatmap_response(_HEATMAP_CACHE["data"], cached=True, stale=True)
    # 최초 기동: 데이터 없음 — 한 번만 대기
    async with _HEATMAP_LOCK:
        if _cache_is_valid(_HEATMAP_CACHE):
            return _heatmap_response(_HEATMAP_CACHE["data"], cached=True)
        try:
            cache_data = await _build_heatmap_data()
            _HEATMAP_CACHE["data"] = cache_data
            _HEATMAP_CACHE["expires"] = _time_module.time() + _compute_cache_ttl(cache_data)
            _HEATMAP_CACHE["market_open"] = _is_market_open()
            _HEATMAP_CACHE["is_nxt"] = _is_nxt_open()
            return _heatmap_response(cache_data, cached=False)
        except Exception as e:
            logger.error("[heatmap] 빌드 실패: %s", e)
            raise HTTPException(status_code=500, detail="heatmap unavailable")


# ── GET /api/positions ────────────────────────────────────────
@router.get("/api/positions")
async def get_positions():
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT p.id, p.ticker,
                       COALESCE(tn.name_ko, k.name_ko,
                                cs.name, SPLIT_PART(p.ticker, '.', 1)) AS name,
                       p.model, p.signal_date,
                       p.entry_actual, p.qty, p.status,
                       p.tp1_pct, p.trail_pct
                FROM   paper_positions p
                {_NAME_RESOLUTION_JOIN}
                WHERE  p.status IN ('open', 'pending')
                ORDER  BY p.signal_date DESC
                """
            )

        tickers = list({r["ticker"] for r in rows})
        prices = await _fetch_current_prices(tickers)

        positions = []
        for r in rows:
            d = dict(r)
            for k in ("entry_actual", "tp1_pct", "trail_pct"):
                if d.get(k) is not None:
                    d[k] = float(d[k])
            curr = prices.get(d["ticker"])
            d["current_price"] = curr
            entry = d.get("entry_actual")
            d["unrealized_pct"] = (
                round((curr / entry - 1) * 100, 2)
                if curr and entry else None
            )
            positions.append(d)
        return {"data": positions}
    except Exception as e:
        logger.error("[positions] 조회 실패: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
