"""
dashboard/backend/routers_top.py
거래대금 상위 종목 라우터.

  GET /api/top — 거래대금 상위 N 종목 (장중 Kiwoom 실시간 / 장마감 스냅샷, SWR 캐시)

의존 방향: routers_* → common/market_snap/database/data.* 만 허용 (main import 금지).
_TOP_CACHE dict는 재대입 금지 — main의 health가 참조를 공유함.
"""
from __future__ import annotations

import asyncio
import logging
import time as _time_module
from pathlib import Path

from fastapi import APIRouter

from common import (
    _AFTERMARKET_TTL,
    _NXT_TTL,
    _bg_refresh,
    _cache_is_valid,
    _compute_cache_ttl,
    _ext_thread,
    _is_market_open,
    _is_nxt_open,
)
from market_snap import (
    _fetch_aftermarket_snap_top_async,
    _fetch_daily_snap_top_async,
    _fetch_nxt_live,
    _fetch_top_kiwoom,
)

import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from data.market_data import _fetch_fundamental  # noqa: E402

logger = logging.getLogger(__name__)

router = APIRouter()

# ── Top 캐시 (5분) ────────────────────────────────────────────
# 캐시는 n=20 기준 단일 슬롯. n이 다른 요청은 캐시된 데이터를 그대로 반환.
# 프론트엔드가 n=20 고정이므로 충돌 없음. n 변경 시 단일 슬롯 가정 재검토 필요.
_TOP_CACHE: dict = {"data": None, "expires": 0.0, "market_open": None, "is_nxt": None}
_TOP_TTL = 300            # 장 중 5분
_TOP_LOCK = asyncio.Lock()


# ── GET /api/top ──────────────────────────────────────────────

async def _enrich_top_with_fundamentals(items: list[dict]) -> None:
    """items 리스트에 EPS/PER/Forward PER를 in-place로 추가."""
    try:
        fund_results = await asyncio.wait_for(
            asyncio.gather(
                *[asyncio.to_thread(_fetch_fundamental, it["ticker"]) for it in items],
                return_exceptions=True,
            ),
            timeout=8.0,
        )
        for it, fund in zip(items, fund_results):
            if isinstance(fund, dict):
                it["eps"]         = fund.get("eps")
                it["per"]         = fund.get("per")
                it["forward_per"] = fund.get("forward_per")
            else:
                it["eps"] = it["per"] = it["forward_per"] = None
    except Exception as e:
        logger.warning("[top] 펀더멘털 enrichment 실패: %s", e)
        for it in items:
            it.setdefault("eps", None)
            it.setdefault("per", None)
            it.setdefault("forward_per", None)


@router.get("/api/top")
async def get_top(n: int = 50, refresh: bool = False):
    """거래대금 상위 N 종목.

    장 중: Kiwoom ka10032 실시간 데이터 (5분 캐시).
    장 마감: aftermarket_snap NXT 종가 데이터.
    EPS/PER/Forward PER는 Naver Finance에서 병렬 조회.
    """
    n = min(max(n, 1), 100)
    if not refresh and _cache_is_valid(_TOP_CACHE):
        return _TOP_CACHE["data"]
    if not refresh and _TOP_CACHE["data"]:
        # stale 또는 market_open/is_nxt 상태 전환 — 즉시 반환하고 백그라운드에서 갱신
        if not _TOP_LOCK.locked():
            async def _fetch_top_with_fundamentals() -> dict:
                if _is_nxt_open():
                    d = await _ext_thread(_fetch_nxt_live, 50, timeout=15.0)
                    await _enrich_top_with_fundamentals(d.get("items", []))
                    return d
                if not _is_market_open():
                    snap = await _fetch_daily_snap_top_async(50)
                    if not snap:
                        snap = await _fetch_aftermarket_snap_top_async(50)
                    if snap:
                        await _enrich_top_with_fundamentals(snap["items"])
                        return snap
                d = await _ext_thread(_fetch_top_kiwoom, 50, timeout=15.0)
                await _enrich_top_with_fundamentals(d.get("items", []))
                return d
            asyncio.create_task(_bg_refresh(
                _TOP_CACHE, _TOP_LOCK, _fetch_top_with_fundamentals, _compute_cache_ttl, "top"
            ))
        return {**_TOP_CACHE["data"], "stale": True}
    # 최초 기동 또는 강제 refresh: 한 번만 대기
    async with _TOP_LOCK:
        if not refresh and _cache_is_valid(_TOP_CACHE):
            return _TOP_CACHE["data"]
        try:
            if _is_nxt_open():
                data = await _ext_thread(_fetch_nxt_live, n, timeout=15.0)
                items = data.get("items", [])
                await _enrich_top_with_fundamentals(items)
                data["items"] = items
                _TOP_CACHE["data"] = data
                _TOP_CACHE["expires"] = _time_module.time() + _NXT_TTL
                _TOP_CACHE["market_open"] = False
                _TOP_CACHE["is_nxt"] = True
                return data
            if not _is_market_open():
                snap = await _fetch_daily_snap_top_async(n)
                if not snap:
                    snap = await _fetch_aftermarket_snap_top_async(n)
                if snap:
                    await _enrich_top_with_fundamentals(snap["items"])
                    _TOP_CACHE["data"] = snap
                    _TOP_CACHE["expires"] = _time_module.time() + _AFTERMARKET_TTL
                    _TOP_CACHE["market_open"] = False
                    _TOP_CACHE["is_nxt"] = False
                    return snap
            data = await _ext_thread(_fetch_top_kiwoom, n, timeout=15.0)
            items = data.get("items", [])
            await _enrich_top_with_fundamentals(items)
            data["items"] = items
            _TOP_CACHE["data"] = data
            _TOP_CACHE["expires"] = _time_module.time() + _TOP_TTL
            _TOP_CACHE["market_open"] = True
            _TOP_CACHE["is_nxt"] = False
            return data
        except Exception as e:
            logger.warning("[top] 조회 오류: %s", e)
            _safe_err = "API 오류 — 서버 로그 확인"
            if _TOP_CACHE["data"]:
                return {**_TOP_CACHE["data"], "stale": True, "error": _safe_err}
            return {"items": [], "fetched_at": "--:--", "error": _safe_err}
