"""
dashboard/backend/routers_ticker.py
종목코드 → 종목명 조회 라우터.

  GET /api/ticker/lookup — 종목코드 → 종목명

의존 방향: routers_* → common/database/core.*/data.* 만 허용 (main import 금지).
"""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException

from database import get_pool

router = APIRouter()


# ── GET /api/ticker/lookup ─────────────────────────────────────
@router.get("/api/ticker/lookup")
async def lookup_ticker(q: str):
    """종목코드로 종목명 조회.

    한국주식: 6자리 숫자 → DB(ticker_names → krx_listings) → Yahoo Finance
    미국주식: 영문 티커 → Yahoo Finance 검색
    """
    q = q.strip().upper()
    if not q or len(q) > 20:
        raise HTTPException(status_code=400, detail="올바른 종목코드를 입력하세요")

    is_kr = q.isdigit()

    if is_kr:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT name_ko FROM ticker_names WHERE ticker LIKE $1 LIMIT 1",
                q + ".%",
            )
            if row and row["name_ko"]:
                return {"ticker": q, "name": row["name_ko"], "market": "KR"}
            row2 = await conn.fetchrow(
                "SELECT name_ko FROM krx_listings WHERE yfinance_symbol LIKE $1 LIMIT 1",
                q + ".%",
            )
            if row2 and row2["name_ko"]:
                return {"ticker": q, "name": row2["name_ko"], "market": "KR"}

    # Yahoo Finance 검색 (한국·미국 공통 폴백)
    market = "KR" if is_kr else "US"
    search_symbols = ([q + ".KS", q + ".KQ"] if is_kr else [q])

    async def _yf_search() -> str | None:
        import httpx
        for sym in search_symbols:
            url = (
                f"https://query1.finance.yahoo.com/v1/finance/search"
                f"?q={sym}&quotesCount=5&newsCount=0&enableFuzzyQuery=false"
            )
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    r = await client.get(
                        url,
                        headers={"User-Agent": "Mozilla/5.0"},
                    )
                    if r.status_code != 200:
                        continue
                    for quote in r.json().get("quotes", []):
                        symbol = quote.get("symbol", "")
                        matched = (
                            symbol.startswith(q + ".") if is_kr else symbol.upper() == q
                        )
                        if matched:
                            name = quote.get("longname") or quote.get("shortname")
                            if name:
                                return name
            except Exception:
                continue
        return None

    try:
        name = await asyncio.wait_for(_yf_search(), timeout=9.0)
        if name:
            return {"ticker": q, "name": name, "market": market}
    except (asyncio.TimeoutError, Exception):
        pass

    raise HTTPException(status_code=404, detail="종목을 찾을 수 없습니다")
