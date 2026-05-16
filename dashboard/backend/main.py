"""
dashboard/backend/main.py
웹 대시보드 FastAPI 앱 — 4 엔드포인트 + React dist 서빙.

엔드포인트:
  GET  /api/heatmap          — Stage 색상 히트맵 데이터 (30분 캐시)
  GET  /api/positions        — paper_positions 미실현 수익률
  GET  /api/signals/stream   — SSE 신호 라이브 피드
  POST /api/scheduler/trigger — 스케줄러 잡 수동 트리거

개발: uvicorn main:app --reload --port 8000
프로덕션: npm run build → FastAPI가 ../frontend/dist 서빙
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from database import close_pool, get_pool

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ── 히트맵 30분 캐시 ──────────────────────────────────────────
_HEATMAP_CACHE: dict = {"data": None, "expires": 0.0}
_HEATMAP_TTL = 1800  # 초


# ── lifespan ──────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await get_pool()
    logger.info("DB 풀 준비 완료")
    yield
    await close_pool()
    logger.info("DB 풀 종료")


app = FastAPI(title="Trading Dashboard", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Vite dev server
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── 히트맵 데이터 빌드 ────────────────────────────────────────
async def _build_heatmap_data() -> list[dict]:
    pool = await get_pool()
    today = date.today()

    # 오늘 Stage 분류 결과
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT ticker, stage
            FROM   stage_classifications
            WHERE  classified_date = $1
            """,
            today,
        )
    stage_map: dict[str, int] = {r["ticker"]: r["stage"] for r in rows}

    if not stage_map:
        return []

    # fdr.StockListing: 블로킹 I/O → 스레드 풀
    import financedata_reader as fdr  # type: ignore

    def _fetch_listing() -> dict[str, dict]:
        result: dict[str, dict] = {}
        for market in ("KOSPI", "KOSDAQ"):
            try:
                df = fdr.StockListing(market)
                for _, r in df.iterrows():
                    sym = str(r.get("Symbol", r.get("Code", ""))).strip()
                    if not sym:
                        continue
                    result[sym] = {
                        "name": str(r.get("Name", sym)),
                        "amount": float(r.get("Amount", 0) or 0),
                        "change_pct": float(r.get("Change", r.get("ChgRatio", 0)) or 0),
                        "market": market,
                    }
            except Exception as e:
                logger.warning("[heatmap] fdr.StockListing(%s) 실패: %s", market, e)
        return result

    listing = await asyncio.to_thread(_fetch_listing)

    # Amount=0 fallback: daily_ohlcv 최신 종가 * 거래량
    zero_tickers = [t for t in stage_map if listing.get(t, {}).get("amount", 0) == 0]
    fallback_amounts: dict[str, float] = {}
    if zero_tickers:
        async with pool.acquire() as conn:
            fb_rows = await conn.fetch(
                """
                SELECT DISTINCT ON (symbol) symbol, close, volume
                FROM   daily_ohlcv
                WHERE  symbol = ANY($1)
                ORDER  BY symbol, date DESC
                """,
                zero_tickers,
            )
        for r in fb_rows:
            if r["close"] and r["volume"]:
                fallback_amounts[r["symbol"]] = float(r["close"]) * float(r["volume"])

    result = []
    for ticker, stage in stage_map.items():
        info = listing.get(ticker, {})
        amount = info.get("amount", 0) or fallback_amounts.get(ticker, 0)
        result.append({
            "ticker": ticker,
            "name": info.get("name", ticker),
            "stage": stage,
            "amount": amount,
            "change_pct": info.get("change_pct", 0),
            "market": info.get("market", ""),
        })

    return sorted(result, key=lambda x: x["amount"], reverse=True)


# ── GET /api/heatmap ──────────────────────────────────────────
@app.get("/api/heatmap")
async def get_heatmap():
    now = time.time()
    if _HEATMAP_CACHE["data"] and now < _HEATMAP_CACHE["expires"]:
        return {"data": _HEATMAP_CACHE["data"], "cached": True}
    try:
        data = await _build_heatmap_data()
        _HEATMAP_CACHE["data"] = data
        _HEATMAP_CACHE["expires"] = now + _HEATMAP_TTL
        return {"data": data, "cached": False}
    except Exception as e:
        logger.error("[heatmap] 빌드 실패: %s", e)
        if _HEATMAP_CACHE["data"]:
            return {"data": _HEATMAP_CACHE["data"], "cached": True, "stale": True}
        raise HTTPException(status_code=500, detail="heatmap unavailable")


# ── GET /api/positions ────────────────────────────────────────
@app.get("/api/positions")
async def get_positions():
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT p.id, p.ticker, p.model, p.entry_date,
                       p.entry_actual, p.qty, p.status,
                       p.tp1_pct, p.trail_pct,
                       o.close AS current_price
                FROM   paper_positions p
                LEFT JOIN LATERAL (
                    SELECT close FROM daily_ohlcv
                    WHERE  symbol = p.ticker
                    ORDER  BY date DESC LIMIT 1
                ) o ON TRUE
                WHERE  p.status IN ('open', 'pending')
                ORDER  BY p.entry_date DESC
                """
            )
        positions = []
        for r in rows:
            d = dict(r)
            if d.get("entry_actual") and d.get("current_price"):
                entry = float(d["entry_actual"])
                curr = float(d["current_price"])
                d["unrealized_pct"] = round((curr / entry - 1) * 100, 2) if entry else None
            else:
                d["unrealized_pct"] = None
            # decimal → float
            for k in ("entry_actual", "current_price", "tp1_pct", "trail_pct"):
                if d.get(k) is not None:
                    d[k] = float(d[k])
            positions.append(d)
        return {"data": positions}
    except Exception as e:
        logger.error("[positions] 조회 실패: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


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


@app.get("/api/signals/stream")
async def signals_stream(request: Request):
    return StreamingResponse(
        _signal_generator(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ── POST /api/scheduler/trigger ───────────────────────────────
_VALID_JOBS = {"stage", "screener", "paper_sample"}


class TriggerBody(BaseModel):
    job: str


@app.post("/api/scheduler/trigger")
async def trigger_job(body: TriggerBody):
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
@app.get("/api/scheduler/status")
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


# ── React 정적 파일 서빙 (프로덕션) ──────────────────────────
_DIST = Path(__file__).parent.parent / "frontend" / "dist"
if _DIST.exists():
    app.mount("/", StaticFiles(directory=str(_DIST), html=True), name="static")
