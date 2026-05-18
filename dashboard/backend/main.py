"""
dashboard/backend/main.py
웹 대시보드 FastAPI 앱 — 9 엔드포인트 + React dist 서빙.

엔드포인트:
  GET  /api/heatmap              — Stage 색상 히트맵 데이터 (5분 캐시)
  GET  /api/positions            — paper_positions 미실현 수익률
  GET  /api/signals/stream       — SSE 신호 라이브 피드
  POST /api/scheduler/trigger    — 스케줄러 잡 수동 트리거
  GET  /api/scheduler/stream     — SSE 스케줄러 상태 스트림
  GET  /api/report/stage         — Stage 분류 결과
  GET  /api/report/screener      — 차트 스크리너 결과
  GET  /api/report/paper         — 모의투자 포지션
  GET  /api/top                  — 당일 거래대금 상위 N 종목 (Kiwoom, 5분 캐시)

개발: uvicorn main:app --reload --port 8000
프로덕션: npm run build → FastAPI가 ../frontend/dist 서빙
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
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

import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from db import upsert_ticker_names as _upsert_ticker_names  # noqa: E402
from kiwoom_aftermarket_sync import KiwoomClient  # noqa: E402

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ── 캐시 (히트맵/가격/Top 모두 5분) ──────────────────────────
_HEATMAP_CACHE: dict = {"data": None, "expires": 0.0}
_PRICE_CACHE: dict = {"data": {}, "expires": 0.0}
_PRICE_TTL = 300     # 5분 — 가격·등락률·히트맵 구조 공통

# ── 키움 토큰 캐시 (au10001 반복 호출 방지, 토큰 유효기간 24h) ──
_KIWOOM_TOKEN: str | None = None
_KIWOOM_TOKEN_TS: float = 0.0
_KIWOOM_TOKEN_TTL = 82800  # 23시간

# ── Top 캐시 (5분) ────────────────────────────────────────────
# 캐시는 n=20 기준 단일 슬롯. n이 다른 요청은 캐시된 데이터를 그대로 반환.
# 프론트엔드가 n=20 고정이므로 충돌 없음. n 변경 시 단일 슬롯 가정 재검토 필요.
_TOP_CACHE: dict = {"data": None, "expires": 0.0}
_TOP_TTL = 300
_TOP_LOCK = asyncio.Lock()


# ── lifespan ──────────────────────────────────────────────────
_INIT_SQL = """
CREATE TABLE IF NOT EXISTS scheduler_triggers (
    id           SERIAL       PRIMARY KEY,
    job_name     VARCHAR(50)  NOT NULL,
    requested_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    executed_at  TIMESTAMPTZ,
    status       VARCHAR(20)  NOT NULL DEFAULT 'pending'
);
CREATE INDEX IF NOT EXISTS idx_sched_trig_status
    ON scheduler_triggers (status, requested_at ASC)
    WHERE status = 'pending';
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(_INIT_SQL)
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS ticker_names ("
            "  ticker TEXT PRIMARY KEY, name_ko TEXT NOT NULL,"
            "  updated_at TIMESTAMPTZ DEFAULT NOW()"
            ")"
        )
        await conn.execute("ALTER TABLE ticker_names ENABLE ROW LEVEL SECURITY")
    logger.info("DB 풀 준비 완료")

    # 오늘 stage 종목 중 이름 캐시 없는 것 채우기 (백그라운드)
    asyncio.create_task(_seed_ticker_names(pool))

    yield
    await close_pool()
    logger.info("DB 풀 종료")


async def _seed_ticker_names(pool) -> None:
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT sc.ticker
                FROM   stage_classifications sc
                WHERE  sc.classified_date >= CURRENT_DATE - 7
                  AND NOT EXISTS (
                    SELECT 1 FROM ticker_names tn
                    WHERE tn.ticker = sc.ticker
                      AND tn.updated_at > NOW() - INTERVAL '7 days'
                  )
                """
            )
        tickers = [r["ticker"] for r in rows]
        if tickers:
            logger.info("[ticker_names] %d종목 이름 조회 중...", len(tickers))
            await _upsert_ticker_names(pool, tickers)
    except Exception as e:
        logger.warning("[ticker_names] 시드 실패: %s", e)


app = FastAPI(title="Trading Dashboard", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Vite dev server
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── 당일 등락률 조회 (yfinance 2d, 5분 캐시) ─────────────────
async def _fetch_stage_prices(tickers: list[str]) -> dict[str, float]:
    now = time.time()
    if _PRICE_CACHE["data"] and now < _PRICE_CACHE["expires"]:
        return _PRICE_CACHE["data"]

    def _fetch() -> dict[str, float]:
        result: dict[str, float] = {}
        try:
            import yfinance as _yf
            import pandas as _pd
            hist = _yf.download(
                tickers, period="2d", interval="1d",
                auto_adjust=True, progress=False, threads=True,
            )
            if hist.empty:
                return result
            close_df = hist["Close"] if isinstance(hist.columns, _pd.MultiIndex) else hist
            for t in tickers:
                try:
                    series = (close_df[t] if t in close_df.columns else close_df.iloc[:, 0]).dropna()
                    if len(series) >= 2:
                        result[t] = round((float(series.iloc[-1]) / float(series.iloc[-2]) - 1) * 100, 2)
                except Exception:
                    pass
        except Exception as e:
            logger.warning("[heatmap] 가격 조회 실패: %s", e)
        return result

    prices = await asyncio.to_thread(_fetch)
    _PRICE_CACHE["data"] = prices
    _PRICE_CACHE["expires"] = now + _PRICE_TTL
    logger.info("[heatmap] 가격 갱신: %d종목", len(prices))
    return prices


# ── 히트맵 데이터 빌드 ────────────────────────────────────────
async def _build_heatmap_data() -> list[dict]:
    pool = await get_pool()
    today = date.today()

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                sc.ticker,
                sc.stage,
                sc.s1_high,
                sc.s1_volume,
                sc.peakout_flag,
                COALESCE(tn.name_ko, k.name_ko, cs.name,
                         SPLIT_PART(sc.ticker, '.', 1))                      AS name,
                CASE WHEN sc.ticker LIKE '%.KS' THEN 'KOSPI'
                     WHEN sc.ticker LIKE '%.KQ' THEN 'KOSDAQ'
                     ELSE '' END                                              AS market
            FROM   stage_classifications sc
            LEFT JOIN ticker_names tn ON tn.ticker = sc.ticker
            LEFT JOIN krx_listings k  ON k.yfinance_symbol = sc.ticker
            LEFT JOIN LATERAL (
                SELECT name FROM chart_signals
                WHERE  ticker = sc.ticker ORDER BY screened_at DESC LIMIT 1
            ) cs ON TRUE
            WHERE  sc.classified_date = $1
            """,
            today,
        )

    if not rows:
        return []

    tickers_list = [r["ticker"] for r in rows]
    prices = await _fetch_stage_prices(tickers_list)

    result = []
    for r in rows:
        s1_high   = float(r["s1_high"])   if r["s1_high"]   else 0.0
        s1_volume = float(r["s1_volume"]) if r["s1_volume"] else 0.0
        amount = s1_high * s1_volume if s1_high and s1_volume else 1.0
        result.append({
            "ticker":     r["ticker"],
            "name":       r["name"],
            "stage":      r["stage"],
            "amount":     amount,
            "change_pct": prices.get(r["ticker"], 0.0),
            "market":     r["market"] or "",
        })

    return sorted(result, key=lambda x: x["amount"], reverse=True)


# ── GET /api/heatmap ──────────────────────────────────────────
@app.get("/api/heatmap")
async def get_heatmap():
    now = time.time()
    # 가격 캐시 만료 시 stage 구조 유지하고 가격만 갱신
    if _HEATMAP_CACHE["data"] and now < _HEATMAP_CACHE["expires"]:
        return {"data": _HEATMAP_CACHE["data"], "cached": True}
    try:
        data = await _build_heatmap_data()
        _HEATMAP_CACHE["data"] = data
        # stage 구조는 30분, 가격은 _PRICE_CACHE가 5분 관리
        # heatmap 캐시는 5분으로 설정 (가격 갱신 주기에 맞춤)
        _HEATMAP_CACHE["expires"] = now + _PRICE_TTL
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
                SELECT p.id, p.ticker,
                       COALESCE(tn.name_ko, k.name_ko,
                                cs.name, SPLIT_PART(p.ticker, '.', 1)) AS name,
                       p.model, p.signal_date,
                       p.entry_actual, p.qty, p.status,
                       p.tp1_pct, p.trail_pct,
                       o.close AS current_price
                FROM   paper_positions p
                LEFT JOIN ticker_names tn ON tn.ticker = p.ticker
                LEFT JOIN krx_listings k  ON k.yfinance_symbol = p.ticker
                LEFT JOIN LATERAL (
                    SELECT name FROM chart_signals
                    WHERE  ticker = p.ticker ORDER BY screened_at DESC LIMIT 1
                ) cs ON TRUE
                LEFT JOIN LATERAL (
                    SELECT close FROM daily_ohlcv
                    WHERE  symbol = p.ticker
                    ORDER  BY date DESC LIMIT 1
                ) o ON TRUE
                WHERE  p.status IN ('open', 'pending')
                ORDER  BY p.signal_date DESC
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


# ── GET /api/scheduler/stream  (SSE) ──────────────────────────
async def _scheduler_stream_generator(request: Request) -> AsyncGenerator[str, None]:
    pool = await get_pool()
    last_payload: str = ""

    async def _fetch() -> str:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, job_name, requested_at, executed_at, status
                FROM   scheduler_triggers
                ORDER  BY requested_at DESC LIMIT 10
                """
            )
        return json.dumps([dict(r) for r in rows], default=str)

    # 초기 전송
    try:
        last_payload = await _fetch()
        yield f"data: {last_payload}\n\n"
    except Exception as e:
        logger.warning("[scheduler-sse] 초기 조회 실패: %s", e)

    # 3초마다 변경 시에만 push
    while True:
        try:
            if await request.is_disconnected():
                break
        except Exception:
            break
        await asyncio.sleep(3)
        try:
            payload = await _fetch()
            if payload != last_payload:
                last_payload = payload
                yield f"data: {payload}\n\n"
        except Exception as e:
            logger.warning("[scheduler-sse] 조회 실패: %s", e)


@app.get("/api/scheduler/stream")
async def scheduler_stream(request: Request):
    return StreamingResponse(
        _scheduler_stream_generator(request),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── GET /api/report/stage ────────────────────────────────────
@app.get("/api/report/stage")
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
@app.get("/api/report/screener")
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
            SELECT ticker, name, close, ma_20w, ma_60w, ma_120w,
                   cloud_top, is_enhanced, has_gapjum, sector, screened_at
            FROM   chart_signals
            WHERE  week_of = $1
            ORDER  BY is_enhanced DESC, has_gapjum DESC, close DESC
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


# ── GET /api/report/paper ─────────────────────────────────────
@app.get("/api/report/paper")
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

        # 최근 청산 이력 (30건)
        closed = await conn.fetch(
            """
            SELECT p.ticker,
                   COALESCE(tn.name_ko, k.name_ko,
                            cs.name, SPLIT_PART(p.ticker, '.', 1)) AS name,
                   p.model, p.signal_date, p.entry_actual,
                   p.exit_date, p.exit_price, p.exit_type,
                   p.blended_return, p.tp1_date
            FROM   paper_positions p
            LEFT JOIN ticker_names tn ON tn.ticker = p.ticker
            LEFT JOIN krx_listings k  ON k.yfinance_symbol = p.ticker
            LEFT JOIN LATERAL (
                SELECT name FROM chart_signals
                WHERE  ticker = p.ticker ORDER BY screened_at DESC LIMIT 1
            ) cs ON TRUE
            WHERE  p.status = 'closed' AND p.blended_return IS NOT NULL
            ORDER  BY p.exit_date DESC NULLS LAST, p.id DESC
            LIMIT  30
            """
        )

        # 현재 오픈 포지션
        open_pos = await conn.fetch(
            """
            SELECT p.ticker,
                   COALESCE(tn.name_ko, k.name_ko,
                            cs.name, SPLIT_PART(p.ticker, '.', 1)) AS name,
                   p.model, p.signal_date, p.entry_actual, p.qty, p.status,
                   o.close AS current_price
            FROM   paper_positions p
            LEFT JOIN ticker_names tn ON tn.ticker = p.ticker
            LEFT JOIN krx_listings k  ON k.yfinance_symbol = p.ticker
            LEFT JOIN LATERAL (
                SELECT name FROM chart_signals
                WHERE  ticker = p.ticker ORDER BY screened_at DESC LIMIT 1
            ) cs ON TRUE
            LEFT JOIN LATERAL (
                SELECT close FROM daily_ohlcv
                WHERE  symbol = p.ticker ORDER BY date DESC LIMIT 1
            ) o ON TRUE
            WHERE  p.status IN ('open', 'pending')
            ORDER  BY p.signal_date DESC
            """
        )

    def _fmt_pos(r) -> dict:
        d = dict(r)
        for k in ("entry_actual", "exit_price", "blended_return", "current_price"):
            if d.get(k) is not None:
                d[k] = float(d[k])
        for k in ("signal_date", "exit_date", "tp1_date"):
            if d.get(k) is not None:
                d[k] = str(d[k])
        if d.get("entry_actual") and d.get("current_price"):
            d["unrealized_pct"] = round((d["current_price"] / d["entry_actual"] - 1) * 100, 2)
        else:
            d["unrealized_pct"] = None
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

    return {
        "data": {
            "model_summary": models,
            "open": [_fmt_pos(r) for r in open_pos],
            "closed": [_fmt_pos(r) for r in closed],
        }
    }


# ── 키움 토큰 관리 ────────────────────────────────────────────

def _get_kiwoom_token() -> str:
    """키움 OAuth 토큰 반환 (23h 캐시, 만료 시 재발급)."""
    global _KIWOOM_TOKEN, _KIWOOM_TOKEN_TS
    now = time.time()
    if _KIWOOM_TOKEN and now - _KIWOOM_TOKEN_TS < _KIWOOM_TOKEN_TTL:
        return _KIWOOM_TOKEN
    appkey = os.environ.get("KIWOOM_APPKEY")
    secretkey = os.environ.get("KIWOOM_SECRETKEY")
    if not appkey or not secretkey:
        raise RuntimeError("KIWOOM_APPKEY / KIWOOM_SECRETKEY 환경변수 미설정")
    client = KiwoomClient(use_mock=False)
    _KIWOOM_TOKEN = client.issue_token(appkey, secretkey)
    _KIWOOM_TOKEN_TS = now
    logger.info("[top] 키움 토큰 재발급 완료")
    return _KIWOOM_TOKEN


def _invalidate_kiwoom_token() -> None:
    global _KIWOOM_TOKEN, _KIWOOM_TOKEN_TS
    _KIWOOM_TOKEN = None
    _KIWOOM_TOKEN_TS = 0.0


def _fetch_top_kiwoom(n: int) -> dict:
    """Kiwoom REST API로 거래대금 상위 N 조회 (동기 — asyncio.to_thread에서 호출).

    401 수신 시 토큰을 무효화하고 1회 재시도합니다.
    """
    import requests as _requests
    for attempt in range(2):
        try:
            client = KiwoomClient(use_mock=False)
            client.inject_token(_get_kiwoom_token())
            items = client.fetch_top_volume(n=n)
            return {"items": items, "fetched_at": time.strftime("%H:%M:%S")}
        except _requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 401 and attempt == 0:
                logger.warning("[top] 401 수신 — 토큰 무효화 후 재시도")
                _invalidate_kiwoom_token()
                continue
            raise


# ── GET /api/top ──────────────────────────────────────────────
@app.get("/api/top")
async def get_top(n: int = 20, refresh: bool = False):
    """당일 거래대금 상위 N 종목 (Kiwoom ka10032 거래대금상위요청, 5분 캐시).

    캐시는 n=20 기준 단일 슬롯. 뮤텍스(_TOP_LOCK)로 동시 API 호출 방지.
    """
    n = min(max(n, 1), 100)
    now = time.time()
    if not refresh and _TOP_CACHE["data"] and now < _TOP_CACHE["expires"]:
        return _TOP_CACHE["data"]
    async with _TOP_LOCK:
        # 락 대기 중 다른 코루틴이 캐시를 채웠을 수 있음 — 재확인
        now = time.time()
        if not refresh and _TOP_CACHE["data"] and now < _TOP_CACHE["expires"]:
            return _TOP_CACHE["data"]
        try:
            data = await asyncio.to_thread(_fetch_top_kiwoom, n)
            _TOP_CACHE["data"] = data
            _TOP_CACHE["expires"] = now + _TOP_TTL
            return data
        except Exception as e:
            logger.warning("[top] Kiwoom API 오류: %s", e)
            _safe_err = "API 오류 — 서버 로그 확인"
            if _TOP_CACHE["data"]:
                return {**_TOP_CACHE["data"], "stale": True, "error": _safe_err}
            return {"items": [], "fetched_at": "--:--", "error": _safe_err}


# ── React 정적 파일 서빙 (프로덕션) ──────────────────────────
_DIST = Path(__file__).parent.parent / "frontend" / "dist"
if _DIST.exists():
    app.mount("/", StaticFiles(directory=str(_DIST), html=True), name="static")
