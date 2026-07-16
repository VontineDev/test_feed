"""
dashboard/backend/main.py
웹 대시보드 FastAPI 앱 — 앱 조립(미들웨어·lifespan·warmup·health) + React dist 서빙.

라우트는 라우터 모듈로 분리됨 (2026-07 리팩토링, 총 30 엔드포인트):
  routers_heatmap    — /api/heatmap, /api/positions
  routers_signals    — /api/signals/stream (SSE)
  routers_scheduler  — /api/scheduler/* (trigger/status/stream)
  routers_report     — /api/report/* (stage/screener/pipeline-status/unified/paper)
  routers_youtube    — /api/youtube/screener
  routers_top        — /api/top
  routers_paper      — /api/paper/* (history/curve/export)
  routers_history    — /api/history/* (stage/screener/ticker)
  routers_macro      — /api/macro, /api/market_index
  routers_feedback   — /api/feedback, /api/auth/me
  routers_portfolio  — /api/portfolio*
  routers_ticker     — /api/ticker/lookup
  routers_dart       — /api/dart/summary
공용 계층: common.py(캘린더·SWR 캐시·스레드 풀), market_snap.py(Kiwoom 시세 소스)

개발: uvicorn main:app --reload --port 8000
프로덕션: npm run build → FastAPI가 ../frontend/dist 서빙
"""
from __future__ import annotations

import asyncio
import base64
import logging
import os
import secrets
import time as _time_module
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from database import close_pool, get_pool
from common import (  # noqa: F401 — 공용 인프라 (재수출 겸용)
    _HEATMAP_CACHE,
    _HEATMAP_LOCK,
    _KST,
    _AFTERMARKET_TTL,
    _NXT_TTL,
    _PRICE_TTL,
    _EXT_EXECUTOR,
    _ext_thread,
    _is_holiday,
    _is_market_open,
    _is_nxt_open,
    _cache_is_valid,
    _bg_refresh,
    _compute_cache_ttl,
    _fetch_current_prices,
    _POS_PRICE_CACHE,
    _SSE_CONNECTIONS,
)

import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from core.db import upsert_ticker_names as _upsert_ticker_names  # noqa: E402

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

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
CREATE INDEX IF NOT EXISTS idx_sched_stream
    ON scheduler_triggers (requested_at DESC);

CREATE TABLE IF NOT EXISTS dart_extractions (
    corp_name        TEXT NOT NULL,
    rcept_no         TEXT NOT NULL,
    report_type      TEXT,
    period           TEXT,
    extraction_text  TEXT,
    model            TEXT,
    prompt_ver       TEXT DEFAULT 'v1',
    xml_chars        INT,
    extracted_at     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (corp_name, rcept_no)
);

CREATE TABLE IF NOT EXISTS manual_portfolio (
    id          BIGSERIAL     PRIMARY KEY,
    ticker      VARCHAR(12)   NOT NULL UNIQUE,
    name        TEXT          NOT NULL,
    avg_price   NUMERIC(14,2) NOT NULL,
    qty         NUMERIC(14,6) NOT NULL,
    created_at  TIMESTAMPTZ   DEFAULT NOW(),
    updated_at  TIMESTAMPTZ   DEFAULT NOW()
);
"""


_STARTUP_TIME: float = 0.0


async def _warmup_caches() -> None:
    """서버 기동 시 캐시를 미리 채워 cold start 지연을 방지한다."""
    logger.info("[warmup] 캐시 사전 로딩 시작")
    # heatmap 먼저: macro 분석이 오늘 TOP 20 종목을 사용하도록 순서 보장
    try:
        # _bg_refresh가 _build_heatmap_data() 결과(dict)를 저장한 뒤
        # TTL은 fetched_at 유무로 결정. warmup 시점에는 아직 데이터 없으므로
        # _is_market_open() 기준으로 초기 TTL 선택.
        ttl_heat = _PRICE_TTL if _is_market_open() else _AFTERMARKET_TTL
        await _bg_refresh(_HEATMAP_CACHE, _HEATMAP_LOCK, _build_heatmap_data, ttl_heat, "heatmap")
        # warmup 완료 후 실제 데이터 기준으로 TTL 재보정
        if _HEATMAP_CACHE["data"] and _HEATMAP_CACHE["data"].get("fetched_at"):
            _HEATMAP_CACHE["expires"] = _time_module.time() + _AFTERMARKET_TTL
    except Exception as e:
        logger.warning("[warmup] heatmap 실패 (무시): %s", e)
    # heatmap 완료 후 macro + market_index 병렬 실행
    results = await asyncio.gather(
        _bg_refresh(_MARKET_INDEX_CACHE, _MARKET_INDEX_LOCK, _fetch_market_index_data, _MARKET_INDEX_TTL, "market_index"),
        _bg_refresh(_MACRO_CACHE, _MACRO_LOCK, lambda: _ext_thread(_run_macro_analysis, timeout=90.0), _MACRO_TTL, "macro"),
        return_exceptions=True,
    )
    for label, exc in zip(["market_index", "macro"], results):
        if isinstance(exc, Exception):
            logger.warning("[warmup] %s 실패 (무시): %s", label, exc)
    logger.info("[warmup] 완료")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _STARTUP_TIME
    _STARTUP_TIME = _time_module.time()
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
        await conn.execute("ALTER TABLE manual_portfolio ENABLE ROW LEVEL SECURITY")
        await conn.execute("ALTER TABLE dart_extractions ENABLE ROW LEVEL SECURITY")
        # qty: INTEGER → NUMERIC(14,6) 마이그레이션 (해외주식 소수 수량 지원)
        await conn.execute("""
            DO $$ BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'manual_portfolio'
                      AND column_name = 'qty'
                      AND data_type = 'integer'
                ) THEN
                    ALTER TABLE manual_portfolio ALTER COLUMN qty TYPE NUMERIC(14,6);
                END IF;
            END $$;
        """)
    # daily_market_snap 테이블 생성 (없으면)
    try:
        from core.db import get_dsn as _get_dsn
        from data.kiwoom_aftermarket_sync import ensure_daily_snap_table as _ensure_snap
        await asyncio.to_thread(_ensure_snap, _get_dsn())
        logger.info("daily_market_snap 테이블 확인 완료")
    except Exception as _e:
        logger.warning("daily_market_snap 테이블 생성 실패 (무시): %s", _e)
    logger.info("DB 풀 준비 완료")

    # 종목 이름 시드 + 캐시 사전 로딩 (백그라운드)
    asyncio.create_task(_seed_ticker_names(pool))
    asyncio.create_task(_warmup_caches())

    yield
    _EXT_EXECUTOR.shutdown(wait=False)
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


class _BasicAuthMiddleware(BaseHTTPMiddleware):
    """
    역할 기반 Basic Auth (3단계).
    - ADMIN_USER/ADMIN_PASSWORD     → role=admin   (스케줄러 트리거 + 포트폴리오)
    - SPECIAL_USER/SPECIAL_PASSWORD → role=special  (포트폴리오 조회 + 읽기)
    - DASHBOARD_USER/DASHBOARD_PASSWORD → role=user (읽기 전용)
    - ADMIN_USER 미설정 시 DASHBOARD_USER도 admin 취급 (하위 호환)
    - 모두 미설정 → 인증 없음, role=admin (로컬 dev)
    """

    async def dispatch(self, request: Request, call_next):
        admin_user   = os.environ.get("ADMIN_USER", "")
        admin_pw     = os.environ.get("ADMIN_PASSWORD", "")
        special_user = os.environ.get("SPECIAL_USER", "")
        special_pw   = os.environ.get("SPECIAL_PASSWORD", "")
        dash_user    = os.environ.get("DASHBOARD_USER", "")
        dash_pw      = os.environ.get("DASHBOARD_PASSWORD", "")

        if not admin_user and not special_user and not dash_user:
            request.state.role = "admin"
            return await call_next(request)

        role: str | None = None
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth[6:]).decode("utf-8", errors="replace")
                req_user, _, req_pw = decoded.partition(":")
                if admin_user and secrets.compare_digest(req_user, admin_user) and secrets.compare_digest(req_pw, admin_pw):
                    role = "admin"
                elif special_user and secrets.compare_digest(req_user, special_user) and secrets.compare_digest(req_pw, special_pw):
                    role = "special"
                elif dash_user and secrets.compare_digest(req_user, dash_user) and secrets.compare_digest(req_pw, dash_pw):
                    role = "admin" if not admin_user else "user"
            except Exception:
                pass

        if role is None:
            return Response(
                status_code=401,
                headers={"WWW-Authenticate": 'Basic realm="Trading Dashboard"'},
                content="Unauthorized",
            )

        request.state.role = role
        return await call_next(request)


app.add_middleware(_BasicAuthMiddleware)

# ── 라우터 등록 ──────────────────────────────────────────────
# 의존 방향: routers_* → common/market_snap/database/core.*/data.*/analysis.*
# 라우터는 main과 서로를 import하지 않음. main만 라우터를 import.
import routers_dart  # noqa: E402
import routers_feedback  # noqa: E402
import routers_heatmap  # noqa: E402
import routers_history  # noqa: E402
import routers_macro  # noqa: E402
import routers_paper  # noqa: E402
import routers_portfolio  # noqa: E402
import routers_report  # noqa: E402
import routers_scheduler  # noqa: E402
import routers_signals  # noqa: E402
import routers_ticker  # noqa: E402
import routers_top  # noqa: E402
import routers_youtube  # noqa: E402
app.include_router(routers_dart.router)
app.include_router(routers_feedback.router)
app.include_router(routers_heatmap.router)
app.include_router(routers_history.router)
app.include_router(routers_macro.router)
app.include_router(routers_paper.router)
app.include_router(routers_portfolio.router)
app.include_router(routers_report.router)
app.include_router(routers_scheduler.router)
app.include_router(routers_signals.router)
app.include_router(routers_ticker.router)
app.include_router(routers_top.router)
app.include_router(routers_youtube.router)

# 하위호환 재수출 — monkeypatch는 각 라우터 모듈에 해야 함
from routers_feedback import FeedbackBody, auth_me, post_feedback  # noqa: E402,F401
from routers_history import (  # noqa: E402,F401
    get_screener_history,
    get_stage_history,
    get_ticker_history,
)
from routers_report import (  # noqa: E402,F401
    get_paper_report,
    get_pipeline_status,
    get_screener_report,
    get_stage_report,
    get_unified_screener,
)
from routers_youtube import get_youtube_screener  # noqa: E402,F401
from routers_portfolio import (  # noqa: E402,F401
    add_holding,
    delete_holding,
    get_portfolio,
    update_holding,
)
from routers_ticker import lookup_ticker  # noqa: E402,F401
from routers_dart import get_dart_summary  # noqa: E402,F401
from routers_paper import (  # noqa: E402,F401
    get_paper_curve,
    get_paper_export,
    get_paper_ticker_history,
)
from routers_signals import signals_stream  # noqa: E402,F401
from routers_heatmap import (  # noqa: E402,F401
    _build_heatmap_data,
    get_heatmap,
    get_positions,
)
from routers_top import _TOP_CACHE, get_top  # noqa: E402,F401
from routers_macro import (  # noqa: E402,F401
    _MACRO_CACHE,
    _MACRO_LOCK,
    _MACRO_TTL,
    _MARKET_INDEX_CACHE,
    _MARKET_INDEX_LOCK,
    _MARKET_INDEX_TTL,
    _fetch_market_index_data,
    _run_macro_analysis,
    get_macro,
    get_market_index,
)
from routers_scheduler import (  # noqa: E402,F401
    TriggerBody,
    scheduler_status,
    scheduler_stream,
    trigger_job,
)


# ── GET /health ───────────────────────────────────────────────
@app.get("/health")
async def health():
    """서버 상태 — Caddy 생존 체크 + 운영 모니터링용."""
    now = _time_module.time()

    def _cache_info(cache: dict) -> dict:
        if not cache["data"]:
            return {"cached": False}
        expires_in = max(0.0, round(cache["expires"] - now, 1))
        return {"cached": True, "expires_in_s": expires_in}

    pool_info: dict = {}
    try:
        pool = await get_pool()
        pool_info = {
            "min_size": pool.get_min_size(),
            "max_size": pool.get_max_size(),
            "size":     pool.get_size(),
            "free":     pool.get_idle_size(),
        }
    except Exception:
        pool_info = {"error": "pool unavailable"}

    return {
        "status":    "ok",
        "uptime_s":  round(now - _STARTUP_TIME, 1) if _STARTUP_TIME else None,
        "db_pool":   pool_info,
        "cache": {
            "heatmap":      _cache_info(_HEATMAP_CACHE),
            "top":          _cache_info(_TOP_CACHE),
            "macro":        _cache_info(_MACRO_CACHE),
            "market_index": _cache_info(_MARKET_INDEX_CACHE),
        },
        "sse": dict(_SSE_CONNECTIONS),
    }


# ── React 정적 파일 서빙 (프로덕션) ──────────────────────────
_DIST = Path(__file__).parent.parent / "frontend" / "dist"
if _DIST.exists():
    app.mount("/", StaticFiles(directory=str(_DIST), html=True), name="static")
