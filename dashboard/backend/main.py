"""
dashboard/backend/main.py
웹 대시보드 FastAPI 앱 — 12 엔드포인트 + React dist 서빙.

엔드포인트:
  GET  /api/heatmap              — Stage 색상 히트맵 데이터 (5분 캐시)
  GET  /api/positions            — paper_positions 미실현 수익률
  GET  /api/signals/stream       — SSE 신호 라이브 피드
  POST /api/scheduler/trigger    — 스케줄러 잡 수동 트리거
  GET  /api/scheduler/stream     — SSE 스케줄러 상태 스트림
  GET  /api/report/stage         — Stage 분류 결과 (최신일)
  GET  /api/report/screener      — 차트 스크리너 결과 (최신주)
  GET  /api/report/paper         — 모의투자 포지션
  GET  /api/top                  — 당일 거래대금 상위 N 종목 (Kiwoom, 5분 캐시)
  GET  /api/history/stage        — 기간별 Stage 분류 집계 (이력 트래킹)
  GET  /api/history/screener     — 기간별 스크리너 집계 (이력 트래킹)
  GET  /api/history/ticker/{t}   — 종목별 Stage+스크리너 이력
  GET  /api/dart/summary/{t}    — DART 최신 보고서 재무요약 (매출/영업이익/사업부문)
  POST /api/feedback             — 피드백 텍스트+스크린샷 → Telegram 전송

개발: uvicorn main:app --reload --port 8000
프로덕션: npm run build → FastAPI가 ../frontend/dist 서빙
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import secrets
import time as _time_module
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
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
from data.kiwoom_aftermarket_sync import KiwoomClient, _parse_int, _parse_float, _VALUE_UNIT  # noqa: E402
from analysis.macro_tracker import MacroTracker, DEFAULT_TICKERS as _MACRO_TICKERS  # noqa: E402
from data.market_data import _fetch_fundamental  # noqa: E402

from market_snap import (  # noqa: E402
    _fetch_aftermarket_snap_top_async,
    _fetch_daily_snap_top_async,
    _fetch_nxt_live,
    _fetch_top_kiwoom,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ── Top 캐시 (5분) ────────────────────────────────────────────
# 캐시는 n=20 기준 단일 슬롯. n이 다른 요청은 캐시된 데이터를 그대로 반환.
# 프론트엔드가 n=20 고정이므로 충돌 없음. n 변경 시 단일 슬롯 가정 재검토 필요.
_TOP_CACHE: dict = {"data": None, "expires": 0.0, "market_open": None, "is_nxt": None}
_TOP_TTL = 300            # 장 중 5분
_TOP_LOCK = asyncio.Lock()

# ── 매크로 캐시 (10분) ───────────────────────────────────────
_MACRO_CACHE: dict = {"data": None, "expires": 0.0}
_MACRO_TTL = 600  # 10분 (yfinance 다운로드 비용 고려)
_MACRO_LOCK = asyncio.Lock()

# ── 시장 지수 캐시 (5분) ─────────────────────────────────────
_MARKET_INDEX_CACHE: dict = {"data": None, "expires": 0.0}
_MARKET_INDEX_TTL = 300  # 5분
_MARKET_INDEX_LOCK = asyncio.Lock()


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
# 라우터 모듈은 common/database/core.*만 의존 — main을 import하지 않음.
import routers_feedback  # noqa: E402
import routers_history  # noqa: E402
import routers_report  # noqa: E402
import routers_heatmap  # noqa: E402
import routers_paper  # noqa: E402
import routers_portfolio  # noqa: E402
import routers_scheduler  # noqa: E402
import routers_signals  # noqa: E402
import routers_youtube  # noqa: E402
app.include_router(routers_feedback.router)
app.include_router(routers_heatmap.router)
app.include_router(routers_history.router)
app.include_router(routers_paper.router)
app.include_router(routers_portfolio.router)
app.include_router(routers_report.router)
app.include_router(routers_scheduler.router)
app.include_router(routers_signals.router)
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
    get_dart_summary,
    get_portfolio,
    lookup_ticker,
    update_holding,
)
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
from routers_scheduler import (  # noqa: E402,F401
    TriggerBody,
    scheduler_status,
    scheduler_stream,
    trigger_job,
)


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


@app.get("/api/top")
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


# ── GET /api/macro ────────────────────────────────────────────

def _fetch_prev_top20_sync() -> dict[str, str] | None:
    """최근 영업일 거래대금 TOP 20 → {ticker: name}. 실패 시 None.

    1순위: daily_market_snap (ka10032 top100, 전 종목)
    2순위: aftermarket_snap  (NXT 거래 종목만, 폴백)
    """
    try:
        from core.db_sync import connect as _db_connect
        conn = _db_connect()
        try:
            with conn.cursor() as cur:
                # 1순위: daily_market_snap
                cur.execute("""
                    SELECT d.ticker,
                           COALESCE(tn.name_ko, d.name,
                                    SPLIT_PART(d.ticker, '.', 1)) AS name
                    FROM   daily_market_snap d
                    LEFT JOIN ticker_names tn ON tn.ticker = d.ticker
                    WHERE  d.trade_date = (SELECT MAX(trade_date) FROM daily_market_snap)
                      AND  d.amount > 0
                    ORDER  BY d.amount DESC
                    LIMIT  20
                """)
                rows = cur.fetchall()
                if rows:
                    result = {row[0]: row[1] for row in rows if row[0]}
                    logger.info("[macro] daily_market_snap 전일 TOP %d 종목 로드", len(result))
                    return result if result else None

                # 2순위: aftermarket_snap 폴백
                cur.execute("""
                    SELECT a.ticker,
                           COALESCE(tn.name_ko, SPLIT_PART(a.ticker, '.', 1)) AS name
                    FROM   aftermarket_snap a
                    LEFT JOIN ticker_names tn ON tn.ticker = a.ticker
                    WHERE  a.trade_date = (SELECT MAX(trade_date) FROM aftermarket_snap)
                      AND  COALESCE(a.reg_value, a.after_value, 0) > 0
                    ORDER  BY COALESCE(a.reg_value, a.after_value, 0) DESC
                    LIMIT  20
                """)
                rows = cur.fetchall()
        finally:
            conn.close()
        if not rows:
            return None
        result = {row[0]: row[1] for row in rows if row[0]}
        logger.info("[macro] aftermarket_snap 전일 TOP %d 종목 로드 (폴백)", len(result))
        return result if result else None
    except Exception as e:
        logger.warning("[macro] 전일 TOP 조회 실패: %s", e)
        return None


def _kiwoom_to_yfinance(ticker: str, market: str = "") -> str | None:
    """Kiwoom REST API 티커를 yfinance 포맷으로 변환.

    Kiwoom ka10032 응답의 stk_cd 는 'XXXXXX_AL'(KOSPI) / 'XXXXXX_AQ'(KOSDAQ) 형식.
    yfinance 는 'XXXXXX.KS' / 'XXXXXX.KQ' 형식을 기대.
    이미 '.' 포함(yfinance 포맷)이면 그대로 반환. 변환 불가 시 None.
    """
    if "." in ticker:
        return ticker
    if ticker.endswith("_AL"):
        return ticker[:-3] + ".KS"
    if ticker.endswith("_AQ"):
        return ticker[:-3] + ".KQ"
    if market == "KOSPI":
        return ticker + ".KS"
    if market == "KOSDAQ":
        return ticker + ".KQ"
    return None


def _run_macro_analysis() -> dict:
    """MacroTracker 분석 실행 (동기, asyncio.to_thread에서 호출)."""
    # 1순위: 오늘 실시간 히트맵 캐시 — 전체 풀에서 변환 가능한 것 모두 분석 후 거래대금 상위 20개 선별
    heatmap_items: list[dict] = (_HEATMAP_CACHE.get("data") or {}).get("items") or []
    heatmap_rank: dict[str, int] = {}  # yf_ticker → 거래대금 순위 (1-based)
    if len(heatmap_items) >= 5:
        live_tickers: dict[str, str] = {}
        rank = 0
        for item in heatmap_items:
            if not item.get("ticker") or not item.get("name"):
                continue
            yf_tk = _kiwoom_to_yfinance(item["ticker"], item.get("market", ""))
            if yf_tk:
                rank += 1
                heatmap_rank[yf_tk] = rank
                live_tickers[yf_tk] = item["name"]
        if live_tickers:
            logger.info("[macro] 히트맵 %d 종목 분석 (거래대금 상위 20 선별)", len(live_tickers))
        else:
            logger.info("[macro] 히트맵 티커 변환 불가 — aftermarket_snap 폴백")
            live_tickers = _fetch_prev_top20_sync()
            if live_tickers is None:
                logger.info("[macro] 전일 aftermarket 없음 — DEFAULT_TICKERS 사용")
    else:
        # 2순위: aftermarket_snap 전날 TOP 20
        live_tickers = _fetch_prev_top20_sync()
        if live_tickers is None:
            logger.info("[macro] 전일 aftermarket 없음 — DEFAULT_TICKERS 사용")

    tracker = MacroTracker(period="2y", min_obs=60)
    tracker.fit(live_tickers)

    snapshot = tracker.snapshot()

    # 종목별 결과 + 팩터별 5일 기여 계산
    stocks = []
    for r in tracker._results:
        factor_contribs: dict[str, float] = {}
        for f in ["rate", "fx", "oil", "vix", "dxy", "export"]:
            beta = r["betas"].get(f, 0.0)
            delta5 = snapshot.get(f, {}).get("change_5d", 0.0)
            factor_contribs[f] = round(beta * delta5, 4)

        stocks.append({
            "ticker":              r["ticker"],
            "name":                r["name"],
            "n_obs":               r["n_obs"],
            "r_squared":           r["r_squared"],
            "adj_r_squared":       r["adj_r_squared"],
            "residual_std":        r["residual_std"],
            "macro_score":         r["macro_score"],
            "macro_score_5d":      r["macro_score_5d"],
            "macro_score_20d":     r["macro_score_20d"],
            "significant_factors": r["significant_factors"],
            "betas":               {k: round(v, 5) for k, v in r["betas"].items() if k != "alpha"},
            "alpha":               round(r["betas"].get("alpha", 0.0), 6),
            "t_stats":             {k: v for k, v in r["t_stats"].items() if k != "alpha"},
            "p_values":            {k: v for k, v in r["p_values"].items() if k != "alpha"},
            "factor_contribs_5d":  factor_contribs,
        })

    # 거래대금 순서 정렬 후 상위 20개 선별 (히트맵 경로), fallback은 macro_score 내림차순
    if heatmap_rank:
        stocks.sort(key=lambda s: heatmap_rank.get(s["ticker"], 9999))
        stocks = stocks[:20]
    else:
        stocks.sort(key=lambda s: s["macro_score"], reverse=True)

    return {
        "snapshot":   snapshot,
        "stocks":     stocks,
        "fetched_at": _time_module.strftime("%H:%M:%S"),
    }


def _market_sentiment(
    kospi_pct: float | None,
    kosdaq_pct: float | None,
) -> tuple[str, str]:
    """KOSPI/KOSDAQ 등락률 → (sentiment, detail) 규칙 기반 분류."""
    if kospi_pct is None and kosdaq_pct is None:
        return "정보없음", "지수 데이터 로딩 실패"

    available = [p for p in (kospi_pct, kosdaq_pct) if p is not None]
    avg = sum(available) / len(available)
    both = kospi_pct is not None and kosdaq_pct is not None

    if avg >= 2.0:
        detail = "코스피/코스닥 모두 급등 — 매수세 강함" if both else "지수 급등 — 매수세 강함 (코스피 기준)"
        return "강세", detail
    elif avg >= 0.5:
        return "상승", f"시장 전반 오름세{'' if both else ' (코스피 기준)'}"
    elif avg >= -0.5:
        return "보합", f"큰 방향성 없이 혼조{'' if both else ' (코스피 기준)'}"
    elif avg >= -2.0:
        return "하락", f"시장 전반 내림세{'' if both else ' (코스피 기준)'}"
    else:
        detail = "코스피/코스닥 모두 하락폭 커짐" if both else "지수 급락 — 낙폭 확대 (코스피 기준)"
        return "급락", detail


async def _fetch_market_index_data() -> dict:
    """KOSPI/KOSDAQ 지수 데이터를 실제로 조회하는 순수 fetch 함수."""
    now_kst = datetime.now(_KST)
    is_open = _is_market_open()
    bas_dd = now_kst.strftime("%Y%m%d")

    def _fetch_krx():
        try:
            from data.krx_openapi import get_client as _krx_client
            client = _krx_client()
            return (
                client.get_kospi_index_ohlcv(bas_dd),
                client.get_kosdaq_index_ohlcv(bas_dd),
            )
        except Exception as e:
            logger.warning("[market_index] KRX 조회 실패: %s", e)
            return None, None

    def _fetch_yf_daily():
        """yfinance 10일 일별 데이터 → (close, prev_close) 쌍 반환.
        KRX 실패 시 current+prev_close 모두 yfinance로 커버.
        오늘 부분 데이터가 마지막 행에 있을 수 있으므로
        오늘 이전(today) 마지막 완결 행을 prev_close로 사용."""
        try:
            import yfinance as _yf
            from datetime import date as _date
            hist = _yf.download(["^KS11", "^KQ11"], period="10d", interval="1d",
                                auto_adjust=True, progress=False, threads=True)
            if hist.empty:
                return None, None, None, None
            close_df = hist["Close"]
            today_str = _date.today().isoformat()

            def _close_prev(s):
                if s is None:
                    return None, None
                s = s.dropna()
                if s.empty:
                    return None, None
                # 오늘 날짜 행과 그 이전 행 분리
                idx_strs = [str(i.date()) if hasattr(i, "date") else str(i)[:10] for i in s.index]
                past = [v for dt, v in zip(idx_strs, s) if dt < today_str]
                current = float(s.iloc[-1])
                prev = float(past[-1]) if past else None
                return current, prev

            ks_s = close_df["^KS11"] if "^KS11" in close_df.columns else None
            kq_s = close_df["^KQ11"] if "^KQ11" in close_df.columns else None
            ks_close, ks_prev = _close_prev(ks_s)
            kq_close, kq_prev = _close_prev(kq_s)
            return ks_close, ks_prev, kq_close, kq_prev
        except Exception as e:
            logger.warning("[market_index] yfinance daily 조회 실패: %s", e)
            return None, None, None, None

    def _fetch_realtime():
        try:
            import yfinance as _yf
            hist = _yf.download(["^KS11", "^KQ11"], period="1d", interval="1m",
                                auto_adjust=True, progress=False, threads=True)
            if hist.empty:
                return None, None
            close_df = hist["Close"]
            ks = float(close_df["^KS11"].dropna().iloc[-1]) if "^KS11" in close_df.columns else None
            kq = float(close_df["^KQ11"].dropna().iloc[-1]) if "^KQ11" in close_df.columns else None
            return ks, kq
        except Exception as e:
            logger.warning("[market_index] yfinance 실시간 조회 실패: %s", e)
            return None, None

    try:
        krx_kospi, krx_kosdaq = await _ext_thread(_fetch_krx, timeout=15.0)
    except asyncio.TimeoutError:
        logger.warning("[market_index] KRX 타임아웃 (15s)")
        krx_kospi, krx_kosdaq = None, None

    is_realtime = False
    kospi_close = krx_kospi["close"] if krx_kospi else None
    kosdaq_close = krx_kosdaq["close"] if krx_kosdaq else None
    kospi_prev  = krx_kospi["prev_close"]  if krx_kospi  else None
    kosdaq_prev = krx_kosdaq["prev_close"] if krx_kosdaq else None

    # KRX 지수 조회 실패 시 yfinance daily로 close + prev_close 보완
    if not krx_kospi and not krx_kosdaq:
        try:
            yf_ks, yf_ks_prev, yf_kq, yf_kq_prev = await _ext_thread(_fetch_yf_daily, timeout=20.0)
        except asyncio.TimeoutError:
            logger.warning("[market_index] yfinance daily 타임아웃 (20s)")
            yf_ks = yf_ks_prev = yf_kq = yf_kq_prev = None
        if yf_ks:
            kospi_close, kospi_prev = yf_ks, yf_ks_prev
        if yf_kq:
            kosdaq_close, kosdaq_prev = yf_kq, yf_kq_prev

    if is_open and (kospi_close or kosdaq_close):
        try:
            rt_ks, rt_kq = await _ext_thread(_fetch_realtime, timeout=20.0)
        except asyncio.TimeoutError:
            logger.warning("[market_index] yfinance 실시간 타임아웃 (20s)")
            rt_ks, rt_kq = None, None
        if rt_ks:
            kospi_close = rt_ks
            is_realtime = True
        if rt_kq:
            kosdaq_close = rt_kq
            is_realtime = True

    def _pct(close, prev):
        if close and prev and prev > 0:
            return round((close - prev) / prev * 100, 2)
        return None
    kospi_pct   = _pct(kospi_close,  kospi_prev)
    kosdaq_pct  = _pct(kosdaq_close, kosdaq_prev)

    sentiment, sentiment_detail = _market_sentiment(kospi_pct, kosdaq_pct)

    return {
        "market_status":    "open" if is_open else "closed",
        "is_realtime":      is_realtime,
        "kospi":  {"change_pct": kospi_pct,  "close": kospi_close,  "prev_close": kospi_prev}  if kospi_close  else None,
        "kosdaq": {"change_pct": kosdaq_pct, "close": kosdaq_close, "prev_close": kosdaq_prev} if kosdaq_close else None,
        "sentiment":        sentiment,
        "sentiment_detail": sentiment_detail,
        "as_of":            now_kst.isoformat(),
    }


@app.get("/api/market_index")
async def get_market_index():
    """KOSPI / KOSDAQ 지수 등락률 + 시장 감성 (5분 캐시).

    장중: yfinance ^KS11/^KQ11 현재가 + KRX BASPRC_IDX(기준가) → change_pct 계산.
    장마감/주말: KRX OpenAPI 확정값.
    응답: {market_status, is_realtime, kospi, kosdaq, sentiment, sentiment_detail, as_of}
    """
    now = _time_module.time()
    if _MARKET_INDEX_CACHE["data"] and now < _MARKET_INDEX_CACHE["expires"]:
        return _MARKET_INDEX_CACHE["data"]
    if _MARKET_INDEX_CACHE["data"]:
        # stale 데이터 있음 — 즉시 반환하고 백그라운드에서 갱신
        if not _MARKET_INDEX_LOCK.locked():
            asyncio.create_task(_bg_refresh(
                _MARKET_INDEX_CACHE, _MARKET_INDEX_LOCK,
                _fetch_market_index_data, _MARKET_INDEX_TTL, "market_index"
            ))
        return _MARKET_INDEX_CACHE["data"]
    # 최초 기동: 한 번만 대기
    async with _MARKET_INDEX_LOCK:
        if _MARKET_INDEX_CACHE["data"] and _time_module.time() < _MARKET_INDEX_CACHE["expires"]:
            return _MARKET_INDEX_CACHE["data"]
        result = await _fetch_market_index_data()
        _MARKET_INDEX_CACHE["data"] = result
        _MARKET_INDEX_CACHE["expires"] = _time_module.time() + _MARKET_INDEX_TTL
        return result


@app.get("/api/macro")
async def get_macro(refresh: bool = False):
    """
    매크로 팩터 분석 결과 (10분 캐시).

    최초 호출 시 yfinance 다운로드로 30~60초 소요.
    이후 캐시에서 즉시 반환.
    """
    now = _time_module.time()
    if not refresh and _MACRO_CACHE["data"] and now < _MACRO_CACHE["expires"]:
        return {**_MACRO_CACHE["data"], "cached": True}
    if not refresh and _MACRO_CACHE["data"]:
        # stale 데이터 있음 — 즉시 반환하고 백그라운드에서 갱신
        if not _MACRO_LOCK.locked():
            asyncio.create_task(_bg_refresh(
                _MACRO_CACHE, _MACRO_LOCK,
                lambda: _ext_thread(_run_macro_analysis, timeout=90.0),
                _MACRO_TTL, "macro"
            ))
        return {**_MACRO_CACHE["data"], "cached": True, "stale": True}
    # 최초 기동 또는 강제 refresh: 한 번만 대기
    async with _MACRO_LOCK:
        now = _time_module.time()
        if not refresh and _MACRO_CACHE["data"] and now < _MACRO_CACHE["expires"]:
            return {**_MACRO_CACHE["data"], "cached": True}
        try:
            data = await _ext_thread(_run_macro_analysis, timeout=90.0)
            _MACRO_CACHE["data"] = data
            _MACRO_CACHE["expires"] = _time_module.time() + _MACRO_TTL
            return {**data, "cached": False}
        except Exception as e:
            logger.error("[macro] 분석 실패: %s", e)
            if _MACRO_CACHE["data"]:
                return {**_MACRO_CACHE["data"], "cached": True, "stale": True,
                        "error": "분석 오류 — 이전 데이터 표시 중"}
            raise HTTPException(status_code=500, detail=str(e))


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
