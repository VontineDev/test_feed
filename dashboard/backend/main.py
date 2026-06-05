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

import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from core.db import upsert_ticker_names as _upsert_ticker_names  # noqa: E402
from data.kiwoom_aftermarket_sync import KiwoomClient  # noqa: E402
from analysis.macro_tracker import MacroTracker, DEFAULT_TICKERS as _MACRO_TICKERS  # noqa: E402
from data.market_data import _fetch_fundamental  # noqa: E402

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ── 외부 API 전용 스레드 풀 ──────────────────────────────────
# yfinance/Kiwoom/KRX 호출을 기본 executor와 분리.
# max_workers=4: 외부 API가 느려도 이벤트 루프와 일반 요청에 영향 없음.
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor
_EXT_EXECUTOR = _ThreadPoolExecutor(max_workers=4, thread_name_prefix="ext-api")


async def _ext_thread(fn, *args, timeout: float):
    """외부 API 전용 풀에서 동기 함수를 실행한다. timeout 초 초과 시 TimeoutError."""
    loop = asyncio.get_running_loop()
    return await asyncio.wait_for(
        loop.run_in_executor(_EXT_EXECUTOR, fn, *args),
        timeout=timeout,
    )

# ── 한국 휴장일 캐시 ─────────────────────────────────────────
# 네이버 Finance siseJson(005930)으로 실제 영업일 목록을 조회해 당일 휴장 여부 확인.
# 미래 날짜는 API 데이터가 없으므로 고정 법정공휴일 fallback 병용.
#
# 캐시 구조:
#   _HOLIDAY_CACHE: {date → bool}  — True=휴장일
#   _HOLIDAY_CACHE_DATE: 마지막 갱신 날짜 (당일 1회만 조회)
_HOLIDAY_CACHE: dict[date, bool] = {}
_HOLIDAY_CACHE_DATE: date | None = None

# 고정 법정공휴일 (연도 무관, 대체공휴일·선거일·임시공휴일 제외)
_FIXED_HOLIDAYS: set[tuple[int, int]] = {
    (1, 1), (3, 1), (5, 1), (5, 5), (6, 6),
    (8, 15), (10, 3), (10, 9), (12, 25),
}


def _fetch_trading_days_naver(start: date, end: date) -> set[date] | None:
    """네이버 Finance siseJson으로 기간 내 실제 영업일 반환. 실패 시 None."""
    try:
        import requests as _req
        resp = _req.get(
            "https://api.finance.naver.com/siseJson.naver",
            params={
                "symbol":      "005930",
                "requestType": 1,
                "startTime":   start.strftime("%Y%m%d"),
                "endTime":     end.strftime("%Y%m%d"),
                "timeframe":   "day",
            },
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer":    "https://finance.naver.com/",
            },
            timeout=8,
        )
        resp.raise_for_status()
        trading: set[date] = set()
        for line in resp.text.splitlines():
            line = line.strip()
            if line.startswith('[\"20') and len(line) > 11:
                ds = line[2:10]
                try:
                    trading.add(date(int(ds[:4]), int(ds[4:6]), int(ds[6:])))
                except ValueError:
                    pass
        return trading if trading else None
    except Exception as e:
        logger.debug("[holidays] 네이버 API 실패: %s", e)
        return None


def _is_holiday(d: date) -> bool:
    """KST 날짜 d가 한국 주식시장 휴장일이면 True.

    판단 순서:
    1. 캐시 히트 → 즉시 반환
    2. 네이버 siseJson으로 ±15일 영업일 조회 → 당일 포함 여부로 판단
       (과거 확정 데이터이므로 정확)
    3. API 실패 → 고정 법정공휴일 fallback
    """
    global _HOLIDAY_CACHE, _HOLIDAY_CACHE_DATE

    if d in _HOLIDAY_CACHE:
        return _HOLIDAY_CACHE[d]

    today = datetime.now(_KST).date()

    # 오늘 처음 조회 시 ±15일 윈도우 일괄 갱신 (API 1회 호출)
    if _HOLIDAY_CACHE_DATE != today:
        win_start = today - timedelta(days=15)
        win_end   = today + timedelta(days=3)   # 근미래 소폭 포함
        trading = _fetch_trading_days_naver(win_start, win_end)
        if trading is not None:
            for offset in range(-15, 4):
                cd = today + timedelta(days=offset)
                if cd.weekday() < 5:   # 평일만 판단
                    if cd < today:
                        # 과거 날짜: 거래 데이터 부재 = 휴장
                        _HOLIDAY_CACHE[cd] = cd not in trading
                    elif cd in trading:
                        # 오늘/미래: 데이터가 있을 때만 영업일로 확정
                        # (장 개시 전엔 데이터 없음 → 캐시 미설정, fallback으로 고정공휴일만 체크)
                        _HOLIDAY_CACHE[cd] = False
            _HOLIDAY_CACHE_DATE = today
            logger.info("[holidays] 영업일 캐시 갱신 (±15일, %d일 반영)", len(trading))
            if d in _HOLIDAY_CACHE:
                return _HOLIDAY_CACHE[d]

    # API 실패 또는 범위 밖 → 고정 법정공휴일 fallback
    result = (d.month, d.day) in _FIXED_HOLIDAYS
    _HOLIDAY_CACHE[d] = result
    return result

# ── 캐시 (히트맵/Top 5분, 포지션 현재가 5분) ─────────────────
# market_open: 캐시 생성 시점의 _is_market_open() 값 — 케이스 전환 감지용
_HEATMAP_CACHE: dict = {"data": None, "expires": 0.0, "market_open": None}
_HEATMAP_LOCK = asyncio.Lock()
_PRICE_TTL = 300     # 5분

# ── 포지션 현재가 캐시 — {ticker: current_price_float} (5분) ──
_POS_PRICE_CACHE: dict = {"data": {}, "expires": 0.0}

# ── 키움 토큰 캐시 (au10001 반복 호출 방지, 토큰 유효기간 24h) ──
_KIWOOM_TOKEN: str | None = None
_KIWOOM_TOKEN_TS: float = 0.0
_KIWOOM_TOKEN_TTL = 82800  # 23시간

# ── Top 캐시 (5분) ────────────────────────────────────────────
# 캐시는 n=20 기준 단일 슬롯. n이 다른 요청은 캐시된 데이터를 그대로 반환.
# 프론트엔드가 n=20 고정이므로 충돌 없음. n 변경 시 단일 슬롯 가정 재검토 필요.
_TOP_CACHE: dict = {"data": None, "expires": 0.0, "market_open": None}
_TOP_TTL = 300            # 장 중 5분
_AFTERMARKET_TTL = 1800   # 장 마감 후 30분 (aftermarket_snap은 하루 종일 불변)
_TOP_LOCK = asyncio.Lock()

# ── 매크로 캐시 (10분) ───────────────────────────────────────
_MACRO_CACHE: dict = {"data": None, "expires": 0.0}
_MACRO_TTL = 600  # 10분 (yfinance 다운로드 비용 고려)
_MACRO_LOCK = asyncio.Lock()

# ── USD/KRW 환율 캐시 (10분) ──────────────────────────────────
_USDKRW_CACHE: dict = {"rate": None, "expires": 0.0}
_USDKRW_TTL = 600  # 10분

# ── 시장 지수 캐시 (5분) ─────────────────────────────────────
_MARKET_INDEX_CACHE: dict = {"data": None, "expires": 0.0}
_MARKET_INDEX_TTL = 300  # 5분
_MARKET_INDEX_LOCK = asyncio.Lock()
_KST = ZoneInfo("Asia/Seoul")

# ── SSE 연결 카운터 ──────────────────────────────────────────
_SSE_CONNECTIONS: dict[str, int] = {"signals": 0, "scheduler": 0}


def _cache_is_valid(cache: dict) -> bool:
    """캐시 유효 여부: TTL + market_open 상태 일치 확인.
    market_open 상태가 바뀌면 TTL이 남아 있어도 무효 처리.
    """
    if not cache["data"]:
        return False
    if _time_module.time() >= cache["expires"]:
        return False
    if cache.get("market_open") is not None and cache["market_open"] != _is_market_open():
        return False
    return True


async def _bg_refresh(cache: dict, lock: asyncio.Lock, fetch_fn, ttl: float, label: str) -> None:
    """stale-while-revalidate: 백그라운드에서 캐시를 갱신한다. 실패 시 stale 유지."""
    async with lock:
        if _cache_is_valid(cache):
            return  # 락 대기 중 이미 다른 태스크가 갱신 완료
        try:
            data = await fetch_fn()
            cache["data"] = data
            cache["expires"] = _time_module.time() + ttl
            cache["market_open"] = _is_market_open()
            logger.info("[cache] %s 갱신 완료", label)
        except Exception as e:
            logger.warning("[cache] %s 백그라운드 갱신 실패 — stale 유지: %s", label, e)


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


# ── 포지션 현재가 조회 (yfinance 1d 1m 인터벌, 5분 캐시) ────
async def _fetch_current_prices(
    tickers: list[str], *, update_cache: bool = True
) -> dict[str, float]:
    """종목 리스트의 최신 종가를 yfinance로 조회. {ticker: price} 반환.

    update_cache=False: 단일 종목 조회 시 공유 캐시 오염 방지용.
    """
    if not tickers:
        return {}
    now = _time_module.time()
    if _POS_PRICE_CACHE["data"] and now < _POS_PRICE_CACHE["expires"]:
        return _POS_PRICE_CACHE["data"]

    def _fetch() -> dict[str, float]:
        result: dict[str, float] = {}
        try:
            import yfinance as _yf
            import pandas as _pd
            hist = _yf.download(
                tickers, period="1d", interval="1m",
                auto_adjust=True, progress=False, threads=True,
            )
            if hist.empty:
                return result
            close_df = hist["Close"] if isinstance(hist.columns, _pd.MultiIndex) else hist
            for t in tickers:
                try:
                    if t not in close_df.columns:
                        continue
                    series = close_df[t].dropna()
                    if len(series) >= 1:
                        result[t] = float(series.iloc[-1])
                except Exception:
                    pass
        except Exception as e:
            logger.warning("[prices] 현재가 조회 실패: %s", e)
        return result

    try:
        prices = await _ext_thread(_fetch, timeout=20.0)
    except asyncio.TimeoutError:
        logger.warning("[prices] yfinance 타임아웃 (20s) — 빈 결과 반환")
        prices = {}
    if update_cache:
        _POS_PRICE_CACHE["data"] = prices
        _POS_PRICE_CACHE["expires"] = now + _PRICE_TTL
        logger.info("[prices] 포지션 현재가 갱신: %d종목", len(prices))
    return prices


# ── 시장 개장 여부 ─────────────────────────────────────────────
# TODO [엣지 11] 금요일 20:00 → 토요일 00:00 경계:
#   캐시 market_open 태그로 대부분 처리되지만, 토요일 00:00 직후
#   첫 요청까지는 금요일 캐시가 살아있을 수 있음.
#   캐시 TTL이 만료되면 자동 해소 — 허용 범위로 판단.
def _is_market_open() -> bool:
    """평일 비공휴일 09:00~15:30 KST 이면 True."""
    now_kst = datetime.now(_KST)
    if now_kst.weekday() >= 5:
        return False
    if _is_holiday(now_kst.date()):
        return False
    return time(9, 0) <= now_kst.time() < time(15, 31)


# ── daily_market_snap에서 거래대금 상위 N 조회 (장마감 1순위) ──────
async def _fetch_daily_snap_top_async(n: int) -> dict | None:
    """daily_market_snap 최신 영업일 거래대금 상위 N 종목.

    aftermarket_snap 대비 장점:
      - NXT 거래 여부와 무관하게 전 종목 커버 (ka10032 top100)
      - amount = KRX+NXT 합산 당일 최종값
      - change_pct = 정규장 기준 당일 등락률
    데이터 없으면 None 반환.
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT d.ticker,
                       COALESCE(tn.name_ko, d.name,
                                SPLIT_PART(d.ticker, '.', 1)) AS name,
                       d.price, d.change_pct, d.amount,
                       d.market, d.trade_date
                FROM   daily_market_snap d
                LEFT JOIN ticker_names tn ON tn.ticker = d.ticker
                WHERE  d.trade_date = (SELECT MAX(trade_date) FROM daily_market_snap)
                  AND  d.amount > 0
                ORDER  BY d.amount DESC
                LIMIT  $1
                """,
                n,
            )
        if not rows:
            return None
        trade_date = str(rows[0]["trade_date"])
        items = []
        for i, r in enumerate(rows, 1):
            items.append({
                "rank":       i,
                "ticker":     r["ticker"],
                "name":       r["name"] or r["ticker"],
                "price":      int(r["price"]) if r["price"] else 0,
                "change_pct": float(r["change_pct"]) if r["change_pct"] is not None else 0.0,
                "amount":     int(r["amount"]),
                "market":     r["market"] or "",
            })
        return {"items": items, "fetched_at": trade_date, "is_aftermarket": True}
    except Exception as e:
        logger.warning("[daily-snap] 조회 실패: %s", e)
        return None


# ── aftermarket_snap에서 합산(KRX+NXT) 거래대금 상위 N 조회 ──────
async def _fetch_aftermarket_snap_top_async(n: int) -> dict | None:
    """aftermarket_snap 최근 영업일 거래대금 상위 N 종목 반환.

    정렬/표시 기준:
      reg_value 있음 → reg_value (ka10032 KRX+NXT 당일 최종, NXT 시간외 포함 여부 미확정이므로
                        after_value를 더하지 않아 이중계산 위험 제거)
      reg_value NULL → after_value (NXT 시간외 전용 폴백)
    데이터 없으면 None 반환.
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT a.ticker,
                       COALESCE(tn.name_ko, SPLIT_PART(a.ticker, '.', 1)) AS name,
                       a.reg_close,
                       a.after_close,
                       a.after_value,
                       a.reg_value,
                       COALESCE(a.reg_value, a.after_value, 0) AS total_value,
                       a.after_chg_pct,
                       a.trade_date,
                       CASE WHEN a.ticker LIKE '%.KS' THEN 'KOSPI'
                            WHEN a.ticker LIKE '%.KQ' THEN 'KOSDAQ'
                            ELSE '' END AS market
                FROM   aftermarket_snap a
                LEFT JOIN ticker_names tn ON tn.ticker = a.ticker
                WHERE  a.trade_date = (SELECT MAX(trade_date) FROM aftermarket_snap)
                  AND  COALESCE(a.reg_value, a.after_value, 0) > 0
                ORDER  BY total_value DESC
                LIMIT  $1
                """,
                n,
            )
        if not rows:
            return None
        trade_date = str(rows[0]["trade_date"])
        items = []
        for i, r in enumerate(rows, 1):
            price = int(r["after_close"]) if r["after_close"] else (int(r["reg_close"]) if r["reg_close"] else 0)
            change_pct = float(r["after_chg_pct"]) if r["after_chg_pct"] is not None else 0.0
            items.append({
                "rank":       i,
                "ticker":     r["ticker"],
                "name":       r["name"] or r["ticker"],
                "price":      price,
                "change_pct": change_pct,
                "amount":     int(r["total_value"]),
                "market":     r["market"] or "",
            })
        return {"items": items, "fetched_at": trade_date, "is_aftermarket": True}
    except Exception as e:
        logger.warning("[aftermarket] snap 조회 실패: %s", e)
        return None


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
    """{"items": list[dict], "fetched_at": str|None} 반환.
    fetched_at: 장마감 시 trade_date(YYYY-MM-DD), 장중 시 None.

    데이터 소스 우선순위 (장 마감 시):
      1순위: daily_market_snap — ka10032 top100, KRX+NXT 합산, 전 종목 커버
      2순위: aftermarket_snap  — NXT 거래 종목만, 폴백
    """
    pool = await get_pool()

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
                   COALESCE(tn.name_ko, SPLIT_PART(sc.ticker, '.', 1)) AS name,
                   CASE WHEN sc.ticker LIKE '%.KS' THEN 'KOSPI'
                        WHEN sc.ticker LIKE '%.KQ' THEN 'KOSDAQ'
                        ELSE '' END AS market
            FROM stage_classifications sc
            LEFT JOIN ticker_names tn ON tn.ticker = sc.ticker
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


@app.get("/api/heatmap")
async def get_heatmap():
    if _cache_is_valid(_HEATMAP_CACHE):
        return _heatmap_response(_HEATMAP_CACHE["data"], cached=True)
    if _HEATMAP_CACHE["data"]:
        # stale 또는 market_open 상태 전환 — 즉시 반환하고 백그라운드에서 갱신
        if not _HEATMAP_LOCK.locked():
            asyncio.create_task(_bg_refresh(
                _HEATMAP_CACHE, _HEATMAP_LOCK, _build_heatmap_data, _PRICE_TTL, "heatmap"
            ))
        return _heatmap_response(_HEATMAP_CACHE["data"], cached=True, stale=True)
    # 최초 기동: 데이터 없음 — 한 번만 대기
    async with _HEATMAP_LOCK:
        if _cache_is_valid(_HEATMAP_CACHE):
            return _heatmap_response(_HEATMAP_CACHE["data"], cached=True)
        try:
            cache_data = await _build_heatmap_data()
            _HEATMAP_CACHE["data"] = cache_data
            ttl = _AFTERMARKET_TTL if cache_data.get("fetched_at") else _PRICE_TTL
            _HEATMAP_CACHE["expires"] = _time_module.time() + ttl
            _HEATMAP_CACHE["market_open"] = _is_market_open()
            return _heatmap_response(cache_data, cached=False)
        except Exception as e:
            logger.error("[heatmap] 빌드 실패: %s", e)
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
                       p.tp1_pct, p.trail_pct
                FROM   paper_positions p
                LEFT JOIN ticker_names tn ON tn.ticker = p.ticker
                LEFT JOIN krx_listings k  ON k.yfinance_symbol = p.ticker
                LEFT JOIN LATERAL (
                    SELECT name FROM chart_signals
                    WHERE  ticker = p.ticker ORDER BY screened_at DESC LIMIT 1
                ) cs ON TRUE
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
    _SSE_CONNECTIONS["signals"] += 1
    try:
        return StreamingResponse(
            _signal_generator(request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )
    finally:
        _SSE_CONNECTIONS["signals"] = max(0, _SSE_CONNECTIONS["signals"] - 1)


# ── POST /api/scheduler/trigger ───────────────────────────────
_VALID_JOBS = {"stage", "screener", "paper_sample", "dart_screened"}


class TriggerBody(BaseModel):
    job: str


@app.post("/api/scheduler/trigger")
async def trigger_job(request: Request, body: TriggerBody):
    if getattr(request.state, "role", "admin") != "admin":
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다")
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
        await asyncio.sleep(10)
        try:
            payload = await _fetch()
            if payload != last_payload:
                last_payload = payload
                yield f"data: {payload}\n\n"
        except Exception as e:
            logger.warning("[scheduler-sse] 조회 실패: %s", e)


@app.get("/api/scheduler/stream")
async def scheduler_stream(request: Request):
    _SSE_CONNECTIONS["scheduler"] += 1
    try:
        return StreamingResponse(
            _scheduler_stream_generator(request),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )
    finally:
        _SSE_CONNECTIONS["scheduler"] = max(0, _SSE_CONNECTIONS["scheduler"] - 1)


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
                   p.model, p.signal_date, p.entry_actual, p.qty, p.status
            FROM   paper_positions p
            LEFT JOIN ticker_names tn ON tn.ticker = p.ticker
            LEFT JOIN krx_listings k  ON k.yfinance_symbol = p.ticker
            LEFT JOIN LATERAL (
                SELECT name FROM chart_signals
                WHERE  ticker = p.ticker ORDER BY screened_at DESC LIMIT 1
            ) cs ON TRUE
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
    now = _time_module.time()
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
            return {"items": items, "fetched_at": _time_module.strftime("%H:%M:%S")}
        except _requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 401 and attempt == 0:
                logger.warning("[top] 401 수신 — 토큰 무효화 후 재시도")
                _invalidate_kiwoom_token()
                continue
            raise


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
        # stale 또는 market_open 상태 전환 — 즉시 반환하고 백그라운드에서 갱신
        if not _TOP_LOCK.locked():
            async def _fetch_top_with_fundamentals() -> dict:
                if not _is_market_open():
                    snap = await _fetch_daily_snap_top_async(50)
                    if not snap:
                        snap = await _fetch_aftermarket_snap_top_async(50)
                    if snap:
                        await _enrich_top_with_fundamentals(snap["items"])
                        return snap
                data = await _ext_thread(_fetch_top_kiwoom, 50, timeout=15.0)
                await _enrich_top_with_fundamentals(data.get("items", []))
                return data
            asyncio.create_task(_bg_refresh(
                _TOP_CACHE, _TOP_LOCK, _fetch_top_with_fundamentals, _TOP_TTL, "top"
            ))
        return {**_TOP_CACHE["data"], "stale": True}
    # 최초 기동 또는 강제 refresh: 한 번만 대기
    async with _TOP_LOCK:
        if not refresh and _cache_is_valid(_TOP_CACHE):
            return _TOP_CACHE["data"]
        try:
            if not _is_market_open():
                snap = await _fetch_daily_snap_top_async(n)
                if not snap:
                    snap = await _fetch_aftermarket_snap_top_async(n)
                if snap:
                    await _enrich_top_with_fundamentals(snap["items"])
                    _TOP_CACHE["data"] = snap
                    _TOP_CACHE["expires"] = _time_module.time() + _AFTERMARKET_TTL
                    _TOP_CACHE["market_open"] = False
                    return snap
            data = await _ext_thread(_fetch_top_kiwoom, n, timeout=15.0)
            items = data.get("items", [])
            await _enrich_top_with_fundamentals(items)
            data["items"] = items
            _TOP_CACHE["data"] = data
            _TOP_CACHE["expires"] = _time_module.time() + _TOP_TTL
            _TOP_CACHE["market_open"] = True
            return data
        except Exception as e:
            logger.warning("[top] 조회 오류: %s", e)
            _safe_err = "API 오류 — 서버 로그 확인"
            if _TOP_CACHE["data"]:
                return {**_TOP_CACHE["data"], "stale": True, "error": _safe_err}
            return {"items": [], "fetched_at": "--:--", "error": _safe_err}


# ── GET /api/paper/history ────────────────────────────────────
@app.get("/api/paper/history")
async def get_paper_ticker_history(ticker: str):
    """특정 종목의 모의투자 전체 이력 (모든 포지션, 신호일 역순)."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    p.id, p.model, p.ticker,
                    COALESCE(tn.name_ko, k.name_ko,
                             cs.name, SPLIT_PART(p.ticker, '.', 1)) AS name,
                    p.signal_date, p.entry_theory, p.entry_actual,
                    p.slippage_pct, p.qty, p.status,
                    p.tp1_pct, p.tp1_ratio, p.tp1_date, p.tp1_price,
                    p.trail_pct, p.hard_stop_pct, p.watermark,
                    p.exit_date, p.exit_price, p.exit_type,
                    p.blended_return, p.created_at,
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
                WHERE  p.ticker = $1
                ORDER  BY p.signal_date DESC, p.id DESC
                """,
                ticker,
            )

        # 오픈 포지션이 있으면 현재가 갱신
        active_rows = [r for r in rows if r["status"] in ("open", "pending")]
        prices: dict[str, float] = {}
        if active_rows:
            # update_cache=False: 단일 종목이 공유 포지션 캐시를 오염시키지 않도록
            prices = await _fetch_current_prices([ticker], update_cache=False)

        def _fmt(r) -> dict:
            d = dict(r)
            for k in ("entry_actual", "entry_theory", "slippage_pct",
                      "tp1_pct", "tp1_ratio", "trail_pct", "hard_stop_pct",
                      "tp1_price", "watermark", "exit_price", "blended_return"):
                if d.get(k) is not None:
                    d[k] = float(d[k])
            # current_price: daily_ohlcv 대신 yfinance 캐시 사용
            d.pop("current_price", None)
            curr = prices.get(ticker) if d["status"] in ("open", "pending") else None
            d["current_price"] = curr
            for k in ("signal_date", "tp1_date", "exit_date"):
                if d.get(k) is not None:
                    d[k] = str(d[k])
            if d.get("created_at") is not None:
                d["created_at"] = d["created_at"].isoformat()
            entry = d.get("entry_actual")
            d["unrealized_pct"] = (
                round((curr / entry - 1) * 100, 2)
                if curr and entry else None
            )
            return d

        positions = [_fmt(r) for r in rows]
        name = positions[0]["name"] if positions else ticker
        return {"data": positions, "ticker": ticker, "name": name}

    except Exception as e:
        logger.error("[paper/history] 조회 실패: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ── GET /api/paper/curve ──────────────────────────────────────
@app.get("/api/paper/curve")
async def get_paper_curve():
    """모델별 누적 P&L 시계열 + 모델 통계 + open 포지션 미실현 반환."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            # Query 1: 누적 실현 P&L 시계열
            series_rows = await conn.fetch(
                """
                SELECT model,
                       exit_date::text AS date,
                       SUM(blended_return) OVER (
                           PARTITION BY model
                           ORDER BY exit_date, id
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                       ) AS cumulative
                FROM   paper_positions
                WHERE  status = 'closed' AND blended_return IS NOT NULL
                ORDER  BY model, exit_date, id
                """
            )

            # Query 2: 모델별 집계 통계
            stats_rows = await conn.fetch(
                """
                SELECT
                    model,
                    COUNT(*)                                                   AS n_trades,
                    COUNT(*) FILTER (WHERE blended_return > 0)                 AS n_wins,
                    AVG(blended_return) FILTER (WHERE blended_return > 0)      AS avg_win,
                    AVG(blended_return) FILTER (WHERE blended_return < 0)      AS avg_loss,
                    SUM(blended_return)                                         AS total_realized
                FROM   paper_positions
                WHERE  status = 'closed' AND blended_return IS NOT NULL
                GROUP  BY model
                """
            )

            # Query 3: 청산 종목명 맵 (krx_listings는 이 환경에서 항상 비어 있으므로 제외)
            name_rows = await conn.fetch(
                """
                SELECT DISTINCT
                    p.ticker,
                    COALESCE(tn.name_ko, SPLIT_PART(p.ticker, '.', 1)) AS name
                FROM   paper_positions p
                LEFT JOIN ticker_names tn ON tn.ticker = p.ticker
                WHERE  p.status = 'closed'
                """
            )

            # Query 4: open 포지션 (미실현 계산용)
            open_rows = await conn.fetch(
                """
                SELECT p.ticker, p.model, p.entry_actual, p.qty,
                       COALESCE(tn.name_ko, k.name_ko,
                                cs.name, SPLIT_PART(p.ticker, '.', 1)) AS name
                FROM   paper_positions p
                LEFT JOIN ticker_names tn ON tn.ticker = p.ticker
                LEFT JOIN krx_listings k  ON k.yfinance_symbol = p.ticker
                LEFT JOIN LATERAL (
                    SELECT name FROM chart_signals
                    WHERE  ticker = p.ticker ORDER BY screened_at DESC LIMIT 1
                ) cs ON TRUE
                WHERE  p.status IN ('open', 'pending')
                ORDER  BY p.signal_date
                """
            )

        # 시계열 구조화
        series: dict = {}
        for r in series_rows:
            m = r["model"]
            if m not in series:
                series[m] = []
            series[m].append({
                "date": r["date"],
                "cumulative": round(float(r["cumulative"]), 4),
            })

        # 모델 통계 구조화
        model_stats: dict = {}
        for r in stats_rows:
            m = r["model"]
            n = int(r["n_trades"])
            nw = int(r["n_wins"])
            model_stats[m] = {
                "n_trades": n,
                "n_wins": nw,
                "win_rate": round(nw / n, 3) if n else 0.0,
                "avg_win": round(float(r["avg_win"]), 4) if r["avg_win"] is not None else None,
                "avg_loss": round(float(r["avg_loss"]), 4) if r["avg_loss"] is not None else None,
                "total_realized": round(float(r["total_realized"]), 4) if r["total_realized"] is not None else 0.0,
                "total_unrealized": 0.0,  # 아래에서 채움
            }

        # 종목명 맵
        ticker_name_map = {r["ticker"]: r["name"] for r in name_rows}

        # open 포지션 미실현 계산
        open_list = [dict(r) for r in open_rows]
        open_positions = []
        if open_list:
            unique_tickers = list({r["ticker"] for r in open_list})
            prices = await _fetch_current_prices(unique_tickers, update_cache=False)
            model_unrealized: dict[str, float] = {}
            for r in open_list:
                ticker = r["ticker"]
                entry = r["entry_actual"]
                curr = prices.get(ticker)
                unrealized_pct: float | None = None
                if curr and entry:
                    unrealized_pct = round((curr / float(entry) - 1), 4)
                open_positions.append({
                    "ticker": ticker,
                    "name": r["name"],
                    "model": r["model"],
                    "unrealized_pct": unrealized_pct,
                })
                if unrealized_pct is not None:
                    model_unrealized[r["model"]] = (
                        model_unrealized.get(r["model"], 0.0) + unrealized_pct
                    )
            for m, v in model_unrealized.items():
                if m in model_stats:
                    model_stats[m]["total_unrealized"] = round(v, 4)

        return {
            "data": {
                "series": series,
                "model_stats": model_stats,
                "ticker_name_map": ticker_name_map,
                "open_positions": open_positions,
            }
        }
    except Exception as e:
        logger.error("[paper/curve] 조회 실패: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ── GET /api/paper/export ─────────────────────────────────────
@app.get("/api/paper/export")
async def get_paper_export():
    """모의투자 포지션 전체 CSV 다운로드 (full dump, 필터링은 Python에서).

    utf-8-sig BOM 인코딩 — Excel에서 한글 종목명 깨짐 방지.
    StreamingResponse 대신 Response — 수백 행 규모에서 async generator 불필요.
    """
    import csv, io
    from datetime import date as _date
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    p.model, p.ticker,
                    COALESCE(tn.name_ko, SPLIT_PART(p.ticker, '.', 1)) AS name,
                    p.signal_date, p.entry_theory, p.entry_actual, p.slippage_pct,
                    p.qty, p.status,
                    p.tp1_pct, p.tp1_ratio, p.tp1_date, p.tp1_price,
                    p.trail_pct, p.hard_stop_pct, p.watermark,
                    p.exit_date, p.exit_price, p.exit_type,
                    p.blended_return, p.created_at
                FROM   paper_positions p
                LEFT JOIN ticker_names tn ON tn.ticker = p.ticker
                ORDER  BY p.model, p.exit_date DESC NULLS LAST, p.id DESC
                """
            )

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "model", "ticker", "name",
            "signal_date", "entry_theory", "entry_actual", "slippage_pct",
            "qty", "status",
            "tp1_pct", "tp1_ratio", "tp1_date", "tp1_price",
            "trail_pct", "hard_stop_pct", "watermark",
            "exit_date", "exit_price", "exit_type",
            "blended_return", "created_at",
        ])
        for row in rows:
            writer.writerow(["" if v is None else str(v) for v in row])

        today = _date.today().strftime("%Y%m%d")
        return Response(
            content=buf.getvalue().encode("utf-8-sig"),
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="paper_positions_{today}.csv"'
            },
        )
    except Exception as e:
        logger.error("[paper/export] 조회 실패: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ── 이력 트래킹 엔드포인트 ────────────────────────────────────
# GET /api/history/stage   — 기간별 Stage 분류 집계
# GET /api/history/screener — 기간별 스크리너 집계
# GET /api/history/ticker/{ticker} — 종목별 이력

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


@app.get("/api/history/stage")
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


@app.get("/api/history/screener")
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


@app.get("/api/history/ticker/{ticker}")
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


# ── GET /api/macro ────────────────────────────────────────────

def _fetch_prev_top20_sync() -> dict[str, str] | None:
    """최근 영업일 거래대금 TOP 20 → {ticker: name}. 실패 시 None.

    1순위: daily_market_snap (ka10032 top100, 전 종목)
    2순위: aftermarket_snap  (NXT 거래 종목만, 폴백)
    """
    try:
        import psycopg2
        from core.db import get_dsn as _get_dsn
        conn = psycopg2.connect(_get_dsn())
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


# ── POST /api/feedback ───────────────────────────────────────
class FeedbackBody(BaseModel):
    text: str
    screenshot: str | None = None  # base64 JPEG


@app.post("/api/feedback")
async def post_feedback(request: Request, body: FeedbackBody):
    token = os.environ.get("TELEGRAM_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        raise HTTPException(status_code=503, detail="Telegram 미설정")

    role = getattr(request.state, "role", "admin")
    caption = f"[피드백] ({role})\n{body.text[:900]}"

    import httpx
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            if body.screenshot:
                img_bytes = base64.b64decode(body.screenshot)
                r = await client.post(
                    f"https://api.telegram.org/bot{token}/sendPhoto",
                    data={"chat_id": chat_id, "caption": caption},
                    files={"photo": ("screenshot.jpg", img_bytes, "image/jpeg")},
                )
            else:
                r = await client.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": caption},
                )
        if r.status_code != 200:
            logger.error("[feedback] Telegram 전송 실패: %s", r.text)
            raise HTTPException(status_code=502, detail="Telegram 전송 실패")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[feedback] 전송 오류: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    return {"status": "sent"}


# ── GET /api/auth/me ──────────────────────────────────────────
@app.get("/api/auth/me")
async def auth_me(request: Request):
    """현재 로그인 사용자의 역할 반환. 프론트엔드 역할 기반 UI 분기용."""
    return {"role": getattr(request.state, "role", "user")}


# ── 수동 포트폴리오 (manual_portfolio 테이블) ──────────────────

class _HoldingInput(BaseModel):
    ticker:    str
    name:      str
    avg_price: float
    qty:       float


async def _get_current_prices(pool, tickers: list[str]) -> dict[str, float]:
    """aftermarket_snap 최신 종가 조회 → 미수록 종목은 yfinance 폴백.

    한국주식: aftermarket_snap (reg_close) → yfinance .KS/.KQ
    미국주식: yfinance 직접 조회 (숫자 없는 티커 = US 주식 판별)
    """
    if not tickers:
        return {}
    prices: dict[str, float] = {}

    # aftermarket_snap은 한국주식 전용
    kr_tickers = [t for t in tickers if any(c.isdigit() for c in t)]
    if kr_tickers:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker) ticker, reg_close
                FROM aftermarket_snap
                WHERE ticker = ANY($1::text[])
                  AND reg_close IS NOT NULL
                ORDER BY ticker, trade_date DESC
                """,
                kr_tickers,
            )
        for r in rows:
            if r["reg_close"]:
                prices[r["ticker"]] = float(r["reg_close"])

    missing = [t for t in tickers if t not in prices]
    if missing:
        def _yf_fetch():
            import yfinance as yf
            result: dict[str, float] = {}
            for t in missing:
                # 미국주식 판별: 티커에 숫자가 없으면 US
                if not any(c.isdigit() for c in t):
                    try:
                        info = yf.Ticker(t).fast_info
                        price = getattr(info, "last_price", None) or getattr(info, "regular_market_price", None)
                        if price:
                            result[t] = float(price)
                            continue
                    except Exception:
                        pass
                else:
                    for suffix in (".KS", ".KQ"):
                        try:
                            info = yf.Ticker(t + suffix).fast_info
                            price = getattr(info, "last_price", None) or getattr(info, "regular_market_price", None)
                            if price:
                                result[t] = float(price)
                                break
                        except Exception:
                            continue
            return result
        try:
            yf_prices = await _ext_thread(_yf_fetch, timeout=15.0)
            prices.update(yf_prices)
        except Exception as e:
            logger.warning("[portfolio] yfinance 폴백 실패: %s", e)

    return prices


async def _get_usdkrw_rate() -> float:
    """USD/KRW 환율 (yfinance USDKRW=X, 10분 캐시). 실패 시 최근 캐시 또는 1350 반환."""
    now = _time_module.time()
    if _USDKRW_CACHE["rate"] and now < _USDKRW_CACHE["expires"]:
        return float(_USDKRW_CACHE["rate"])

    def _fetch() -> float | None:
        import yfinance as yf
        try:
            fi = yf.Ticker("USDKRW=X").fast_info
            rate = getattr(fi, "last_price", None)
            if rate and float(rate) > 100:
                return float(rate)
        except Exception:
            pass
        return None

    try:
        rate = await _ext_thread(_fetch, timeout=8.0)
        if rate:
            _USDKRW_CACHE["rate"] = rate
            _USDKRW_CACHE["expires"] = now + _USDKRW_TTL
            logger.info("[portfolio] USD/KRW 환율 갱신: %.2f", rate)
            return rate
    except Exception as e:
        logger.warning("[portfolio] USD/KRW 환율 조회 실패: %s", e)

    return float(_USDKRW_CACHE.get("rate") or 1350.0)


def _calc_holdings(rows, prices: dict[str, float], usd_krw: float) -> tuple[list[dict], dict]:
    """DB 행 + 현재가 + 환율 → holdings 리스트 + summary (합계는 모두 원화 환산 기준)."""
    holdings = []
    for r in rows:
        ticker = r["ticker"]
        is_us  = not any(c.isdigit() for c in ticker)
        rate   = usd_krw if is_us else 1.0
        avg_p  = float(r["avg_price"])
        qty    = float(r["qty"])
        cur_prc = prices.get(ticker)

        # 원화 환산 금액 (KR 주식은 rate=1 이므로 그대로)
        pur_amt_krw    = round(avg_p * qty * rate)
        evlt_amt_krw   = round(cur_prc * qty * rate) if cur_prc is not None else None
        evltv_prft_krw = (evlt_amt_krw - pur_amt_krw) if evlt_amt_krw is not None else None

        # 네이티브 통화 금액 (US: USD 소수점 유지, KR: KRW 정수)
        if is_us:
            pur_amt    = round(avg_p * qty, 2)
            evlt_amt   = round(cur_prc * qty, 2) if cur_prc is not None else None
            evltv_prft = round((cur_prc - avg_p) * qty, 2) if cur_prc is not None else None
        else:
            pur_amt    = pur_amt_krw
            evlt_amt   = evlt_amt_krw
            evltv_prft = evltv_prft_krw

        prft_rt = (
            round(evltv_prft_krw / pur_amt_krw * 100, 2)
            if evltv_prft_krw is not None and pur_amt_krw else None
        )

        holdings.append({
            "id":             r["id"],
            "stk_cd":         ticker,
            "stk_nm":         r["name"],
            "market":         "US" if is_us else "KR",
            "avg_price":      round(avg_p, 2) if is_us else int(avg_p),
            "qty":            qty,
            "cur_prc":        cur_prc,
            "pur_amt":        pur_amt,
            "evlt_amt":       evlt_amt,
            "evltv_prft":     evltv_prft,
            "pur_amt_krw":    pur_amt_krw,
            "evlt_amt_krw":   evlt_amt_krw,
            "evltv_prft_krw": evltv_prft_krw,
            "prft_rt":        prft_rt,
            "poss_rt":        None,
        })

    # 총계·비중 모두 원화 기준으로 계산
    tot_pur  = sum(h["pur_amt_krw"] for h in holdings)
    tot_evlt = sum(h["evlt_amt_krw"] for h in holdings if h["evlt_amt_krw"] is not None)
    tot_pl   = tot_evlt - tot_pur if holdings else 0
    tot_rt   = round(tot_pl / tot_pur * 100, 2) if tot_pur else None

    for h in holdings:
        if h["evlt_amt_krw"] is not None and tot_evlt:
            h["poss_rt"] = round(h["evlt_amt_krw"] / tot_evlt * 100, 1)

    summary = {
        "tot_pur_amt":  tot_pur,
        "tot_evlt_amt": tot_evlt if holdings else None,
        "tot_evlt_pl":  tot_pl if holdings else None,
        "tot_prft_rt":  tot_rt,
    }
    return holdings, summary


@app.get("/api/portfolio")
async def get_portfolio(request: Request):
    """수동 입력 포트폴리오 조회 (admin + special 전용)."""
    role = getattr(request.state, "role", "user")
    if role not in ("admin", "special"):
        raise HTTPException(status_code=403, detail="포트폴리오 조회 권한이 없습니다")

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, ticker, name, avg_price, qty FROM manual_portfolio ORDER BY created_at"
        )

    tickers = [r["ticker"] for r in rows]
    prices, usd_krw = await asyncio.gather(
        _get_current_prices(pool, tickers),
        _get_usdkrw_rate(),
    )
    holdings, summary = _calc_holdings(rows, prices, usd_krw)
    return {"summary": summary, "holdings": holdings, "usd_krw": round(usd_krw, 2)}


@app.post("/api/portfolio/holdings", status_code=201)
async def add_holding(request: Request, body: _HoldingInput):
    """종목 추가 (admin 전용)."""
    if getattr(request.state, "role", "user") != "admin":
        raise HTTPException(status_code=403, detail="관리자만 종목을 추가할 수 있습니다")
    if body.qty <= 0 or body.avg_price <= 0:
        raise HTTPException(status_code=422, detail="수량·단가는 양수여야 합니다")
    ticker = body.ticker.strip().upper()
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO manual_portfolio (ticker, name, avg_price, qty)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (ticker) DO UPDATE
                  SET name=$2, avg_price=$3, qty=$4, updated_at=NOW()
                RETURNING id, ticker, name, avg_price, qty
                """,
                ticker, body.name.strip(), body.avg_price, body.qty,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    return {"id": row["id"], "ticker": row["ticker"], "name": row["name"],
            "avg_price": float(row["avg_price"]), "qty": row["qty"]}


@app.put("/api/portfolio/holdings/{holding_id}")
async def update_holding(request: Request, holding_id: int, body: _HoldingInput):
    """종목 수정 (admin 전용)."""
    if getattr(request.state, "role", "user") != "admin":
        raise HTTPException(status_code=403, detail="관리자만 종목을 수정할 수 있습니다")
    if body.qty <= 0 or body.avg_price <= 0:
        raise HTTPException(status_code=422, detail="수량·단가는 양수여야 합니다")
    ticker = body.ticker.strip().upper()
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE manual_portfolio
            SET ticker=$1, name=$2, avg_price=$3, qty=$4, updated_at=NOW()
            WHERE id=$5
            RETURNING id
            """,
            ticker, body.name.strip(), body.avg_price, body.qty, holding_id,
        )
    if not row:
        raise HTTPException(status_code=404, detail="해당 종목을 찾을 수 없습니다")
    return {"ok": True}


@app.delete("/api/portfolio/holdings/{holding_id}", status_code=204)
async def delete_holding(request: Request, holding_id: int):
    """종목 삭제 (admin 전용)."""
    if getattr(request.state, "role", "user") != "admin":
        raise HTTPException(status_code=403, detail="관리자만 종목을 삭제할 수 있습니다")
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM manual_portfolio WHERE id=$1", holding_id
        )
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="해당 종목을 찾을 수 없습니다")


# ── GET /api/ticker/lookup ─────────────────────────────────────
@app.get("/api/ticker/lookup")
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


# ── GET /api/dart/summary/{ticker} ───────────────────────────
@app.get("/api/dart/summary/{ticker}")
async def get_dart_summary(ticker: str):
    """DART 재무 현황 — 최신 보고서 기준 매출/영업이익/사업부문.

    ticker: yfinance 형식 (005930.KS, 005930.KQ)
    dart_companies.stock_code(6자리)와 매핑 후 가장 최근 추출 결과 반환.
    응답: {data: {corp_name, period, report_type, extracted_at, revenue, segments}}
    """
    stock_code = ticker.split(".")[0]
    if not stock_code.isdigit() or len(stock_code) != 6:
        return {"data": None}

    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT de.corp_name, de.period, de.report_type, de.extracted_at,
                       de.revenue_json, de.segments_json
                FROM   dart_extractions de
                JOIN   dart_companies dc ON dc.corp_name = de.corp_name
                WHERE  dc.stock_code = $1
                  AND  de.revenue_json IS NOT NULL
                ORDER  BY de.extracted_at DESC
                LIMIT  1
                """,
                stock_code,
            )
    except Exception as e:
        logger.warning("[dart/summary] DB 조회 실패 (%s): %s", ticker, e)
        return {"data": None}

    if not row:
        return {"data": None}

    def _parse(v):
        if v is None:
            return None
        if isinstance(v, (dict, list)):
            return v
        try:
            return json.loads(v)
        except Exception:
            return None

    return {
        "data": {
            "corp_name":   row["corp_name"],
            "period":      row["period"],
            "report_type": row["report_type"],
            "extracted_at": str(row["extracted_at"]) if row["extracted_at"] else None,
            "revenue":     _parse(row["revenue_json"]),
            "segments":    _parse(row["segments_json"]),
        }
    }


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
