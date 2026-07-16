"""
db.py  —  PostgreSQL 연동 모듈 (인프라 + facade)
────────────────────────────────────────────────────────────
asyncpg 기반 비동기 커넥션 풀 사용.

이 파일이 직접 담당:
    get_dsn / create_pool

DDL 상수·RLS 활성화 목록·init_db는 core/db_schema.py로 분리.
도메인별 접근 함수는 sibling 모듈에 분리, 하단에서 re-export:
    core/db_schema.py    — DDL 상수 + RLS 목록 + init_db
    core/db_news.py      — news_articles
    core/db_signals.py   — trade_signals
    core/db_market.py    — ticker_names / intraday_volumes / daily_ohlcv / daily_flow
    core/db_screener.py  — chart_signals
    core/db_stage.py     — stage_classifications / watchlist_vol_log / sector_daily_stats
    core/db_trades.py    — trade_log
기존 `from core.db import X` 경로와 mock.patch("core.db.X")는 그대로 동작.

설정:
    환경변수 DATABASE_URL 또는 DB_* 개별 변수로 DSN 지정
    예) DATABASE_URL=postgresql://news_user:<password>@localhost:5432/news_db
"""

from __future__ import annotations

import logging
import os
from typing import Optional
from urllib.parse import quote

import asyncpg

# ── .env 파일 자동 로드 ──────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)

# ── DSN 설정 ─────────────────────────────────────────────────
# 환경변수 DATABASE_URL 우선, 없으면 개별 변수 조합
def get_dsn() -> str:
    if url := os.environ.get("DATABASE_URL"):
        return url
    host     = os.environ.get("DB_HOST",     "localhost")
    port     = os.environ.get("DB_PORT",     "5432")
    dbname   = os.environ.get("DB_NAME",     "news_db")
    user     = os.environ.get("DB_USER",     "news_user")
    password = os.environ.get("DB_PASSWORD")
    if not password:
        raise RuntimeError(
            "DB_PASSWORD 환경변수가 설정되지 않았습니다. "
            ".env 파일에 DB_PASSWORD=<password>를 추가하거나 DATABASE_URL을 사용하세요."
        )
    # URL-encode password so special chars (&, #, /, @, etc.) don't break DSN parsing
    return f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}@{host}:{port}/{dbname}"


# ── 풀 초기화 ─────────────────────────────────────────────────
async def create_pool(
    dsn: Optional[str] = None, *, min_size: int = 2, max_size: int = 8
) -> asyncpg.Pool:
    dsn = dsn or get_dsn()
    # statement_cache_size=0: Supabase PgBouncer(port 6543) 트랜잭션 풀링 모드에서
    # asyncpg 기본 prepared statement 캐시가 "prepared statement already exists" 오류를 유발함.
    # 직접 연결(port 5432)에서도 무해하므로 항상 비활성화.
    pool = await asyncpg.create_pool(
        dsn, min_size=min_size, max_size=max_size, statement_cache_size=0
    )
    logger.info("DB 풀 생성 완료 — %s", dsn.split("@")[-1])  # 비밀번호 숨기고 host/db만 출력
    return pool


# ── 도메인 모듈 re-export ─────────────────────────────────────
# core.db 공개 API 하위호환: 기존 import 경로/mock.patch("core.db.X") 그대로 동작.
from core.db_schema import (    # noqa: E402,F401
    init_db,
    _CREATE_TABLE, _CREATE_TRADE_LOG, _CREATE_SCHEDULER_TRIGGERS,
    _CREATE_KRX_TABLE, _CREATE_TICKER_NAMES, _CREATE_DART_TABLES,
    _CREATE_SECTOR_DAILY_STATS, _RLS_ALWAYS, _RLS_IF_EXISTS,
)
from core.db_news import (      # noqa: E402,F401
    is_duplicate, save_article, fetch_latest, load_seen_hashes,
)
from core.db_signals import (   # noqa: E402,F401
    save_signal, fetch_latest_signals,
)
from core.db_market import (    # noqa: E402,F401
    upsert_ticker_names,
    save_intraday_volumes, fetch_intraday_volumes,
    save_daily_ohlcv, fetch_daily_ohlcv, get_daily_ohlcv_symbols,
    save_daily_flow, get_prev_streak,
)
from core.db_screener import (  # noqa: E402,F401
    save_chart_signals, load_chart_signals_latest, get_chart_signals_this_week,
)
from core.db_stage import (     # noqa: E402,F401
    get_stage1_history, get_stage2_history, save_stage_classifications,
    get_active_stage_tickers, get_stage1_watchlist, get_stage3_peakout_map,
    upsert_watchlist_vol_log, get_watchlist_vol_log,
    upsert_sector_daily_stats,
)
from core.db_trades import (    # noqa: E402,F401
    save_trade, close_position, get_open_positions, get_pnl_summary,
)
