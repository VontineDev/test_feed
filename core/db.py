"""
db.py  —  PostgreSQL 연동 모듈
────────────────────────────────────────────────────────────
테이블 자동 생성, 중복 체크, 기사 저장 담당.
asyncpg 기반 비동기 커넥션 풀 사용.

설정:
    환경변수 DATABASE_URL 또는 DB_* 개별 변수로 DSN 지정
    예) DATABASE_URL=postgresql://news_user:<password>@localhost:5432/news_db
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timezone
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


# ── 테이블 DDL ────────────────────────────────────────────────
_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS news_articles (
    id           BIGSERIAL    PRIMARY KEY,
    url_hash     CHAR(16)     NOT NULL UNIQUE,
    url          TEXT         NOT NULL,
    source       VARCHAR(32)  NOT NULL,
    category     VARCHAR(32)  NOT NULL,
    title_en     TEXT         NOT NULL,
    summary_en   TEXT,
    summary_ko   TEXT,
    llm_backend  VARCHAR(16),
    published_at TIMESTAMPTZ,
    fetched_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_news_cat     ON news_articles (category);
CREATE INDEX IF NOT EXISTS idx_news_src     ON news_articles (source);
CREATE INDEX IF NOT EXISTS idx_news_pub     ON news_articles (published_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_news_fetched ON news_articles (fetched_at  DESC);

CREATE TABLE IF NOT EXISTS trade_signals (
    id               BIGSERIAL    PRIMARY KEY,
    article_id       BIGINT       REFERENCES news_articles(id) ON DELETE CASCADE,
    direction        VARCHAR(8)   NOT NULL,
    strength         SMALLINT     NOT NULL,
    reason           TEXT,
    tickers          TEXT[],
    llm_backend      VARCHAR(16),
    macro_usd_krw    FLOAT,
    macro_base_rate  FLOAT,
    detected_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sig_direction ON trade_signals (direction);
CREATE INDEX IF NOT EXISTS idx_sig_detected  ON trade_signals (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_sig_strength  ON trade_signals (strength DESC);
-- Idempotent migration: add macro columns to existing deployments
ALTER TABLE trade_signals ADD COLUMN IF NOT EXISTS macro_usd_krw   FLOAT;
ALTER TABLE trade_signals ADD COLUMN IF NOT EXISTS macro_base_rate FLOAT;
-- Idempotent migration: add article_type classification
ALTER TABLE trade_signals ADD COLUMN IF NOT EXISTS article_type VARCHAR(20) DEFAULT 'other';

-- ── 일봉 OHLCV (1년치 히스토리) ─────────────────────────────
CREATE TABLE IF NOT EXISTS daily_ohlcv (
    id              BIGSERIAL       PRIMARY KEY,
    symbol          VARCHAR(32)     NOT NULL,
    market          VARCHAR(4)      NOT NULL,      -- 'US' | 'KR' | 'IDX' | 'CMD'
    date            DATE            NOT NULL,
    open            FLOAT,
    high            FLOAT,
    low             FLOAT,
    close           FLOAT           NOT NULL,
    volume          BIGINT,
    source          VARCHAR(16)     NOT NULL,       -- 'yfinance'
    fetched_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    UNIQUE (symbol, date)
);
CREATE INDEX IF NOT EXISTS idx_daily_sym_date
    ON daily_ohlcv (symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_market
    ON daily_ohlcv (market, date DESC);

-- ── 일별 외국인/기관 순매수 (3단계 스크리닝용) ─────────────
CREATE TABLE IF NOT EXISTS daily_flow (
    ticker           TEXT        NOT NULL,
    trade_date       DATE        NOT NULL,
    foreign_net      BIGINT,                  -- 외국인 순매수 (주)
    inst_net         BIGINT,                  -- 기관 순매수 (주)
    foreign_streak   SMALLINT,               -- 외국인 연속 순매수일 (음수 = 순매도)
    inst_streak      SMALLINT,               -- 기관 연속 순매수일 (음수 = 순매도)
    personal_net     BIGINT,                  -- 개인 순매수 (주). 음수 = 개인 순매도
    personal_streak  SMALLINT,               -- 개인 연속 순매수일
    created_at       TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (ticker, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_daily_flow_date ON daily_flow (trade_date DESC);

-- ── 주봉 차트 스크리닝 결과 ──────────────────────────────────
CREATE TABLE IF NOT EXISTS chart_signals (
    id          SERIAL PRIMARY KEY,
    ticker      VARCHAR(20)  NOT NULL,
    name        VARCHAR(100),
    close       FLOAT,
    ma_20w      FLOAT,
    ma_60w      FLOAT,
    cloud_top   FLOAT,
    is_enhanced BOOLEAN      DEFAULT FALSE,
    has_gapjum  BOOLEAN      DEFAULT FALSE,
    week_of     VARCHAR(10)  NOT NULL,
    screened_at TIMESTAMPTZ  DEFAULT NOW(),
    sector      VARCHAR(80)  DEFAULT '',
    ma_120w     FLOAT,
    UNIQUE(ticker, week_of)
);
CREATE INDEX IF NOT EXISTS idx_chart_signals_week_ticker ON chart_signals(week_of, ticker);

-- ── 3단계 분류 결과 (일별, 전 종목 대상) ────────────────────
CREATE TABLE IF NOT EXISTS stage_classifications (
    ticker          TEXT        NOT NULL,
    classified_date DATE        NOT NULL,
    stage           SMALLINT    NOT NULL,  -- 1, 2, 3
    s1_entry_date   DATE,                  -- Stage 1 감지일
    s1_high         NUMERIC,               -- Stage 1 당일 고가
    s1_volume       BIGINT,                -- Stage 1 당일 거래량 (구버전 호환용, 신규는 s1_txamt 사용)
    s1_txamt        BIGINT,                -- Stage 1 당일 거래대금 = Volume × Close (원)
    peakout_flag    BOOLEAN     DEFAULT false,
    created_at      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (ticker, classified_date)
);
CREATE INDEX IF NOT EXISTS idx_stage_class_date ON stage_classifications (classified_date DESC);
CREATE INDEX IF NOT EXISTS idx_stage_class_ticker ON stage_classifications (ticker, classified_date DESC);

-- ── 워치리스트 일별 거래대금 비율 로그 (rally death / vol delta용) ──
CREATE TABLE IF NOT EXISTS watchlist_vol_log (
    ticker          TEXT        NOT NULL,
    trade_date      DATE        NOT NULL,
    vol_ratio       FLOAT,                   -- today_txamt / s1_txamt (거래대금 비율)
    s1_vol          BIGINT,                  -- Stage 1 진입 거래량 (구버전 호환용)
    s1_txamt        BIGINT,                  -- Stage 1 진입 거래대금 = Volume × Close (원)
    PRIMARY KEY (ticker, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_watchlist_vol_ticker ON watchlist_vol_log (ticker, trade_date DESC);

-- ── 분봉 거래량 데이터 (StockData.org / yfinance) ────────────
CREATE TABLE IF NOT EXISTS intraday_volumes (
    id              BIGSERIAL       PRIMARY KEY,
    symbol          VARCHAR(32)     NOT NULL,
    market          VARCHAR(4)      NOT NULL,      -- 'US' | 'KR'
    ts              TIMESTAMPTZ     NOT NULL,       -- 캔들 시작 시각 (UTC)
    interval        VARCHAR(8)      NOT NULL,       -- '1m' | '5m'
    open            FLOAT,
    high            FLOAT,
    low             FLOAT,
    close           FLOAT,
    volume          BIGINT          NOT NULL,
    is_extended     BOOLEAN         NOT NULL DEFAULT FALSE,
    source          VARCHAR(16)     NOT NULL,       -- 'stockdata' | 'yfinance'
    fetched_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    UNIQUE (symbol, ts, interval)
);
CREATE INDEX IF NOT EXISTS idx_intraday_sym_ts
    ON intraday_volumes (symbol, ts DESC);
CREATE INDEX IF NOT EXISTS idx_intraday_market
    ON intraday_volumes (market, ts DESC);
"""


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


# ── 테이블 생성 ───────────────────────────────────────────────
_CREATE_TRADE_LOG = """
CREATE TABLE IF NOT EXISTS trade_log (
    id              SERIAL          PRIMARY KEY,
    ticker          VARCHAR(12)     NOT NULL,
    entry_date      DATE            NOT NULL,
    entry_price     NUMERIC(12, 0)  NOT NULL,
    qty             INTEGER         NOT NULL,
    exit_date       DATE,
    exit_price      NUMERIC(12, 0),
    signal_date     DATE,
    stage_at_entry  SMALLINT,
    stage_at_exit   SMALLINT,
    entry_delay_days INTEGER GENERATED ALWAYS AS
                        ((entry_date - signal_date)) STORED,
    pnl             NUMERIC(14, 0) GENERATED ALWAYS AS
                        (CASE WHEN exit_price IS NOT NULL
                         THEN (exit_price - entry_price) * qty ELSE NULL END) STORED,
    pnl_pct         NUMERIC(7, 3) GENERATED ALWAYS AS
                        (CASE WHEN exit_price IS NOT NULL AND entry_price > 0
                         THEN ROUND((exit_price::numeric / entry_price - 1) * 100, 3)
                         ELSE NULL END) STORED,
    after_close_at_signal   NUMERIC(12, 0),
    after_chg_pct_at_signal NUMERIC(6, 2),
    memo            TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_trade_log_ticker
    ON trade_log (ticker, entry_date DESC);
CREATE INDEX IF NOT EXISTS idx_trade_log_stage
    ON trade_log (stage_at_entry, entry_date DESC);
CREATE INDEX IF NOT EXISTS idx_trade_log_open
    ON trade_log (exit_date) WHERE exit_date IS NULL;
"""

_CREATE_SCHEDULER_TRIGGERS = """
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

_CREATE_KRX_TABLE = """
CREATE TABLE IF NOT EXISTS krx_listings (
    isin_code       TEXT PRIMARY KEY,
    short_code      TEXT NOT NULL,
    name_ko         TEXT NOT NULL,
    name_ko_abbr    TEXT,
    name_en         TEXT,
    listed_at       DATE,
    market          TEXT,
    security_type   TEXT,
    sector          TEXT,
    stock_type      TEXT,
    par_value       TEXT,
    listed_shares   BIGINT,
    yfinance_symbol TEXT NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_krx_listings_name_ko
    ON krx_listings (name_ko);
CREATE INDEX IF NOT EXISTS idx_krx_listings_name_ko_abbr
    ON krx_listings (name_ko_abbr);
CREATE INDEX IF NOT EXISTS idx_krx_listings_short_code
    ON krx_listings (short_code);
CREATE INDEX IF NOT EXISTS idx_krx_listings_updated_at
    ON krx_listings (updated_at);
"""

_CREATE_TICKER_NAMES = """
CREATE TABLE IF NOT EXISTS ticker_names (
    ticker      TEXT PRIMARY KEY,   -- yfinance 심볼: '005930.KS'
    name_ko     TEXT NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);
"""

_CREATE_DART_TABLES = """
CREATE TABLE IF NOT EXISTS dart_companies (
    corp_code   VARCHAR(8)   PRIMARY KEY,  -- DART 8자리 기업고유번호
    corp_name   TEXT         NOT NULL,
    stock_code  VARCHAR(6),               -- KRX 6자리 종목코드 (상장사만)
    market      TEXT,                     -- KOSPI | KOSDAQ | NULL
    updated_at  TIMESTAMPTZ  DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dart_companies_stock_code
    ON dart_companies (stock_code)
    WHERE stock_code IS NOT NULL;

CREATE TABLE IF NOT EXISTS dart_disclosures (
    rcept_no    VARCHAR(14)  PRIMARY KEY,   -- 공시 접수번호 (14자리)
    corp_code   VARCHAR(8)   NOT NULL REFERENCES dart_companies(corp_code),
    corp_name   TEXT,
    report_nm   TEXT,                       -- 공시 제목
    rcept_dt    DATE,                       -- 접수일
    pblntf_ty   VARCHAR(4),                -- 공시유형 코드
    rm          TEXT,                       -- 비고 (유상증자 등 키워드)
    fetched_at  TIMESTAMPTZ  DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dart_disc_corp_dt
    ON dart_disclosures (corp_code, rcept_dt DESC);
CREATE INDEX IF NOT EXISTS idx_dart_disc_rcept_dt
    ON dart_disclosures (rcept_dt DESC);

CREATE TABLE IF NOT EXISTS dart_xbrl (
    id          BIGSERIAL    PRIMARY KEY,
    corp_code   VARCHAR(8)   NOT NULL REFERENCES dart_companies(corp_code),
    bsns_year   VARCHAR(4)   NOT NULL,   -- 사업연도 'YYYY'
    reprt_code  VARCHAR(5)   NOT NULL,   -- 11011=사업보고서, 11012=반기, 11013=1Q, 11014=3Q
    account_nm  TEXT         NOT NULL,
    fs_div      VARCHAR(4)   NOT NULL,   -- CFS=연결, OFS=별도
    amount      BIGINT,
    currency    VARCHAR(3)   DEFAULT 'KRW',
    fetched_at  TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (corp_code, bsns_year, reprt_code, account_nm, fs_div)
);
CREATE INDEX IF NOT EXISTS idx_dart_xbrl_corp_year
    ON dart_xbrl (corp_code, bsns_year DESC);

CREATE TABLE IF NOT EXISTS dart_segments (
    id          BIGSERIAL    PRIMARY KEY,
    corp_code   VARCHAR(8)   NOT NULL REFERENCES dart_companies(corp_code),
    bsns_year   VARCHAR(4)   NOT NULL,
    section     VARCHAR(10)  NOT NULL,   -- 'II-2' | 'II-4'
    raw_text    TEXT,                    -- Ollama 입력 원본
    parsed_json JSONB,                   -- Ollama 파싱 결과
    parse_ok    BOOLEAN      DEFAULT FALSE,
    model       TEXT,                    -- 사용 모델명
    fetched_at  TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (corp_code, bsns_year, section)
);
CREATE INDEX IF NOT EXISTS idx_dart_segments_corp_year
    ON dart_segments (corp_code, bsns_year DESC);
"""

_CREATE_SECTOR_DAILY_STATS = """
CREATE TABLE IF NOT EXISTS sector_daily_stats (
    sector          TEXT        NOT NULL,
    trade_date      DATE        NOT NULL,
    ticker_count    INT         NOT NULL DEFAULT 0,
    avg_return_pct  FLOAT,          -- 당일 평균 수익률 (Close/prev_Close - 1)
    foreign_net_sum BIGINT,         -- 섹터 외국인 순매수 합계
    inst_net_sum    BIGINT,         -- 섹터 기관 순매수 합계
    avg_flow_score  FLOAT,          -- 평균 flow_score (stage_classifications 기준)
    stage1_count    INT,            -- 당일 Stage 1 종목 수
    stage2_count    INT,            -- 당일 Stage 2 종목 수
    stage3_count    INT,            -- 당일 Stage 3 종목 수
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (sector, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_sector_daily_stats_date
    ON sector_daily_stats (trade_date DESC);
"""


# ── RLS 활성화 (Supabase PostgREST 노출 차단) ────────────────────
# 이 백엔드는 asyncpg 직접 연결(postgres/service_role)을 사용하므로 RLS 영향 없음.
# anon/authenticated 롤이 PostgREST를 통해 테이블에 접근하는 것만 차단.
# 주의: 배치 실행 금지. 한 테이블이 없으면 나머지도 모두 실패하므로 개별 실행.
# aftermarket_snap / paper_positions 는 별도 스크립트에서 생성 → 여기선 IF EXISTS 형태로 처리.
_RLS_ALWAYS: list[str] = [
    "news_articles",
    "trade_signals",
    "daily_ohlcv",
    "daily_flow",
    "chart_signals",
    "stage_classifications",
    "watchlist_vol_log",
    "intraday_volumes",
    "krx_listings",
    "ticker_names",
    "trade_log",
    "scheduler_triggers",
    "dart_companies",
    "dart_disclosures",
    "dart_xbrl",
    "dart_segments",
    "sector_daily_stats",
]

# init_db 호출 시점에 아직 없을 수 있는 테이블 — DO 블록으로 안전하게 처리
_RLS_IF_EXISTS: list[str] = [
    "aftermarket_snap",   # krx/kiwoom_aftermarket_sync.py ensure_table()에서 생성
    "paper_positions",    # kiwoom_paper_trader.init_paper_positions()에서 생성
]


async def init_db(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(_CREATE_TABLE)
        await conn.execute(_CREATE_TRADE_LOG)
        await conn.execute(_CREATE_KRX_TABLE)
        await conn.execute(_CREATE_TICKER_NAMES)
        await conn.execute(_CREATE_SCHEDULER_TRIGGERS)
        await conn.execute(_CREATE_DART_TABLES)
        await conn.execute(_CREATE_SECTOR_DAILY_STATS)
        await conn.execute(
            "ALTER TABLE chart_signals ADD COLUMN IF NOT EXISTS sector VARCHAR(80) DEFAULT ''"
        )
        await conn.execute(
            "ALTER TABLE chart_signals ADD COLUMN IF NOT EXISTS ma_120w FLOAT"
        )
        await conn.execute(
            "ALTER TABLE chart_signals ADD COLUMN IF NOT EXISTS high_w FLOAT"
        )
        await conn.execute(
            "ALTER TABLE chart_signals ADD COLUMN IF NOT EXISTS volume_w BIGINT"
        )
        await conn.execute(
            "ALTER TABLE stage_classifications ADD COLUMN IF NOT EXISTS s1_volume BIGINT"
        )
        await conn.execute(
            "ALTER TABLE stage_classifications ADD COLUMN IF NOT EXISTS s1_txamt BIGINT"
        )
        await conn.execute(
            "ALTER TABLE stage_classifications ADD COLUMN IF NOT EXISTS foreign_chg_14d_pct FLOAT"
        )
        await conn.execute(
            "ALTER TABLE stage_classifications ADD COLUMN IF NOT EXISTS flow_score FLOAT"
        )
        await conn.execute(
            "ALTER TABLE watchlist_vol_log ADD COLUMN IF NOT EXISTS s1_txamt BIGINT"
        )
        # RLS: 반드시 존재하는 테이블은 개별 실행 (한 번에 보내면 한 테이블 실패 시 전체 롤백)
        for _tbl in _RLS_ALWAYS:
            await conn.execute(
                f"ALTER TABLE {_tbl} ENABLE ROW LEVEL SECURITY;"
            )
            await conn.execute(f"""
                DO $$ BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM pg_policies
                    WHERE schemaname='public' AND tablename='{_tbl}' AND policyname='backend_all'
                  ) THEN
                    CREATE POLICY backend_all ON {_tbl} FOR ALL USING (true) WITH CHECK (true);
                  END IF;
                END $$;
            """)
        # RLS: init_db 시점에 아직 없을 수 있는 테이블 — IF EXISTS 가드
        for _tbl in _RLS_IF_EXISTS:
            await conn.execute(
                f"""
                DO $$ BEGIN
                  IF EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = '{_tbl}'
                  ) THEN
                    ALTER TABLE {_tbl} ENABLE ROW LEVEL SECURITY;
                    IF NOT EXISTS (
                      SELECT 1 FROM pg_policies
                      WHERE schemaname='public' AND tablename='{_tbl}' AND policyname='backend_all'
                    ) THEN
                      CREATE POLICY backend_all ON {_tbl} FOR ALL USING (true) WITH CHECK (true);
                    END IF;
                  END IF;
                END $$;
                """
            )
    logger.info("DB 테이블 준비 완료 (news_articles, stage_classifications, krx_listings, …)")


# ── ticker_names: pykrx 개별 조회로 종목명 캐시 ─────────────────
async def upsert_ticker_names(pool: asyncpg.Pool, tickers: list[str]) -> int:
    """
    yfinance 심볼 목록(예: ['066570.KS', '035720.KQ'])에 대해
    pykrx get_market_ticker_name()으로 이름을 조회하고 ticker_names 테이블에 upsert.
    이미 존재하는 ticker는 건너뜀(updated_at 기준 7일 초과 시 갱신).
    반환: 신규/갱신된 행 수.
    """
    if not tickers:
        return 0

    async with pool.acquire() as conn:
        existing = await conn.fetch(
            """
            SELECT ticker FROM ticker_names
            WHERE ticker = ANY($1)
              AND updated_at > NOW() - INTERVAL '7 days'
            """,
            tickers,
        )
    skip = {r["ticker"] for r in existing}
    to_fetch = [t for t in tickers if t not in skip]
    if not to_fetch:
        return 0

    import asyncio as _aio
    from concurrent.futures import ThreadPoolExecutor as _TPE

    def _lookup(ticker: str) -> tuple[str, str] | None:
        code = ticker.split(".")[0]
        try:
            import pykrx.stock as _ps
            name = _ps.get_market_ticker_name(code)
            return (ticker, name) if name else None
        except Exception:
            return None

    loop = _aio.get_event_loop()
    with _TPE(max_workers=4) as ex:
        results = await loop.run_in_executor(
            None,
            lambda: [r for r in map(_lookup, to_fetch) if r],
        )

    if not results:
        return 0

    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO ticker_names (ticker, name_ko, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (ticker) DO UPDATE
              SET name_ko = EXCLUDED.name_ko, updated_at = NOW()
            """,
            results,
        )
    logger.info("[ticker_names] %d/%d 종목명 upsert", len(results), len(to_fetch))
    return len(results)


# ── 분봉 거래량 저장 (placeholder — keep section below) ─────────


# ── 분봉 거래량 저장 ─────────────────────────────────────────
async def save_intraday_volumes(
    pool: asyncpg.Pool,
    rows: list[dict],
) -> int:
    """
    분봉 거래량 데이터 일괄 저장. 중복(symbol+ts+interval) 시 건너뛴다.
    저장된 건수 반환.
    """
    if not rows:
        return 0
    inserted = 0
    try:
        async with pool.acquire() as conn:
            for r in rows:
                result = await conn.execute(
                    """
                    INSERT INTO intraday_volumes
                        (symbol, market, ts, interval,
                         open, high, low, close, volume,
                         is_extended, source)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                    ON CONFLICT (symbol, ts, interval) DO NOTHING
                    """,
                    r["symbol"],
                    r["market"],
                    r["ts"],
                    r["interval"],
                    r.get("open"),
                    r.get("high"),
                    r.get("low"),
                    r.get("close"),
                    r["volume"],
                    r.get("is_extended", False),
                    r["source"],
                )
                if result.endswith("1"):
                    inserted += 1
        logger.info("[분봉] %s 저장 %d/%d건", rows[0]["symbol"], inserted, len(rows))
    except Exception as e:
        logger.error("[분봉] 저장 실패: %s", e)
    return inserted


async def fetch_intraday_volumes(
    pool: asyncpg.Pool,
    symbol: str,
    interval: str = "5m",
    limit: int = 2000,
) -> list[dict]:
    """저장된 분봉 거래량 조회 (최신순)."""
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, market, ts, interval,
                       open, high, low, close, volume,
                       is_extended, source
                FROM   intraday_volumes
                WHERE  symbol = $1 AND interval = $2
                ORDER  BY ts DESC
                LIMIT  $3
                """,
                symbol,
                interval,
                limit,
            )
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("[분봉] 조회 실패: %s", e)
        return []


# ── 일봉 OHLCV 저장 ─────────────────────────────────────────
async def save_daily_ohlcv(
    pool: asyncpg.Pool,
    rows: list[dict],
) -> int:
    """
    일봉 OHLCV 데이터 일괄 저장. 중복(symbol+date) 시 최신 값으로 갱신.
    저장/갱신된 건수 반환.
    """
    if not rows:
        return 0
    upserted = 0
    try:
        async with pool.acquire() as conn:
            for r in rows:
                result = await conn.execute(
                    """
                    INSERT INTO daily_ohlcv
                        (symbol, market, date, open, high, low, close, volume, source)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                        open = EXCLUDED.open, high = EXCLUDED.high,
                        low = EXCLUDED.low, close = EXCLUDED.close,
                        volume = EXCLUDED.volume, fetched_at = now()
                    """,
                    r["symbol"],
                    r["market"],
                    r["date"],
                    r.get("open"),
                    r.get("high"),
                    r.get("low"),
                    r["close"],
                    r.get("volume"),
                    r["source"],
                )
                if result.endswith("1"):
                    upserted += 1
        logger.info("[일봉] %s 저장 %d/%d건", rows[0]["symbol"], upserted, len(rows))
    except Exception as e:
        logger.error("[일봉] 저장 실패: %s", e)
    return upserted


async def fetch_daily_ohlcv(
    pool: asyncpg.Pool,
    symbol: str,
    limit: int = 365,
) -> list[dict]:
    """저장된 일봉 OHLCV 조회 (최신순)."""
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, market, date, open, high, low, close, volume, source
                FROM   daily_ohlcv
                WHERE  symbol = $1
                ORDER  BY date DESC
                LIMIT  $2
                """,
                symbol,
                limit,
            )
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("[일봉] 조회 실패: %s", e)
        return []


async def get_daily_ohlcv_symbols(pool: asyncpg.Pool) -> list[dict]:
    """저장된 종목별 일봉 데이터 현황 조회."""
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, market, COUNT(*) AS cnt,
                       MIN(date) AS first_date, MAX(date) AS last_date
                FROM   daily_ohlcv
                GROUP  BY symbol, market
                ORDER  BY symbol
                """
            )
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("[일봉] 종목 현황 조회 실패: %s", e)
        return []


# ── 차트 스크리닝 결과 저장 ──────────────────────────────────
async def save_chart_signals(
    pool: asyncpg.Pool,
    results: list,      # list[ScreenResult] — 순환 import 방지를 위해 타입 미지정
) -> int:
    """
    차트 스크리닝 결과를 chart_signals 테이블에 저장.
    동일 (ticker, week_of) 충돌 시 최신 데이터로 갱신.
    저장/갱신된 건수 반환.
    """
    if not results:
        return 0
    count = 0
    try:
        async with pool.acquire() as conn:
            for r in results:
                await conn.execute(
                    """
                    INSERT INTO chart_signals
                        (ticker, name, close, ma_20w, ma_60w, cloud_top,
                         is_enhanced, has_gapjum, week_of, screened_at,
                         sector, ma_120w, high_w, volume_w)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                    ON CONFLICT (ticker, week_of) DO UPDATE SET
                        close       = EXCLUDED.close,
                        ma_20w      = EXCLUDED.ma_20w,
                        ma_60w      = EXCLUDED.ma_60w,
                        cloud_top   = EXCLUDED.cloud_top,
                        is_enhanced = EXCLUDED.is_enhanced,
                        has_gapjum  = EXCLUDED.has_gapjum,
                        screened_at = EXCLUDED.screened_at,
                        sector      = EXCLUDED.sector,
                        ma_120w     = EXCLUDED.ma_120w,
                        high_w      = EXCLUDED.high_w,
                        volume_w    = EXCLUDED.volume_w
                    """,
                    r.ticker,
                    r.name,
                    r.close,
                    r.ma_20w,
                    r.ma_60w,
                    r.cloud_top,
                    r.is_enhanced,
                    r.has_gapjum,
                    r.week_of,
                    datetime.fromisoformat(r.screened_at),
                    r.sector,
                    r.ma_120w,
                    r.high_w,
                    r.volume_w,
                )
                count += 1
        logger.info("[차트스크리너] chart_signals %d건 저장/갱신", count)
    except Exception as e:
        logger.error("[차트스크리너] 저장 실패: %s", e)
    return count


async def save_daily_flow(
    pool: asyncpg.Pool,
    ticker: str,
    trade_date: "date",
    foreign_net: Optional[int],
    inst_net: Optional[int],
    foreign_streak: Optional[int] = None,
    inst_streak: Optional[int] = None,
    personal_net: Optional[int] = None,
    personal_streak: Optional[int] = None,
) -> None:
    """Upsert one daily_flow row. trade_date: 'YYYY-MM-DD' string."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO daily_flow
                (ticker, trade_date, foreign_net, inst_net, foreign_streak, inst_streak,
                 personal_net, personal_streak)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (ticker, trade_date)
            DO UPDATE SET
                foreign_net     = EXCLUDED.foreign_net,
                inst_net        = EXCLUDED.inst_net,
                foreign_streak  = EXCLUDED.foreign_streak,
                inst_streak     = EXCLUDED.inst_streak,
                personal_net    = EXCLUDED.personal_net,
                personal_streak = EXCLUDED.personal_streak
            """,
            ticker,
            trade_date,
            foreign_net,
            inst_net,
            foreign_streak,
            inst_streak,
            personal_net,
            personal_streak,
        )


async def load_chart_signals_latest(pool: asyncpg.Pool) -> tuple[str, list]:
    """
    가장 최근 week_of의 차트 스크리닝 결과 전체 반환.
    반환: (week_of, list[dict])  — 결과 없으면 ("", [])
    """
    try:
        async with pool.acquire() as conn:
            week = await conn.fetchval(
                "SELECT week_of FROM chart_signals ORDER BY screened_at DESC LIMIT 1"
            )
            if not week:
                return ("", [])
            rows = await conn.fetch(
                """
                SELECT ticker, name, close, ma_20w, ma_60w, cloud_top,
                       is_enhanced, has_gapjum, week_of, screened_at,
                       sector, ma_120w, high_w, volume_w
                FROM chart_signals
                WHERE week_of = $1
                ORDER BY has_gapjum DESC, close DESC
                """,
                week,
            )
        return (week, [dict(r) for r in rows])
    except Exception as e:
        logger.warning("[차트스크리너] 최근 결과 조회 실패: %s", e)
        return ("", [])


async def get_chart_signals_this_week(pool: asyncpg.Pool) -> set[str]:
    """
    이번 주 스크리닝 통과 종목의 yfinance 심볼 set 반환.
    run_scheduler.py에서 collect_job() 사이클마다 한 번 호출 (v2 HIGH CONFIDENCE용).
    """
    try:
        from analysis.chart_screener import current_week_of
        week = current_week_of()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT ticker FROM chart_signals WHERE week_of = $1",
                week,
            )
        return {r["ticker"] for r in rows}
    except Exception as e:
        logger.warning("[차트스크리너] 이번 주 신호 조회 실패: %s", e)
        return set()


# ── 3단계 분류 이력 조회 ─────────────────────────────────────
async def get_stage1_history(
    pool: asyncpg.Pool,
    tickers: list[str],
    since_date: "datetime | date",
) -> dict[str, list[dict]]:
    """
    stage_classifications WHERE stage=1 에서 Stage 1 이력 반환.

    D1 결정: chart_signals(Ichimoku 통과만) 대신 stage_classifications 조회.
    이유: 전 종목 Stage 1 일별 감지와 일관 — Ichimoku 통과 여부와 독립.
    Stage 2 lookback은 최소 5일이므로 당일 Stage 1과 충돌 없음 (D9).

    반환: {ticker: [{classified_date, s1_high, s1_volume, s1_txamt}, ...]}
    s1_high / s1_txamt이 NULL인 행은 해당 조건 스킵.
    """
    if not tickers:
        return {}
    if hasattr(since_date, "date"):
        since_date = since_date.date()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT ticker, classified_date, s1_high, s1_volume, s1_txamt
                FROM   stage_classifications
                WHERE  ticker = ANY($1)
                  AND  stage  = 1
                  AND  classified_date >= $2
                ORDER  BY classified_date DESC
                """,
                tickers,
                since_date,
            )
        result: dict[str, list[dict]] = {}
        for r in rows:
            result.setdefault(r["ticker"], []).append(dict(r))
        return result
    except Exception as e:
        logger.error("[stage] get_stage1_history 실패: %s", e)
        raise


async def get_stage2_history(
    pool: asyncpg.Pool,
    tickers: list[str],
    since_date: "datetime | date",
) -> dict[str, list[dict]]:
    """
    stage_classifications WHERE stage=2 에서 Stage 2 이력 반환.

    classify_stage_v15(_check_stage3_v12)의 Stage 3 전제 조건(직전 Stage 2 발동)
    체크에 사용. 존재 여부만 보므로 classified_date만 반환.

    반환: {ticker: [{classified_date}, ...]}
    """
    if not tickers:
        return {}
    if hasattr(since_date, "date"):
        since_date = since_date.date()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT ticker, classified_date
                FROM   stage_classifications
                WHERE  ticker = ANY($1)
                  AND  stage  = 2
                  AND  classified_date >= $2
                ORDER  BY classified_date DESC
                """,
                tickers,
                since_date,
            )
        result: dict[str, list[dict]] = {}
        for r in rows:
            result.setdefault(r["ticker"], []).append(dict(r))
        return result
    except Exception as e:
        logger.error("[stage] get_stage2_history 실패: %s", e)
        raise


async def save_stage_classifications(
    pool: asyncpg.Pool,
    rows: list[dict],
) -> int:
    """
    stage_classifications upsert.
    rows: list of {ticker, classified_date, stage, s1_entry_date, s1_high, s1_volume,
                   s1_txamt, peakout_flag, foreign_chg_14d_pct, flow_score}
    반환: 저장/갱신 건수.
    """
    if not rows:
        return 0
    count = 0
    try:
        async with pool.acquire() as conn:
            for r in rows:
                await conn.execute(
                    """
                    INSERT INTO stage_classifications
                        (ticker, classified_date, stage,
                         s1_entry_date, s1_high, s1_volume, s1_txamt, peakout_flag,
                         foreign_chg_14d_pct, flow_score)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    ON CONFLICT (ticker, classified_date) DO UPDATE SET
                        stage               = EXCLUDED.stage,
                        s1_entry_date       = EXCLUDED.s1_entry_date,
                        s1_high             = EXCLUDED.s1_high,
                        s1_volume           = EXCLUDED.s1_volume,
                        s1_txamt            = EXCLUDED.s1_txamt,
                        peakout_flag        = EXCLUDED.peakout_flag,
                        foreign_chg_14d_pct = EXCLUDED.foreign_chg_14d_pct,
                        flow_score          = EXCLUDED.flow_score
                    """,
                    r["ticker"],
                    r["classified_date"],
                    r["stage"],
                    r.get("s1_entry_date"),
                    r.get("s1_high"),
                    r.get("s1_volume"),
                    r.get("s1_txamt"),
                    r.get("peakout_flag", False),
                    r.get("foreign_chg_14d_pct"),
                    r.get("flow_score"),
                )
                count += 1
        logger.info("[stage] stage_classifications %d건 저장/갱신", count)
    except Exception as e:
        logger.error("[stage] save_stage_classifications 실패: %s", e)
    return count


async def upsert_sector_daily_stats(
    pool: asyncpg.Pool,
    rows: list[dict],
) -> int:
    """
    sector_daily_stats upsert.
    rows: list of {sector, trade_date, ticker_count, avg_return_pct,
                   foreign_net_sum, inst_net_sum, avg_flow_score,
                   stage1_count, stage2_count, stage3_count}
    반환: 저장/갱신 건수.
    """
    if not rows:
        return 0
    count = 0
    try:
        async with pool.acquire() as conn:
            for r in rows:
                await conn.execute(
                    """
                    INSERT INTO sector_daily_stats
                        (sector, trade_date, ticker_count, avg_return_pct,
                         foreign_net_sum, inst_net_sum, avg_flow_score,
                         stage1_count, stage2_count, stage3_count)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    ON CONFLICT (sector, trade_date) DO UPDATE SET
                        ticker_count    = EXCLUDED.ticker_count,
                        avg_return_pct  = EXCLUDED.avg_return_pct,
                        foreign_net_sum = EXCLUDED.foreign_net_sum,
                        inst_net_sum    = EXCLUDED.inst_net_sum,
                        avg_flow_score  = EXCLUDED.avg_flow_score,
                        stage1_count    = EXCLUDED.stage1_count,
                        stage2_count    = EXCLUDED.stage2_count,
                        stage3_count    = EXCLUDED.stage3_count
                    """,
                    r["sector"],
                    r["trade_date"],
                    r["ticker_count"],
                    r.get("avg_return_pct"),
                    r.get("foreign_net_sum"),
                    r.get("inst_net_sum"),
                    r.get("avg_flow_score"),
                    r.get("stage1_count", 0),
                    r.get("stage2_count", 0),
                    r.get("stage3_count", 0),
                )
                count += 1
        logger.info("[sector] sector_daily_stats %d건 저장/갱신", count)
    except Exception as e:
        logger.error("[sector] upsert_sector_daily_stats 실패: %s", e)
    return count


async def get_active_stage_tickers(
    pool: asyncpg.Pool,
    days: int = 7,
) -> set[str]:
    """최근 days일 이내에 Stage 1/2/3으로 분류된 종목 yfinance 심볼 set.

    뉴스 게이팅: 스크리너(주봉) 미통과 종목이라도 Stage 활성 중이면 신호 전달.
    """
    from datetime import date as _date, timedelta as _td
    cutoff = _date.today() - _td(days=days)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ticker
                FROM   stage_classifications
                WHERE  classified_date >= $1
                  AND  stage IS NOT NULL
                """,
                cutoff,
            )
        return {r["ticker"] for r in rows}
    except Exception as e:
        logger.warning("[stage_classifications] 활성 티커 조회 실패: %s", e)
        return set()


async def get_stage1_watchlist(
    pool: asyncpg.Pool,
    days: int = 14,
) -> list[dict]:
    """
    최근 `days` 캘린더일 이내에 Stage 1로 분류된 종목의 최신 기록 조회.
    종목당 가장 최근 Stage 1 날짜 1건만 반환.
    반환: [{ticker, s1_date, s1_volume, s1_txamt}]
    """
    from datetime import date as _date, timedelta as _td
    cutoff = _date.today() - _td(days=days)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker)
                       ticker,
                       classified_date AS s1_date,
                       s1_volume,
                       s1_txamt
                FROM   stage_classifications
                WHERE  stage = 1
                  AND  classified_date >= $1
                ORDER  BY ticker, classified_date DESC
                """,
                cutoff,
            )
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("[watchlist] get_stage1_watchlist 실패: %s", e)
        return []


async def upsert_watchlist_vol_log(
    pool: asyncpg.Pool,
    rows: list[dict],
) -> None:
    """Upsert daily vol_ratio for watchlist tickers.
    rows: [{ticker, trade_date, vol_ratio, s1_txamt, s1_vol(optional)}]
    vol_ratio = today_txamt / s1_txamt (거래대금 기준).
    """
    if not rows:
        return
    try:
        async with pool.acquire() as conn:
            for r in rows:
                await conn.execute(
                    """
                    INSERT INTO watchlist_vol_log (ticker, trade_date, vol_ratio, s1_vol, s1_txamt)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (ticker, trade_date)
                    DO UPDATE SET vol_ratio = EXCLUDED.vol_ratio,
                                  s1_vol    = EXCLUDED.s1_vol,
                                  s1_txamt  = EXCLUDED.s1_txamt
                    """,
                    r["ticker"],
                    r["trade_date"],
                    r.get("vol_ratio"),
                    r.get("s1_vol"),
                    r.get("s1_txamt"),
                )
    except Exception as e:
        logger.warning("[watchlist_vol_log] upsert 실패: %s", e)


async def get_watchlist_vol_log(
    pool: asyncpg.Pool,
    tickers: list[str],
    lookback: int = 3,
) -> dict[str, list[float]]:
    """
    Return last `lookback` vol_ratios per ticker (most-recent first).
    Used for rally death detection: if all lookback entries < 0.6, alert.
    """
    if not tickers:
        return {}
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker, rn)
                       ticker, vol_ratio
                FROM (
                    SELECT ticker, vol_ratio,
                           ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) AS rn
                    FROM   watchlist_vol_log
                    WHERE  ticker = ANY($1)
                ) sub
                WHERE rn <= $2
                ORDER BY ticker, rn
                """,
                tickers,
                lookback,
            )
        result: dict[str, list[float]] = {}
        for r in rows:
            result.setdefault(r["ticker"], []).append(r["vol_ratio"])
        return result
    except Exception as e:
        logger.warning("[watchlist_vol_log] 조회 실패: %s", e)
        return {}


async def get_prev_streak(
    pool: asyncpg.Pool,
    ticker: str,
    trade_date: "date",
) -> tuple[Optional[int], Optional[int], Optional[int]]:
    """전일 foreign_streak, inst_streak, personal_streak 반환. 없으면 (None, None, None)."""
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT foreign_streak, inst_streak, personal_streak
                FROM   daily_flow
                WHERE  ticker = $1
                  AND  trade_date < $2
                ORDER  BY trade_date DESC
                LIMIT  1
                """,
                ticker,
                trade_date,
            )
        if row:
            return row["foreign_streak"], row["inst_streak"], row["personal_streak"]
    except Exception as e:
        logger.debug("[stage] get_prev_streak 실패 (%s): %s", ticker, e)
    return None, None, None


# ── 거래 저널 (trade_log) ─────────────────────────────────────

async def save_trade(
    pool: asyncpg.Pool,
    *,
    ticker: str,
    entry_date,        # datetime.date — NOT a string
    entry_price: int,
    qty: int,
    memo: Optional[str] = None,
) -> Optional[int]:
    """
    /buy 명령 처리 — trade_log INSERT.
    signal_date, stage_at_entry, after_*_at_signal 는 내부에서 자동 조회.
    반환: 저장된 row id, 실패 시 None.
    """
    from datetime import date as _date
    try:
        async with pool.acquire() as conn:
            # 가장 최근 Stage 신호 조회 (classified_date 사용)
            sig = await conn.fetchrow(
                """
                SELECT MAX(classified_date) AS sig_date,
                       (SELECT stage FROM stage_classifications sc2
                        WHERE sc2.ticker = $1
                          AND sc2.classified_date = MAX(sc1.classified_date)
                        LIMIT 1) AS stage
                FROM stage_classifications sc1
                WHERE ticker = $1
                  AND classified_date <= $2
                  AND stage IN (1, 2, 3)
                """,
                ticker,
                entry_date,
            )
            signal_date = sig["sig_date"] if sig else None
            stage_at_entry = sig["stage"] if sig else None

            # 시간외 데이터 조회 (aftermarket_snap — same PG instance)
            ref_date = signal_date or (entry_date - __import__("datetime").timedelta(days=1))
            after_row = await conn.fetchrow(
                """
                SELECT after_close, after_chg_pct
                FROM   aftermarket_snap
                WHERE  ticker = $1 AND trade_date = $2
                """,
                ticker,
                ref_date,
            )
            after_close    = after_row["after_close"]    if after_row else None
            after_chg_pct  = after_row["after_chg_pct"] if after_row else None

            row = await conn.fetchrow(
                """
                INSERT INTO trade_log
                    (ticker, entry_date, entry_price, qty,
                     signal_date, stage_at_entry,
                     after_close_at_signal, after_chg_pct_at_signal,
                     memo)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                RETURNING id
                """,
                ticker,
                entry_date,      # date object — asyncpg requirement
                entry_price,
                qty,
                signal_date,
                stage_at_entry,
                after_close,
                after_chg_pct,
                memo,
            )
        return row["id"] if row else None
    except Exception as e:
        logger.error("[trade] save_trade 실패: %s", e)
        return None


async def close_position(
    pool: asyncpg.Pool,
    *,
    ticker: str,
    exit_date,         # datetime.date
    exit_price: int,
) -> Optional[dict]:
    """
    /sell 명령 처리 — FIFO (entry_date 가장 오래된 미청산 포지션).
    SELECT FOR UPDATE로 더블탭 레이스 컨디션 차단.
    반환: 닫힌 row dict, 없으면 None.
    """
    from datetime import date as _date
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # 1. FIFO 포지션 선택 (SELECT FOR UPDATE)
                row = await conn.fetchrow(
                    """
                    SELECT id, entry_price, qty
                    FROM   trade_log
                    WHERE  ticker = $1
                      AND  exit_date IS NULL
                    ORDER  BY entry_date ASC
                    LIMIT  1
                    FOR UPDATE
                    """,
                    ticker,
                )
                if not row:
                    return None

                # 2. stage_at_exit 조회 (오늘 기준)
                stage_row = await conn.fetchrow(
                    """
                    SELECT stage FROM stage_classifications
                    WHERE  ticker = $1
                      AND  classified_date = $2
                    """,
                    ticker,
                    exit_date,
                )
                stage_at_exit = stage_row["stage"] if stage_row else None

                # 3. 청산 기록
                updated = await conn.fetchrow(
                    """
                    UPDATE trade_log
                    SET    exit_date     = $1,
                           exit_price    = $2,
                           stage_at_exit = $3,
                           updated_at    = now()
                    WHERE  id = $4
                    RETURNING *
                    """,
                    exit_date,
                    exit_price,
                    stage_at_exit,
                    row["id"],
                )
        return dict(updated) if updated else None
    except Exception as e:
        logger.error("[trade] close_position 실패: %s", e)
        return None


async def get_open_positions(pool: asyncpg.Pool) -> list[dict]:
    """미청산 포지션 전체 반환 (/port 명령용)."""
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, ticker, entry_date, entry_price, qty,
                       signal_date, stage_at_entry
                FROM   trade_log
                WHERE  exit_date IS NULL
                ORDER  BY entry_date ASC
                """,
            )
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("[trade] get_open_positions 실패: %s", e)
        return []


async def get_pnl_summary(
    pool: asyncpg.Pool,
    *,
    period: str = "all",   # "week" | "month" | "all"
) -> dict:
    """
    실현 P&L 요약 (/pnl 명령용).
    period:
      week  — 직전 캘린더 주 월~일 (KST 기준)
      month — 이번 달 1일~오늘
      all   — 전체
    반환: {total_pnl, trade_cnt, win_cnt, avg_win, avg_loss, by_stage}
    """
    from datetime import date as _date, timedelta as _td
    from zoneinfo import ZoneInfo
    kst = ZoneInfo("Asia/Seoul")
    today = _date.today()

    where = "exit_date IS NOT NULL"
    args: list = []

    if period == "week":
        # 직전 월~일
        dow = today.weekday()  # 0=Mon
        last_mon = today - _td(days=dow + 7)
        last_sun = last_mon + _td(days=6)
        where += " AND exit_date BETWEEN $1 AND $2"
        args = [last_mon, last_sun]
    elif period == "month":
        first = today.replace(day=1)
        where += " AND exit_date BETWEEN $1 AND $2"
        args = [first, today]

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT pnl, pnl_pct, stage_at_entry
                FROM   trade_log
                WHERE  {where}
                """,
                *args,
            )
        records = [dict(r) for r in rows]

        pnls   = [r["pnl"] for r in records if r["pnl"] is not None]
        wins   = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]

        by_stage: dict = {}
        for r in records:
            s = r["stage_at_entry"]
            if s is None:
                continue
            entry = by_stage.setdefault(s, {"total": 0, "wins": 0})
            entry["total"] += 1
            if r["pnl"] and r["pnl"] > 0:
                entry["wins"] += 1

        return {
            "trade_cnt": len(records),
            "win_cnt":   len(wins),
            "total_pnl": int(sum(pnls)) if pnls else 0,
            "avg_win":   int(sum(wins)   / len(wins))   if wins   else 0,
            "avg_loss":  int(sum(losses) / len(losses)) if losses else 0,
            "by_stage":  by_stage,
        }
    except Exception as e:
        logger.error("[trade] get_pnl_summary 실패: %s", e)
        return {"trade_cnt": 0, "win_cnt": 0, "total_pnl": 0,
                "avg_win": 0, "avg_loss": 0, "by_stage": {}}


async def get_stage3_peakout_map(
    pool,
    tickers: list[str],
    start: date,
    end: date,
    dsn: Optional[str] = None,
) -> dict[str, frozenset]:
    """Stage 3 peakout_flag=TRUE 날짜를 티커별 집합으로 반환.

    pool이 None이면 dsn으로 임시 연결을 생성한다.
    peakout_flag 컬럼이 없거나 DB 오류 시 빈 dict 반환 (use_stage3_peak=False 동작).

    반환: {ticker: frozenset({date, ...})}
    """
    async def _query(conn) -> dict[str, frozenset]:
        try:
            rows = await conn.fetch(
                """
                SELECT ticker, classified_date
                FROM   stage_classifications
                WHERE  ticker = ANY($1)
                  AND  classified_date BETWEEN $2 AND $3
                  AND  stage = 3
                  AND  peakout_flag = TRUE
                """,
                tickers, start, end,
            )
        except Exception:
            return {}
        result: dict[str, list] = {}
        for r in rows:
            result.setdefault(r["ticker"], []).append(r["classified_date"])
        return {t: frozenset(dates) for t, dates in result.items()}

    try:
        if pool is not None:
            async with pool.acquire() as conn:
                return await _query(conn)
        elif dsn:
            import asyncpg as _apg
            conn = await _apg.connect(dsn)
            try:
                return await _query(conn)
            finally:
                await conn.close()
        return {}
    except Exception as e:
        logger.warning("[db] get_stage3_peakout_map 실패: %s", e)
        return {}


# ── 도메인 모듈 re-export ─────────────────────────────────────
# core.db 공개 API 하위호환: 기존 import 경로/mock.patch("core.db.X") 그대로 동작.
from core.db_news import (      # noqa: E402,F401
    is_duplicate, save_article, fetch_latest, load_seen_hashes,
)
from core.db_signals import (   # noqa: E402,F401
    save_signal, fetch_latest_signals,
)
