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
from datetime import datetime, timezone
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

-- ── 백테스팅: 교차분석 결과 ──────────────────────────────────
CREATE TABLE IF NOT EXISTS cross_analysis_results (
    id              BIGSERIAL    PRIMARY KEY,
    signal_id       BIGINT       NOT NULL REFERENCES trade_signals(id) ON DELETE CASCADE,
    verdict         VARCHAR(16)  NOT NULL,
    score           SMALLINT     NOT NULL,
    summary         TEXT,
    confirm_count   SMALLINT,
    conflict_count  SMALLINT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cross_signal  ON cross_analysis_results (signal_id);
CREATE INDEX IF NOT EXISTS idx_cross_verdict ON cross_analysis_results (verdict);

-- ── 백테스팅: 교차분석 시점 종목별 시세 스냅샷 ───────────────
CREATE TABLE IF NOT EXISTS cross_analysis_prices (
    id              BIGSERIAL    PRIMARY KEY,
    cross_id        BIGINT       NOT NULL REFERENCES cross_analysis_results(id) ON DELETE CASCADE,
    ticker          VARCHAR(64)  NOT NULL,
    symbol          VARCHAR(32)  NOT NULL,
    price_at_signal FLOAT        NOT NULL,
    change_pct      FLOAT,
    rsi             FLOAT,
    volume_ratio    FLOAT,
    near_52w_high   BOOLEAN,
    near_52w_low    BOOLEAN
);
CREATE INDEX IF NOT EXISTS idx_cross_prices ON cross_analysis_prices (cross_id);

-- ── 백테스팅: 미래 시점 가격 체크포인트 ──────────────────────
CREATE TABLE IF NOT EXISTS price_outcomes (
    id              BIGSERIAL    PRIMARY KEY,
    cross_price_id  BIGINT       NOT NULL REFERENCES cross_analysis_prices(id) ON DELETE CASCADE,
    checkpoint      VARCHAR(8)   NOT NULL,
    price           FLOAT,
    return_pct      FLOAT,
    fetched_at      TIMESTAMPTZ,
    UNIQUE (cross_price_id, checkpoint)
);
CREATE INDEX IF NOT EXISTS idx_outcomes_unfilled ON price_outcomes (fetched_at) WHERE fetched_at IS NULL;

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
async def create_pool(dsn: Optional[str] = None) -> asyncpg.Pool:
    dsn = dsn or get_dsn()
    pool = await asyncpg.create_pool(dsn, min_size=2, max_size=8)
    logger.info("DB 풀 생성 완료 — %s", dsn.split("@")[-1])  # 비밀번호 숨기고 host/db만 출력
    return pool


# ── 테이블 생성 ───────────────────────────────────────────────
async def init_db(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(_CREATE_TABLE)
    logger.info("DB 테이블 준비 완료 (news_articles)")


# ── 중복 체크 ─────────────────────────────────────────────────
async def is_duplicate(pool: asyncpg.Pool, url_hash: str) -> bool:
    async with pool.acquire() as conn:
        row = await conn.fetchval(
            "SELECT 1 FROM news_articles WHERE url_hash = $1", url_hash
        )
    return row is not None


# ── 기사 저장 ─────────────────────────────────────────────────
async def save_article(
    pool: asyncpg.Pool,
    *,
    url_hash: str,
    url: str,
    source: str,
    category: str,
    title_en: str,
    summary_en: str,
    summary_ko: str,
    llm_backend: str,          # "ollama" | "lm_studio" | "failed" | "disabled"
    published_at: Optional[datetime],
) -> bool:
    """
    기사를 DB에 저장합니다.
    중복(url_hash 충돌) 시 무시하고 False 반환.
    저장 성공 시 True 반환.
    """
    try:
        async with pool.acquire() as conn:
            result = await conn.execute(
                """
                INSERT INTO news_articles
                    (url_hash, url, source, category,
                     title_en, summary_en, summary_ko,
                     llm_backend, published_at, fetched_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT (url_hash) DO NOTHING
                """,
                url_hash,
                url,
                source,
                category,
                title_en,
                summary_en,
                summary_ko,
                llm_backend,
                published_at,
                datetime.now(timezone.utc),
            )
        # result = "INSERT 0 1" (성공) or "INSERT 0 0" (중복)
        return result.endswith("1")
    except Exception as e:
        logger.error("DB 저장 실패 [%s]: %s", url_hash, e)
        return False


# ── 최신 기사 조회 (트레이딩 시스템 연동용) ───────────────────
async def fetch_latest(
    pool: asyncpg.Pool,
    category: Optional[str] = None,
    source: Optional[str] = None,
    limit: int = 20,
) -> list[dict]:
    """
    최신 기사 조회. category / source 로 필터링 가능.
    예) fetch_latest(pool, category="macro", limit=10)
    """
    conditions = []
    args: list = [limit]

    if category:
        args.append(category)
        conditions.append(f"category = ${len(args)}")
    if source:
        args.append(source)
        conditions.append(f"source = ${len(args)}")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    query = f"""
        SELECT id, source, category, title_en, summary_ko,
               llm_backend, published_at, fetched_at
        FROM   news_articles
        {where}
        ORDER  BY fetched_at DESC
        LIMIT  $1
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *args)
    return [dict(r) for r in rows]


# ── 신호 저장 ─────────────────────────────────────────────────
async def save_signal(
    pool: asyncpg.Pool,
    *,
    article_id: int,
    direction: str,
    strength: int,
    reason: str,
    tickers: list[str],
    llm_backend: str,
    macro_usd_krw: Optional[float] = None,
    macro_base_rate: Optional[float] = None,
) -> Optional[int]:
    """
    매매 신호를 trade_signals 테이블에 저장.
    저장된 신호의 id 반환, 실패 시 None.
    macro_usd_krw / macro_base_rate: nullable — 매크로 컨텍스트 스냅샷 (백테스팅용)
    """
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO trade_signals
                    (article_id, direction, strength, reason, tickers, llm_backend,
                     macro_usd_krw, macro_base_rate)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING id
                """,
                article_id,
                direction,
                strength,
                reason,
                tickers,
                llm_backend,
                macro_usd_krw,
                macro_base_rate,
            )
        return row["id"] if row else None
    except Exception as e:
        logger.error("신호 저장 실패: %s", e)
        return None


# ── 최신 신호 조회 ────────────────────────────────────────────
async def fetch_latest_signals(
    pool: asyncpg.Pool,
    direction: Optional[str] = None,
    min_strength: int = 1,
    limit: int = 20,
) -> list[dict]:
    """
    최신 매매 신호 조회.
    예) fetch_latest_signals(pool, direction="BUY", min_strength=3)
    """
    args: list = [min_strength, limit]
    dir_filter = ""
    if direction:
        args.insert(0, direction)
        dir_filter = "AND s.direction = $1"
        args = [direction, min_strength, limit]

    query = f"""
        SELECT s.id, s.direction, s.strength, s.reason, s.tickers,
               s.detected_at, a.title_en, a.summary_ko, a.url,
               a.source, a.category
        FROM   trade_signals s
        JOIN   news_articles a ON a.id = s.article_id
        WHERE  s.strength >= ${'2' if direction else '1'}
               {dir_filter}
        ORDER  BY s.detected_at DESC
        LIMIT  ${'3' if direction else '2'}
    """
    # 깔끔한 파라미터 바인딩으로 재작성
    if direction:
        query = """
            SELECT s.id, s.direction, s.strength, s.reason, s.tickers,
                   s.detected_at, a.title_en, a.summary_ko, a.url,
                   a.source, a.category
            FROM   trade_signals s
            JOIN   news_articles a ON a.id = s.article_id
            WHERE  s.strength >= $2 AND s.direction = $1
            ORDER  BY s.detected_at DESC
            LIMIT  $3
        """
    else:
        query = """
            SELECT s.id, s.direction, s.strength, s.reason, s.tickers,
                   s.detected_at, a.title_en, a.summary_ko, a.url,
                   a.source, a.category
            FROM   trade_signals s
            JOIN   news_articles a ON a.id = s.article_id
            WHERE  s.strength >= $1
            ORDER  BY s.detected_at DESC
            LIMIT  $2
        """
        args = [min_strength, limit]

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *args)
    return [dict(r) for r in rows]


# ── 교차분석 결과 저장 (백테스팅) ─────────────────────────────
_DEFAULT_CHECKPOINTS = ("1h", "4h", "1d", "3d")


async def save_cross_analysis(
    pool: asyncpg.Pool,
    signal_id: int,
    cross,                          # market_data.CrossAnalysis
    checkpoints: tuple[str, ...] = _DEFAULT_CHECKPOINTS,
) -> Optional[int]:
    """
    교차분석 결과 + 종목별 시세 스냅샷 + 미래 체크포인트(빈 행) 저장.
    단일 트랜잭션으로 처리. 저장된 cross_analysis_results.id 반환.
    """
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # 1) cross_analysis_results
                cross_id = await conn.fetchval(
                    """
                    INSERT INTO cross_analysis_results
                        (signal_id, verdict, score, summary,
                         confirm_count, conflict_count)
                    VALUES ($1,$2,$3,$4,$5,$6)
                    RETURNING id
                    """,
                    signal_id,
                    cross.verdict,
                    cross.score,
                    cross.summary,
                    cross.confirm_count,
                    cross.conflict_count,
                )

                # 2) cross_analysis_prices + price_outcomes
                for ctx in cross.price_contexts:
                    price_id = await conn.fetchval(
                        """
                        INSERT INTO cross_analysis_prices
                            (cross_id, ticker, symbol, price_at_signal,
                             change_pct, rsi, volume_ratio,
                             near_52w_high, near_52w_low)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                        RETURNING id
                        """,
                        cross_id,
                        ctx.ticker,
                        ctx.symbol,
                        float(ctx.current),
                        ctx.change_pct,
                        ctx.rsi,
                        ctx.volume_ratio,
                        ctx.near_52w_high,
                        ctx.near_52w_low,
                    )

                    # 3) 체크포인트 빈 행 미리 생성
                    for cp in checkpoints:
                        await conn.execute(
                            """
                            INSERT INTO price_outcomes
                                (cross_price_id, checkpoint)
                            VALUES ($1, $2)
                            ON CONFLICT DO NOTHING
                            """,
                            price_id,
                            cp,
                        )

        logger.info(
            "[백테스트] 교차분석 저장 — signal_id=%d verdict=%s 종목:%d개 체크포인트:%d개",
            signal_id, cross.verdict,
            len(cross.price_contexts), len(checkpoints),
        )
        return cross_id
    except Exception as e:
        logger.error("[백테스트] 교차분석 저장 실패: %s", e)
        return None


async def fetch_pending_outcomes(
    pool: asyncpg.Pool,
    limit: int = 500,
) -> list[dict]:
    """
    아직 채워지지 않은(fetched_at IS NULL) 가격 체크포인트 행 조회.
    체크포인트 시간이 경과한 것만 반환.
    """
    query = """
        SELECT po.id            AS outcome_id,
               po.checkpoint,
               cap.symbol,
               cap.ticker,
               cap.price_at_signal,
               car.created_at   AS signal_time,
               car.signal_id
        FROM   price_outcomes po
        JOIN   cross_analysis_prices cap ON cap.id = po.cross_price_id
        JOIN   cross_analysis_results car ON car.id = cap.cross_id
        WHERE  po.fetched_at IS NULL
          AND  car.created_at + (
                 CASE po.checkpoint
                   WHEN '1h' THEN INTERVAL '1 hour'
                   WHEN '4h' THEN INTERVAL '4 hours'
                   WHEN '1d' THEN INTERVAL '1 day'
                   WHEN '3d' THEN INTERVAL '3 days'
                 END
               ) <= now()
        ORDER BY car.created_at ASC
        LIMIT $1
    """
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, limit)
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("[백테스트] 미완 체크포인트 조회 실패: %s", e)
        return []


async def update_outcome(
    pool: asyncpg.Pool,
    outcome_id: int,
    price: Optional[float],
    return_pct: Optional[float],
) -> bool:
    """가격 체크포인트 결과 채우기."""
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE price_outcomes
                SET    price = $2, return_pct = $3, fetched_at = now()
                WHERE  id = $1
                """,
                outcome_id,
                price,
                return_pct,
            )
        return True
    except Exception as e:
        logger.error("[백테스트] 체크포인트 업데이트 실패 id=%d: %s", outcome_id, e)
        return False


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


# ── 재시작 시 중복 해시 복원 ──────────────────────────────────
async def load_seen_hashes(pool: asyncpg.Pool) -> set[str]:
    """
    DB에 저장된 url_hash 전체를 반환.
    run_scheduler 재시작 시 _seen_hashes 를 복원하는 데 사용.
    """
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT url_hash FROM news_articles")
        hashes = {r["url_hash"] for r in rows}
        return hashes
    except Exception as e:
        logger.error("해시 로드 실패: %s", e)
        return set()
