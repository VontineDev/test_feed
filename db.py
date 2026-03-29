"""
db.py  —  PostgreSQL 연동 모듈
────────────────────────────────────────────────────────────
테이블 자동 생성, 중복 체크, 기사 저장 담당.
asyncpg 기반 비동기 커넥션 풀 사용.

설정:
    환경변수 DATABASE_URL 또는 DB_* 개별 변수로 DSN 지정
    예) DATABASE_URL=postgresql://news_user:news1234@localhost:5432/news_db
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Optional

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
    password = os.environ.get("DB_PASSWORD", "news1234")
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


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
    id           BIGSERIAL    PRIMARY KEY,
    article_id   BIGINT       REFERENCES news_articles(id) ON DELETE CASCADE,
    direction    VARCHAR(8)   NOT NULL,
    strength     SMALLINT     NOT NULL,
    reason       TEXT,
    tickers      TEXT[],
    llm_backend  VARCHAR(16),
    detected_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sig_direction ON trade_signals (direction);
CREATE INDEX IF NOT EXISTS idx_sig_detected  ON trade_signals (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_sig_strength  ON trade_signals (strength DESC);
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
) -> Optional[int]:
    """
    매매 신호를 trade_signals 테이블에 저장.
    저장된 신호의 id 반환, 실패 시 None.
    """
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO trade_signals
                    (article_id, direction, strength, reason, tickers, llm_backend)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id
                """,
                article_id,
                direction,
                strength,
                reason,
                tickers,
                llm_backend,
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
