"""
db_signals.py  —  trade_signals 테이블 접근 함수
────────────────────────────────────────────────────────────
매매 신호 저장·조회 (매크로 스냅샷 컬럼 포함). core.db facade를 통해 re-export됨.
"""

from __future__ import annotations

import logging
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)


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
    article_type: str = "other",
) -> Optional[int]:
    """
    매매 신호를 trade_signals 테이블에 저장.
    저장된 신호의 id 반환, 실패 시 None.
    macro_usd_krw / macro_base_rate: nullable — 매크로 컨텍스트 스냅샷 (백테스팅용)
    article_type: 기사 유형 분류 (earnings|ma|management|analyst|regulatory|product|macro|other)
    """
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO trade_signals
                    (article_id, direction, strength, reason, tickers, llm_backend,
                     macro_usd_krw, macro_base_rate, article_type)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
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
                article_type,
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
                   s.detected_at, s.article_type, a.title_en, a.summary_ko, a.url,
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
                   s.detected_at, s.article_type, a.title_en, a.summary_ko, a.url,
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
