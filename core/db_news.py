"""
db_news.py  —  news_articles 테이블 접근 함수
────────────────────────────────────────────────────────────
기사 저장·중복 체크·조회. core.db facade를 통해 re-export됨.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)


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
