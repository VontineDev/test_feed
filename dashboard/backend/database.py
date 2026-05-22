"""
dashboard/backend/database.py
FastAPI 대시보드용 asyncpg 풀 — 기존 run_scheduler.py DSN 재사용.
"""
from __future__ import annotations

import sys
from pathlib import Path

import asyncpg

# 프로젝트 루트의 db.py에서 DSN 함수 재사용
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from core.db import get_dsn  # noqa: E402

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            get_dsn(),
            min_size=2,
            max_size=6,
            statement_cache_size=0,  # Supabase PgBouncer 필수
        )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
