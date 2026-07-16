"""jobs/_common.py — 백필/워밍 스크립트 공통 부트스트랩.

standalone CLI로 실행되는 jobs/*.py (stage_backfill.py, screener_backfill.py 등)가
공유하는 .env 로드 / logging 설정 / DB pool 생성 보일러플레이트.

주의: sys.path에 프로젝트 루트를 추가하는 작업은 이 모듈을 import하기
*이전에* 각 스크립트가 직접 수행해야 한다 (그래야 `jobs` 패키지 자체를
import할 수 있음) — 그래서 bootstrap()은 .env/logging만 다루고
sys.path 설정은 호출부에 남긴다.
"""

from __future__ import annotations

import logging
import os

import asyncpg
from dotenv import load_dotenv


def bootstrap(root: str) -> None:
    """.env 로드 + logging 설정."""
    load_dotenv(os.path.join(root, ".env"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


async def get_pool() -> asyncpg.Pool:
    # core.db.create_pool 사용 (2026-07 Phase B): DATABASE_URL 지원과
    # statement_cache_size=0(PgBouncer 호환)을 얻음. 풀 사이즈는 기존값 유지.
    from core.db import create_pool
    return await create_pool(min_size=2, max_size=5)
