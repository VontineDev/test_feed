"""동기(psycopg2) DB 연결 헬퍼 — 단일 진입점.

기존에 core/ohlcv_cache.py, data/youtube_narrative_sync.py,
data/krx_aftermarket_sync.py, data/kiwoom_aftermarket_sync.py 등에
각각 복사돼 있던 `_connect(dsn)` 헬퍼를 통합 (2026-07 리팩토링 Phase B).

비동기 경로는 core.db.create_pool(asyncpg) 사용 — 두 스택은 의도적으로
공존한다 (통일은 동작 변경이므로 범위 밖).
"""

from __future__ import annotations

from typing import Optional

import psycopg2

from core.db import get_dsn


def connect(dsn: Optional[str] = None):
    """psycopg2 connection 반환. dsn 생략 시 core.db.get_dsn() 사용."""
    return psycopg2.connect(dsn or get_dsn())
