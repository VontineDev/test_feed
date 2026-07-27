"""
test_trade_integration.py — 실제 PostgreSQL 대상 DDL 검증

GENERATED COLUMN(entry_delay_days, pnl, pnl_pct) 계산이
실제 DB에서 올바르게 동작하는지 확인한다.

실행 조건: DATABASE_URL 또는 DB_* 환경변수가 설정된 경우에만 실행.
CI에서 PostgreSQL 서비스가 없으면 자동 skip.
"""

from __future__ import annotations

import os
import pytest
import pytest_asyncio
from datetime import date

# DB 접근 불가 시 전체 모듈 skip
def _db_available() -> bool:
    return bool(
        os.environ.get("DATABASE_URL")
        or (os.environ.get("DB_USER") and os.environ.get("DB_PASSWORD"))
    )


pytestmark = pytest.mark.skipif(
    not _db_available(),
    reason="DB 환경변수 미설정 — integration 테스트 skip"
)


@pytest_asyncio.fixture
async def pool():
    import asyncpg
    from core.db import get_dsn, _CREATE_TRADE_LOG
    dsn = get_dsn()
    p = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    # trade_log 테이블 생성 (없으면)
    async with p.acquire() as conn:
        for stmt in _CREATE_TRADE_LOG.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                await conn.execute(stmt)
    yield p
    await p.close()


@pytest_asyncio.fixture
async def clean_trade(pool):
    """테스트 전후 test ticker의 trade_log 정리."""
    ticker = "000000.TS"  # 실존하지 않는 테스트 ticker
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM trade_log WHERE ticker = $1", ticker)
    yield ticker
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM trade_log WHERE ticker = $1", ticker)


@pytest.mark.asyncio
async def test_generated_column_entry_delay_days(pool, clean_trade):
    """
    entry_delay_days GENERATED COLUMN — signal_date와 entry_date 차이가
    PostgreSQL에서 정확히 계산되는지 검증.
    """
    ticker = clean_trade
    signal_d = date(2026, 5, 1)
    entry_d  = date(2026, 5, 3)   # D+2

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO trade_log
                (ticker, entry_date, entry_price, qty, signal_date)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id, entry_delay_days
            """,
            ticker, entry_d, 70000, 100, signal_d,
        )

    assert row["entry_delay_days"] == 2, (
        f"entry_delay_days expected 2, got {row['entry_delay_days']}"
    )


@pytest.mark.asyncio
async def test_generated_column_pnl(pool, clean_trade):
    """
    pnl / pnl_pct GENERATED COLUMN — exit 후 자동 계산 검증.
    entry=70000, exit=73500, qty=100 → pnl=350000, pnl_pct≈5.000
    """
    ticker = clean_trade
    entry_d = date(2026, 5, 1)
    exit_d  = date(2026, 5, 5)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO trade_log
                (ticker, entry_date, entry_price, qty, exit_date, exit_price)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, pnl, pnl_pct
            """,
            ticker, entry_d, 70000, 100, exit_d, 73500,
        )

    assert row["pnl"] == 350_000, f"pnl 예상 350000, 실제 {row['pnl']}"
    assert abs(float(row["pnl_pct"]) - 5.0) < 0.01, (
        f"pnl_pct 예상 ~5.000, 실제 {row['pnl_pct']}"
    )


@pytest.mark.asyncio
async def test_generated_column_null_for_open_position(pool, clean_trade):
    """미청산 포지션의 pnl은 NULL — exit_price가 없으면 계산 안 됨."""
    ticker = clean_trade

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO trade_log
                (ticker, entry_date, entry_price, qty)
            VALUES ($1, $2, $3, $4)
            RETURNING id, pnl, pnl_pct
            """,
            ticker, date(2026, 5, 8), 70000, 50,
        )

    assert row["pnl"] is None, "오픈 포지션 pnl은 NULL이어야 함"
    assert row["pnl_pct"] is None, "오픈 포지션 pnl_pct는 NULL이어야 함"
