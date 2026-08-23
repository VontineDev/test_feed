"""jobs/sector_stats_job.py — T-1 날짜 회귀 테스트.

2026-08-23 발견: daily_flow_sync_job은 trade_date를 항상 전일 영업일(T-1)로
저장하는데, sector_stats_job은 trade_date=today()로 조회하고 있었다 — daily_flow에
"오늘" 날짜 행이 이 잡이 도는 시점(평일 20:30 KST)에 존재한 적이 없어
sector_daily_stats가 영구히 0건이었다(2026-08-06 수정은 컬럼명 불일치로 인한
쿼리 크래시만 고쳤을 뿐 이 날짜 불일치는 그대로 남아있었음). T-1로 조회하도록
수정 — daily_flow_sync_job이 실제로 채워둔 날짜와 맞춘다.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.dates import last_trading_day
from jobs.sector_stats_job import sector_stats_job


def _make_pool(fetch_return):
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=fetch_return)
    acq = AsyncMock()
    acq.__aenter__ = AsyncMock(return_value=conn)
    acq.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acq)
    return pool, conn


@pytest.mark.asyncio
async def test_queries_trade_date_t_minus_1_not_today():
    """daily_flow_sync_job이 항상 T-1로 저장하므로, 이 잡도 오늘이 아니라
    어제 날짜로 daily_flow를 조회해야 한다."""
    pool, conn = _make_pool([])

    with patch("jobs.sector_stats_job.upsert_sector_daily_stats", AsyncMock(return_value=0)):
        await sector_stats_job(pool)

    queried_date = conn.fetch.call_args[0][1]  # (sql, trade_date) 중 두 번째 인자
    assert queried_date == last_trading_day(date.today())
    assert queried_date != date.today()


@pytest.mark.asyncio
async def test_upserts_rows_with_t_minus_1_trade_date():
    """쿼리 결과가 있으면 upsert되는 각 행의 trade_date도 T-1이어야 한다."""
    fake_row = {
        "sector": "반도체", "ticker_count": 10, "avg_return_pct": 0.01,
        "foreign_net_sum": 1000, "inst_net_sum": -500, "avg_flow_score": 0.3,
        "stage1_count": 1, "stage2_count": 0, "stage3_count": 0,
    }
    pool, _conn = _make_pool([fake_row])

    upsert_mock = AsyncMock(return_value=1)
    with patch("jobs.sector_stats_job.upsert_sector_daily_stats", upsert_mock):
        n = await sector_stats_job(pool)

    assert n == 1
    upsert_mock.assert_called_once()
    upserted_rows = upsert_mock.call_args[0][1]
    assert upserted_rows[0]["trade_date"] == last_trading_day(date.today())
    assert upserted_rows[0]["sector"] == "반도체"


@pytest.mark.asyncio
async def test_no_pool_returns_zero_without_query():
    n = await sector_stats_job(None)  # type: ignore[arg-type]
    assert n == 0
