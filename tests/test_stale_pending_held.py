"""data/kiwoom_paper_trader.py::mark_stale_pending_as_held 회귀 테스트.

get_pending_positions()의 docstring이 오래전부터 "오래 방치된 pending은
status='held'로 전환해 재시도 대상에서 뺀다"는 정책을 설명하고 있었지만,
실제로 이걸 하는 코드는 없었다(2026-08-22 adversarial review 발견: kosdaq
144/145번 pending이 이 정책 부재로 매일 재시도만 반복되며 무한히 쌓이고
있었음, 당시엔 수동 SQL로 처리). 이 파일은 그 정책을 실제로 구현한
mark_stale_pending_as_held()를 검증한다.
"""
from __future__ import annotations

import logging
import sys
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.kiwoom_paper_trader import STALE_PENDING_MAX_AGE_DAYS, mark_stale_pending_as_held


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
async def test_no_stale_rows_returns_zero_and_does_not_log():
    pool, conn = _make_pool([])

    n = await mark_stale_pending_as_held(pool)

    assert n == 0
    conn.fetch.assert_called_once()
    sql, max_age = conn.fetch.call_args[0]
    assert "SET status='held'" in sql
    assert "status='pending'" in sql
    assert max_age == STALE_PENDING_MAX_AGE_DAYS


@pytest.mark.asyncio
async def test_stale_rows_returns_count_and_logs_tickers(caplog):
    rows = [
        {"id": 144, "ticker": "070300.KQ", "model": "kosdaq", "signal_date": date(2026, 8, 10)},
        {"id": 145, "ticker": "294570.KQ", "model": "kosdaq", "signal_date": date(2026, 8, 11)},
    ]
    pool, _conn = _make_pool(rows)

    with caplog.at_level(logging.WARNING, logger="data.kiwoom_paper_trader"):
        n = await mark_stale_pending_as_held(pool)

    assert n == 2
    assert any("070300.KQ" in r.message and "294570.KQ" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_custom_max_age_days_is_passed_through():
    pool, conn = _make_pool([])

    await mark_stale_pending_as_held(pool, max_age_days=10)

    _sql, max_age = conn.fetch.call_args[0]
    assert max_age == 10
