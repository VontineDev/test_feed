"""paper_eod_sampler_job() 중복 진입 방지 테스트.

2026-08-13 발견: 슬롯 수(get_open_slot_count)만 확인하고 당일 신호를 무작위
샘플링해 pending을 삽입하다 보니, 모델이 이미 보유 중인 티커에 신호가 다시
뜨면 같은 모델이 같은 티커에 포지션을 하나 더 여는 문제가 있었다
(241710.KQ 사례: kosdaq/cross 모델이 이틀 연속 신호를 받아 각각 2개씩
동시보유). get_open_or_pending_tickers()로 이미 보유 중인 티커를 후보에서
제외해 재발을 막는다.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _make_pool(stage1_rows):
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=stage1_rows)
    acq = AsyncMock()
    acq.__aenter__ = AsyncMock(return_value=conn)
    acq.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acq)
    return pool, conn


def _row(ticker, price=10000.0):
    return {"ticker": ticker, "s1_high": price}


@pytest.mark.asyncio
async def test_already_held_ticker_excluded_from_new_entry():
    """모델이 이미 open/pending으로 보유 중인 티커는 재신호가 떠도 다시 진입하지 않는다."""
    from jobs.paper_jobs import paper_eod_sampler_job

    pool, _conn = _make_pool([_row("241710.KQ"), _row("999999.KQ")])
    trader = MagicMock()

    insert_mock = AsyncMock(return_value=1)
    with (
        patch("jobs.paper_jobs.load_chart_signals_latest",
              AsyncMock(return_value=("2026-W33", []))),
        patch("jobs.paper_jobs.get_open_slot_count", AsyncMock(return_value=0)),
        patch("jobs.paper_jobs.get_open_or_pending_tickers",
              AsyncMock(return_value={"241710.KQ"})),
        patch("jobs.paper_jobs.insert_pending", insert_mock),
        patch("jobs.paper_jobs._post_message", AsyncMock()),
    ):
        await paper_eod_sampler_job(pool, trader)

    inserted_tickers = {c.kwargs["ticker"] for c in insert_mock.call_args_list}
    assert "241710.KQ" not in inserted_tickers
    assert "999999.KQ" in inserted_tickers


@pytest.mark.asyncio
async def test_all_signals_already_held_skips_insert_entirely():
    """당일 신호 전부가 이미 보유 중인 티커면 insert_pending을 아예 호출하지 않는다."""
    from jobs.paper_jobs import paper_eod_sampler_job

    pool, _conn = _make_pool([_row("241710.KQ")])
    trader = MagicMock()

    insert_mock = AsyncMock(return_value=1)
    with (
        patch("jobs.paper_jobs.load_chart_signals_latest",
              AsyncMock(return_value=("2026-W33", []))),
        patch("jobs.paper_jobs.get_open_slot_count", AsyncMock(return_value=0)),
        patch("jobs.paper_jobs.get_open_or_pending_tickers",
              AsyncMock(return_value={"241710.KQ"})),
        patch("jobs.paper_jobs.insert_pending", insert_mock),
        patch("jobs.paper_jobs._post_message", AsyncMock()),
    ):
        await paper_eod_sampler_job(pool, trader)

    insert_mock.assert_not_called()


@pytest.mark.asyncio
async def test_no_held_tickers_inserts_all_within_slot_limit():
    """보유 중인 티커가 없으면(빈 집합) 기존처럼 슬롯 한도까지 정상 삽입된다."""
    from jobs.paper_jobs import paper_eod_sampler_job

    pool, _conn = _make_pool([_row("241710.KQ"), _row("999999.KQ")])
    trader = MagicMock()

    insert_mock = AsyncMock(return_value=1)
    with (
        patch("jobs.paper_jobs.load_chart_signals_latest",
              AsyncMock(return_value=("2026-W33", []))),
        patch("jobs.paper_jobs.get_open_slot_count", AsyncMock(return_value=0)),
        patch("jobs.paper_jobs.get_open_or_pending_tickers",
              AsyncMock(return_value=set())),
        patch("jobs.paper_jobs.insert_pending", insert_mock),
        patch("jobs.paper_jobs._post_message", AsyncMock()),
    ):
        await paper_eod_sampler_job(pool, trader)

    assert insert_mock.call_count == 2
