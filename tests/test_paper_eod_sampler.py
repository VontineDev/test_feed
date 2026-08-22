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
    """모델이 이미 open/pending으로 보유 중인 티커는 재신호가 떠도 다시 진입하지 않는다.

    .KS 티커 사용: .KQ는 stage_kosdaq(모델명 'kosdaq')로 분류되는데, kosdaq은
    ACTIVE_MODELS 밖이라 2026-08-22 가드 이후 후보 단계에서 걸러진다 — 이 테스트가
    검증하려는 건 held-ticker 중복 방지 로직이므로, ACTIVE_MODELS 안에 있는
    'stage'(kospi) 모델로 라우팅되는 .KS 티커로 대체."""
    from jobs.paper_jobs import paper_eod_sampler_job

    pool, _conn = _make_pool([_row("241710.KS"), _row("999999.KS")])
    trader = MagicMock()

    insert_mock = AsyncMock(return_value=1)
    with (
        patch("jobs.paper_jobs.load_chart_signals_latest",
              AsyncMock(return_value=("2026-W33", []))),
        patch("jobs.paper_jobs.get_open_slot_count", AsyncMock(return_value=0)),
        patch("jobs.paper_jobs.get_open_or_pending_tickers",
              AsyncMock(return_value={"241710.KS"})),
        patch("jobs.paper_jobs.insert_pending", insert_mock),
        patch("jobs.paper_jobs._post_message", AsyncMock()),
    ):
        await paper_eod_sampler_job(pool, trader)

    inserted_tickers = {c.kwargs["ticker"] for c in insert_mock.call_args_list}
    assert "241710.KS" not in inserted_tickers
    assert "999999.KS" in inserted_tickers


@pytest.mark.asyncio
async def test_all_signals_already_held_skips_insert_entirely():
    """당일 신호 전부가 이미 보유 중인 티커면 insert_pending을 아예 호출하지 않는다."""
    from jobs.paper_jobs import paper_eod_sampler_job

    pool, _conn = _make_pool([_row("241710.KS")])
    trader = MagicMock()

    insert_mock = AsyncMock(return_value=1)
    with (
        patch("jobs.paper_jobs.load_chart_signals_latest",
              AsyncMock(return_value=("2026-W33", []))),
        patch("jobs.paper_jobs.get_open_slot_count", AsyncMock(return_value=0)),
        patch("jobs.paper_jobs.get_open_or_pending_tickers",
              AsyncMock(return_value={"241710.KS"})),
        patch("jobs.paper_jobs.insert_pending", insert_mock),
        patch("jobs.paper_jobs._post_message", AsyncMock()),
    ):
        await paper_eod_sampler_job(pool, trader)

    insert_mock.assert_not_called()


@pytest.mark.asyncio
async def test_no_held_tickers_inserts_all_within_slot_limit():
    """보유 중인 티커가 없으면(빈 집합) 기존처럼 슬롯 한도까지 정상 삽입된다."""
    from jobs.paper_jobs import paper_eod_sampler_job

    pool, _conn = _make_pool([_row("241710.KS"), _row("999999.KS")])
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


@pytest.mark.asyncio
async def test_kosdaq_model_skipped_no_pending_created():
    """kosdaq은 ACTIVE_MODELS 밖(자본배분 대상 아님) — 신호가 있어도 pending을
    아예 만들지 않는다.

    2026-08-22 code-review 발견: 이 가드가 없으면 paper_open_entry_job이 매번
    스킵만 하고 절대 처리되지 않는 pending이 쌓인다 (144/145번 사례, 8/20·8/21
    생성 후 계속 미처리)."""
    from jobs.paper_jobs import paper_eod_sampler_job

    pool, _conn = _make_pool([_row("241710.KQ")])
    trader = MagicMock()

    insert_mock = AsyncMock(return_value=1)
    slot_count_mock = AsyncMock(return_value=0)
    held_tickers_mock = AsyncMock(return_value=set())
    with (
        patch("jobs.paper_jobs.load_chart_signals_latest",
              AsyncMock(return_value=("2026-W33", []))),
        patch("jobs.paper_jobs.get_open_slot_count", slot_count_mock),
        patch("jobs.paper_jobs.get_open_or_pending_tickers", held_tickers_mock),
        patch("jobs.paper_jobs.insert_pending", insert_mock),
        patch("jobs.paper_jobs._post_message", AsyncMock()),
    ):
        await paper_eod_sampler_job(pool, trader)

    insert_mock.assert_not_called()
    # 2026-08-22 review 발견: ACTIVE_MODELS 밖 모델은 슬롯 조회/held-ticker
    # 조회 이전에 걸러져야 한다 — insert만 안 됐다고 효율성 의도까지
    # 검증되는 건 아니다.
    slot_count_mock.assert_not_called()
    held_tickers_mock.assert_not_called()
