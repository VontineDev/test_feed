"""liquidate_and_reenter() 단위 테스트.

2026-08-30: 여러 모델이 동시보유해 모델별 귀속이 불가능해진 티커를 브로커
기준 전량 매도 후 깨끗한 pending으로 재출발시키는 스크립트. FIFO 근사 귀속
(jobs/paper_jobs.py::_reconcile_multi_model_ticker)이 적용될 수 없는
qty_ordered 없는 구 데이터(2026-08-24~08-28 동시보유분)를 위한 대안.
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


def _row(id_, model, qty, tp1_pct=0.15, tp1_ratio=0.50, trail_pct=0.10, hard_stop_pct=0.10):
    return {"id": id_, "model": model, "ticker": "085620.KS", "qty": qty,
            "tp1_pct": tp1_pct, "tp1_ratio": tp1_ratio,
            "trail_pct": trail_pct, "hard_stop_pct": hard_stop_pct}


def _make_pool(fetch_rows):
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=None)
    conn.fetch = AsyncMock(return_value=fetch_rows)
    tx = AsyncMock()
    tx.__aenter__ = AsyncMock(return_value=None)
    tx.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=tx)
    acq = AsyncMock()
    acq.__aenter__ = AsyncMock(return_value=conn)
    acq.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acq)
    return pool, conn


@pytest.mark.asyncio
async def test_no_open_rows_is_noop():
    from scripts.liquidate_reenter_paper_positions import liquidate_and_reenter

    pool, conn = _make_pool([])
    trader = MagicMock()

    await liquidate_and_reenter(pool, trader, "085620.KS", apply=True)

    trader.get_position_qty.assert_not_called()
    conn.execute.assert_not_called()


@pytest.mark.asyncio
async def test_dry_run_never_calls_trader_or_writes():
    from scripts.liquidate_reenter_paper_positions import liquidate_and_reenter

    rows = [_row(1, "compose-funnel1", 46), _row(2, "compose-score1", 46)]
    pool, conn = _make_pool(rows)
    trader = MagicMock()
    trader.get_position_qty.return_value = 92

    await liquidate_and_reenter(pool, trader, "085620.KS", apply=False)

    trader.place_sell.assert_not_called()
    conn.execute.assert_not_called()


@pytest.mark.asyncio
async def test_full_liquidation_closes_old_rows_and_inserts_fresh_pending():
    from scripts.liquidate_reenter_paper_positions import liquidate_and_reenter

    rows = [_row(1, "compose-funnel1", 46, tp1_ratio=0.70, trail_pct=0.15),
            _row(2, "compose-score1", 46)]
    pool, conn = _make_pool(rows)
    trader = MagicMock()
    trader.get_position_qty.return_value = 92
    trader.place_sell.return_value = "0099999"
    trader.confirm_fill.return_value = 92          # 전량 체결 확인
    trader.get_current_price.return_value = 24450

    mock_insert = AsyncMock(side_effect=[10, 11])
    with patch("scripts.liquidate_reenter_paper_positions.insert_pending", mock_insert):
        await liquidate_and_reenter(pool, trader, "085620.KS", apply=True)

    trader.place_sell.assert_called_once_with("085620.KS", 92)
    trader.confirm_fill.assert_called_once_with("085620.KS", "0099999", 92, False, 92)

    conn.execute.assert_called_once()
    sql, exit_date, exit_price, ids = conn.execute.call_args[0]
    assert exit_date == date.today()
    assert exit_price == 24450.0
    assert set(ids) == {1, 2}
    assert "manual_liquidate_reentry" in sql
    assert "blended_return=NULL" in sql

    assert mock_insert.call_count == 2
    call_models = {c.args[1] for c in mock_insert.call_args_list}
    assert call_models == {"compose-funnel1", "compose-score1"}
    funnel_call = next(c for c in mock_insert.call_args_list if c.args[1] == "compose-funnel1")
    assert funnel_call.kwargs["entry_theory"] == 0.0
    assert funnel_call.kwargs["tp1_ratio"] == 0.70   # 모델별 튜닝값 유지
    assert funnel_call.kwargs["trail_pct"] == 0.15


@pytest.mark.asyncio
async def test_unconfirmed_sell_aborts_without_touching_db():
    """체결 미확인(폴링 창 안 못 잡음)이면 아무 것도 정리하지 않는다 —
    재실행은 안전해야 하므로 절반 처리된 상태를 만들지 않는다."""
    from scripts.liquidate_reenter_paper_positions import liquidate_and_reenter

    rows = [_row(1, "compose-funnel1", 46), _row(2, "compose-score1", 46)]
    pool, conn = _make_pool(rows)
    trader = MagicMock()
    trader.get_position_qty.return_value = 92
    trader.place_sell.return_value = "0099999"
    trader.confirm_fill.return_value = 0            # 폴링 창 안에 못 잡음

    mock_insert = AsyncMock()
    with patch("scripts.liquidate_reenter_paper_positions.insert_pending", mock_insert):
        await liquidate_and_reenter(pool, trader, "085620.KS", apply=True)

    conn.execute.assert_not_called()
    mock_insert.assert_not_called()


@pytest.mark.asyncio
async def test_already_zero_actual_skips_sell_but_still_reenters():
    """브로커 실보유가 이미 0이면(예: 직전 실행에서 매도만 확인 안 됐던 경우)
    매도는 생략하고 바로 정리+재진입한다."""
    from scripts.liquidate_reenter_paper_positions import liquidate_and_reenter

    rows = [_row(1, "compose-funnel1", 46), _row(2, "compose-score1", 46)]
    pool, conn = _make_pool(rows)
    trader = MagicMock()
    trader.get_position_qty.return_value = 0
    trader.get_current_price.return_value = 24450

    mock_insert = AsyncMock(side_effect=[10, 11])
    with patch("scripts.liquidate_reenter_paper_positions.insert_pending", mock_insert):
        await liquidate_and_reenter(pool, trader, "085620.KS", apply=True)

    trader.place_sell.assert_not_called()
    conn.execute.assert_called_once()
    assert mock_insert.call_count == 2


@pytest.mark.asyncio
async def test_price_fetch_failure_after_sell_aborts_without_touching_db():
    from scripts.liquidate_reenter_paper_positions import liquidate_and_reenter

    rows = [_row(1, "compose-funnel1", 46), _row(2, "compose-score1", 46)]
    pool, conn = _make_pool(rows)
    trader = MagicMock()
    trader.get_position_qty.return_value = 92
    trader.place_sell.return_value = "0099999"
    trader.confirm_fill.return_value = 92
    trader.get_current_price.return_value = None

    mock_insert = AsyncMock()
    with patch("scripts.liquidate_reenter_paper_positions.insert_pending", mock_insert):
        await liquidate_and_reenter(pool, trader, "085620.KS", apply=True)

    conn.execute.assert_not_called()
    mock_insert.assert_not_called()
