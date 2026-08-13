"""_reconcile_stale_positions() 단위 테스트.

2026-08-11: confirm_fill()의 폴링 창(수 초)이 이 모의투자 서버의 실제 체결
지연(수 분~10분+)보다 훨씬 짧아 "같은 실행 안 확인"이 구조적으로 거의
불가능함을 발견 — 직전 실행에서 미확인 처리된 (부분)체결 매도가 부풀려진
qty로 open 남아 다음 실행에서 재매도 시 "매도가능수량 부족" 재발로 이어짐.
paper_exit_checker_job이 exit 조건 판정 전에 매번 브로커 실보유와 대조해
self-heal하도록 도입.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _make_pool():
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=None)
    acq = AsyncMock()
    acq.__aenter__ = AsyncMock(return_value=conn)
    acq.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acq)
    return pool, conn


def _pos(id_, ticker, qty, entry_actual=1000.0, entry_theory=1000.0):
    return {"id": id_, "ticker": ticker, "qty": qty,
            "entry_actual": entry_actual, "entry_theory": entry_theory}


@pytest.mark.asyncio
async def test_no_open_positions_returns_zero():
    from jobs.paper_jobs import _reconcile_stale_positions

    pool, _conn = _make_pool()
    trader = MagicMock()
    with patch("jobs.paper_jobs.get_open_positions", AsyncMock(return_value=[])):
        n = await _reconcile_stale_positions(pool, trader)
    assert n == 0
    trader.get_position_qty.assert_not_called()


@pytest.mark.asyncio
async def test_matching_qty_no_change():
    from jobs.paper_jobs import _reconcile_stale_positions

    pool, conn = _make_pool()
    trader = MagicMock()
    trader.get_position_qty.return_value = 100
    positions = [_pos(1, "005930.KS", 100)]
    with patch("jobs.paper_jobs.get_open_positions", AsyncMock(return_value=positions)):
        n = await _reconcile_stale_positions(pool, trader)
    assert n == 0
    conn.execute.assert_not_called()


@pytest.mark.asyncio
async def test_fully_sold_closes_position():
    """실제 브로커 보유가 0이면 직전 미확인 매도가 전량체결됐던 것 — closed 처리."""
    from jobs.paper_jobs import _reconcile_stale_positions

    pool, _conn = _make_pool()
    trader = MagicMock()
    trader.get_position_qty.return_value = 0
    trader.get_current_price.return_value = 900
    positions = [_pos(1, "005930.KS", 100, entry_actual=1000.0)]

    mock_update_closed = AsyncMock()
    with (
        patch("jobs.paper_jobs.get_open_positions", AsyncMock(return_value=positions)),
        patch("jobs.paper_jobs.update_to_closed", mock_update_closed),
    ):
        n = await _reconcile_stale_positions(pool, trader)

    assert n == 1
    mock_update_closed.assert_called_once()
    call_args = mock_update_closed.call_args[0]
    assert call_args[1] == 1               # pos_id
    assert call_args[2] == 900.0           # exit_price = 현재가
    assert call_args[3] == "reconciled_prev_fill"
    assert call_args[5] == pytest.approx((900.0 - 1000.0) / 1000.0)  # blended_return


@pytest.mark.asyncio
async def test_partially_sold_updates_qty_keeps_open():
    """실제 브로커 보유가 0보다 크고 DB보다 작으면 부분체결 — qty만 정정, open 유지."""
    from jobs.paper_jobs import _reconcile_stale_positions

    pool, conn = _make_pool()
    trader = MagicMock()
    trader.get_position_qty.return_value = 60  # DB엔 100인데 실제 60
    positions = [_pos(1, "005930.KS", 100)]

    mock_update_closed = AsyncMock()
    with (
        patch("jobs.paper_jobs.get_open_positions", AsyncMock(return_value=positions)),
        patch("jobs.paper_jobs.update_to_closed", mock_update_closed),
    ):
        n = await _reconcile_stale_positions(pool, trader)

    assert n == 1
    mock_update_closed.assert_not_called()  # 여전히 open — 전량체결 아님
    conn.execute.assert_called_once()
    sql, qty_arg, id_arg = conn.execute.call_args[0]
    assert qty_arg == 60
    assert id_arg == 1


@pytest.mark.asyncio
async def test_broker_qty_higher_than_db_updates_qty_and_avg_price():
    """브로커 보유가 DB보다 많으면(매수 잔량 추가체결) 평균단가 기준으로
    qty/entry_actual을 정정하고 open을 유지한다 (2026-08-13)."""
    from jobs.paper_jobs import _reconcile_stale_positions

    pool, conn = _make_pool()
    trader = MagicMock()
    trader.get_position_qty.return_value = 150  # DB(100)보다 많음
    trader.get_position_avg_price.return_value = 34200
    positions = [_pos(1, "005930.KS", 100)]

    mock_update_closed = AsyncMock()
    with (
        patch("jobs.paper_jobs.get_open_positions", AsyncMock(return_value=positions)),
        patch("jobs.paper_jobs.update_to_closed", mock_update_closed),
    ):
        n = await _reconcile_stale_positions(pool, trader)

    assert n == 1
    mock_update_closed.assert_not_called()  # 여전히 open
    conn.execute.assert_called_once()
    sql, qty_arg, price_arg, id_arg = conn.execute.call_args[0]
    assert qty_arg == 150
    assert price_arg == 34200.0
    assert id_arg == 1


@pytest.mark.asyncio
async def test_broker_qty_higher_than_db_skipped_when_avg_price_unavailable():
    """평균단가 조회가 실패하면(0/None) 정정하지 않고 다음 실행에 재시도한다."""
    from jobs.paper_jobs import _reconcile_stale_positions

    pool, conn = _make_pool()
    trader = MagicMock()
    trader.get_position_qty.return_value = 150  # DB(100)보다 많음
    trader.get_position_avg_price.return_value = None
    positions = [_pos(1, "005930.KS", 100)]

    mock_update_closed = AsyncMock()
    with (
        patch("jobs.paper_jobs.get_open_positions", AsyncMock(return_value=positions)),
        patch("jobs.paper_jobs.update_to_closed", mock_update_closed),
    ):
        n = await _reconcile_stale_positions(pool, trader)

    assert n == 0
    mock_update_closed.assert_not_called()
    conn.execute.assert_not_called()


@pytest.mark.asyncio
async def test_same_ticker_multiple_models_skipped():
    """같은 티커를 여러 모델이 동시 보유하면 브로커 잔고를 모델별로 분리할
    수 없어 자동 정정하지 않는다."""
    from jobs.paper_jobs import _reconcile_stale_positions

    pool, conn = _make_pool()
    trader = MagicMock()
    positions = [
        _pos(1, "028670.KS", 2755),
        _pos(2, "028670.KS", 500),
    ]

    mock_update_closed = AsyncMock()
    with (
        patch("jobs.paper_jobs.get_open_positions", AsyncMock(return_value=positions)),
        patch("jobs.paper_jobs.update_to_closed", mock_update_closed),
    ):
        n = await _reconcile_stale_positions(pool, trader)

    assert n == 0
    trader.get_position_qty.assert_not_called()
    mock_update_closed.assert_not_called()
    conn.execute.assert_not_called()


@pytest.mark.asyncio
async def test_zero_recorded_qty_still_zero_confirms_never_filled():
    """DB qty=0(매수 체결 미확인 보류 상태)에서 브로커도 여전히 0이면 —
    재확인 결과 진짜 미체결 확정, closed 처리한다 (2026-08-13, 003010.KS 등
    유령 보유 재발 원인 수정)."""
    from jobs.paper_jobs import _reconcile_stale_positions

    pool, _conn = _make_pool()
    trader = MagicMock()
    trader.get_position_qty.return_value = 0
    # 실제 흐름에서 update_to_open()은 qty=0이어도 entry_actual을 시가로 채운다
    # (jobs/paper_jobs.py paper_open_entry_job의 미확인 보류 분기 참고).
    positions = [_pos(1, "005930.KS", 0, entry_actual=1000.0, entry_theory=1000.0)]

    mock_update_closed = AsyncMock()
    with (
        patch("jobs.paper_jobs.get_open_positions", AsyncMock(return_value=positions)),
        patch("jobs.paper_jobs.update_to_closed", mock_update_closed),
    ):
        n = await _reconcile_stale_positions(pool, trader)

    assert n == 1
    mock_update_closed.assert_called_once()
    call_args = mock_update_closed.call_args[0]
    assert call_args[1] == 1                  # pos_id
    assert call_args[2] == 1000.0              # exit_price = entry_actual(시가)
    assert call_args[3] == "buy_never_filled"
    assert call_args[5] is None                # blended_return — 실거래 없었으므로 통계 제외


@pytest.mark.asyncio
async def test_zero_recorded_qty_late_fill_uses_avg_price_catchup():
    """DB qty=0인데 브로커에 실제로 체결된 게 있으면(뒤늦은 체결) — 매수
    잔량 추가체결과 동일한 브로커 평균단가 재조정 경로를 그대로 탄다."""
    from jobs.paper_jobs import _reconcile_stale_positions

    pool, conn = _make_pool()
    trader = MagicMock()
    trader.get_position_qty.return_value = 80
    trader.get_position_avg_price.return_value = 15000
    positions = [_pos(1, "005930.KS", 0)]

    mock_update_closed = AsyncMock()
    with (
        patch("jobs.paper_jobs.get_open_positions", AsyncMock(return_value=positions)),
        patch("jobs.paper_jobs.update_to_closed", mock_update_closed),
    ):
        n = await _reconcile_stale_positions(pool, trader)

    assert n == 1
    mock_update_closed.assert_not_called()  # open 유지 — 체결된 매수로 확정
    conn.execute.assert_called_once()
    sql, qty_arg, price_arg, id_arg = conn.execute.call_args[0]
    assert qty_arg == 80
    assert price_arg == 15000.0
    assert id_arg == 1


@pytest.mark.asyncio
async def test_current_price_fetch_failure_skips_without_crashing():
    """전량체결 감지됐는데 현재가 조회가 실패하면(0/None) closed 처리하지
    않고 다음 실행에 재시도하도록 건너뛴다."""
    from jobs.paper_jobs import _reconcile_stale_positions

    pool, _conn = _make_pool()
    trader = MagicMock()
    trader.get_position_qty.return_value = 0
    trader.get_current_price.return_value = None
    positions = [_pos(1, "005930.KS", 100)]

    mock_update_closed = AsyncMock()
    with (
        patch("jobs.paper_jobs.get_open_positions", AsyncMock(return_value=positions)),
        patch("jobs.paper_jobs.update_to_closed", mock_update_closed),
    ):
        n = await _reconcile_stale_positions(pool, trader)

    assert n == 0
    mock_update_closed.assert_not_called()
