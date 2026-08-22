"""paper_open_entry_job() 매수 체결 미확인 처리 테스트.

2026-08-13 발견(003010.KS 유령 보유 재발 계기): confirm_fill()의 짧은 폴링
창 안에서 체결이 0으로 확인되면 기존엔 즉시 status='closed', exit_type=
'buy_never_filled'로 영구 확정했다 — 실제로는 이 모의투자 서버 특성상
체결이 몇 분 뒤에 이뤄지는 경우가 흔한데(매도 쪽에서 이미 문서화된 문제),
매수 쪽은 즉시 확정이라 나중에 진짜 체결된 게 밝혀져도 브로커 실보유와
대조할 열린 DB 행이 아예 없어 유령 보유로 이어졌다. 즉시 확정 대신
open(qty=0)으로 보류해 다음 실행의 _reconcile_stale_positions()가 최종
판정하도록 바꿨다.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _pending(id_, ticker="005930.KS", model="stage"):
    return {"id": id_, "ticker": ticker, "model": model}


def _trader(open_px=70000, qty_before=0, ord_no="0099999", filled=0):
    trader = MagicMock()
    trader.get_balance.return_value = {"tot_pur_amt": 0, "prsm_dpst_aset_amt": 100_000_000}
    trader.get_open_price.return_value = open_px
    trader.get_current_price.return_value = open_px
    trader.get_position_qty.return_value = qty_before
    trader.place_buy.return_value = ord_no
    trader.confirm_fill.return_value = filled
    return trader


@pytest.mark.asyncio
async def test_unconfirmed_fill_defers_to_open_qty_zero_instead_of_closing():
    """체결 미확인(filled=0)이면 update_to_closed가 아니라 update_to_open을
    qty=0으로 호출해 판정을 다음 재조정으로 미룬다."""
    from jobs.paper_jobs import paper_open_entry_job

    trader = _trader(filled=0)
    mock_open = AsyncMock()
    mock_closed = AsyncMock()
    with (
        patch("jobs.paper_jobs.get_pending_positions", AsyncMock(return_value=[_pending(1)])),
        patch("jobs.paper_jobs.compute_slot_krw", return_value={"stage": 10_000_000}),
        patch("jobs.paper_jobs.deployable_capital", return_value=100_000_000),
        patch("jobs.paper_jobs.update_to_open", mock_open),
        patch("jobs.paper_jobs.update_to_closed", mock_closed),
        patch("jobs.paper_jobs._post_message", AsyncMock()),
    ):
        await paper_open_entry_job(MagicMock(), trader)

    mock_closed.assert_not_called()
    mock_open.assert_called_once()
    call_args = mock_open.call_args[0]
    assert call_args[1] == 1          # pos_id
    assert call_args[3] == 0          # qty=0 — 미확인 보류
    assert call_args[4] == "0099999"  # 주문번호는 보존(추후 재조정 추적용)


@pytest.mark.asyncio
async def test_confirmed_fill_updates_to_open_with_real_qty():
    """정상 체결(filled>0)이면 기존처럼 update_to_open으로 실체결 수량이 기록된다."""
    from jobs.paper_jobs import paper_open_entry_job

    trader = _trader(filled=50)
    mock_open = AsyncMock()
    mock_closed = AsyncMock()
    with (
        patch("jobs.paper_jobs.get_pending_positions", AsyncMock(return_value=[_pending(1)])),
        patch("jobs.paper_jobs.compute_slot_krw", return_value={"stage": 10_000_000}),
        patch("jobs.paper_jobs.deployable_capital", return_value=100_000_000),
        patch("jobs.paper_jobs.update_to_open", mock_open),
        patch("jobs.paper_jobs.update_to_closed", mock_closed),
        patch("jobs.paper_jobs._post_message", AsyncMock()),
    ):
        await paper_open_entry_job(MagicMock(), trader)

    mock_closed.assert_not_called()
    mock_open.assert_called_once()
    call_args = mock_open.call_args[0]
    assert call_args[3] == 50


@pytest.mark.asyncio
async def test_model_outside_active_models_skips_before_price_fetch():
    """ACTIVE_MODELS 밖 모델(예: kosdaq)은 가격 조회(0.5초 딜레이 + 실 API 호출)
    전에 걸러야 한다 — 매번 스킵될 걸 알면서 API를 낭비하지 않도록
    (2026-08-22 code-review 발견)."""
    from jobs.paper_jobs import paper_open_entry_job

    trader = _trader()
    mock_open = AsyncMock()
    mock_closed = AsyncMock()
    with (
        patch("jobs.paper_jobs.get_pending_positions",
              AsyncMock(return_value=[_pending(1, ticker="294570.KQ", model="kosdaq")])),
        patch("jobs.paper_jobs.compute_slot_krw", return_value={"stage": 10_000_000}),
        patch("jobs.paper_jobs.deployable_capital", return_value=100_000_000),
        patch("jobs.paper_jobs.update_to_open", mock_open),
        patch("jobs.paper_jobs.update_to_closed", mock_closed),
        patch("jobs.paper_jobs._post_message", AsyncMock()),
    ):
        await paper_open_entry_job(MagicMock(), trader)

    trader.get_open_price.assert_not_called()
    trader.get_current_price.assert_not_called()
    # 2026-08-22 review 발견: 가격조회 미호출만 검증하면 스킵된 pending이
    # 실제로 그대로 pending 유지되는지(재시도 대상으로 남는지)는 검증이 안 됨.
    # 스킵 = "아무것도 하지 않음"이어야 하므로 open/closed 전환도 없어야 한다.
    mock_open.assert_not_called()
    mock_closed.assert_not_called()
    trader.place_buy.assert_not_called()


@pytest.mark.asyncio
async def test_active_model_still_processed_after_inactive_model_skipped():
    """pending 목록에 ACTIVE_MODELS 밖 모델과 안 모델이 섞여 있으면, 앞쪽이
    스킵되더라도 뒤쪽(활성 모델)은 정상적으로 계속 처리된다 — 루프 continuation
    회귀 방지(2026-08-22 review 발견: 단일 항목만으로는 스킵이 루프 전체를
    끊는 회귀를 못 잡음)."""
    from jobs.paper_jobs import paper_open_entry_job

    trader = _trader()
    mock_open = AsyncMock()
    with (
        patch("jobs.paper_jobs.get_pending_positions", AsyncMock(return_value=[
            _pending(1, ticker="294570.KQ", model="kosdaq"),
            _pending(2, ticker="005930.KS", model="stage"),
        ])),
        patch("jobs.paper_jobs.compute_slot_krw", return_value={"stage": 10_000_000}),
        patch("jobs.paper_jobs.deployable_capital", return_value=100_000_000),
        patch("jobs.paper_jobs.update_to_open", mock_open),
        patch("jobs.paper_jobs.update_to_closed", AsyncMock()),
        patch("jobs.paper_jobs._post_message", AsyncMock()),
    ):
        await paper_open_entry_job(MagicMock(), trader)

    trader.get_open_price.assert_called_once_with("005930.KS")
    mock_open.assert_called_once()
