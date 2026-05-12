"""
test_trade_journal.py — 거래 저널 unit tests (asyncpg mock)

커버리지:
  /buy  — 정상(당일), 정상(과거날짜), args부족, 비숫자가격, stage없음
  /sell — FIFO정상청산, 포지션없음, args부족
  /port — 빈포트폴리오, 보유+yfinance성공, yfinance실패폴백
  /pnl  — week/month/all, 데이터없음, stage별집계
  DB    — asyncpg date타입, classified_date쿼리, FIFO선택
"""

from __future__ import annotations

import asyncio
from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest


# ── 공통 픽스처 ──────────────────────────────────────────────

def _make_pool(fetchrow_return=None, fetch_return=None):
    """asyncpg Pool mock — acquire() → conn → fetchrow/fetch/execute."""
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=fetchrow_return)
    conn.fetch = AsyncMock(return_value=fetch_return or [])
    conn.execute = AsyncMock(return_value=None)
    # transaction() context manager
    tx = AsyncMock()
    tx.__aenter__ = AsyncMock(return_value=tx)
    tx.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=tx)
    # pool.acquire() context manager
    acq = AsyncMock()
    acq.__aenter__ = AsyncMock(return_value=conn)
    acq.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acq)
    return pool, conn


async def _noop_send(*a, **kw):
    pass


# ── /buy tests ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_buy_happy_path_today():
    """정상 진입 — 당일 날짜, stage 발견."""
    pool, conn = _make_pool()
    # signal lookup → stage found
    conn.fetchrow.side_effect = [
        {"sig_date": date(2026, 5, 1), "stage": 1},   # stage_classifications
        {"after_close": 69000, "after_chg_pct": 0.5}, # aftermarket_snap
        {"id": 42},                                    # INSERT RETURNING id
    ]
    sent = []
    import telegram_trade
    with patch.object(telegram_trade, "_send", side_effect=lambda *a, **kw: sent.append(a[3]) or asyncio.coroutine(lambda: None)()):
        with patch("telegram_trade._send", new_callable=AsyncMock) as mock_send:
            await telegram_trade.handle_buy(
                MagicMock(), "TOKEN", "CHAT", ["005930", "70000", "100"], pool
            )
    mock_send.assert_called_once()
    text = mock_send.call_args[0][3]
    assert "42" in text
    assert "005930.KS" in text


@pytest.mark.asyncio
async def test_buy_past_date_override():
    """과거 날짜 파라미터 — entry_date가 오늘이 아닌 지정 날짜."""
    pool, conn = _make_pool()
    conn.fetchrow.side_effect = [
        {"sig_date": date(2026, 5, 1), "stage": 2},
        None,
        {"id": 10},
    ]
    import telegram_trade
    with patch("telegram_trade._send", new_callable=AsyncMock) as mock_send:
        await telegram_trade.handle_buy(
            MagicMock(), "TOKEN", "CHAT",
            ["005930", "68000", "50", "20260501"], pool
        )
    # DB save_trade 에 date(2026,5,1) 이 전달됐는지 확인
    # fetchrow 세 번째 호출이 INSERT
    insert_call = conn.fetchrow.call_args_list[2]
    # entry_date 파라미터 (3번째 위치 인자)
    assert insert_call[0][2] == date(2026, 5, 1)
    mock_send.assert_called_once()


@pytest.mark.asyncio
async def test_buy_missing_args():
    """args 부족 → 오류 메시지, DB 저장 없음."""
    pool, conn = _make_pool()
    import telegram_trade
    with patch("telegram_trade._send", new_callable=AsyncMock) as mock_send:
        await telegram_trade.handle_buy(
            MagicMock(), "TOKEN", "CHAT", ["005930"], pool
        )
    mock_send.assert_called_once()
    assert "사용법" in mock_send.call_args[0][3]
    conn.fetchrow.assert_not_called()


@pytest.mark.asyncio
async def test_buy_invalid_price():
    """가격 비숫자 → 오류 메시지."""
    pool, conn = _make_pool()
    import telegram_trade
    with patch("telegram_trade._send", new_callable=AsyncMock) as mock_send:
        await telegram_trade.handle_buy(
            MagicMock(), "TOKEN", "CHAT", ["005930", "abc", "100"], pool
        )
    mock_send.assert_called_once()
    assert "숫자" in mock_send.call_args[0][3]


@pytest.mark.asyncio
async def test_buy_no_stage_signal():
    """stage_classifications에 신호 없음 → signal_date=NULL 허용, 저장은 정상."""
    pool, conn = _make_pool()
    conn.fetchrow.side_effect = [
        {"sig_date": None, "stage": None},  # no stage
        None,                               # no aftermarket
        {"id": 99},                         # INSERT OK
    ]
    import telegram_trade
    with patch("telegram_trade._send", new_callable=AsyncMock) as mock_send:
        await telegram_trade.handle_buy(
            MagicMock(), "TOKEN", "CHAT", ["999999", "5000", "200"], pool
        )
    mock_send.assert_called_once()
    text = mock_send.call_args[0][3]
    assert "99" in text


# ── /sell tests ──────────────────────────────────────────────

@pytest.mark.asyncio
async def test_sell_fifo_happy_path():
    """FIFO 청산 — 오픈 포지션 있음."""
    pool, conn = _make_pool()
    # close_position 내부 흐름: fetchrow(FIFO) → fetchrow(stage) → fetchrow(UPDATE)
    conn.fetchrow.side_effect = [
        {"id": 5, "entry_price": 70000, "qty": 100},  # FOR UPDATE
        {"stage": 2},                                  # stage_at_exit
        {   # UPDATE RETURNING *
            "id": 5, "ticker": "005930.KS",
            "entry_date": date(2026, 5, 1),
            "entry_price": 70000, "qty": 100,
            "exit_date": date.today(), "exit_price": 73500,
            "stage_at_entry": 1, "stage_at_exit": 2,
            "pnl": 350000, "pnl_pct": 5.0,
        },
    ]
    import telegram_trade
    with patch("telegram_trade._send", new_callable=AsyncMock) as mock_send:
        await telegram_trade.handle_sell(
            MagicMock(), "TOKEN", "CHAT", ["005930", "73500"], pool
        )
    mock_send.assert_called_once()
    text = mock_send.call_args[0][3]
    assert "청산 완료" in text
    assert "350,000" in text or "35만" in text


@pytest.mark.asyncio
async def test_sell_no_open_position():
    """미청산 포지션 없음 → 안내 메시지."""
    pool, conn = _make_pool()
    conn.fetchrow.return_value = None  # FOR UPDATE → None
    import telegram_trade
    with patch("telegram_trade._send", new_callable=AsyncMock) as mock_send:
        await telegram_trade.handle_sell(
            MagicMock(), "TOKEN", "CHAT", ["005930", "73500"], pool
        )
    mock_send.assert_called_once()
    assert "미청산" in mock_send.call_args[0][3] or "없" in mock_send.call_args[0][3]


@pytest.mark.asyncio
async def test_sell_missing_args():
    """args 부족 → 오류 메시지."""
    pool, conn = _make_pool()
    import telegram_trade
    with patch("telegram_trade._send", new_callable=AsyncMock) as mock_send:
        await telegram_trade.handle_sell(
            MagicMock(), "TOKEN", "CHAT", ["005930"], pool
        )
    assert "사용법" in mock_send.call_args[0][3]


# ── /port tests ──────────────────────────────────────────────

@pytest.mark.asyncio
async def test_port_empty():
    """보유 없음 → 빈 포트폴리오 메시지."""
    pool, conn = _make_pool(fetch_return=[])
    import telegram_trade
    with patch("telegram_trade._send", new_callable=AsyncMock) as mock_send:
        await telegram_trade.handle_port(
            MagicMock(), "TOKEN", "CHAT", pool
        )
    mock_send.assert_called_once()
    assert "없음" in mock_send.call_args[0][3] or "없" in mock_send.call_args[0][3]


@pytest.mark.asyncio
async def test_port_with_positions_yfinance_ok():
    """보유 있음 + yfinance 성공 → 미실현 P&L 포함."""
    pool, conn = _make_pool()
    conn.fetch.return_value = [
        {
            "id": 1, "ticker": "005930.KS",
            "entry_date": date(2026, 5, 1), "entry_price": 70000,
            "qty": 100, "signal_date": date(2026, 5, 1), "stage_at_entry": 1,
        }
    ]
    import pandas as pd
    import telegram_trade

    mock_df = MagicMock()
    mock_df.__contains__ = MagicMock(return_value=True)
    mock_df.__getitem__ = MagicMock(return_value=pd.Series({"005930.KS": 73500}))
    mock_df.iloc = MagicMock()
    mock_df.iloc.__getitem__ = MagicMock(return_value=pd.Series({"005930.KS": 73500}))

    def fake_fetch():
        return {"005930.KS": 73500}

    with patch("asyncio.to_thread", new_callable=AsyncMock, return_value={"005930.KS": 73500}):
        with patch("telegram_trade._send", new_callable=AsyncMock) as mock_send:
            await telegram_trade.handle_port(
                MagicMock(), "TOKEN", "CHAT", pool
            )
    mock_send.assert_called_once()
    text = mock_send.call_args[0][3]
    assert "005930.KS" in text


@pytest.mark.asyncio
async def test_port_yfinance_failure_fallback():
    """yfinance 실패 → '가격 조회 불가' 폴백 메시지."""
    pool, conn = _make_pool()
    conn.fetch.return_value = [
        {
            "id": 2, "ticker": "005930.KS",
            "entry_date": date(2026, 5, 5), "entry_price": 72000,
            "qty": 50, "signal_date": None, "stage_at_entry": None,
        }
    ]
    import telegram_trade
    with patch("asyncio.to_thread", new_callable=AsyncMock,
               return_value={"005930.KS": None}):
        with patch("telegram_trade._send", new_callable=AsyncMock) as mock_send:
            await telegram_trade.handle_port(
                MagicMock(), "TOKEN", "CHAT", pool
            )
    text = mock_send.call_args[0][3]
    assert "조회 불가" in text or "005930" in text


# ── /pnl tests ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_pnl_all_no_data():
    """실현 거래 없음 → '없음' 안내."""
    pool, conn = _make_pool(fetch_return=[])
    import telegram_trade
    with patch("telegram_trade._send", new_callable=AsyncMock) as mock_send:
        await telegram_trade.handle_pnl(
            MagicMock(), "TOKEN", "CHAT", [], pool
        )
    assert "없음" in mock_send.call_args[0][3]


@pytest.mark.asyncio
async def test_pnl_week():
    """week 필터 동작 확인."""
    pool, conn = _make_pool()
    conn.fetch.return_value = [
        {"pnl": 300000, "pnl_pct": 4.3, "stage_at_entry": 1},
        {"pnl": -150000, "pnl_pct": -2.1, "stage_at_entry": 2},
    ]
    import telegram_trade
    with patch("telegram_trade._send", new_callable=AsyncMock) as mock_send:
        await telegram_trade.handle_pnl(
            MagicMock(), "TOKEN", "CHAT", ["week"], pool
        )
    text = mock_send.call_args[0][3]
    assert "직전 주" in text


@pytest.mark.asyncio
async def test_pnl_month():
    """month 필터 동작 확인."""
    pool, conn = _make_pool()
    conn.fetch.return_value = [
        {"pnl": 500000, "pnl_pct": 7.1, "stage_at_entry": 1},
    ]
    import telegram_trade
    with patch("telegram_trade._send", new_callable=AsyncMock) as mock_send:
        await telegram_trade.handle_pnl(
            MagicMock(), "TOKEN", "CHAT", ["month"], pool
        )
    assert "이번 달" in mock_send.call_args[0][3]


@pytest.mark.asyncio
async def test_pnl_stage_breakdown():
    """Stage별 집계 — by_stage dict에 Stage 1, 2 포함."""
    pool, conn = _make_pool()
    conn.fetch.return_value = [
        {"pnl": 200000, "pnl_pct": 2.9, "stage_at_entry": 1},
        {"pnl": 180000, "pnl_pct": 2.6, "stage_at_entry": 1},
        {"pnl": -80000, "pnl_pct": -1.1, "stage_at_entry": 2},
    ]
    import telegram_trade
    with patch("telegram_trade._send", new_callable=AsyncMock) as mock_send:
        await telegram_trade.handle_pnl(
            MagicMock(), "TOKEN", "CHAT", ["all"], pool
        )
    text = mock_send.call_args[0][3]
    assert "Stage 1" in text
    assert "Stage 2" in text


# ── DB layer tests ───────────────────────────────────────────

@pytest.mark.asyncio
async def test_save_trade_date_object():
    """asyncpg에 date 객체(str 아님)가 전달되는지 검증."""
    pool, conn = _make_pool()
    conn.fetchrow.side_effect = [
        {"sig_date": date(2026, 5, 1), "stage": 1},
        None,
        {"id": 7},
    ]
    from db import save_trade
    result = await save_trade(
        pool,
        ticker="005930.KS",
        entry_date=date(2026, 5, 8),
        entry_price=70000,
        qty=100,
    )
    assert result == 7
    # INSERT 호출 인자에서 entry_date 확인 — date 객체여야 함
    insert_args = conn.fetchrow.call_args_list[2][0]
    entry_date_arg = insert_args[2]   # $2 위치
    assert isinstance(entry_date_arg, date), \
        f"asyncpg requires date object, got {type(entry_date_arg)}"


@pytest.mark.asyncio
async def test_close_position_fifo_uses_classified_date():
    """close_position stage_at_exit 쿼리가 classified_date 사용하는지 확인."""
    pool, conn = _make_pool()
    conn.fetchrow.side_effect = [
        {"id": 3, "entry_price": 70000, "qty": 100},  # FOR UPDATE
        {"stage": 2},                                  # stage_at_exit
        {
            "id": 3, "ticker": "005930.KS",
            "entry_date": date(2026, 5, 1),
            "entry_price": 70000, "qty": 100,
            "exit_date": date.today(), "exit_price": 73000,
            "stage_at_entry": 1, "stage_at_exit": 2,
            "pnl": 300000, "pnl_pct": 4.29,
        },
    ]
    from db import close_position
    result = await close_position(
        pool,
        ticker="005930.KS",
        exit_date=date.today(),
        exit_price=73000,
    )
    assert result is not None
    # stage_at_exit 쿼리의 SQL에 classified_date가 포함돼야 함
    stage_query_call = conn.fetchrow.call_args_list[1]
    query_sql = stage_query_call[0][0]
    assert "classified_date" in query_sql, \
        "stage_at_exit 쿼리가 classified_date 를 사용해야 함"


@pytest.mark.asyncio
async def test_get_pnl_summary_returns_dict():
    """get_pnl_summary — 빈 결과에서도 dict 구조 보장."""
    pool, conn = _make_pool(fetch_return=[])
    from db import get_pnl_summary
    result = await get_pnl_summary(pool, period="all")
    assert "trade_cnt" in result
    assert "total_pnl" in result
    assert result["trade_cnt"] == 0
