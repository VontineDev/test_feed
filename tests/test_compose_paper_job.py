"""tests/test_compose_paper_job.py — compose_paper_entry_job 단위 테스트."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, call

import pandas as pd
import pytest

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ── 공통 mock 헬퍼 ──────────────────────────────────────────────

def _make_pool(fetchval_return=None):
    conn = AsyncMock()
    conn.fetchval = AsyncMock(return_value=fetchval_return)
    acq = AsyncMock()
    acq.__aenter__ = AsyncMock(return_value=conn)
    acq.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acq)
    return pool, conn


def _funnel_frame(tickers: list[str]) -> pd.DataFrame:
    return pd.DataFrame({"ticker": tickers, "week": ["2026-W24"] * len(tickers)})


# ── _get_this_week_signals_sync ─────────────────────────────────

def test_get_signals_returns_tickers(monkeypatch):
    """정상 신호 추출 — FUNNEL-1 이번 주 top_n 반환."""
    from jobs.compose_paper_job import _get_this_week_signals_sync
    from analysis.strategy_compose import STRATEGIES

    frame = _funnel_frame(["005930.KS", "000660.KS", "035420.KS"])

    with (
        patch("jobs.compose_paper_job.load_signal_frame", return_value=frame),
        patch("jobs.compose_paper_job.derive_flags", return_value=frame),
        patch("jobs.compose_paper_job.iso_week", return_value="2026-W24"),
        patch.object(STRATEGIES["FUNNEL-1"], "run", return_value=frame),
    ):
        result = _get_this_week_signals_sync("postgresql://...", "FUNNEL-1", 10)

    assert result == ["005930.KS", "000660.KS", "035420.KS"]


def test_get_signals_empty_frame(monkeypatch):
    """빈 프레임 → 빈 리스트 반환."""
    from jobs.compose_paper_job import _get_this_week_signals_sync

    with patch("jobs.compose_paper_job.load_signal_frame", return_value=pd.DataFrame()):
        result = _get_this_week_signals_sync("postgresql://...", "FUNNEL-1", 10)

    assert result == []


def test_get_signals_top_n_truncation(monkeypatch):
    """top_n=2 → head(2) 적용."""
    from jobs.compose_paper_job import _get_this_week_signals_sync
    from analysis.strategy_compose import STRATEGIES

    frame = _funnel_frame(["A.KS", "B.KS", "C.KS", "D.KS", "E.KS"])

    with (
        patch("jobs.compose_paper_job.load_signal_frame", return_value=frame),
        patch("jobs.compose_paper_job.derive_flags", return_value=frame),
        patch("jobs.compose_paper_job.iso_week", return_value="2026-W24"),
        patch.object(STRATEGIES["FUNNEL-1"], "run", return_value=frame),
    ):
        result = _get_this_week_signals_sync("postgresql://...", "FUNNEL-1", 2)

    assert result == ["A.KS", "B.KS"]


def test_get_signals_fallback_prev_week(monkeypatch):
    """cur_week 신호 없을 때 prev_week fallback."""
    from jobs.compose_paper_job import _get_this_week_signals_sync
    from analysis.strategy_compose import STRATEGIES

    frame = pd.DataFrame({"ticker": ["005930.KS"], "week": ["2026-W23"]})

    with (
        patch("jobs.compose_paper_job.load_signal_frame", return_value=frame),
        patch("jobs.compose_paper_job.derive_flags", return_value=frame),
        patch("jobs.compose_paper_job.iso_week", side_effect=["2026-W24", "2026-W23"]),
        patch.object(STRATEGIES["FUNNEL-1"], "run", return_value=frame),
    ):
        result = _get_this_week_signals_sync("postgresql://...", "FUNNEL-1", 10)

    assert result == ["005930.KS"]


# ── compose_paper_entry_job ─────────────────────────────────────

@pytest.mark.asyncio
async def test_entry_job_no_dsn():
    """DSN 없으면 조기 반환."""
    from jobs.compose_paper_job import compose_paper_entry_job
    pool, _ = _make_pool()
    await compose_paper_entry_job("", pool)  # 빈 DSN


@pytest.mark.asyncio
async def test_entry_job_inserts_pending(monkeypatch):
    """정상 신호 → insert_pending 호출 확인."""
    from jobs.compose_paper_job import compose_paper_entry_job

    pool, conn = _make_pool(fetchval_return=None)  # 중복 없음

    insert_mock = AsyncMock(return_value=1)
    slot_mock = AsyncMock(return_value=0)  # 빈 슬롯

    with (
        patch("jobs.compose_paper_job._get_this_week_signals_sync",
              side_effect=[["005930.KS"], []]),  # FUNNEL-1 신호 1개, AND-1 없음
        patch("jobs.compose_paper_job.insert_pending", insert_mock),
        patch("jobs.compose_paper_job.get_open_slot_count", slot_mock),
    ):
        await compose_paper_entry_job("postgresql://...", pool)

    insert_mock.assert_called_once()
    call_kwargs = insert_mock.call_args
    assert call_kwargs.args[1] == "compose-funnel1"
    assert call_kwargs.args[2] == "005930.KS"
    assert call_kwargs.kwargs["entry_theory"] == 0.0


@pytest.mark.asyncio
async def test_entry_job_skips_duplicate(monkeypatch):
    """이미 pending/open 존재 시 insert 스킵."""
    from jobs.compose_paper_job import compose_paper_entry_job

    pool, conn = _make_pool(fetchval_return=1)  # 중복 있음

    insert_mock = AsyncMock(return_value=1)
    slot_mock = AsyncMock(return_value=0)

    with (
        patch("jobs.compose_paper_job._get_this_week_signals_sync",
              side_effect=[["005930.KS"], []]),
        patch("jobs.compose_paper_job.insert_pending", insert_mock),
        patch("jobs.compose_paper_job.get_open_slot_count", slot_mock),
    ):
        await compose_paper_entry_job("postgresql://...", pool)

    insert_mock.assert_not_called()


@pytest.mark.asyncio
async def test_entry_job_respects_max_slots(monkeypatch):
    """슬롯 만료 → 이하 종목 전체 스킵 (break)."""
    from jobs.compose_paper_job import compose_paper_entry_job

    pool, conn = _make_pool(fetchval_return=None)

    insert_mock = AsyncMock(return_value=1)
    slot_mock = AsyncMock(return_value=10)  # max_slots=10 꽉 참

    with (
        patch("jobs.compose_paper_job._get_this_week_signals_sync",
              side_effect=[["005930.KS", "000660.KS"], []]),
        patch("jobs.compose_paper_job.insert_pending", insert_mock),
        patch("jobs.compose_paper_job.get_open_slot_count", slot_mock),
    ):
        await compose_paper_entry_job("postgresql://...", pool)

    insert_mock.assert_not_called()


@pytest.mark.asyncio
async def test_entry_job_timeout_handled(monkeypatch):
    """신호 추출 타임아웃 → 해당 전략 스킵, 크래시 없음."""
    import asyncio
    from jobs.compose_paper_job import compose_paper_entry_job

    async def _timeout_side_effect(*a, **kw):
        raise asyncio.TimeoutError

    pool, _ = _make_pool()

    with patch("asyncio.wait_for", side_effect=_timeout_side_effect):
        await compose_paper_entry_job("postgresql://...", pool)
