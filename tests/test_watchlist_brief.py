"""
test_watchlist_brief.py — pytest coverage for Layer 6 watchlist brief.

Tests:
- get_stage1_watchlist (mocked asyncpg pool)
- send_watchlist_brief message format (all vol_ratio / streak / ichimoku branches)
"""
from datetime import date, timedelta
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _make_entry(
    ticker: str = "005930.KS",
    days_since: Optional[int] = 4,
    vol_ratio: Optional[float] = 1.18,
    f_streak: Optional[int] = 3,
    i_streak: Optional[int] = 2,
    ichimoku_ok: Optional[bool] = True,
    current_stage: Optional[int] = 2,
):
    return {
        "ticker":        ticker,
        "days_since":    days_since,
        "vol_ratio":     vol_ratio,
        "f_streak":      f_streak,
        "i_streak":      i_streak,
        "ichimoku_ok":   ichimoku_ok,
        "current_stage": current_stage,
    }


def _make_pool(fake_fetch_coro):
    """Build a minimal asyncpg pool mock. fake_fetch_coro is an async callable."""
    conn = AsyncMock()
    conn.fetch = fake_fetch_coro
    pool = MagicMock()
    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)
    pool.acquire = MagicMock(return_value=cm)
    return pool


# ─── send_watchlist_brief — message format ────────────────────────────────────

async def _capture_message(entries) -> str:
    """Run send_watchlist_brief and return the text that would be sent."""
    sent = {}

    async def fake_post(http, token, chat_id, text, label="", parse_mode=None):
        sent["text"] = text
        return True

    import telegram.telegram_notify as tn
    with (
        patch.object(tn, "_get_token", return_value="tok"),
        patch.object(tn, "_get_chat_id", return_value="cid"),
        patch.object(tn, "_post_message", new=fake_post),
    ):
        await tn.send_watchlist_brief(entries)

    return sent.get("text", "")


@pytest.mark.asyncio
async def test_empty_watchlist():
    msg = await _capture_message([])
    assert "워치리스트 없음" in msg
    assert "장 마감" in msg


@pytest.mark.asyncio
async def test_vol_ratio_healthy():
    msg = await _capture_message([_make_entry(vol_ratio=1.18)])
    assert "✅" in msg
    assert "+18%" in msg


@pytest.mark.asyncio
async def test_vol_ratio_warning():
    msg = await _capture_message([_make_entry(vol_ratio=0.75)])
    assert "⚠️" in msg
    assert "-25%" in msg


@pytest.mark.asyncio
async def test_vol_ratio_dead():
    msg = await _capture_message([_make_entry(vol_ratio=0.35)])
    assert "❌" in msg
    assert "소멸" in msg
    assert "-65%" in msg


@pytest.mark.asyncio
async def test_vol_ratio_boundary_exact_1():
    """ratio == 1.0 → ✅ (boundary)"""
    msg = await _capture_message([_make_entry(vol_ratio=1.0)])
    assert "✅" in msg
    assert "+0%" in msg


@pytest.mark.asyncio
async def test_vol_ratio_boundary_exact_06():
    """ratio == 0.6 → ⚠️ (boundary)"""
    msg = await _capture_message([_make_entry(vol_ratio=0.6)])
    assert "⚠️" in msg


@pytest.mark.asyncio
async def test_vol_ratio_none():
    msg = await _capture_message([_make_entry(vol_ratio=None)])
    assert "N/A" in msg


@pytest.mark.asyncio
async def test_streak_positive():
    msg = await _capture_message([_make_entry(f_streak=3, i_streak=2)])
    assert "🔵 외국인 +3일" in msg
    assert "🔵 기관 +2일" in msg


@pytest.mark.asyncio
async def test_streak_negative():
    msg = await _capture_message([_make_entry(f_streak=-2, i_streak=-1)])
    assert "🔴 외국인 -2일" in msg
    assert "🔴 기관 -1일" in msg


@pytest.mark.asyncio
async def test_streak_none_shows_question_mark():
    msg = await _capture_message([_make_entry(f_streak=None, i_streak=None)])
    assert "❓ 외국인 N/A" in msg
    assert "❓ 기관 N/A" in msg


@pytest.mark.asyncio
async def test_streak_mixed_none_and_value():
    msg = await _capture_message([_make_entry(f_streak=2, i_streak=None)])
    assert "🔵 외국인 +2일" in msg
    assert "❓ 기관 N/A" in msg


@pytest.mark.asyncio
async def test_ichimoku_pass():
    msg = await _capture_message([_make_entry(ichimoku_ok=True)])
    assert "구름 상단 ✅" in msg


@pytest.mark.asyncio
async def test_ichimoku_fail():
    msg = await _capture_message([_make_entry(ichimoku_ok=False)])
    assert "통과 ❌" in msg


@pytest.mark.asyncio
async def test_ichimoku_none():
    msg = await _capture_message([_make_entry(ichimoku_ok=None)])
    assert "일목: N/A" in msg


@pytest.mark.asyncio
async def test_days_since_shown():
    msg = await _capture_message([_make_entry(days_since=9)])
    assert "D+9" in msg


@pytest.mark.asyncio
async def test_stage_shown():
    msg = await _capture_message([_make_entry(current_stage=2)])
    assert "Stage 2" in msg


@pytest.mark.asyncio
async def test_stage_none_shows_question():
    msg = await _capture_message([_make_entry(current_stage=None)])
    assert "Stage ?" in msg


@pytest.mark.asyncio
async def test_multiple_tickers():
    entries = [
        _make_entry("005930.KS", vol_ratio=1.05),
        _make_entry("000660.KS", vol_ratio=0.50, days_since=2),
    ]
    msg = await _capture_message(entries)
    assert "005930" in msg
    assert "000660" in msg


@pytest.mark.asyncio
async def test_no_markdownv2_escaping_in_plain_text():
    """Plain text mode — no MarkdownV2 escape backslashes in output."""
    entries = [_make_entry(vol_ratio=0.80, f_streak=-1)]
    msg = await _capture_message(entries)
    assert "\\+" not in msg
    assert "\\-" not in msg


# ─── get_stage1_watchlist ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_stage1_watchlist_returns_list():
    from core.db import get_stage1_watchlist

    fake_row = {
        "ticker": "005930.KS",
        "s1_date": date.today() - timedelta(days=3),
        "s1_volume": 18_000_000,
    }
    row_obj = MagicMock()
    row_obj.__iter__ = lambda s: iter(fake_row.items())
    row_obj.items = lambda: fake_row.items()
    row_obj.__getitem__ = lambda s, k: fake_row[k]

    async def fake_fetch(query, cutoff):
        return [row_obj]

    pool = _make_pool(fake_fetch)
    result = await get_stage1_watchlist(pool, days=14)
    assert isinstance(result, list)
    assert len(result) == 1


@pytest.mark.asyncio
async def test_get_stage1_watchlist_empty():
    from core.db import get_stage1_watchlist

    async def fake_fetch(query, cutoff):
        return []

    pool = _make_pool(fake_fetch)
    result = await get_stage1_watchlist(pool, days=14)
    assert result == []


@pytest.mark.asyncio
async def test_get_stage1_watchlist_db_error_returns_empty():
    from core.db import get_stage1_watchlist

    async def fake_fetch(query, cutoff):
        raise Exception("DB unavailable")

    pool = _make_pool(fake_fetch)
    result = await get_stage1_watchlist(pool, days=14)
    assert result == []
