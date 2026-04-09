"""
test_macro_signal.py  —  Macro Signal Enrichment (USD/KRW + Base Rate)
────────────────────────────────────────────────────────────────────────
15 test cases covering:
  Group 1: _fetch_usd_krw_sync          (3 tests)
  Group 2: get_macro_context async       (4 tests)
  Group 3: _build_macro_section          (4 tests)
  Group 4: detect_signal macro injection (2 tests)
  Group 5: save_signal macro floats      (2 tests)
  Group 6: _get_macro TTL cache          (3 tests) — see run_scheduler tests below
  Group 7: SIGNAL_PROMPT format integrity (1 test)

Total: 15 tests  (Groups 1-5 + 7 here; Group 6 in separate block)
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from market_data import MacroContext, _fetch_usd_krw_sync, get_macro_context
from signal_detector import SIGNAL_PROMPT, _build_macro_section


# ═══════════════════════════════════════════════════════════════
# Group 1: _fetch_usd_krw_sync
# ═══════════════════════════════════════════════════════════════

def _make_hist(close: float) -> pd.DataFrame:
    return pd.DataFrame({"Close": [close]})


def test_fetch_usd_krw_returns_float():
    """yfinance returns DataFrame with Close=[1470.5] → result == 1470.5"""
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = _make_hist(1470.5)
    with patch("market_data.yf") as mock_yf, patch("market_data.YFINANCE_OK", True):
        mock_yf.Ticker.return_value = mock_ticker
        result = _fetch_usd_krw_sync()
    assert result == 1470.5


def test_fetch_usd_krw_empty_hist_returns_none():
    """yfinance returns empty DataFrame → result is None"""
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = pd.DataFrame({"Close": []})
    with patch("market_data.yf") as mock_yf, patch("market_data.YFINANCE_OK", True):
        mock_yf.Ticker.return_value = mock_ticker
        result = _fetch_usd_krw_sync()
    assert result is None


def test_fetch_usd_krw_exception_returns_none():
    """yfinance raises Exception → result is None (no crash)"""
    mock_ticker = MagicMock()
    mock_ticker.history.side_effect = Exception("network error")
    with patch("market_data.yf") as mock_yf, patch("market_data.YFINANCE_OK", True):
        mock_yf.Ticker.return_value = mock_ticker
        result = _fetch_usd_krw_sync()
    assert result is None


# ═══════════════════════════════════════════════════════════════
# Group 2: get_macro_context (async)
# ═══════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_get_macro_context_fresh():
    """USD/KRW fetch succeeds → MacroContext(usd_krw=1470.5, is_fresh=True)"""
    with (
        patch("market_data._fetch_usd_krw_sync", return_value=1470.5),
        patch.dict("os.environ", {"KOREA_BASE_RATE": "2.5"}),
    ):
        ctx = await get_macro_context()
    assert ctx.usd_krw == 1470.5
    assert ctx.korea_base_rate == 2.5
    assert ctx.is_fresh is True
    assert ctx.fetched_at  # non-empty ISO timestamp


@pytest.mark.asyncio
async def test_get_macro_context_usd_krw_fails():
    """USD/KRW fetch returns None → is_fresh=False, base_rate still set"""
    with (
        patch("market_data._fetch_usd_krw_sync", return_value=None),
        patch.dict("os.environ", {"KOREA_BASE_RATE": "2.5"}),
    ):
        ctx = await get_macro_context()
    assert ctx.usd_krw is None
    assert ctx.is_fresh is False
    assert ctx.korea_base_rate == 2.5


@pytest.mark.asyncio
async def test_get_macro_context_default_base_rate():
    """No KOREA_BASE_RATE env var → defaults to 2.5"""
    env = {k: v for k, v in __import__("os").environ.items() if k != "KOREA_BASE_RATE"}
    with (
        patch("market_data._fetch_usd_krw_sync", return_value=1400.0),
        patch.dict("os.environ", env, clear=True),
    ):
        ctx = await get_macro_context()
    assert ctx.korea_base_rate == 2.5


@pytest.mark.asyncio
async def test_get_macro_context_bad_base_rate():
    """KOREA_BASE_RATE='not_a_number' → korea_base_rate is None (graceful)"""
    with (
        patch("market_data._fetch_usd_krw_sync", return_value=1400.0),
        patch.dict("os.environ", {"KOREA_BASE_RATE": "not_a_number"}),
    ):
        ctx = await get_macro_context()
    assert ctx.korea_base_rate is None


# ═══════════════════════════════════════════════════════════════
# Group 3: _build_macro_section
# ═══════════════════════════════════════════════════════════════

def test_build_macro_section_none():
    """_build_macro_section(None) → empty string"""
    assert _build_macro_section(None) == ""


def test_build_macro_section_base_rate_only():
    """usd_krw=None, base_rate=2.5 → contains base rate data line, no USD/KRW data line"""
    macro = MacroContext(usd_krw=None, korea_base_rate=2.5, fetched_at="ts", is_fresh=False)
    result = _build_macro_section(macro)
    assert "2.5%" in result
    assert "USD/KRW exchange rate:" not in result  # data line absent (note text is ok)


def test_build_macro_section_usd_krw_only():
    """usd_krw=1470.5, base_rate=None → contains USD/KRW data line, no base rate data line"""
    macro = MacroContext(usd_krw=1470.5, korea_base_rate=None, fetched_at="ts", is_fresh=True)
    result = _build_macro_section(macro)
    assert "1470.5" in result
    assert "Korea base rate:" not in result  # data line absent (note text is ok)


def test_build_macro_section_full():
    """Both present → both lines + explanatory note"""
    macro = MacroContext(usd_krw=1470.5, korea_base_rate=2.5, fetched_at="ts", is_fresh=True)
    result = _build_macro_section(macro)
    assert "1470.5" in result
    assert "2.5%" in result
    assert "Samsung" in result  # note is present


# ═══════════════════════════════════════════════════════════════
# Group 4: detect_signal macro injection (async)
# ═══════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_detect_signal_macro_none_no_injection():
    """macro=None → prompt does not contain 'Macro context'"""
    from signal_detector import detect_signal

    captured_prompt: list[str] = []

    async def _fake_call(http, model, prompt, **kw):
        captured_prompt.append(prompt)
        return '{"direction":"NONE","strength":0,"reason":"테스트","tickers":[]}'

    with patch("signal_detector._ollama_is_alive", return_value=True), \
         patch("signal_detector._call_ollama_native", side_effect=_fake_call):
        import httpx
        async with httpx.AsyncClient() as http:
            await detect_signal("test title", "테스트 요약", http=http, macro=None)

    assert captured_prompt, "LLM was never called"
    assert "Macro context" not in captured_prompt[0]


@pytest.mark.asyncio
async def test_detect_signal_macro_present_injected():
    """macro provided → 'USD/KRW' appears in prompt"""
    from signal_detector import detect_signal

    captured_prompt: list[str] = []

    async def _fake_call(http, model, prompt, **kw):
        captured_prompt.append(prompt)
        return '{"direction":"BUY","strength":3,"reason":"테스트","tickers":[]}'

    macro = MacroContext(usd_krw=1470.5, korea_base_rate=2.5, fetched_at="ts", is_fresh=True)

    with patch("signal_detector._ollama_is_alive", return_value=True), \
         patch("signal_detector._call_ollama_native", side_effect=_fake_call):
        import httpx
        async with httpx.AsyncClient() as http:
            await detect_signal("test title", "테스트 요약", http=http, macro=macro)

    assert captured_prompt, "LLM was never called"
    assert "USD/KRW" in captured_prompt[0]


# ═══════════════════════════════════════════════════════════════
# Group 5: save_signal macro floats (async)
# ═══════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_save_signal_with_macro_floats():
    """INSERT receives macro_usd_krw=1470.5, macro_base_rate=2.5"""
    from db import save_signal

    captured: list[tuple] = []

    mock_row = MagicMock()
    mock_row.__getitem__ = lambda self, k: 42 if k == "id" else None

    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(side_effect=lambda q, *args: (captured.append(args), mock_row)[1])

    mock_pool = MagicMock()
    mock_pool.acquire = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=mock_conn),
        __aexit__=AsyncMock(return_value=False),
    ))

    result = await save_signal(
        mock_pool,
        article_id=1,
        direction="BUY",
        strength=3,
        reason="테스트",
        tickers=["삼성전자"],
        llm_backend="ollama",
        macro_usd_krw=1470.5,
        macro_base_rate=2.5,
    )

    assert result == 42
    assert captured, "fetchrow was never called"
    args = captured[0]
    assert args[6] == 1470.5, f"macro_usd_krw not passed: {args}"
    assert args[7] == 2.5,    f"macro_base_rate not passed: {args}"


@pytest.mark.asyncio
async def test_save_signal_without_macro_floats():
    """No macro args → INSERT succeeds, macro positions are None"""
    from db import save_signal

    captured: list[tuple] = []

    mock_row = MagicMock()
    mock_row.__getitem__ = lambda self, k: 99 if k == "id" else None

    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(side_effect=lambda q, *args: (captured.append(args), mock_row)[1])

    mock_pool = MagicMock()
    mock_pool.acquire = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=mock_conn),
        __aexit__=AsyncMock(return_value=False),
    ))

    result = await save_signal(
        mock_pool,
        article_id=2,
        direction="SELL",
        strength=2,
        reason="테스트2",
        tickers=[],
        llm_backend="lm_studio",
    )

    assert result == 99
    assert captured, "fetchrow was never called"
    args = captured[0]
    assert args[6] is None  # macro_usd_krw default
    assert args[7] is None  # macro_base_rate default


# ═══════════════════════════════════════════════════════════════
# Group 6: _get_macro TTL cache (async, run_scheduler.py)
# ═══════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_get_macro_cold_cache():
    """First call → get_macro_context called once"""
    import run_scheduler

    fresh_macro = MacroContext(usd_krw=1470.5, korea_base_rate=2.5, fetched_at="ts", is_fresh=True)

    with patch("run_scheduler.get_macro_context", new_callable=AsyncMock, return_value=fresh_macro) as mock_get, \
         patch.object(run_scheduler, "_macro_cache", None), \
         patch.object(run_scheduler, "_macro_cache_ts", 0.0):
        result = await run_scheduler._get_macro()

    mock_get.assert_called_once()
    assert result is fresh_macro


@pytest.mark.asyncio
async def test_get_macro_warm_cache():
    """Second call immediately → get_macro_context NOT called again"""
    import time
    import run_scheduler

    warm_macro = MacroContext(usd_krw=1470.5, korea_base_rate=2.5, fetched_at="ts", is_fresh=True)

    with patch("run_scheduler.get_macro_context", new_callable=AsyncMock, return_value=warm_macro) as mock_get, \
         patch.object(run_scheduler, "_macro_cache", warm_macro), \
         patch.object(run_scheduler, "_macro_cache_ts", time.monotonic()):
        result = await run_scheduler._get_macro()

    mock_get.assert_not_called()
    assert result is warm_macro


@pytest.mark.asyncio
async def test_get_macro_stale_cache():
    """Call after TTL expires → get_macro_context called again"""
    import time
    import run_scheduler

    stale_macro = MacroContext(usd_krw=1400.0, korea_base_rate=2.5, fetched_at="ts", is_fresh=True)
    fresh_macro = MacroContext(usd_krw=1470.5, korea_base_rate=2.5, fetched_at="ts2", is_fresh=True)

    # Simulate stale cache: ts far in the past (TTL + 1 second ago)
    stale_ts = time.monotonic() - run_scheduler.MACRO_CACHE_TTL - 1.0

    with patch("run_scheduler.get_macro_context", new_callable=AsyncMock, return_value=fresh_macro) as mock_get, \
         patch.object(run_scheduler, "_macro_cache", stale_macro), \
         patch.object(run_scheduler, "_macro_cache_ts", stale_ts):
        result = await run_scheduler._get_macro()

    mock_get.assert_called_once()
    assert result is fresh_macro


# ═══════════════════════════════════════════════════════════════
# Group 7: SIGNAL_PROMPT format string integrity
# ═══════════════════════════════════════════════════════════════

def test_signal_prompt_format_no_error():
    """SIGNAL_PROMPT.format() with all placeholders must not raise KeyError.

    Any unescaped {brace} in the prompt would crash detect_signal() in production
    for every single article. This test catches that before it ships.
    """
    try:
        result = SIGNAL_PROMPT.format(
            title="테스트 제목",
            summary_ko="테스트 요약",
            macro_section="- USD/KRW: 1470.5\n",
        )
    except KeyError as e:
        pytest.fail(f"SIGNAL_PROMPT.format() raised KeyError: {e} — unescaped {{brace}} in prompt")
    assert "테스트 제목" in result
    assert "테스트 요약" in result
    assert "1470.5" in result
