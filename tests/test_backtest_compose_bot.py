"""
test_backtest_compose_bot.py  —  /backtest compose 명령어 라우팅 단위 테스트
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
import pytest


# ── helpers ──────────────────────────────────────────────────

def _make_http() -> MagicMock:
    return MagicMock()


async def _call_handle_backtest(args: list[str]) -> list[str]:
    """_handle_backtest 호출 후 _send_plain으로 전송된 메시지 목록 반환"""
    from telegram.telegram_bot import _handle_backtest

    sent: list[str] = []

    async def _fake_send_plain(http, chat_id, text):
        sent.append(text)

    with patch("telegram.telegram_bot._send_plain", side_effect=_fake_send_plain):
        await _handle_backtest(_make_http(), "123", args)

    return sent


async def _call_handle_backtest_compose(args: list[str]) -> list[str]:
    """_handle_backtest_compose 호출 후 _send_plain 메시지 반환"""
    from telegram.telegram_bot import _handle_backtest_compose

    sent: list[str] = []

    async def _fake_send_plain(http, chat_id, text):
        sent.append(text)

    with patch("telegram.telegram_bot._send_plain", side_effect=_fake_send_plain):
        await _handle_backtest_compose(_make_http(), "123", args)

    return sent


# ── 라우팅 ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_backtest_compose_routes_to_compose_handler():
    """/backtest compose ... → _handle_backtest_compose 호출 확인"""
    with patch("telegram.telegram_bot._handle_backtest_compose", new_callable=AsyncMock) as mock_compose:
        from telegram.telegram_bot import _handle_backtest
        await _handle_backtest(_make_http(), "123", ["compose", "FUNNEL-1", "2025-01-01", "2026-06-14"])

    mock_compose.assert_called_once()
    _, _, passed_args = mock_compose.call_args[0]
    assert passed_args == ["FUNNEL-1", "2025-01-01", "2026-06-14"]


@pytest.mark.asyncio
async def test_backtest_non_compose_not_routed():
    """/backtest ichimoku → compose 핸들러 미호출"""
    with patch("telegram.telegram_bot._handle_backtest_compose", new_callable=AsyncMock) as mock_compose:
        with patch("telegram.telegram_bot._backtest_lock") as mock_lock:
            mock_lock.locked.return_value = False
            mock_lock.__aenter__ = AsyncMock(return_value=None)
            mock_lock.__aexit__ = AsyncMock(return_value=None)
            with patch("telegram.telegram_bot._send_plain", new_callable=AsyncMock):
                with patch("analysis.backtest_engine.BacktestConfig"), \
                     patch("analysis.backtest_engine.run_backtest", return_value=MagicMock(
                         to_telegram_report=lambda: "ok",
                         to_html_report=lambda p: None,
                         top_bottom_telegram_text=lambda n: "",
                     )), \
                     patch("core.db.get_dsn", return_value=None):
                    from telegram.telegram_bot import _handle_backtest
                    await _handle_backtest(_make_http(), "123",
                                           ["ichimoku", "2025-01-01", "2026-01-01"])

    mock_compose.assert_not_called()


# ── 인수 파싱 ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_compose_invalid_strategy_gives_error():
    msgs = await _call_handle_backtest_compose(["INVALID", "2025-01-01", "2026-06-14"])
    assert msgs
    assert any("strategy는" in m or "AND-1" in m for m in msgs)


@pytest.mark.asyncio
async def test_compose_too_few_args_gives_usage():
    msgs = await _call_handle_backtest_compose(["FUNNEL-1", "2025-01-01"])
    assert msgs
    assert any("사용법" in m for m in msgs)


@pytest.mark.asyncio
async def test_compose_bad_date_gives_error():
    msgs = await _call_handle_backtest_compose(["AND-1", "not-a-date", "2026-06-14"])
    assert msgs
    assert any("날짜 형식" in m for m in msgs)


@pytest.mark.asyncio
async def test_compose_start_after_end_gives_error():
    msgs = await _call_handle_backtest_compose(["AND-1", "2026-06-14", "2025-01-01"])
    assert msgs
    assert any("이전이어야" in m for m in msgs)


@pytest.mark.asyncio
async def test_compose_no_dsn_gives_error():
    with patch("core.db.get_dsn", return_value=None):
        msgs = await _call_handle_backtest_compose(["FUNNEL-1", "2025-01-01", "2026-06-14"])
    assert msgs
    assert any("DSN" in m or "DB" in m for m in msgs)


# ── 정상 실행 (run_backtest mock) ────────────────────────────

@pytest.mark.asyncio
async def test_compose_single_strategy_sends_report():
    from unittest.mock import MagicMock

    fake_result = MagicMock()
    fake_result.to_telegram_report.return_value = "FUNNEL-1 결과\n신호 100건"
    fake_result.to_html_report.return_value = None

    with patch("core.db.get_dsn", return_value="postgresql://test"), \
         patch("telegram.telegram_bot._backtest_lock") as mock_lock, \
         patch("analysis.backtest_engine.BacktestConfig"), \
         patch("analysis.backtest_engine.run_backtest", return_value=fake_result):

        mock_lock.locked.return_value = False
        mock_lock.__aenter__ = AsyncMock(return_value=None)
        mock_lock.__aexit__ = AsyncMock(return_value=None)

        msgs = await _call_handle_backtest_compose(["FUNNEL-1", "2025-01-01", "2026-06-14"])

    # 시작 메시지 + 결과 메시지
    assert len(msgs) >= 2
    combined = "\n".join(msgs)
    assert "FUNNEL-1" in combined


@pytest.mark.asyncio
async def test_compose_all_strategy_sends_table():
    from unittest.mock import MagicMock

    def _make_result(n_signals, wr, ret, sharpe, mdd):
        m = MagicMock()
        m.n = n_signals
        m.win_rate_28d = wr
        m.avg_return_28d = ret
        m.sharpe_28d = sharpe
        m.mdd = mdd
        r = MagicMock()
        r.overall = m
        r.to_html_report.return_value = None
        return r

    results_by_strategy = {
        "AND-1":    _make_result(8,    0.80, 0.219, 1.75, -0.036),
        "AND-2":    _make_result(3,    1.00, 0.865, None, 0.0),
        "SCORE-1":  _make_result(1500, 0.55, 0.042, 0.62, -1.0),
        "FUNNEL-1": _make_result(2009, 0.67, 0.108, 0.74, -1.0),
    }

    call_count = [0]
    strategies = list(results_by_strategy.keys())

    def _fake_run(cfg):
        s = strategies[call_count[0] % len(strategies)]
        call_count[0] += 1
        return results_by_strategy[s]

    with patch("core.db.get_dsn", return_value="postgresql://test"), \
         patch("telegram.telegram_bot._backtest_lock") as mock_lock, \
         patch("analysis.backtest_engine.BacktestConfig"), \
         patch("analysis.backtest_engine.run_backtest", side_effect=_fake_run), \
         patch("analysis.strategy_compose.STRATEGIES", results_by_strategy):

        mock_lock.locked.return_value = False
        mock_lock.__aenter__ = AsyncMock(return_value=None)
        mock_lock.__aexit__ = AsyncMock(return_value=None)

        msgs = await _call_handle_backtest_compose(["ALL", "2025-01-01", "2026-06-14"])

    combined = "\n".join(msgs)
    assert "Tier-1" in combined or "비교" in combined
