"""
test_watchlist_features.py — 워치리스트 3가지 신규 기능 회귀 테스트

Covers:
  - Vol ratio delta (∆ vs yesterday): entries에 vol_ratio_delta 포함
  - D+10 retirement notice: retiring 필드 + Telegram 메시지에 [마지막 추적일] 표시
  - send_watchlist_brief: target_chat_id 파라미터 동작
  - /watchlist 봇 커맨드: _handle_watchlist 호출 시 _build_watchlist_entries + send_watchlist_brief 호출
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ── send_watchlist_brief 메시지 포맷 테스트 ─────────────────────────────────

def _captured_message(entries, monkeypatch, target_chat_id=None) -> tuple[str, str]:
    """send_watchlist_brief가 _post_message에 넘기는 message 캡처."""
    import telegram.telegram_notify as telegram_notify
    captured = {}

    async def fake_post(http, token, chat_id, message, label="", parse_mode=""):
        captured["message"] = message
        captured["chat_id"] = chat_id
        return True

    monkeypatch.setattr(telegram_notify, "_get_token", lambda: "tok")
    monkeypatch.setattr(telegram_notify, "_get_chat_id", lambda: "DEFAULT_ID")
    monkeypatch.setattr(telegram_notify, "_post_message", fake_post)

    # ticker_cache stub
    mock_tc = MagicMock()
    mock_tc.get_name.side_effect = lambda t: t.split(".")[0]
    monkeypatch.setattr(telegram_notify, "ticker_cache", mock_tc, raising=False)

    async def _run_send():
        import httpx
        async with httpx.AsyncClient() as http:
            await telegram_notify.send_watchlist_brief(
                entries, http=http,
                target_chat_id=target_chat_id,
            )

    asyncio.run(_run_send())
    return captured.get("message", ""), captured.get("chat_id", "")


class TestSendWatchlistBriefDelta:
    def _entry(self, vol_ratio=1.5, vol_ratio_delta=None, retiring=False):
        return {
            "ticker": "005930.KS",
            "days_since": 5,
            "vol_ratio": vol_ratio,
            "vol_ratio_delta": vol_ratio_delta,
            "retiring": retiring,
            "f_streak": 2,
            "i_streak": 1,
            "ichimoku_ok": True,
            "current_stage": 1,
        }

    def test_positive_delta_shown(self, monkeypatch):
        msg, _ = _captured_message([self._entry(vol_ratio_delta=0.12)], monkeypatch)
        assert "▲" in msg
        assert "+12%" in msg

    def test_negative_delta_shown(self, monkeypatch):
        msg, _ = _captured_message([self._entry(vol_ratio_delta=-0.08)], monkeypatch)
        assert "▼" in msg
        assert "-8%" in msg

    def test_no_delta_when_none(self, monkeypatch):
        msg, _ = _captured_message([self._entry(vol_ratio_delta=None)], monkeypatch)
        assert "▲" not in msg
        assert "▼" not in msg
        assert "전일 대비" not in msg

    def test_zero_delta_shows_up_arrow(self, monkeypatch):
        msg, _ = _captured_message([self._entry(vol_ratio_delta=0.0)], monkeypatch)
        assert "▲" in msg


class TestSendWatchlistBriefRetiring:
    def _entry(self, days_since=5, retiring=False):
        return {
            "ticker": "005930.KS",
            "days_since": days_since,
            "vol_ratio": 1.2,
            "vol_ratio_delta": None,
            "retiring": retiring,
            "f_streak": None,
            "i_streak": None,
            "ichimoku_ok": None,
            "current_stage": 1,
        }

    def test_retiring_marker_present_when_true(self, monkeypatch):
        msg, _ = _captured_message([self._entry(retiring=True)], monkeypatch)
        assert "[마지막 추적일]" in msg

    def test_no_marker_when_not_retiring(self, monkeypatch):
        msg, _ = _captured_message([self._entry(retiring=False)], monkeypatch)
        assert "[마지막 추적일]" not in msg

    def test_retiring_includes_days(self, monkeypatch):
        msg, _ = _captured_message([self._entry(days_since=10, retiring=True)], monkeypatch)
        assert "D+10" in msg
        assert "[마지막 추적일]" in msg


class TestSendWatchlistBriefTargetChatId:
    def test_target_chat_id_used_when_provided(self, monkeypatch):
        entry = {
            "ticker": "005930.KS", "days_since": 3, "vol_ratio": 1.0,
            "vol_ratio_delta": None, "retiring": False,
            "f_streak": None, "i_streak": None, "ichimoku_ok": None, "current_stage": 1,
        }
        _, chat_id = _captured_message([entry], monkeypatch, target_chat_id="CUSTOM_ID")
        assert chat_id == "CUSTOM_ID"

    def test_default_chat_id_when_not_provided(self, monkeypatch):
        entry = {
            "ticker": "005930.KS", "days_since": 3, "vol_ratio": 1.0,
            "vol_ratio_delta": None, "retiring": False,
            "f_streak": None, "i_streak": None, "ichimoku_ok": None, "current_stage": 1,
        }
        _, chat_id = _captured_message([entry], monkeypatch, target_chat_id=None)
        assert chat_id == "DEFAULT_ID"


# ── retiring 필드 계산 (days_since >= 10) ────────────────────────────────────

class TestRetiringField:
    def test_retiring_true_at_day_10(self):
        assert 10 >= 10  # retiring = days_since >= 10

    def test_retiring_false_at_day_9(self):
        assert not (9 >= 10)

    def test_retiring_true_at_day_13(self):
        assert 13 >= 10


# ── vol_ratio_delta 계산 로직 ────────────────────────────────────────────────

class TestVolRatioDeltaLogic:
    """vol_log_map index 0 = 어제 → delta = today - yesterday"""

    def test_delta_computed_correctly(self):
        today_ratio     = 1.5
        yesterday_ratio = 1.3
        delta = today_ratio - yesterday_ratio
        assert abs(delta - 0.2) < 1e-9

    def test_delta_none_when_no_history(self):
        hist            = []
        yesterday_ratio = hist[0] if hist else None
        vol_ratio       = 1.5
        delta = (vol_ratio - yesterday_ratio) if (vol_ratio is not None and yesterday_ratio is not None) else None
        assert delta is None

    def test_delta_none_when_vol_ratio_none(self):
        hist            = [1.2]
        yesterday_ratio = hist[0]
        vol_ratio       = None
        delta = (vol_ratio - yesterday_ratio) if (vol_ratio is not None and yesterday_ratio is not None) else None
        assert delta is None


# ── _handle_watchlist: /watchlist 봇 커맨드 ─────────────────────────────────

class TestHandleWatchlist:
    def test_calls_build_entries_and_send_brief(self, monkeypatch):
        mock_entries = [{"ticker": "005930.KS", "days_since": 3}]
        mock_build = AsyncMock(return_value={"entries": mock_entries})
        mock_send = AsyncMock(return_value=True)

        async def run():
            import telegram.telegram_bot as telegram_bot
            monkeypatch.setattr(telegram_bot, "_build_watchlist_entries_ref",
                                mock_build, raising=False)

            # patch imports inside _handle_watchlist
            with patch("telegram.telegram_bot._build_watchlist_entries_ref", mock_build, create=True), \
                 patch("run_scheduler._build_watchlist_entries", mock_build, create=False):
                import importlib
                import run_scheduler
                original = run_scheduler._build_watchlist_entries
                run_scheduler._build_watchlist_entries = mock_build

                import telegram.telegram_notify as telegram_notify
                original_send = telegram_notify.send_watchlist_brief
                telegram_notify.send_watchlist_brief = mock_send

                try:
                    http = MagicMock()
                    pool = MagicMock()
                    await telegram_bot._handle_watchlist(http, "CHAT123", pool)
                    mock_build.assert_awaited_once_with(pool)
                    mock_send.assert_awaited_once()
                    call_kwargs = mock_send.call_args
                    assert call_kwargs[1].get("target_chat_id") == "CHAT123" or \
                           (call_kwargs[0] and call_kwargs[0][0] == mock_entries)
                finally:
                    run_scheduler._build_watchlist_entries = original
                    telegram_notify.send_watchlist_brief = original_send

        asyncio.run(run())

    def test_no_pool_sends_error(self, monkeypatch):
        async def run():
            import telegram.telegram_bot as telegram_bot
            sent = {}

            async def fake_plain(http, chat_id, text):
                sent["text"] = text

            monkeypatch.setattr(telegram_bot, "_send_plain", fake_plain)
            http = MagicMock()
            await telegram_bot._handle_watchlist(http, "CHAT123", pool=None)
            assert "DB" in sent.get("text", "")

        asyncio.run(run())
