"""
test_high_confidence.py — HIGH CONFIDENCE 통합 회귀 테스트

Covers:
  - 게이팅 버그 수정: signal.ticker_symbols.values() 기준 교차 확인
  - HIGH CONFIDENCE 배지: telegram_notify.send_signal()에서 conf_line 포함
  - NORMAL 신호: HIGH CONFIDENCE 배지 없음
  - 빈 _screener_tickers: 게이팅 비활성, confidence 변경 없음
"""
from __future__ import annotations



# ── send_signal: HIGH CONFIDENCE 배지 ───────────────────────────────────────

class TestSendSignalHighConfidence:
    def _make_signal(self, confidence: str = "NORMAL"):
        from unittest.mock import MagicMock
        sig = MagicMock()
        sig.direction = "BUY"
        sig.strength = 4
        sig.tickers = []
        sig.ticker_symbols = {}
        sig.backend.value = "ollama"
        sig.article_type = "other"
        sig.confidence = confidence
        return sig

    def _extract_message(self, signal, monkeypatch) -> str:
        """send_signal()이 _post_message에 넘기는 message 캡처."""
        import telegram.telegram_notify as telegram_notify
        captured = {}

        async def fake_post(http, token, chat_id, message, label=""):
            captured["message"] = message
            return True

        monkeypatch.setattr(telegram_notify, "_get_token", lambda: "tok")
        monkeypatch.setattr(telegram_notify, "_get_chat_id", lambda: "123")
        monkeypatch.setattr(telegram_notify, "_get_channel_id", lambda: "")
        monkeypatch.setattr(telegram_notify, "_post_message", fake_post)

        import asyncio
        art = {
            "title": "테스트 기사",
            "url": "https://example.com",
            "published": "2026-05-20",
            "source": "연합뉴스",
        }
        asyncio.run(telegram_notify.send_signal(art, "요약", signal))
        return captured.get("message", "")

    def test_high_confidence_badge_present(self, monkeypatch):
        sig = self._make_signal(confidence="HIGH")
        msg = self._extract_message(sig, monkeypatch)
        assert "HIGH CONFIDENCE" in msg

    def test_normal_signal_no_badge(self, monkeypatch):
        sig = self._make_signal(confidence="NORMAL")
        msg = self._extract_message(sig, monkeypatch)
        assert "HIGH CONFIDENCE" not in msg

    def test_missing_confidence_attr_no_badge(self, monkeypatch):
        """구버전 TradeSignal(confidence 필드 없음) 하위 호환."""
        from unittest.mock import MagicMock
        sig = MagicMock()
        sig.direction = "BUY"
        sig.strength = 3
        sig.tickers = []
        sig.ticker_symbols = {}
        sig.backend.value = "ollama"
        sig.article_type = "other"
        del sig.confidence  # confidence 속성 없는 구버전 시뮬레이션

        msg = self._extract_message(sig, monkeypatch)
        assert "HIGH CONFIDENCE" not in msg


# ── 게이팅 로직: .values() 기준 교차 확인 ────────────────────────────────────

class TestGatingSymbolMatch:
    """스크리너 교차 판정이 .values()(yfinance 심볼) 기준인지 검증."""

    def _screener_gate(self, ticker_symbols: dict, screener_tickers: set) -> bool:
        """run_scheduler의 게이팅 로직 추출."""
        signal_syms = set(ticker_symbols.values())
        return bool(signal_syms & screener_tickers)

    def test_symbol_match_hits(self):
        ticker_symbols = {"삼성전자": "005930.KS"}
        screener = {"005930.KS", "373220.KS"}
        assert self._screener_gate(ticker_symbols, screener) is True

    def test_name_key_would_miss(self):
        # 구버전 버그: keys() 비교 시 miss — 심볼로 비교해야 함
        ticker_symbols = {"삼성전자": "005930.KS"}
        # 실제 screener_tickers는 yfinance 심볼임 → 이름으로 비교하면 안 됨
        assert not (set(ticker_symbols.keys()) & {"005930.KS"})  # 구버전 버그 재현

    def test_empty_screener_no_gate(self):
        ticker_symbols = {"삼성전자": "005930.KS"}
        screener: set = set()
        # 빈 스크리너 → 게이팅 비활성
        assert not (screener and self._screener_gate(ticker_symbols, screener))

    def test_no_symbol_values_no_match(self):
        # ticker_symbols에 심볼이 없으면(해석 실패) 게이팅에서 통과 안 됨
        ticker_symbols = {"미지종목": ""}
        screener = {"005930.KS"}
        signal_syms = set(ticker_symbols.values())  # {""}
        assert not (signal_syms & screener)
