"""core/tor.py — NEWNYM/지터 공용 헬퍼 테스트."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from core.tor import jittered_delay, new_identity, send_newnym


def _mock_controller():
    controller = MagicMock()
    controller.__enter__ = MagicMock(return_value=controller)
    controller.__exit__ = MagicMock(return_value=False)
    return controller


class TestJitteredDelay:
    def test_no_tor_proxy_returns_base(self, monkeypatch):
        monkeypatch.delenv("TOR_PROXY", raising=False)
        assert jittered_delay(0.5) == 0.5

    def test_tor_proxy_randomizes_with_floor(self, monkeypatch):
        monkeypatch.setenv("TOR_PROXY", "socks5h://127.0.0.1:9250")
        for _ in range(50):
            assert 1.0 <= jittered_delay(0.0) <= 2.5


class TestSendNewnym:
    def test_sends_authenticate_then_newnym(self):
        controller = _mock_controller()
        with patch("stem.control.Controller.from_port", return_value=controller) as mock_fp:
            send_newnym(9251)
        mock_fp.assert_called_once_with(port=9251)
        controller.authenticate.assert_called_once_with()
        controller.signal.assert_called_once()

    def test_propagates_failure(self):
        """new_identity와 달리 예외를 삼키지 않는다 — 호출자가 구분 처리."""
        controller = _mock_controller()
        controller.authenticate.side_effect = Exception("auth failed")
        with patch("stem.control.Controller.from_port", return_value=controller):
            with pytest.raises(Exception, match="auth failed"):
                send_newnym(9251)


class TestNewIdentity:
    def test_default_port_from_env(self, monkeypatch):
        monkeypatch.setenv("TOR_CONTROL_PORT", "9051")
        controller = _mock_controller()
        with patch("stem.control.Controller.from_port", return_value=controller) as mock_fp:
            assert new_identity() is True
        mock_fp.assert_called_once_with(port=9051)

    def test_explicit_port_overrides_env(self, monkeypatch):
        monkeypatch.setenv("TOR_CONTROL_PORT", "9051")
        controller = _mock_controller()
        with patch("stem.control.Controller.from_port", return_value=controller) as mock_fp:
            assert new_identity(9251) is True
        mock_fp.assert_called_once_with(port=9251)

    def test_never_raises_on_failure(self):
        import stem
        with patch("stem.control.Controller.from_port", side_effect=stem.SocketError("refused")):
            assert new_identity() is False
