"""
Regression tests for summarizer.py bugs found by /qa on 2026-04-06.
Report: .gstack/qa-reports/qa-report-korean-stock-alert-2026-04-06.md

ISSUE-001: _lmstudio_is_alive false positive when no model is loaded
ISSUE-002: Ollama think:false unreliable — /no_think prefix not added to prompt
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
import pytest
import httpx


# ── ISSUE-001 regressions ─────────────────────────────────────────────────────

class TestLmstudioIsAlive:
    """_lmstudio_is_alive must return False when inference fails (400)."""

    @pytest.mark.asyncio
    async def test_returns_false_when_inference_returns_400(self):
        """
        Regression: ISSUE-001 — LM Studio false positive health check
        Found by /qa on 2026-04-06
        Report: .gstack/qa-reports/qa-report-korean-stock-alert-2026-04-06.md
        """
        from summarizer import _lmstudio_is_alive

        models_response = MagicMock()
        models_response.status_code = 200
        models_response.json.return_value = {
            "data": [{"id": "eeve-korean-instruct-10.8b-v1.0", "object": "model"}]
        }

        probe_response = MagicMock()
        probe_response.status_code = 400

        http = AsyncMock(spec=httpx.AsyncClient)
        http.get.return_value = models_response
        http.post.return_value = probe_response

        result = await _lmstudio_is_alive(http)
        assert result is False, (
            "Should return False when /v1/models is 200 but inference probe returns 400"
        )

    @pytest.mark.asyncio
    async def test_returns_true_when_both_endpoints_succeed(self):
        from summarizer import _lmstudio_is_alive

        models_response = MagicMock()
        models_response.status_code = 200
        models_response.json.return_value = {
            "data": [{"id": "some-model", "object": "model"}]
        }

        probe_response = MagicMock()
        probe_response.status_code = 200

        http = AsyncMock(spec=httpx.AsyncClient)
        http.get.return_value = models_response
        http.post.return_value = probe_response

        result = await _lmstudio_is_alive(http)
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_models_endpoint_fails(self):
        from summarizer import _lmstudio_is_alive

        models_response = MagicMock()
        models_response.status_code = 503

        http = AsyncMock(spec=httpx.AsyncClient)
        http.get.return_value = models_response

        result = await _lmstudio_is_alive(http)
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_network_error(self):
        from summarizer import _lmstudio_is_alive

        http = AsyncMock(spec=httpx.AsyncClient)
        http.get.side_effect = httpx.ConnectError("connection refused")

        result = await _lmstudio_is_alive(http)
        assert result is False


# ── ISSUE-002 regressions ─────────────────────────────────────────────────────

class TestCallOllamaNativeNoThink:
    """_call_ollama_native must prepend /no_think when enable_thinking=False."""

    @pytest.mark.asyncio
    async def test_no_think_prefix_added_when_thinking_disabled(self):
        """
        Regression: ISSUE-002 — /no_think prefix missing from Ollama prompt
        Found by /qa on 2026-04-06
        Report: .gstack/qa-reports/qa-report-korean-stock-alert-2026-04-06.md
        """
        from summarizer import _call_ollama_native

        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"message": {"content": "hello"}}

        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = resp

        await _call_ollama_native(
            http, model="qwen3:8b", prompt="original prompt",
            enable_thinking=False,
        )

        call_kwargs = http.post.call_args
        body = call_kwargs[1]["json"] if "json" in call_kwargs[1] else call_kwargs.kwargs["json"]
        user_message = next(m for m in body["messages"] if m["role"] == "user")
        assert user_message["content"].startswith("/no_think"), (
            "Prompt must start with /no_think when enable_thinking=False"
        )
        assert "original prompt" in user_message["content"]

    @pytest.mark.asyncio
    async def test_no_think_prefix_not_added_when_thinking_enabled(self):
        from summarizer import _call_ollama_native

        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"message": {"content": "hello"}}

        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = resp

        await _call_ollama_native(
            http, model="qwen3:8b", prompt="original prompt",
            enable_thinking=True,
        )

        call_kwargs = http.post.call_args
        body = call_kwargs[1]["json"] if "json" in call_kwargs[1] else call_kwargs.kwargs["json"]
        user_message = next(m for m in body["messages"] if m["role"] == "user")
        assert not user_message["content"].startswith("/no_think"), (
            "Prompt must NOT have /no_think prefix when enable_thinking=True"
        )
        assert user_message["content"] == "original prompt"

    @pytest.mark.asyncio
    async def test_think_false_sent_to_api_when_disabled(self):
        from summarizer import _call_ollama_native

        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"message": {"content": "hello"}}

        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = resp

        await _call_ollama_native(
            http, model="qwen3:8b", prompt="test",
            enable_thinking=False,
        )

        call_kwargs = http.post.call_args
        body = call_kwargs[1]["json"] if "json" in call_kwargs[1] else call_kwargs.kwargs["json"]
        assert body["think"] is False
