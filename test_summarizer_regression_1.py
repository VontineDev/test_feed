"""
Regression tests for summarizer.py bugs found by /qa on 2026-04-06 and /investigate on 2026-04-11.
Report: .gstack/qa-reports/qa-report-korean-stock-alert-2026-04-06.md

ISSUE-001: _lmstudio_is_alive false positive when no model is loaded
ISSUE-002: Ollama think:false unreliable — /no_think prefix not added to prompt
ISSUE-003: repeat_penalty:1.3 caused empty JSON responses from Ollama (penalised { " : )
ISSUE-004: max_tokens:300 too small — thinking block filled budget, summary was empty
ISSUE-005: Empty response when Ollama returns thinking in message.thinking (no content)
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


# ── ISSUE-003 regressions ─────────────────────────────────────────────────────

class TestCallOllamaNativeRepeatPenalty:
    """repeat_penalty must be 1.0 (disabled) so JSON characters are not penalised."""

    @pytest.mark.asyncio
    async def test_repeat_penalty_is_one(self):
        """
        Regression: ISSUE-003 — repeat_penalty:1.3 penalised { " : which appear
        many times in the SIGNAL_PROMPT JSON template, causing empty Ollama responses.
        Fix: repeat_penalty set to 1.0 (disabled).
        """
        from summarizer import _call_ollama_native

        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"message": {"content": "ok"}}

        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = resp

        await _call_ollama_native(http, model="qwen3:8b", prompt="test")

        body = http.post.call_args.kwargs["json"]
        assert body["options"]["repeat_penalty"] == 1.0, (
            "repeat_penalty must be 1.0 — values > 1.0 suppress { \" : tokens needed for JSON"
        )


# ── ISSUE-004 regressions ─────────────────────────────────────────────────────

class TestSummarizeOllamaMaxTokens:
    """_summarize_ollama must use max_tokens=600 to survive thinking blocks."""

    @pytest.mark.asyncio
    async def test_summarizer_uses_600_max_tokens(self):
        """
        Regression: ISSUE-004 — max_tokens=300 was exhausted by ~290-token <think>
        block, leaving no room for the actual Korean summary. Raised to 600.
        """
        from summarizer import _summarize_ollama

        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"message": {"content": "연준이 금리를 동결했습니다."}}

        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = resp

        await _summarize_ollama(http, title="Fed holds rates", body="The Fed held rates.")

        body = http.post.call_args.kwargs["json"]
        assert body["options"]["num_predict"] == 600, (
            "max_tokens must be 600 — 300 was too small when model generates a think block"
        )


# ── ISSUE-005 regressions ─────────────────────────────────────────────────────

class TestCallOllamaNativeThinkingContent:
    """When Ollama separates thinking from content, empty content raises a clear error."""

    @pytest.mark.asyncio
    async def test_empty_content_with_thinking_raises_descriptive_error(self):
        """
        Regression: ISSUE-005 — newer Ollama versions return thinking in
        message.thinking (not message.content). When all tokens go to thinking,
        message.content is empty. The error message should include thinking length
        so the operator knows to increase max_tokens.
        """
        from summarizer import _call_ollama_native

        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {
            "message": {
                "content": "",
                "thinking": "<think>" + "x" * 800 + "</think>",
            }
        }

        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = resp

        with pytest.raises(ValueError, match="think="):
            await _call_ollama_native(http, model="qwen3:8b", prompt="test")

    @pytest.mark.asyncio
    async def test_empty_content_without_thinking_raises_generic_error(self):
        """Empty content with no thinking raises the legacy generic error."""
        from summarizer import _call_ollama_native

        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"message": {"content": ""}}

        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = resp

        with pytest.raises(ValueError, match="Ollama 네이티브 빈 응답 반환"):
            await _call_ollama_native(http, model="qwen3:8b", prompt="test")

    @pytest.mark.asyncio
    async def test_ollama_http_200_with_error_field_raises(self):
        """
        Some Ollama versions return HTTP 200 with {"error": "model not found"}.
        Without this check the caller retries twice against a missing model,
        then falls to LM Studio — all with a misleading 'empty response' log.
        """
        from summarizer import _call_ollama_native

        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"error": "model 'qwen3:8b' not found, try pulling it first"}

        http = AsyncMock(spec=httpx.AsyncClient)
        http.post.return_value = resp

        with pytest.raises(ValueError, match="Ollama 오류 응답"):
            await _call_ollama_native(http, model="qwen3:8b", prompt="test")
