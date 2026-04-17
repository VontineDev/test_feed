"""
Regression tests for signal_detector.py prompt construction.

ISSUE-006: SIGNAL_PROMPT ended with {macro_section} — when macro was present,
the last text the model saw was the Note: / Macro context: text, causing it to
echo that text instead of outputting JSON.
Fix: added "Output ONLY the JSON object..." instruction at the end of the prompt.
"""
from __future__ import annotations

import pytest
from signal_detector import SIGNAL_PROMPT, _build_macro_section
from market_data import MacroContext


class TestSignalPromptEndsWithOutputInstruction:
    """SIGNAL_PROMPT must end with an output instruction, not macro context text."""

    def test_prompt_ends_with_output_instruction_no_macro(self):
        """Without macro context, prompt still ends with JSON output instruction."""
        prompt = SIGNAL_PROMPT.format(
            title="Test",
            summary_ko="테스트 요약입니다.",
            macro_section="",
        )
        assert prompt.strip().endswith("No preamble, no explanation."), (
            "Prompt must end with the JSON output instruction so model knows what to output"
        )

    def test_prompt_ends_with_output_instruction_with_macro(self):
        """
        Regression: ISSUE-006 — when macro was present, prompt ended with the
        Note: text from _build_macro_section. Model echoed that text instead of JSON.
        Fix: output instruction added AFTER {macro_section} in the template.
        """
        macro = MacroContext(usd_krw=1380.0, korea_base_rate=3.5,
                             fetched_at="2026-04-11T00:00:00+09:00", is_fresh=True)
        macro_section = _build_macro_section(macro)
        assert macro_section, "macro_section should be non-empty for this test"

        prompt = SIGNAL_PROMPT.format(
            title="Test",
            summary_ko="테스트 요약입니다.",
            macro_section=macro_section,
        )
        # The prompt must NOT end with the macro note text
        assert not prompt.strip().endswith("high-debt companies."), (
            "Prompt must NOT end with macro note text — that caused model to echo it"
        )
        # The prompt MUST end with the JSON output instruction
        assert "Output ONLY the JSON object" in prompt, (
            "Prompt must contain the JSON output instruction"
        )
        # The output instruction must come AFTER the macro section
        output_instruction_pos = prompt.rfind("Output ONLY the JSON object")
        macro_section_pos = prompt.rfind(macro_section.strip())
        assert output_instruction_pos > macro_section_pos, (
            "Output instruction must appear AFTER the macro context section"
        )
        # The prompt must END with the output instruction (no text after it)
        trailing = prompt[output_instruction_pos:].strip()
        assert trailing.endswith("No preamble, no explanation."), (
            "Nothing should appear after the output instruction — text after it would "
            "cause the model to ignore the instruction"
        )

    def test_prompt_without_macro_contains_no_macro_text(self):
        """When no macro context, prompt has no Macro context: text."""
        prompt = SIGNAL_PROMPT.format(
            title="Test",
            summary_ko="테스트 요약입니다.",
            macro_section=_build_macro_section(None),
        )
        assert "Macro context" not in prompt
        assert "USD/KRW" not in prompt


class TestIsActionableThreshold:
    """is_actionable must require strength>=3 for WATCH to reduce notification noise."""

    def _make_signal(self, direction, strength):
        from signal_detector import TradeSignal, Backend
        return TradeSignal(
            direction=direction, strength=strength, reason="테스트",
            tickers=[], ticker_symbols={}, backend=Backend.OLLAMA,
            success=True, article_type="other",
        )

    # WATCH: threshold is 3, not 2
    def test_watch_strength2_not_actionable(self):
        assert not self._make_signal("WATCH", 2).is_actionable

    def test_watch_strength3_actionable(self):
        assert self._make_signal("WATCH", 3).is_actionable

    def test_watch_strength4_actionable(self):
        assert self._make_signal("WATCH", 4).is_actionable

    # BUY/SELL: threshold stays at 2
    def test_buy_strength2_actionable(self):
        assert self._make_signal("BUY", 2).is_actionable

    def test_sell_strength2_actionable(self):
        assert self._make_signal("SELL", 2).is_actionable

    def test_buy_strength1_not_actionable(self):
        assert not self._make_signal("BUY", 1).is_actionable

    # NONE: never actionable
    def test_none_not_actionable(self):
        assert not self._make_signal("NONE", 5).is_actionable
