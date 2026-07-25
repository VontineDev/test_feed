"""
test_news_gating.py  —  News gating eligibility filter (run_scheduler._gate_signal)
──────────────────────────────────────────────────────────────────
run_scheduler._gate_signal()이 실제 게이팅 판정(스크리너 Ichimoku 주봉 통과
OR 최근 7일 활성 Stage 1/2/3 종목만 신호 통과)을 담당하며, summary_worker()가
이 함수를 호출한다. 이 테스트는 그 실제 함수를 직접 호출해 검증한다.

이전 버전(Sprint 1)은 게이팅 로직을 테스트 함수 안에 복사해 넣고 그 사본만
검증했음 — run_scheduler.py가 단일 _screener_tickers 모듈 전역에서
state.screener_tickers/state.active_stage_tickers 이중 게이팅(Sprint 2,
v0.9.3.0)으로 바뀐 뒤에도 갱신되지 않아, 프로덕션 게이팅이 다시 깨져도 이
테스트는 계속 통과하는 상태였다(2026-07-25 /health pyright 수정 세션 중 발견,
TODOS.md 기록). run_scheduler.py에서 게이팅 판정을 _gate_signal()로 추출해
같은 세션에서 이 파일을 재작성했다.
"""
from __future__ import annotations

from run_scheduler import _gate_signal


class TestGateSignal:
    """_gate_signal(signal_syms, screener_tickers, active_stage_tickers)
    → (suppressed, in_screener, in_stage)."""

    def test_screener_match_not_suppressed(self):
        suppressed, in_screener, in_stage = _gate_signal(
            {"005930.KS"}, screener_tickers={"005930.KS"}, active_stage_tickers=set(),
        )
        assert not suppressed
        assert in_screener
        assert not in_stage

    def test_stage_match_not_suppressed(self):
        suppressed, in_screener, in_stage = _gate_signal(
            {"005930.KS"}, screener_tickers=set(), active_stage_tickers={"005930.KS"},
        )
        assert not suppressed
        assert not in_screener
        assert in_stage

    def test_neither_match_suppressed(self):
        suppressed, in_screener, in_stage = _gate_signal(
            {"005930.KS"}, screener_tickers={"373220.KS"}, active_stage_tickers={"066570.KS"},
        )
        assert suppressed
        assert not in_screener
        assert not in_stage

    def test_both_caches_empty_disables_gating(self):
        """게이팅 캐시 둘 다 미로드(스크리너·Stage 잡이 아직 안 돈 경우) → 항상 통과."""
        suppressed, _, _ = _gate_signal(
            {"999999.KS"}, screener_tickers=set(), active_stage_tickers=set(),
        )
        assert not suppressed

    def test_partial_overlap_not_suppressed(self):
        """여러 종목 중 하나만 스크리너/Stage에 있어도 통과."""
        suppressed, _, _ = _gate_signal(
            {"035720.KQ", "000660.KS"}, screener_tickers={"000660.KS"}, active_stage_tickers=set(),
        )
        assert not suppressed

    def test_both_gates_match_reports_both(self):
        """스크리너·Stage 둘 다 교차 — 호출부(summary_worker)가 in_screener를
        먼저 확인해 HIGH CONFIDENCE를 부여하므로 두 플래그 모두 True로 반환돼야 함."""
        _, in_screener, in_stage = _gate_signal(
            {"005930.KS"}, screener_tickers={"005930.KS"}, active_stage_tickers={"005930.KS"},
        )
        assert in_screener
        assert in_stage

    def test_empty_signal_syms_not_suppressed(self):
        """signal_syms 없는 신호(거시경제 기사 등 종목 미탐지) → 게이팅 대상 아님."""
        suppressed, _, _ = _gate_signal(
            set(), screener_tickers={"005930.KS"}, active_stage_tickers={"005930.KS"},
        )
        assert not suppressed
