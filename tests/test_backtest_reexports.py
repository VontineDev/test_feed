"""backtest_engine re-export 완전성 테스트 (Phase C).

analysis/backtest_engine.py가 순수 심이 된 뒤에도 기존 소비자
(테스트/paper_jobs/텔레그램봇/scripts)가 쓰는 모든 public/private 이름이
기존 경로에서 계속 resolve되는지 고정한다.
"""

from __future__ import annotations

import pytest

# 실제 소비자별 import 이름 (grep으로 수집):
# tests/test_backtest_engine.py, test_exit_model.py, test_replay_stage2.py,
# test_compose_parity.py, test_compose_seam.py, jobs/paper_jobs.py,
# telegram/telegram_bot.py, scripts/run_sweep.py, scripts/compare_*.py
_CONSUMED_NAMES = [
    # dataclasses
    "BacktestConfig", "BacktestResult", "GroupMetrics", "SignalRecord",
    # config
    "TX_COST_DEFAULT", "MODE_KOR", "_STOP_LOSS_PCT",
    "OPTIMAL_EXIT_PARAMS", "OPTIMAL_EXIT_PARAMS_KOSDAQ",
    "OPTIMAL_EXIT_PARAMS_CROSS", "OPTIMAL_EXIT_PARAMS_ICHIMOKU",
    # helpers
    "_week_label", "_build_price_lookup", "_nearest_price",
    "_entry_on_or_after", "_compute_sharpe", "_compute_mdd",
    "_build_weekly_ichimoku", "_find_ichimoku_sell", "_compute_rsi",
    "_compute_group_metrics", "_fill_returns",
    # fetch
    "_fetch_single_ohlcv", "_fetch_index", "_batch_fetch_ohlcv",
    # replay
    "_replay_ichimoku", "_replay_stage", "_replay_stage2",
    "_apply_cross_filter",
    # exit models
    "_compute_exit_logic", "_compute_sell_signals_and_s2",
    # engine
    "run_backtest",
]


@pytest.mark.parametrize("name", _CONSUMED_NAMES)
def test_name_resolves_from_backtest_engine(name):
    import analysis.backtest_engine as be
    assert hasattr(be, name), f"analysis.backtest_engine.{name} 이 사라짐 — 심 갱신 필요"


def test_result_report_methods_still_bound():
    """BacktestResult 인스턴스 메서드 인터페이스 유지 (report.py 위임)."""
    from analysis.backtest_engine import BacktestResult
    for meth in ("to_telegram_report", "to_text_report",
                 "top_bottom_telegram_text", "to_html_report"):
        assert callable(getattr(BacktestResult, meth))
