"""backtest_engine — 하위호환 re-export 심 (Phase C).

구 3,360줄 단일 모듈은 analysis/backtest/ 패키지로 분해됨:
  config.py      상수·튜닝 파라미터
  models.py      BacktestConfig/SignalRecord/GroupMetrics/BacktestResult
  report.py      리포트 빌더 4종 (텔레그램/텍스트/Top-Bottom/HTML)
  helpers.py     지표·가격 조회·그룹 메트릭·_fill_returns
  fetch.py       yfinance OHLCV/지수 수집
  replay.py      전략 세대별 신호 재현 11종 + cross 필터
  exit_models.py 청산 모델 3종 + 디스패처
  engine.py      run_backtest / _run_compose

기존 import 경로(from analysis.backtest_engine import ...)는 그대로 동작.
진입점은 텔레그램 /backtest 명령(telegram/telegram_bot.py) — 별도 CLI 없음.
"""

from __future__ import annotations

from analysis.backtest.config import (  # noqa: F401
    _TX_BUY,
    _TX_SELL,
    TX_COST_DEFAULT,
    _S1_THRESHOLD,
    _STOP_LOSS_PCT,
    MODE_KOR,
    OPTIMAL_EXIT_PARAMS,
    OPTIMAL_EXIT_PARAMS_KOSDAQ,
    OPTIMAL_EXIT_PARAMS_CROSS,
    OPTIMAL_EXIT_PARAMS_ICHIMOKU,
)
from analysis.backtest.models import (  # noqa: F401
    BacktestConfig,
    SignalRecord,
    GroupMetrics,
    BacktestResult,
)
from analysis.backtest.helpers import (  # noqa: F401
    _week_label,
    _build_price_lookup,
    _nearest_price,
    _entry_on_or_after,
    _compute_sharpe,
    _compute_mdd,
    _build_weekly_ichimoku,
    _find_ichimoku_sell,
    _compute_rsi,
    _compute_group_metrics,
    _fill_returns,
)
from analysis.backtest.fetch import (  # noqa: F401
    _fetch_single_ohlcv,
    _fetch_index,
    _batch_fetch_ohlcv,
)
from analysis.backtest.replay import (  # noqa: F401
    _replay_ichimoku,
    _replay_stage,
    _replay_stage_v11,
    _replay_stage2_v11,
    _replay_stage_v12,
    _replay_stage2_v12,
    _replay_stage_v13,
    _replay_stage_v14,
    _replay_stage_v15,
    _replay_stage2_v13,
    _apply_cross_filter,
    _replay_stage2,
)
from analysis.backtest.exit_models import (  # noqa: F401
    _compute_exit_logic,
    _compute_atr,
    _compute_exit_logic_model_a,
    _compute_exit_logic_model_b,
    _compute_sell_signals_and_s2,
)
from analysis.backtest.engine import (  # noqa: F401
    run_backtest,
    _run_compose,
)
