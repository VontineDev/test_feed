"""
test_compose_parity.py — compose AND-gate ↔ legacy cross 패리티 회귀 (T4, CRITICAL)

JOIN+백필 경로(compose)가 검증된 replay 경로(cross 모드, val_sharpe 5.11)를
재현하는지 신호 집합 (ticker, ISO주) 겹침으로 측정한다. 두 경로가 충분히
겹치면 _replay_* 퇴역(plan-eng-review issue 2)을 정당화한다.

DB + 네트워크(yfinance) 의존 + 수 분 소요 → 기본 스킵.
실행:  RUN_PARITY_TEST=1 python -m pytest tests/test_compose_parity.py -s

비교 방법:
    cross   = run_backtest(mode="cross")  → _replay_ichimoku ∩ _replay_stage(S1)
    compose = and_gate(frame, ["ichimoku","stage1"])  # 백필 chart_signals ∩ stage==1
    동일 티커 집합·동일 기간에서 (ticker, ISO주) 집합 Jaccard.

주의: 완전 일치는 불가능(설계상):
    - cross는 일별 replay, compose는 주간(금요일) 스냅샷
    - _replay_stage 와 classify_stage 는 별개 구현(이 테스트가 측정 대상)
    - classify_stage는 주간 MAX(stage) 저장 → 그 주 Stage2면 stage1=False
  따라서 느슨한 하한만 단언하고 실제 수치는 출력해 사람이 판정.
"""

from __future__ import annotations

import os
from datetime import date

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("RUN_PARITY_TEST"),
    reason="DB+yfinance 통합 테스트 — RUN_PARITY_TEST=1 로 실행",
)

# 백필 chart_signals 와 stage_classifications 가 겹치는 구간
PARITY_START = date(2026, 4, 13)   # 2026-W16
PARITY_END   = date(2026, 6, 7)    # 2026-W23
MAX_TICKERS  = 400                 # cross replay 대상 (compose도 같은 집합으로 제한)


def _load_dotenv():
    from dotenv import load_dotenv
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(root, ".env"))


def test_compose_and_reproduces_cross():
    _load_dotenv()
    from core.db import get_dsn
    from analysis.backtest.models import BacktestConfig
    from analysis.backtest.engine import run_backtest
    from analysis.backtest.helpers import _week_label
    from analysis.chart_screener import get_all_tickers
    from analysis import strategy_compose as sc

    dsn = get_dsn()
    universe = [t for t, _, _ in get_all_tickers(None)][:MAX_TICKERS]
    uni_set = set(universe)

    # 1. cross 신호 집합
    cross_cfg = BacktestConfig(
        mode="cross", start=PARITY_START, end=PARITY_END,
        market="ALL", max_tickers=MAX_TICKERS, dsn=dsn,
    )
    cross_res = run_backtest(cross_cfg)
    cross_set = {
        (s.ticker, _week_label(s.signal_date))
        for s in cross_res.signals if s.ticker in uni_set
    }

    # 2. compose AND(ichimoku ∩ stage1) — 같은 티커 집합으로 제한
    frame = sc.load_signal_frame(dsn, PARITY_START, PARITY_END, ["ichimoku", "stage"])
    frame = sc.derive_flags(frame)
    comp = sc.and_gate(frame, ["ichimoku", "stage1"])
    comp_set = {
        (r.ticker, r.week) for r in comp.itertuples(index=False)
        if r.ticker in uni_set
    }

    inter = cross_set & comp_set
    union = cross_set | comp_set
    jaccard = len(inter) / len(union) if union else 0.0

    print("\n=== compose ↔ cross 패리티 ===")
    print(f"기간 {PARITY_START}~{PARITY_END}, 티커 {MAX_TICKERS}")
    print(f"cross   신호: {len(cross_set)}")
    print(f"compose 신호: {len(comp_set)}")
    print(f"교집합: {len(inter)}  합집합: {len(union)}  Jaccard: {jaccard:.3f}")
    only_cross = sorted(cross_set - comp_set)[:5]
    only_comp  = sorted(comp_set - cross_set)[:5]
    print(f"cross에만: {only_cross}")
    print(f"compose에만: {only_comp}")

    # 느슨한 하한 (완전 일치는 설계상 불가). 실제 판정은 출력 수치로.
    assert len(cross_set) > 0, "cross 신호 0 — 비교 불가"
    assert len(comp_set) > 0, "compose 신호 0 — 백필/AND 경로 점검"
    assert jaccard > 0.0, "교집합 0 — 두 경로가 전혀 겹치지 않음 (조사 필요)"


if __name__ == "__main__":
    os.environ.setdefault("RUN_PARITY_TEST", "1")
    test_compose_and_reproduces_cross()
    print("PASS")
