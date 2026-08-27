"""모멘텀/QVM 유니버스(SCENARIO2 RSI30 반등 진입) × quant 자기완결청산 변형 교차조합.

**새 로직 없음** — run_cross_combo_backtest.py의 기존 진입 생성기
(`_generate_quant_entries`)와 청산 적용기(`_apply_quant_exit`)를 그대로
재사용, 지금까지 시도되지 않은 조합만 골라 돌린다.

기존에 이미 검증된 것 (재실행 안 함):
  - MOMENTUM_TOP20 / QVM_TOP20 × quant_original (RSI70/-7%) — run_quant_qvm_*.py
  - MOMENTUM_TOP20 / QVM_TOP20 × compose 분할청산(cross/score1/funnel1) — run_cross_combo_backtest.py

아직 안 해본 것 (이 스크립트가 채움):
  - MOMENTUM_TOP20 / QVM_TOP20 × quant_optimized (RSI80/-12%, SCENARIO2 5단계 최적화청산)
  - MOMENTUM_TOP20 / QVM_TOP20 × quant_scenario1 (+20%/-5%/MA20, 1안 원안청산)

가설: quant_optimized 청산은 SCENARIO2의 "RSI30 반등" 진입에 맞춰
최적화된 것이라, FUNNEL-1/SCORE-1처럼 완전히 다른 계열 진입(이치모쿠/
Stage z-score)에 이식하면 승률이 폭락했다(TechnicalQuant.md 확인 완료).
반면 MOMENTUM_TOP20/QVM_TOP20 진입도 내부적으로 동일한 SCENARIO2 RSI30
반등 타이밍을 재사용하고 유니버스만 다르므로, quant_optimized 청산이
이식돼도 성능이 유지/개선될 가능성이 있다 — 검증 목적.

사용법:
    python scripts/run_momentum_qvm_quantexit_combo.py --start 2025-01-02 --end 2026-08-06
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

from dotenv import load_dotenv

load_dotenv(os.path.join(Path(__file__).parent.parent, ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

from analysis.backtest.config import QUANT_EXIT_VARIANTS  # noqa: E402
from analysis.backtest.helpers import (  # noqa: E402
    _compute_group_metrics,
    _compute_signal_interval_mdd,
)
from run_cross_combo_backtest import (  # noqa: E402
    _apply_quant_exit,
    _generate_quant_entries,
)

ENTRY_UNIVERSES = ["MOMENTUM_TOP20", "QVM_TOP20"]
EXIT_VARIANTS = ["quant_optimized", "quant_scenario1"]


def _pct(v, dp: int = 1) -> str:
    return f"{v * 100:+.{dp}f}%" if v is not None else "N/A"


def main() -> None:
    parser = argparse.ArgumentParser(description="모멘텀/QVM 진입 × quant 자기완결청산 교차조합")
    parser.add_argument("--start", default="2025-01-02")
    parser.add_argument("--end", default="2026-08-06")
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        u, p = os.environ.get("DB_USER", ""), os.environ.get("DB_PASSWORD", "")
        h = os.environ.get("DB_HOST", "localhost")
        port = os.environ.get("DB_PORT", "5432")
        db = os.environ.get("DB_NAME", "news_db")
        if u and p:
            from urllib.parse import quote
            dsn = f"postgresql://{u}:{quote(p)}@{h}:{port}/{db}"
    if not dsn:
        sys.exit("DATABASE_URL (또는 DB_USER/DB_PASSWORD) 환경변수가 필요합니다")

    rows = []
    t0 = time.time()

    for uni in ENTRY_UNIVERSES:
        # 진입은 유니버스당 1번만 생성(quant_exit 변형 2개가 같은 신호 리스트를 공유) —
        # _generate_quant_entries 자체가 이미 무거운 fetch(전종목 OHLCV)라 재사용.
        # ohlcv_map은 compose 분할청산 경로 전용 — _apply_quant_exit는 자체적으로
        # 다시 fetch하므로 여기선 버린다(entry 생성 함수의 반환 시그니처를 그대로 둠).
        signals_base, _ohlcv_map = _generate_quant_entries(uni, dsn, start, end, args.workers)
        if not signals_base:
            logger.warning("[%s] 신호 없음 — 스킵", uni)
            continue
        logger.info("[%s] 진입신호 %d건 생성", uni, len(signals_base))

        for exit_key in EXIT_VARIANTS:
            import copy
            signals = copy.deepcopy(signals_base)
            for s in signals:
                s.sell_date = None
                s.sell_reason = ""
                s.sell_return = None
                s.hold_days = None
                s.blended_return = None

            _apply_quant_exit(signals, dsn, start, end, args.workers, QUANT_EXIT_VARIANTS[exit_key])

            m = _compute_group_metrics(signals, rf_annual=0.03, hold_weeks=None)
            mdd = m.mdd if m.mdd is not None else _compute_signal_interval_mdd(signals)
            row = {
                "combo": f"{uni}매수 × {exit_key}청산",
                "entry": uni,
                "exit": exit_key,
                "n_signals": len(signals),
                "win_rate": m.win_rate_sell,
                "avg_return": m.avg_return_sell,
                "median_return": m.median_return_sell,
                "mdd": mdd,
            }
            rows.append(row)
            logger.info(
                "  [%s] 신호%d 승률%s 평균%s 중앙값%s MDD%s",
                row["combo"], row["n_signals"], _pct(row["win_rate"]),
                _pct(row["avg_return"]), _pct(row["median_return"]), _pct(row["mdd"]),
            )

    elapsed = time.time() - t0
    logger.info("전체 완료 — %.0f초", elapsed)

    print("\n" + "=" * 100)
    print(f"{'조합':<40} {'신호':>6} {'승률':>8} {'평균수익':>10} {'중앙값':>10} {'MDD':>10}")
    print("-" * 100)
    for r in sorted(rows, key=lambda x: (x["win_rate"] or 0), reverse=True):
        print(f"{r['combo']:<40} {r['n_signals']:>6} {_pct(r['win_rate']):>8} "
              f"{_pct(r['avg_return']):>10} {_pct(r['median_return']):>10} {_pct(r['mdd']):>10}")
    print("=" * 100)

    out_path = Path(__file__).parent.parent / "results" / "momentum_qvm_quantexit_combo.csv"
    out_path.parent.mkdir(exist_ok=True)
    import pandas as pd
    pd.DataFrame(rows).to_csv(out_path, index=False)
    logger.info("결과 저장: %s", out_path)


if __name__ == "__main__":
    main()
