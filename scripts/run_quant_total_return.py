"""
run_quant_total_return.py — MOMENTUM_TOP20 진입 × 3개 분할청산(cross/score1/
funnel1) 각각의 "전체 기간 총 수익률"을 동일비중 동시보유 포트폴리오 기준으로 계산.

2026-08-14: 신호별 avg_return(건당 평균수익)은 이미 문서화돼 있지만, 신호들이
서로 다른 종목을 병렬 보유하는 구조라 "전체 기간에 얼마를 벌었는지"는 별도
계산이 필요하다. 단순히 신호 수익률을 순차 복리로 곱하면(총자본 올인 가정)
왜곡되므로, 이미 MDD 계산에 쓰인 것과 동일한 방법
(analysis/backtest/helpers.py:_compute_portfolio_returns_from_intervals —
활성 포지션 동일비중 평균의 주간 수익률 시계열)을 재사용해 누적수익률을 뽑는다.

사용법:
    python scripts/run_quant_total_return.py --entry MOMENTUM_TOP20
"""
from __future__ import annotations

import argparse
import copy
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")  # Windows cp949 콘솔 한글 깨짐 방지

from dotenv import load_dotenv

load_dotenv(os.path.join(Path(__file__).parent.parent, ".env"))

from analysis.backtest.helpers import _compute_portfolio_returns_from_intervals  # noqa: E402
from run_cross_combo_backtest import _apply_compose_exit, _generate_quant_entries  # noqa: E402


def _dsn() -> str:
    dsn = os.environ.get("DATABASE_URL")
    if dsn:
        return dsn
    u, p = os.environ.get("DB_USER", ""), os.environ.get("DB_PASSWORD", "")
    h = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", "5432")
    db = os.environ.get("DB_NAME", "news_db")
    if u and p:
        from urllib.parse import quote
        return f"postgresql://{u}:{quote(p)}@{h}:{port}/{db}"
    sys.exit("DATABASE_URL (또는 DB_USER/DB_PASSWORD) 환경변수가 필요합니다")


def main() -> None:
    parser = argparse.ArgumentParser(description="분할청산 3종 총 수익률 비교")
    parser.add_argument("--entry", default="MOMENTUM_TOP20", choices=["MOMENTUM_TOP20", "QVM_TOP20"])
    parser.add_argument("--start", default="2025-01-02")
    parser.add_argument("--end", default="2026-08-06")
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    dsn = _dsn()

    base_signals, ohlcv_map = _generate_quant_entries(args.entry, dsn, start, end, args.workers)
    if not base_signals:
        sys.exit(f"{args.entry} 유니버스에서 신호가 생성되지 않았습니다.")

    print(f"\n{'='*70}")
    print(f"{args.entry} 진입 × 3개 분할청산 — 전체 기간 총 수익률 (동일비중 동시보유 가정)")
    print(f"{'='*70}")

    for exit_key in ("cross", "score1", "funnel1"):
        signals = copy.deepcopy(base_signals)
        _apply_compose_exit(signals, ohlcv_map, start, end, exit_key)

        intervals = [
            (s.signal_date, s.final_exit_date, s.blended_return)
            for s in signals
            if s.final_exit_date is not None and s.blended_return is not None
        ]
        n_closed = len(intervals)
        avg_ret = sum(r for _, _, r in intervals) / n_closed if n_closed else None
        win_rate = sum(1 for _, _, r in intervals if r > 0) / n_closed if n_closed else None

        period_returns = _compute_portfolio_returns_from_intervals(intervals, period_days=7)
        total_return = None
        n_weeks = len(period_returns)
        if period_returns:
            cum = 1.0
            for r in period_returns:
                cum *= (1.0 + r)
            total_return = cum - 1.0

        print(f"\n[{exit_key}] n={n_closed}  승률={win_rate*100:.1f}%  건당평균={avg_ret*100:+.1f}%"
              if n_closed else f"\n[{exit_key}] 신호 없음")
        if total_return is not None:
            years = n_weeks * 7 / 365.25
            cagr = (1 + total_return) ** (1 / years) - 1 if years > 0 else None
            print(f"  포트폴리오 총수익률(동일비중, {n_weeks}주 시계열, 약 {years:.1f}년) = {total_return*100:+.1f}%"
                  + (f"  (CAGR ≈ {cagr*100:+.1f}%)" if cagr is not None else ""))


if __name__ == "__main__":
    main()
