"""
compare_v11_v12.py — Stage v1.1 / v1.2 / v1.3 / v1.4 / v1.5 백테스트 비교

사용법:
    python scripts/compare_v11_v12.py
    python scripts/compare_v11_v12.py --start 2025-01-01 --end 2026-06-17 --max-tickers 0
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

OPTIMAL = dict(tp1_pct=0.25, tp1_ratio=0.50, trail_pct=0.10, hard_stop_pct=0.10)


def _metrics_from_result(result) -> dict:
    m = result.overall

    def _r(v, dp=3):
        return round(v, dp) if v is not None else None

    return {
        "n":           m.n,
        "sharpe_28d":  _r(m.sharpe_28d),
        "sharpe_sell": _r(m.sharpe_custom),
        "win_28d":     _r(m.win_rate_28d),
        "win_sell":    _r(m.win_rate_sell),
        "avg_28d":     _r(m.avg_return_28d),
        "avg_sell":    _r(m.avg_return_sell),
        "mdd":         _r(m.mdd),
        "s2_rate":     _r(m.s2_progression_rate),
        "s3_rate":     _r(m.s3_progression_rate),
    }


def run_one(mode: str, start: date, end: date, max_tickers: int, dsn: str) -> dict:
    from analysis.backtest.models import BacktestConfig
    from analysis.backtest.engine import run_backtest
    cfg = BacktestConfig(
        mode=mode,
        start=start,
        end=end,
        market="ALL",
        max_tickers=max_tickers,
        dsn=dsn,
        workers=8,
        tp1_pct=OPTIMAL["tp1_pct"],
        tp1_ratio=OPTIMAL["tp1_ratio"],
        trail_pct=OPTIMAL["trail_pct"],
        hard_stop_pct=OPTIMAL["hard_stop_pct"],
        use_stage3_peak=True,
    )
    logger.info("===== %s 백테스트 시작 =====", mode)
    result = run_backtest(cfg)
    return _metrics_from_result(result)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start",        default="2025-01-01")
    p.add_argument("--end",          default="2026-06-17")
    p.add_argument("--max-tickers",  type=int, default=200, help="0=전종목")
    args = p.parse_args()

    dsn = os.environ.get("DATABASE_URL") or _build_dsn()
    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)

    results = {}
    for mode in ("stage_v11", "stage_v12", "stage_v13", "stage_v14", "stage_v15"):
        results[mode] = run_one(mode, start, end, args.max_tickers, dsn)

    keys = ("n", "sharpe_28d", "win_28d", "avg_28d", "win_sell", "avg_sell", "mdd", "s2_rate", "s3_rate")
    pct_keys = {"win_28d", "win_sell", "avg_28d", "avg_sell", "mdd", "s2_rate", "s3_rate"}
    fmt = lambda key, v: f"{v*100:.1f}%" if (v is not None and key in pct_keys) else str(v)

    print("\n" + "=" * 96)
    print(f"{'지표':<16} {'v1.1':>10} {'v1.2':>10} {'v1.3':>10} {'v1.4':>10} {'v1.5':>10} {'v14-v13':>8} {'v15-v14':>8}")
    print("-" * 96)
    for key in keys:
        v11 = results["stage_v11"].get(key)
        v12 = results["stage_v12"].get(key)
        v13 = results["stage_v13"].get(key)
        v14 = results["stage_v14"].get(key)
        v15 = results["stage_v15"].get(key)
        if key == "n":
            d14 = f"{v14 - v13:+d}" if (v14 is not None and v13 is not None) else ""
            d15 = f"{v15 - v14:+d}" if (v15 is not None and v14 is not None) else ""
        else:
            d14 = f"{v14 - v13:+.3f}" if (v14 is not None and v13 is not None) else ""
            d15 = f"{v15 - v14:+.3f}" if (v15 is not None and v14 is not None) else ""
        print(f"{key:<16} {fmt(key,v11):>10} {fmt(key,v12):>10} {fmt(key,v13):>10} {fmt(key,v14):>10} {fmt(key,v15):>10} {d14:>8} {d15:>8}")
    print("=" * 96)


def _build_dsn() -> str:
    from urllib.parse import quote
    host = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", "5432")
    name = os.environ.get("DB_NAME", "postgres")
    user = os.environ.get("DB_USER", "postgres")
    pw   = os.environ.get("DB_PASSWORD", "")
    return f"postgresql://{quote(user,safe='')}:{quote(pw,safe='')}@{host}:{port}/{name}"


if __name__ == "__main__":
    main()
