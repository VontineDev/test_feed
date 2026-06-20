"""
compare_exit_models.py — 청산 모델 3종 백테스트 비교

원본(default) vs 모델A(ATR+Breakeven) vs 모델B(3단계분할)
신호 소스: stage_v13 (가장 최신 조건)

사용법:
    python scripts/compare_exit_models.py
    python scripts/compare_exit_models.py --max-tickers 0 --start 2025-01-01
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

# ── 비교 대상 모델 정의 ────────────────────────────────────────────
MODELS = {
    "원본":   dict(exit_model="default",  tp1_pct=0.25, tp1_ratio=0.50, trail_pct=0.10, hard_stop_pct=0.10),
    "모델A":  dict(exit_model="model_a",  tp1_pct=0.25, tp1_ratio=0.50, trail_pct=0.00, hard_stop_pct=0.10),
    "모델B":  dict(exit_model="model_b",  tp1_pct=0.00, tp1_ratio=0.50, trail_pct=0.00, hard_stop_pct=0.10),
}


def _metrics(result) -> dict:
    m = result.overall

    def _r(v, dp=3):
        return round(v, dp) if v is not None else None

    return {
        "n":          m.n,
        "sharpe_28d": _r(m.sharpe_28d),
        "win_28d":    _r(m.win_rate_28d),
        "avg_28d":    _r(m.avg_return_28d),
        "win_sell":   _r(m.win_rate_sell),
        "avg_sell":   _r(m.avg_return_sell),
        "mdd":        _r(m.mdd),
        "s2_rate":    _r(m.s2_progression_rate),
        "s3_rate":    _r(m.s3_progression_rate),
    }


def run_one(label: str, params: dict, start: date, end: date, max_tickers: int, dsn: str) -> dict:
    from analysis.backtest_engine import BacktestConfig, run_backtest
    cfg = BacktestConfig(
        mode="stage_v13",
        start=start,
        end=end,
        market="ALL",
        max_tickers=max_tickers,
        dsn=dsn,
        workers=8,
        use_stage3_peak=True,
        **params,
    )
    logger.info("===== %s 백테스트 시작 =====", label)
    return _metrics(run_backtest(cfg))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--start",       default="2025-01-01")
    p.add_argument("--end",         default="2026-06-17")
    p.add_argument("--max-tickers", type=int, default=200, help="0=전종목")
    args = p.parse_args()

    dsn   = os.environ.get("DATABASE_URL") or _build_dsn()
    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)

    results: dict[str, dict] = {}
    for label, params in MODELS.items():
        results[label] = run_one(label, params, start, end, args.max_tickers, dsn)

    labels   = list(MODELS.keys())
    keys     = ("n", "sharpe_28d", "win_28d", "avg_28d", "win_sell", "avg_sell", "mdd", "s2_rate", "s3_rate")
    pct_keys = {"win_28d", "win_sell", "avg_28d", "avg_sell", "mdd", "s2_rate", "s3_rate"}

    def fmt(key: str, v) -> str:
        if v is None:
            return "N/A"
        return f"{v*100:.1f}%" if key in pct_keys else str(v)

    col_w = 12
    head  = f"{'지표':<16}" + "".join(f"{lb:>{col_w}}" for lb in labels)
    sep   = "-" * (16 + col_w * len(labels))

    print("\n" + "=" * len(sep))
    print(head)
    print(sep)
    for key in keys:
        row = f"{key:<16}"
        for lb in labels:
            row += f"{fmt(key, results[lb].get(key)):>{col_w}}"
        print(row)
    print("=" * len(sep))

    # 모델A/B vs 원본 델타
    base = results[labels[0]]
    print(f"\n{'델타 (vs 원본)':<16}" + "".join(f"{lb:>{col_w}}" for lb in labels[1:]))
    print(sep)
    for key in keys:
        if key == "n":
            row = f"{key:<16}"
            for lb in labels[1:]:
                bv = base.get(key)
                cv = results[lb].get(key)
                d  = f"{cv - bv:+d}" if (bv is not None and cv is not None) else ""
                row += f"{d:>{col_w}}"
        else:
            row = f"{key:<16}"
            for lb in labels[1:]:
                bv = base.get(key)
                cv = results[lb].get(key)
                d  = f"{cv - bv:+.3f}" if (bv is not None and cv is not None) else ""
                row += f"{d:>{col_w}}"
        print(row)
    print("=" * len(sep))


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
