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
from types import SimpleNamespace

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


def _build_shared_data(start: date, end: date, max_tickers: int, dsn: str):
    """OHLCV·수급·streak·상장주식수·진입신호를 1회만 수집해 모델 3개가 공유.

    run_backtest()를 모델별로 3번 호출하면 동일한 전종목 OHLCV를 yfinance로
    3번 반복 다운로드하게 되어 (특히 --max-tickers 0) Yahoo rate limit에
    걸려 후순위 모델의 신호가 누락되는 문제가 있었다 (2026-06-23 확인:
    원본 9027건 vs 모델B 21건). 진입 신호 생성(stage_v13)은 exit_model과
    무관하므로 1회만 계산하고, 모델별로는 매도 판정(_compute_sell_signals_and_s2)
    부터 deepcopy된 신호로 따로 돌린다.
    """
    from datetime import timedelta as _td

    from analysis.backtest.models import BacktestConfig
    from analysis.backtest.helpers import _build_price_lookup
    from analysis.backtest.fetch import _fetch_index, _fetch_single_ohlcv
    from analysis.backtest.replay import _replay_stage_v13
    from analysis.chart_screener import fetch_kind_sector_map, get_all_tickers
    from core.ohlcv_cache import (
        batch_fetch_cached, fetch_index_cached, load_flow_data,
        load_flow_streaks, load_listed_shares,
    )

    base_cfg = BacktestConfig(
        mode="stage_v13", start=start, end=end, market="ALL",
        max_tickers=max_tickers, dsn=dsn, workers=8, use_stage3_peak=True,
    )

    sector_map  = fetch_kind_sector_map()
    all_tickers = get_all_tickers(sector_map=sector_map if sector_map else None)
    tickers     = all_tickers[:max_tickers] if max_tickers > 0 else all_tickers
    logger.info("[공유수집] 대상 티커 %d개", len(tickers))

    fetch_start = start - _td(days=760)
    fetch_end   = end + _td(days=105)

    ticker_pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t, _, _ in tickers]
    ohlcv_map = batch_fetch_cached(
        ticker_pairs, fetch_start, fetch_end, base_cfg.workers, dsn, _fetch_single_ohlcv,
    )
    kospi_df     = fetch_index_cached("^KS11", "IDX", fetch_start, fetch_end, dsn, _fetch_index)
    kospi_lookup = _build_price_lookup(kospi_df) if kospi_df is not None else {}

    ticker_syms   = [t for t, _, _ in tickers]
    flow_lookup   = load_flow_data(dsn, ticker_syms, start, fetch_end)
    streak_lookup = load_flow_streaks(dsn, ticker_syms, start, fetch_end)
    shares_lookup = load_listed_shares(dsn)
    logger.info("[공유수집] 수급=%d건 streak=%d건 상장주식수=%d종목",
                len(flow_lookup), len(streak_lookup), len(shares_lookup))

    base_signals = []
    for ticker, name, _ in tickers:
        df = ohlcv_map.get(ticker)
        if df is None or df.empty:
            continue
        mkt = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"
        base_signals.extend(_replay_stage_v13(
            ticker, name, df, mkt, base_cfg, flow_lookup, streak_lookup, shares_lookup,
        ))
    logger.info("[공유수집] 진입신호 %d건", len(base_signals))

    ticker_sector = {t: s for t, _, s in tickers}

    stage3_peakout_map = None
    try:
        import asyncio
        from core.db import get_stage3_peakout_map
        stage3_peakout_map = asyncio.run(
            get_stage3_peakout_map(None, ticker_syms, start, fetch_end, dsn=dsn)
        )
    except Exception as e:
        logger.warning("[공유수집] Stage3 peakout 조회 실패: %s", e)

    return dict(
        base_cfg=base_cfg, base_signals=base_signals, ohlcv_map=ohlcv_map,
        kospi_lookup=kospi_lookup, flow_lookup=flow_lookup, streak_lookup=streak_lookup,
        ticker_sector=ticker_sector, stage3_peakout_map=stage3_peakout_map,
    )


def run_one(label: str, params: dict, shared: dict) -> dict:
    import copy
    from dataclasses import replace as dc_replace

    from analysis.backtest.helpers import (
        _build_price_lookup, _compute_group_metrics, _fill_returns,
    )
    from analysis.backtest.exit_models import _compute_sell_signals_and_s2

    cfg     = dc_replace(shared["base_cfg"], **params)
    signals = copy.deepcopy(shared["base_signals"])
    ohlcv_map = shared["ohlcv_map"]

    logger.info("===== %s 백테스트 시작 (공유 데이터 재사용) =====", label)

    stock_lookup_cache: dict[str, dict] = {}
    for sig in signals:
        df = ohlcv_map.get(sig.ticker)
        if df is None:
            continue
        if sig.ticker not in stock_lookup_cache:
            stock_lookup_cache[sig.ticker] = _build_price_lookup(df)
        _fill_returns(sig, stock_lookup_cache[sig.ticker], shared["kospi_lookup"], cfg.tx_cost_rt, cfg.hold_weeks)
        sig.sector = shared["ticker_sector"].get(sig.ticker, "")

    _compute_sell_signals_and_s2(
        signals, ohlcv_map, cfg.tx_cost_rt,
        stop_loss_pct=cfg.hard_stop_pct,
        flow_lookup=shared["flow_lookup"],
        cfg=cfg,
        stage3_peakout_map=shared["stage3_peakout_map"],
        streak_lookup=shared["streak_lookup"],
    )
    signals.sort(key=lambda s: s.signal_date)
    overall = _compute_group_metrics(signals, cfg.rf_rate_annual, cfg.hold_weeks)

    return _metrics(SimpleNamespace(overall=overall))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--start",       default="2025-01-01")
    p.add_argument("--end",         default="2026-06-17")
    p.add_argument("--max-tickers", type=int, default=200, help="0=전종목")
    args = p.parse_args()

    dsn   = os.environ.get("DATABASE_URL") or _build_dsn()
    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)

    shared = _build_shared_data(start, end, args.max_tickers, dsn)

    results: dict[str, dict] = {}
    for label, params in MODELS.items():
        results[label] = run_one(label, params, shared)

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
