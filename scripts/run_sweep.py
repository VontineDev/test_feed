"""
run_sweep.py — 진입×청산 파라미터 그리드서치 스위프

사용법:
    python scripts/run_sweep.py --mode stage --train-start 2023-01-01 \\
        --train-end 2025-06-30 --val-start 2025-07-01 --val-end 2026-05-12 \\
        --market KOSPI --max-tickers 200 --workers 8 \\
        --output results/sweep_stage.csv

기본 파라미터 그리드 (288 조합):
    tp1_pct    : 0.10 0.15 0.20 0.25
    tp1_ratio  : 0.30 0.50 0.70
    trail_pct  : 0.08 0.10 0.12 0.15
    hard_stop  : 0.06 0.08 0.10
    stage3_peak: True False

결과 CSV 컬럼:
    entry_mode, tp1_pct, tp1_ratio, trail_pct, hard_stop_pct, use_stage3_peak,
    train_sharpe, train_win_rate, train_mdd, train_cagr, train_n,
    val_sharpe, val_win_rate, val_mdd, val_cagr, val_n,
    overfit_gap
"""
from __future__ import annotations

import argparse
import itertools
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── 기본 파라미터 그리드 ──────────────────────────────────────────────
DEFAULT_GRID = {
    "tp1_pct":         [0.10, 0.15, 0.20, 0.25],
    "tp1_ratio":       [0.30, 0.50, 0.70],
    "trail_pct":       [0.08, 0.10, 0.12, 0.15],
    "hard_stop_pct":   [0.06, 0.08, 0.10],
    "use_stage3_peak": [True, False],
}

# compose 모드는 S2/S3 로직을 쓰지 않으므로(use_stage3_peak 무의미) 그리드에서 제외.
COMPOSE_GRID = {
    "tp1_pct":       [0.10, 0.15, 0.20, 0.25],
    "tp1_ratio":     [0.30, 0.50, 0.70],
    "trail_pct":     [0.08, 0.10, 0.12, 0.15],
    "hard_stop_pct": [0.06, 0.08, 0.10],
}


def _sharpe(returns: list[float], rf: float = 0.03) -> float | None:
    if len(returns) < 2:
        return None
    import statistics
    mean = sum(returns) / len(returns)
    std  = statistics.stdev(returns)
    if std == 0:
        return None
    daily_rf = rf / 252
    return ((mean - daily_rf) / std) * (252 ** 0.5)


def _metrics(signals: list, use_blended: bool) -> dict:
    """신호 리스트에서 핵심 지표 계산.

    MDD/CAGR은 동시보유 포트폴리오(동일비중 주간 리밸런싱) 기준으로 계산한다.
    신호를 그대로 순차 올인 복리하면 겹치는 보유기간을 무시해 MDD가 항상
    -100%에 근접하는 버그가 있었다 (2026-07 발견, analysis.backtest.helpers 동일 수정).
    """
    from analysis.backtest.helpers import _compute_portfolio_returns_from_intervals, _compute_mdd

    rets = []
    intervals: list[tuple] = []
    for s in signals:
        if use_blended and s.blended_return is not None:
            r, exit_date = s.blended_return, s.final_exit_date
        else:
            r, exit_date = s.sell_return, s.sell_date
        if r is None:
            continue
        rets.append(r)
        if exit_date is not None and exit_date > s.signal_date:
            intervals.append((s.signal_date, exit_date, r))

    n = len(rets)
    if n == 0:
        return {"n": 0, "sharpe": None, "win_rate": None, "mdd": None, "cagr": None}

    wins = sum(1 for r in rets if r > 0)

    portfolio_rs = _compute_portfolio_returns_from_intervals(intervals, period_days=7) if intervals else []
    mdd = _compute_mdd(portfolio_rs) if portfolio_rs else None

    cagr = None
    if intervals:
        span_start = min(d0 for d0, _, _ in intervals)
        span_end   = max(d1 for _, d1, _ in intervals)
        total_days = (span_end - span_start).days
        if total_days > 0:
            equity_final = 1.0
            for r in portfolio_rs:
                equity_final *= (1.0 + r)
            if equity_final > 0:
                cagr = equity_final ** (365.0 / total_days) - 1.0

    return {
        "n":        n,
        "sharpe":   _sharpe(rets),
        "win_rate": wins / n,
        "mdd":      mdd,
        "cagr":     cagr,
    }


def sweep_backtest(
    entry_mode: str,
    tickers: list[tuple],
    train_start: date,
    train_end: date,
    val_start: date,
    val_end: date,
    param_grid: dict,
    dsn: str | None,
    workers: int = 8,
    max_tickers: int = 200,
    market: str = "ALL",
) -> pd.DataFrame:
    """OHLCV를 한 번 로드한 뒤 모든 파라미터 조합을 순차 실행.

    반환: DataFrame (1행 = 1 파라미터 조합)
    """
    from analysis.backtest.models import BacktestConfig
    from analysis.backtest.replay import (
        _replay_ichimoku, _replay_stage, _replay_stage2, _apply_cross_filter,
    )
    from analysis.backtest.helpers import _fill_returns, _build_price_lookup
    from analysis.backtest.exit_models import _compute_sell_signals_and_s2
    from analysis.chart_screener import get_all_tickers, fetch_kind_sector_map
    from core.ohlcv_cache import batch_fetch_cached, fetch_index_cached

    # ── 1. 티커 목록 ───────────────────────────────────────────────
    from analysis.backtest.config import TX_COST_DEFAULT
    from analysis.backtest.fetch import _fetch_single_ohlcv, _fetch_index
    from datetime import timedelta

    fetch_start = min(train_start, val_start) - timedelta(days=760)
    fetch_end   = max(train_end, val_end)     + timedelta(days=105)

    logger.info("[sweep] 티커 %d개 OHLCV 로드 중 (%s ~ %s)...", len(tickers), fetch_start, fetch_end)
    t0 = time.time()

    if dsn:
        ticker_pairs = [
            (t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI")
            for t, _, _ in tickers
        ]
        ohlcv_map = batch_fetch_cached(
            ticker_pairs, fetch_start, fetch_end, workers, dsn, _fetch_single_ohlcv,
        )
        kospi_df = fetch_index_cached("^KS11", "IDX", fetch_start, fetch_end, dsn, _fetch_index)
    else:
        from analysis.backtest.fetch import _batch_fetch_ohlcv
        ticker_syms = [t for t, _, _ in tickers]
        ohlcv_map   = _batch_fetch_ohlcv(ticker_syms, fetch_start, fetch_end, workers)
        kospi_df    = _fetch_index("^KS11", fetch_start, fetch_end)

    kospi_lookup = _build_price_lookup(kospi_df) if kospi_df is not None else {}
    logger.info("[sweep] OHLCV 로드 완료 — %.1f초, %d티커", time.time() - t0, len(ohlcv_map))

    # ── 2. 수급 데이터 ─────────────────────────────────────────────
    flow_lookup = None
    if dsn:
        try:
            from core.ohlcv_cache import load_flow_data
            ticker_syms = [t for t, _, _ in tickers]
            flow_lookup = load_flow_data(dsn, ticker_syms, min(train_start, val_start), fetch_end)
            logger.info("[sweep] 수급 데이터 %d건 로드", len(flow_lookup))
        except Exception as e:
            logger.warning("[sweep] 수급 로드 실패: %s", e)

    # ── 2b. Stage3 peakout map (use_stage3_peak=True 조합에서 사용) ──
    stage3_peakout_map: dict = {}
    if dsn:
        try:
            import asyncio as _asyncio
            from core.db import get_stage3_peakout_map as _get_peakout
            ticker_syms = [t for t, _, _ in tickers]
            stage3_peakout_map = _asyncio.run(
                _get_peakout(None, ticker_syms,
                             min(train_start, val_start),
                             max(train_end, val_end),
                             dsn=dsn)
            )
            total_peakout = sum(len(v) for v in stage3_peakout_map.values())
            logger.info("[sweep] Stage3 peakout 로드: %d종목 %d건",
                        len(stage3_peakout_map), total_peakout)
        except Exception as e:
            logger.warning("[sweep] Stage3 peakout 로드 실패 (use_stage3_peak 비활성): %s", e)

    # ── 3. 업종 정보 ───────────────────────────────────────────────
    ticker_sector = {t: s for t, _n, s in tickers}

    # ── 4. 베이스 신호 재현 (파라미터 독립, 한 번만) ──────────────
    # 전체 구간 신호 생성 후 train/val로 분리
    full_cfg = BacktestConfig(
        mode=entry_mode, start=min(train_start, val_start),
        end=max(train_end, val_end), market=market,
        max_tickers=max_tickers, workers=workers, dsn=dsn,
    )
    all_signals_raw = []
    for ticker, name, _ in tickers:
        df = ohlcv_map.get(ticker)
        if df is None or df.empty:
            continue
        mkt = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"
        if entry_mode in ("ichimoku", "cross"):
            all_signals_raw.extend(_replay_ichimoku(ticker, name, df, mkt, full_cfg))
        if entry_mode in ("stage", "cross"):
            all_signals_raw.extend(_replay_stage(ticker, name, df, mkt, full_cfg, flow_lookup))
        if entry_mode == "stage2":
            all_signals_raw.extend(_replay_stage2(ticker, name, df, mkt, full_cfg))

    if entry_mode == "cross":
        all_signals_raw = _apply_cross_filter(all_signals_raw)

    for sig in all_signals_raw:
        sig.sector = ticker_sector.get(sig.ticker, "")

    logger.info("[sweep] 베이스 신호 %d개 (train+val 합산)", len(all_signals_raw))

    # ── 5. 파라미터 조합 순차 스위프 ─────────────────────────────
    keys   = list(param_grid.keys())
    combos = list(itertools.product(*[param_grid[k] for k in keys]))
    logger.info("[sweep] 파라미터 조합 %d개 시작...", len(combos))

    rows = []
    import copy

    for idx, values in enumerate(combos):
        params = dict(zip(keys, values))
        if (idx + 1) % 50 == 0:
            logger.info("[sweep] %d/%d 완료", idx + 1, len(combos))

        # 신호 복사 (in-place 수정이 필요하므로)
        sigs = copy.deepcopy(all_signals_raw)

        # _fill_returns: 고정된 기간 기반 수익률 (1/4/13w)
        stock_lc: dict = {}
        for sig in sigs:
            df = ohlcv_map.get(sig.ticker)
            if df is None:
                continue
            if sig.ticker not in stock_lc:
                stock_lc[sig.ticker] = _build_price_lookup(df)
            _fill_returns(sig, stock_lc[sig.ticker], kospi_lookup, TX_COST_DEFAULT)

        # 분할 청산 계산
        cfg = BacktestConfig(
            mode=entry_mode, start=min(train_start, val_start),
            end=max(train_end, val_end), market=market,
            max_tickers=max_tickers, workers=workers, dsn=dsn,
            tp1_pct=params["tp1_pct"],
            tp1_ratio=params["tp1_ratio"],
            trail_pct=params["trail_pct"],
            hard_stop_pct=params["hard_stop_pct"],
            use_stage3_peak=params["use_stage3_peak"],
        )
        _compute_sell_signals_and_s2(
            sigs, ohlcv_map, TX_COST_DEFAULT,
            stop_loss_pct=cfg.hard_stop_pct,
            flow_lookup=flow_lookup,
            cfg=cfg,
            stage3_peakout_map=stage3_peakout_map if cfg.use_stage3_peak else None,
        )

        use_blended = (cfg.tp1_pct > 0 or cfg.trail_pct > 0)

        train_sigs = [s for s in sigs if train_start <= s.signal_date <= train_end]
        val_sigs   = [s for s in sigs if val_start   <= s.signal_date <= val_end]

        tm = _metrics(train_sigs, use_blended)
        vm = _metrics(val_sigs,   use_blended)

        row = {
            "entry_mode":     entry_mode,
            "tp1_pct":        params["tp1_pct"],
            "tp1_ratio":      params["tp1_ratio"],
            "trail_pct":      params["trail_pct"],
            "hard_stop_pct":  params["hard_stop_pct"],
            "use_stage3_peak": params["use_stage3_peak"],
            "train_sharpe":   tm["sharpe"],
            "train_win_rate": tm["win_rate"],
            "train_mdd":      tm["mdd"],
            "train_cagr":     tm["cagr"],
            "train_n":        tm["n"],
            "val_sharpe":     vm["sharpe"],
            "val_win_rate":   vm["win_rate"],
            "val_mdd":        vm["mdd"],
            "val_cagr":       vm["cagr"],
            "val_n":          vm["n"],
            "overfit_gap":    (
                (tm["sharpe"] or 0) - (vm["sharpe"] or 0)
                if tm["sharpe"] is not None and vm["sharpe"] is not None
                else None
            ),
        }
        rows.append(row)

    df_result = pd.DataFrame(rows)
    logger.info("[sweep] 완료 — %d 조합", len(rows))
    return df_result


def sweep_compose_backtest(
    strategy: str,
    train_start: date,
    train_end: date,
    val_start: date,
    val_end: date,
    param_grid: dict,
    dsn: str,
    workers: int = 8,
    market: str = "ALL",
) -> pd.DataFrame:
    """compose 모드(SCORE-1/FUNNEL-1 등) 전용 청산 파라미터 스위프.

    sweep_backtest()와 달리 replay(_replay_*)가 아니라 strategy_compose의
    (ticker, ISO주) 합성 신호에서 출발한다 — analysis/backtest/engine.py의
    _run_compose()와 동일한 신호 생성 경로를 재사용하되, 청산 파라미터만
    조합별로 바꿔가며 반복 계산한다 (베이스 신호/OHLCV/수급은 1회만 로드).
    """
    from analysis import strategy_compose as sc
    from analysis.chart_screener import get_all_tickers, fetch_kind_sector_map
    from analysis.backtest.models import BacktestConfig, SignalRecord
    from analysis.backtest.helpers import _build_price_lookup, _entry_on_or_after, _fill_returns
    from analysis.backtest.exit_models import _compute_sell_signals_and_s2
    from analysis.backtest.config import TX_COST_DEFAULT
    from analysis.backtest.fetch import _fetch_single_ohlcv, _fetch_index
    from core.ohlcv_cache import batch_fetch_cached, fetch_index_cached, load_flow_data
    from datetime import timedelta
    import copy

    full_start, full_end = min(train_start, val_start), max(train_end, val_end)

    spec = sc.STRATEGIES.get(strategy)
    if spec is None:
        raise ValueError(f"알 수 없는 전략: {strategy!r} (가능: {sorted(sc.STRATEGIES)})")

    frame = sc.load_signal_frame(dsn, full_start, full_end, spec.sources)
    if frame.empty:
        raise RuntimeError(f"{strategy}: 소스 데이터 0건")
    frame = sc.derive_flags(frame)
    sig_df = spec.run(frame)
    if sig_df.empty:
        raise RuntimeError(f"{strategy}: 합성 신호 0건")

    sector_map  = fetch_kind_sector_map()
    all_tickers = get_all_tickers(sector_map=sector_map if sector_map else None)
    meta: dict[str, tuple[str, str]] = {t: (n, s) for t, n, s in all_tickers}

    entries: list[tuple[str, date]] = []
    for row in sig_df.itertuples(index=False):
        ticker = row.ticker
        friday = sc.week_to_friday(row.week)
        if not (full_start <= friday <= full_end):
            continue
        mkt = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"
        if market != "ALL" and mkt != market:
            continue
        entries.append((ticker, friday))
    if not entries:
        raise RuntimeError(f"{strategy}: 기간/시장 필터 후 신호 0건")

    needed = sorted({t for t, _ in entries})
    logger.info("[sweep-compose] %s 합성 신호 %d건 · 대상 티커 %d개", strategy, len(entries), len(needed))

    fetch_start = full_start - timedelta(days=760)
    fetch_end   = full_end + timedelta(days=105)
    ticker_pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t in needed]
    ohlcv_map = batch_fetch_cached(ticker_pairs, fetch_start, fetch_end, workers, dsn, _fetch_single_ohlcv)
    kospi_df     = fetch_index_cached("^KS11", "IDX", fetch_start, fetch_end, dsn, _fetch_index)
    kospi_lookup = _build_price_lookup(kospi_df) if kospi_df is not None else {}

    flow_lookup = None
    try:
        flow_lookup = load_flow_data(dsn, needed, full_start, fetch_end)
    except Exception as e:
        logger.warning("[sweep-compose] 수급 로드 실패: %s", e)

    price_lookup_cache: dict[str, dict] = {}
    seen: set[tuple[str, date]] = set()
    base_signals: list[SignalRecord] = []
    for ticker, friday in entries:
        df = ohlcv_map.get(ticker)
        if df is None or df.empty:
            continue
        plook = price_lookup_cache.get(ticker)
        if plook is None:
            plook = _build_price_lookup(df)
            price_lookup_cache[ticker] = plook
        entry = _entry_on_or_after(plook, friday)
        if entry is None:
            continue
        edate, eclose = entry
        if eclose <= 0 or (ticker, edate) in seen:
            continue
        seen.add((ticker, edate))
        mkt  = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"
        name = meta.get(ticker, (ticker, ""))[0]
        sig = SignalRecord(
            ticker=ticker, name=name, signal_date=edate,
            close_at_signal=eclose, mode="compose", market=mkt,
        )
        sig.sector = meta.get(ticker, ("", ""))[1]
        base_signals.append(sig)
    if not base_signals:
        raise RuntimeError(f"{strategy}: 진입가 산정 후 신호 0건")

    for sig in base_signals:
        plook = price_lookup_cache.get(sig.ticker)
        if plook is not None:
            _fill_returns(sig, plook, kospi_lookup, TX_COST_DEFAULT)

    logger.info("[sweep-compose] 베이스 신호 %d건", len(base_signals))

    keys   = list(param_grid.keys())
    combos = list(itertools.product(*[param_grid[k] for k in keys]))
    logger.info("[sweep-compose] 파라미터 조합 %d개 시작...", len(combos))

    rows = []
    for idx, values in enumerate(combos):
        params = dict(zip(keys, values))
        if (idx + 1) % 20 == 0:
            logger.info("[sweep-compose] %d/%d 완료", idx + 1, len(combos))

        sigs = copy.deepcopy(base_signals)
        cfg = BacktestConfig(
            mode="compose", strategy=strategy, start=full_start, end=full_end,
            market=market, workers=workers, dsn=dsn,
            tp1_pct=params["tp1_pct"], tp1_ratio=params["tp1_ratio"],
            trail_pct=params["trail_pct"], hard_stop_pct=params["hard_stop_pct"],
        )
        _compute_sell_signals_and_s2(
            sigs, ohlcv_map, TX_COST_DEFAULT,
            stop_loss_pct=cfg.hard_stop_pct, flow_lookup=flow_lookup, cfg=cfg,
            stage3_peakout_map=None,
        )
        use_blended = (cfg.tp1_pct > 0 or cfg.trail_pct > 0)

        train_sigs = [s for s in sigs if train_start <= s.signal_date <= train_end]
        val_sigs   = [s for s in sigs if val_start   <= s.signal_date <= val_end]

        tm = _metrics(train_sigs, use_blended)
        vm = _metrics(val_sigs, use_blended)

        rows.append({
            "strategy":       strategy,
            "tp1_pct":        params["tp1_pct"],
            "tp1_ratio":      params["tp1_ratio"],
            "trail_pct":      params["trail_pct"],
            "hard_stop_pct":  params["hard_stop_pct"],
            "train_sharpe":   tm["sharpe"],
            "train_win_rate": tm["win_rate"],
            "train_mdd":      tm["mdd"],
            "train_cagr":     tm["cagr"],
            "train_n":        tm["n"],
            "val_sharpe":     vm["sharpe"],
            "val_win_rate":   vm["win_rate"],
            "val_mdd":        vm["mdd"],
            "val_cagr":       vm["cagr"],
            "val_n":          vm["n"],
            "overfit_gap": (
                (tm["sharpe"] or 0) - (vm["sharpe"] or 0)
                if tm["sharpe"] is not None and vm["sharpe"] is not None
                else None
            ),
        })

    df_result = pd.DataFrame(rows)
    logger.info("[sweep-compose] 완료 — %d 조합", len(rows))
    return df_result


def main() -> None:
    parser = argparse.ArgumentParser(description="파라미터 그리드서치 스위프")
    parser.add_argument("--mode",         required=True,
                        choices=["stage", "stage2", "ichimoku", "cross", "compose"])
    parser.add_argument("--strategy",     default=None,
                        help="mode=compose 시 필수 (예: SCORE-1, FUNNEL-1)")
    parser.add_argument("--train-start",  required=True)
    parser.add_argument("--train-end",    required=True)
    parser.add_argument("--val-start",    required=True)
    parser.add_argument("--val-end",      required=True)
    parser.add_argument("--market",       default="ALL", choices=["KOSPI", "KOSDAQ", "ALL"])
    parser.add_argument("--max-tickers",  type=int, default=200)
    parser.add_argument("--workers",      type=int, default=8)
    parser.add_argument("--output",       default="results/sweep.csv")
    parser.add_argument("--top",          type=int, default=10, help="val_sharpe 기준 상위 N개 출력")
    args = parser.parse_args()

    if args.mode == "compose" and not args.strategy:
        sys.exit("--mode compose 사용 시 --strategy 필수 (예: --strategy SCORE-1)")

    train_start = date.fromisoformat(args.train_start)
    train_end   = date.fromisoformat(args.train_end)
    val_start   = date.fromisoformat(args.val_start)
    val_end     = date.fromisoformat(args.val_end)

    if train_start >= train_end:
        sys.exit("train-start < train-end 이어야 합니다")
    if val_start >= val_end:
        sys.exit("val-start < val-end 이어야 합니다")
    if val_start <= train_end:
        logger.warning("val-start(%s)가 train-end(%s)보다 이르거나 같습니다 — 구간 겹침!", val_start, train_end)

    dsn = os.environ.get("DATABASE_URL") or None
    if not dsn:
        db_user = os.environ.get("DB_USER", "")
        db_pass = os.environ.get("DB_PASSWORD", "")
        db_host = os.environ.get("DB_HOST", "localhost")
        db_port = os.environ.get("DB_PORT", "5432")
        db_name = os.environ.get("DB_NAME", "news_db")
        if db_user and db_pass:
            from urllib.parse import quote
            dsn = f"postgresql://{db_user}:{quote(db_pass)}@{db_host}:{db_port}/{db_name}"

    if args.mode == "compose":
        if not dsn:
            sys.exit("mode=compose는 DATABASE_URL(또는 DB_USER/DB_PASSWORD)이 필요합니다")
        logger.info("[sweep-compose] 전략=%s 학습=%s~%s 검증=%s~%s",
                    args.strategy, train_start, train_end, val_start, val_end)
        df = sweep_compose_backtest(
            strategy=args.strategy,
            train_start=train_start,
            train_end=train_end,
            val_start=val_start,
            val_end=val_end,
            param_grid=COMPOSE_GRID,
            dsn=dsn,
            workers=args.workers,
            market=args.market,
        )
        peak_cols = ["tp1_pct", "tp1_ratio", "trail_pct", "hard_stop_pct",
                     "val_sharpe", "val_win_rate", "val_mdd", "val_n", "overfit_gap"]
    else:
        from analysis.chart_screener import get_all_tickers, fetch_kind_sector_map
        sector_map  = fetch_kind_sector_map()
        all_tickers = get_all_tickers(sector_map=sector_map if sector_map else None)

        if args.market == "KOSPI":
            tickers = [(t, n, s) for t, n, s in all_tickers if t.endswith(".KS")]
        elif args.market == "KOSDAQ":
            tickers = [(t, n, s) for t, n, s in all_tickers if t.endswith(".KQ")]
        else:
            tickers = all_tickers

        if args.max_tickers > 0:
            tickers = tickers[:args.max_tickers]

        logger.info("[sweep] 모드=%s 티커=%d 학습=%s~%s 검증=%s~%s",
                    args.mode, len(tickers), train_start, train_end, val_start, val_end)

        df = sweep_backtest(
            entry_mode=args.mode,
            tickers=tickers,
            train_start=train_start,
            train_end=train_end,
            val_start=val_start,
            val_end=val_end,
            param_grid=DEFAULT_GRID,
            dsn=dsn,
            workers=args.workers,
            max_tickers=args.max_tickers,
            market=args.market,
        )
        peak_cols = ["tp1_pct", "tp1_ratio", "trail_pct", "hard_stop_pct",
                     "use_stage3_peak", "val_sharpe", "val_win_rate", "val_mdd", "val_n", "overfit_gap"]

    # 결과 저장
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("[sweep] 결과 저장: %s", out_path)

    # 상위 결과 출력
    valid = df[df["val_sharpe"].notna() & (df["val_n"] >= 10)].copy()
    if valid.empty:
        logger.warning("val_n >= 10 인 조합 없음 — 검증 기간에 신호 부족")
        return

    valid = valid.sort_values("val_sharpe", ascending=False)
    top   = valid.head(args.top)

    print(f"\n{'='*70}")
    print(f"Top {args.top} 파라미터 조합 (val_sharpe 기준, val_n≥10)")
    print(f"{'='*70}")
    print(top[peak_cols].to_string(index=False))

    # 과적합 경고
    overfit = valid[valid["overfit_gap"].notna() & (valid["overfit_gap"] > 0.3)]
    if len(overfit) > len(valid) * 0.8:
        print("\n⚠️  과적합 주의: 80% 이상 조합의 train-val Sharpe 갭이 0.3 초과")


if __name__ == "__main__":
    main()
