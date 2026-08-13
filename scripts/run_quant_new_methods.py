"""
run_quant_new_methods.py — 신규 방법론 3종(F/G/H) 백테스트 CLI

2026-08-11: 사용자 요청으로 TechnicalQuant.md에 추가한 5개 신규 방법론 중
백필 없이 바로 검증 가능한 3종(daily_ohlcv만 사용)을 A-E와 동일한 방식(펀더
멘털 필터 없이 기술적 조건 단독)으로 백테스트한다. 나머지 2종은 별도 스크립트:
  - QVM 복합 팩터(방법론4) → scripts/run_quant_qvm_backtest.py (dart_fundamentals
    백필 필요)
  - Walk-forward 검증(방법론5) → scripts/run_quant_walkforward.py

조건 목록:
    F_bb_rsi_volume     — 볼린저밴드 하단 이탈 + RSI30미만 + 거래량스파이크
                           (청산: 진입캔들저점 손절/중심선복귀 목표/RSI50 탈출,
                           replay_quant_bb_exit 전용 로직)
    G_volatility_breakout — Larry Williams 변동성 돌파(k=0.5)
                           (청산: A-E와 동일한 기본 청산 — 손절-5%/목표+15%/MA20이탈)
    H_donchian_atr      — D_new_high20 진입 + ATR 2배 손절/Donchian10일 이탈 청산
                           (D 자체와 비교하기 위해 D_new_high20 원안도 같이 출력)

사용법:
    python scripts/run_quant_new_methods.py --start 2025-01-02 --end 2026-08-06
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import pandas as pd
from dotenv import load_dotenv

load_dotenv(os.path.join(Path(__file__).parent.parent, ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

ALL_METHODS = ["F_bb_rsi_volume", "G_volatility_breakout", "H_donchian_atr", "D_new_high20_baseline"]


def _pct(v: Optional[float], dp: int = 1) -> str:
    return f"{v * 100:+.{dp}f}%" if v is not None else "N/A"


def _metrics_from_signals(signals: list) -> dict:
    rets = [s.blended_return for s in signals if s.blended_return is not None]
    win = sum(1 for r in rets if r > 0)
    hold_days = [s.hold_days for s in signals if s.hold_days is not None]
    return {
        "n": len(signals),
        "n_closed": len(rets),
        "win_rate": (win / len(rets)) if rets else None,
        "avg_return": (sum(rets) / len(rets)) if rets else None,
        "avg_hold_days": (sum(hold_days) / len(hold_days)) if hold_days else None,
    }


def run_method(
    method: str,
    ohlcv_map: dict[str, pd.DataFrame],
    tickers: list[tuple[str, str, str]],
    start: date,
    end: date,
) -> dict:
    from analysis.backtest.quant_signals import (
        replay_quant,
        replay_quant_bb_exit,
        replay_quant_donchian_atr,
    )

    all_signals = []
    for ticker, name, _sector in tickers:
        df = ohlcv_map.get(ticker)
        if df is None or df.empty:
            continue
        market = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"
        try:
            if method == "F_bb_rsi_volume":
                sigs = replay_quant_bb_exit(ticker, name, df, market, start, end)
            elif method == "G_volatility_breakout":
                sigs = replay_quant(
                    ticker, name, df, market, start, end,
                    entry_key="G_volatility_breakout",
                    hard_stop_pct=0.05, target_pct=0.15, use_ma20_exit=True, use_rsi70_exit=False,
                )
            elif method == "H_donchian_atr":
                sigs = replay_quant_donchian_atr(ticker, name, df, market, start, end, atr_stop_mult=2.0)
            elif method == "D_new_high20_baseline":
                sigs = replay_quant(
                    ticker, name, df, market, start, end,
                    entry_key="D_new_high20",
                    hard_stop_pct=0.05, target_pct=0.15, use_ma20_exit=True, use_rsi70_exit=False,
                )
            else:
                raise ValueError(f"알 수 없는 방법론: {method!r}")
        except Exception as e:
            logger.debug("  %s 재현 실패(무시): %s", ticker, e)
            continue
        all_signals.extend(sigs)

    return _metrics_from_signals(all_signals)


def main() -> None:
    parser = argparse.ArgumentParser(description="신규 방법론 3종(F/G/H) 백테스트")
    parser.add_argument("--method", default="ALL", help=f"방법론 이름 또는 ALL. 가능: {', '.join(ALL_METHODS)}, ALL")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--market", default="ALL", choices=["KOSPI", "KOSDAQ", "ALL"])
    parser.add_argument("--max-tickers", type=int, default=0, help="0=전종목")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--output", default="results/quant_new_methods.csv")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    if start >= end:
        sys.exit("--start 는 --end 보다 이전이어야 합니다")

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

    targets = ALL_METHODS if args.method.upper() == "ALL" else [args.method]
    for m in targets:
        if m not in ALL_METHODS:
            sys.exit(f"알 수 없는 방법론: {m!r}. 가능: {', '.join(ALL_METHODS)}, ALL")

    from analysis.chart_screener import get_all_tickers, fetch_kind_sector_map
    from analysis.backtest.fetch import _fetch_single_ohlcv
    from core.ohlcv_cache import batch_fetch_cached

    logger.info("[quant-new] 종목 목록 조회 중...")
    sector_map = fetch_kind_sector_map()
    all_tickers = get_all_tickers(sector_map=sector_map if sector_map else None)
    if args.market == "KOSPI":
        tickers = [(t, n, s) for t, n, s in all_tickers if t.endswith(".KS")]
    elif args.market == "KOSDAQ":
        tickers = [(t, n, s) for t, n, s in all_tickers if t.endswith(".KQ")]
    else:
        tickers = all_tickers
    if args.max_tickers > 0:
        tickers = tickers[: args.max_tickers]
    logger.info("[quant-new] 대상 티커 %d개", len(tickers))

    fetch_start = start - timedelta(days=400)
    ticker_pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t, _, _ in tickers]

    t0 = time.time()
    logger.info("[quant-new] OHLCV 로드 중 (캐시 우선)...")
    ohlcv_map = batch_fetch_cached(ticker_pairs, fetch_start, end, args.workers, dsn, _fetch_single_ohlcv)
    logger.info("[quant-new] OHLCV 로드 완료 — %.1f초, %d/%d 티커", time.time() - t0, len(ohlcv_map), len(tickers))

    rows = []
    for method in targets:
        logger.info("[quant-new] 방법론 %s 실행 중...", method)
        t1 = time.time()
        m = run_method(method, ohlcv_map, tickers, start, end)
        logger.info("[quant-new] %s 완료 — %.1f초, 신호 %d건 승률 %s",
                     method, time.time() - t1, m["n"], _pct(m["win_rate"]))
        rows.append({"방법론": method, **m})

    df_out = pd.DataFrame(rows)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("[quant-new] 결과 저장: %s", out_path)

    print(f"\n{'='*80}")
    print(f"신규 방법론(F/G/H) 백테스트 ({start}~{end})")
    print(f"{'='*80}")
    print(f"{'방법론':24s} {'신호수':>8s} {'청산완료':>8s} {'승률':>8s} {'평균수익':>10s} {'평균보유일':>10s}")
    for r in rows:
        hd = f"{r['avg_hold_days']:.0f}일" if r["avg_hold_days"] is not None else "N/A"
        print(f"{r['방법론']:24s} {r['n']:>8d} {r['n_closed']:>8d} "
              f"{_pct(r['win_rate']):>8s} {_pct(r['avg_return']):>10s} {hd:>10s}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
