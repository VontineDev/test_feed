"""
run_quant_entry_exit_sweep.py — SCENARIO2 진입/청산 파라미터(RSI 임계값 · 손절폭)
그리드서치.

2026-08-07: 필터 최적화(scripts/run_quant_filter_sweep.py)에서 PER≤18/시총상위200이
문서 원안(PER≤15/시총상위200)보다 우수함을 확인 — 이 유니버스를 고정하고, 이번엔
반대로 진입/청산(RSI 진입 임계값 · RSI 청산 임계값 · 손절폭)만 바꿔가며 비교한다.
필터와 진입/청산을 동시에 스윕하지 않는 이유: 두 축을 한 번에 최적화하면 표본이
과최적화(overfitting)될 위험이 커지고, 원인 분리도 어려워지기 때문 — 단계를
의도적으로 나눴다.

train/val 분리는 하지 않는다(표본 자체가 100~150건 안팎이라 분리하면 무의미 —
AND-1 스윕 사례 참고). 신호 30건 미만 조합은 비교 표에서 제외.

사용법:
    python scripts/run_quant_entry_exit_sweep.py --start 2025-01-02 --end 2026-08-06
    python scripts/run_quant_entry_exit_sweep.py --start 2025-01-02 --end 2026-08-06 \\
        --per-max 15 --mktcap-top-n 200   # 문서 원안 유니버스로 비교하고 싶을 때
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import cast

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))
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

# 문서 원래 값(RSI 30/70, 손절 -7%)을 중심으로 위아래를 넣는다.
RSI_OVERSOLD_GRID = [20.0, 25.0, 30.0, 35.0, 40.0]
RSI_OVERBOUGHT_GRID = [60.0, 65.0, 70.0, 75.0, 80.0]
HARD_STOP_GRID = [0.05, 0.07, 0.09, 0.12]


def main() -> None:
    parser = argparse.ArgumentParser(description="SCENARIO2 진입/청산 파라미터 그리드서치")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--per-max", type=float, default=18.0,
                        help="유니버스 PER 상한 (기본: 필터 스윕 최적값 18)")
    parser.add_argument("--mktcap-top-n", type=int, default=200,
                        help="유니버스 시가총액 상위 N (기본: 200)")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--output", default="results/quant_scenario2_entry_exit_sweep.csv")
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

    from analysis.backtest.fetch import _fetch_single_ohlcv
    from analysis.backtest.quant_signals import replay_quant
    from analysis.chart_screener import fetch_kind_sector_map, get_all_tickers
    from analysis.fundamentals import RatioThresholds, compute_ratios, screen
    from core.ohlcv_cache import batch_fetch_cached, load_listed_shares
    from run_quant_filter_sweep import _rank_by_market_cap

    logger.info("[sweep] 종목 목록 조회 중...")
    sector_map = fetch_kind_sector_map()
    tickers = get_all_tickers(sector_map=sector_map if sector_map else None)
    name_of = {t: n for t, n, _ in tickers}
    logger.info("[sweep] 대상 티커 %d개", len(tickers))

    fetch_start = start - timedelta(days=400)
    ticker_pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t, _, _ in tickers]

    t0 = time.time()
    logger.info("[sweep] OHLCV 로드 중 (캐시 우선)...")
    ohlcv_map = batch_fetch_cached(ticker_pairs, fetch_start, end, args.workers, dsn, _fetch_single_ohlcv)
    logger.info("[sweep] OHLCV 로드 완료 — %.1f초, %d/%d 티커", time.time() - t0, len(ohlcv_map), len(tickers))

    listed_shares = load_listed_shares(dsn)
    ranked_mktcap = _rank_by_market_cap(ohlcv_map, listed_shares, start, end)
    mktcap_universe = {t for t, _ in ranked_mktcap[:args.mktcap_top_n]}

    logger.info("[sweep] 펀더멘털(PER) 로드 중...")
    ratios_df = compute_ratios(dsn)
    per_universe = screen(ratios_df, RatioThresholds(
        pbr_min=None, pbr_max=None, per_min=0.0, per_max=args.per_max,
        roe_min=None, debt_ratio_max=None, revenue_growth_min=None,
    ))

    universe = mktcap_universe & per_universe
    logger.info("[sweep] 유니버스 고정: PER≤%.0f ∩ 시총상위%d = %d종목",
                args.per_max, args.mktcap_top_n, len(universe))

    # 유니버스는 고정이므로 OHLCV를 티커별로 한 번만 슬라이스해두고 재사용.
    universe_ohlcv = {t: ohlcv_map[t] for t in universe if t in ohlcv_map}

    rows = []
    for rsi_oversold in RSI_OVERSOLD_GRID:
        for rsi_overbought in RSI_OVERBOUGHT_GRID:
            for hard_stop in HARD_STOP_GRID:
                all_signals = []
                for ticker, df in universe_ohlcv.items():
                    market = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"
                    try:
                        sigs = replay_quant(
                            ticker, name_of.get(ticker) or ticker, df, market, start, end,
                            entry_key="SCENARIO2",
                            hard_stop_pct=hard_stop, target_pct=None,
                            use_ma20_exit=False, use_rsi70_exit=True,
                            rsi_oversold=rsi_oversold, rsi_overbought=rsi_overbought,
                        )
                    except Exception as e:
                        logger.debug("  %s 재현 실패(무시): %s", ticker, e)
                        continue
                    all_signals.extend(sigs)

                rets = [s.blended_return for s in all_signals if s.blended_return is not None]
                win = sum(1 for r in rets if r > 0)
                row = {
                    "rsi_oversold": rsi_oversold,
                    "rsi_overbought": rsi_overbought,
                    "hard_stop_pct": hard_stop,
                    "n_signals": len(all_signals),
                    "n_closed": len(rets),
                    "win_rate": (win / len(rets)) if rets else None,
                    "avg_return": (sum(rets) / len(rets)) if rets else None,
                }
                rows.append(row)

        logger.info("[sweep] rsi_oversold=%.0f 완료 (%d/%d)",
                    rsi_oversold, RSI_OVERSOLD_GRID.index(rsi_oversold) + 1, len(RSI_OVERSOLD_GRID))

    df_out = pd.DataFrame(rows)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("[sweep] 결과 저장: %s (%d 조합)", out_path, len(rows))

    filtered = cast(pd.DataFrame, df_out[df_out["n_signals"] >= 30])
    reliable = filtered.sort_values(by="avg_return", ascending=False)
    print(f"\n{'='*100}")
    print(f"SCENARIO2 진입/청산 파라미터 스윕 — 유니버스 PER≤{args.per_max:.0f}∩시총상위{args.mktcap_top_n} "
          f"고정, 신호 30건 이상만, 평균수익 내림차순 ({start}~{end})")
    print(f"{'='*100}")
    if reliable.empty:
        print("신호 30건 이상인 조합이 없습니다 — 전체 결과는 CSV 참고.")
    else:
        cols = ["rsi_oversold", "rsi_overbought", "hard_stop_pct", "n_signals", "win_rate", "avg_return"]
        print(reliable[cols].head(20).to_string(index=False))
    print(f"{'='*100}")
    print("(참고) 문서 원안: RSI 진입 30 / RSI 청산 70 / 손절 -7%")


if __name__ == "__main__":
    main()
