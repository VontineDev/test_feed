"""
run_quant_filter_sweep.py — SCENARIO2 종목선택 필터(PER 상한 / 시가총액 유니버스
크기) 그리드서치.

2026-08-06: TechnicalQuant.md 2안(시가총액 상위200 + PER≤15)만 세 가지 검증
방식에서 일관되게 양호(승률43%, +2.9%, 신호100건)한 것으로 확인 — 문서가
명시한 두 숫자(PER 15, 상위 200)가 최적값인지, 다른 컷오프가 더 나은지 확인.

매매타이밍(RSI 30 진입/RSI 70 익절/-7% 손절)은 문서 그대로 고정하고, "필터"만
(PER 상한, 시가총액 유니버스 크기) 바꿔가며 비교한다 — 진입/청산 로직까지
바꾸면 "2안을 최적화"가 아니라 "다른 전략을 만드는" 것이 되므로 범위를
의도적으로 좁혔다.

train/val 분리는 하지 않는다 — 2안 전체 신호가 100건 안팎이라 분리하면
표본이 통계적으로 무의미해진다(AND-1 스윕 사례 참고, project_compose_strategies
메모리). 대신 전체 구간 결과에 신호수를 항상 같이 표시해 소표본 조합을
가려낼 수 있게 한다.

사용법:
    python scripts/run_quant_filter_sweep.py --start 2025-01-02 --end 2026-08-06
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, cast

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

# 문서 원래 값(PER 15, 상위 200)을 중심으로 위아래 컷오프를 넣는다.
PER_MAX_GRID = [10.0, 12.0, 15.0, 18.0, 20.0, 25.0]
MKTCAP_TOPN_GRID = [100, 150, 200, 300, 500]


def _rank_by_market_cap(
    ohlcv_map: dict[str, pd.DataFrame],
    listed_shares: dict[str, int],
    start: date,
    end: date,
) -> list[tuple[str, float]]:
    """기간 평균 시가총액 내림차순 (ticker, market_cap) 리스트.

    한 번만 계산해두고 top_n을 슬라이싱만 다르게 해서 그리드서치 비용을 줄인다.
    """
    stats: dict[str, float] = {}
    for ticker, df in ohlcv_map.items():
        window = df[(df.index.date >= start) & (df.index.date <= end)]  # type: ignore[attr-defined]
        if window.empty:
            continue
        shares = listed_shares.get(ticker)
        if not shares:
            continue
        avg_close = float(window["Close"].mean())
        stats[ticker] = avg_close * shares
    return sorted(stats.items(), key=lambda kv: kv[1], reverse=True)


def _pct(v: Optional[float], dp: int = 1) -> str:
    return f"{v * 100:+.{dp}f}%" if v is not None else "N/A"


def main() -> None:
    parser = argparse.ArgumentParser(description="SCENARIO2 필터(PER상한/시총유니버스) 그리드서치")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--output", default="results/quant_scenario2_filter_sweep.csv")
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
    from analysis.backtest.helpers import _compute_group_metrics
    from analysis.backtest.quant_signals import replay_quant
    from analysis.chart_screener import fetch_kind_sector_map, get_all_tickers
    from analysis.fundamentals import RatioThresholds, compute_ratios, screen
    from core.ohlcv_cache import batch_fetch_cached, load_listed_shares

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
    logger.info("[sweep] 시가총액 랭킹 계산 완료 — %d종목", len(ranked_mktcap))

    logger.info("[sweep] 펀더멘털(PER) 로드 중...")
    ratios_df = compute_ratios(dsn)
    logger.info("[sweep] 펀더멘털 비율 계산 완료 — %d종목", len(ratios_df))

    rows = []
    for per_max in PER_MAX_GRID:
        # PER 하한 0(적자기업 제외)은 문서 취지를 따라 전 조합 공통 고정.
        per_universe = screen(ratios_df, RatioThresholds(
            pbr_min=None, pbr_max=None, per_min=0.0, per_max=per_max,
            roe_min=None, debt_ratio_max=None, revenue_growth_min=None,
        ))
        for top_n in MKTCAP_TOPN_GRID:
            mktcap_universe = {t for t, _ in ranked_mktcap[:top_n]}
            universe = mktcap_universe & per_universe

            all_signals = []
            for ticker in universe:
                df = ohlcv_map.get(ticker)
                if df is None or df.empty:
                    continue
                market = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"
                try:
                    sigs = replay_quant(
                        ticker, name_of.get(ticker) or ticker, df, market, start, end,
                        entry_key="SCENARIO2",
                        hard_stop_pct=0.07, target_pct=None,
                        use_ma20_exit=False, use_rsi70_exit=True,
                    )
                except Exception as e:
                    logger.debug("  %s 재현 실패(무시): %s", ticker, e)
                    continue
                all_signals.extend(sigs)

            rets = [s.blended_return for s in all_signals if s.blended_return is not None]
            win = sum(1 for r in rets if r > 0)
            m = _compute_group_metrics(all_signals, rf_annual=0.03)
            row = {
                "per_max": per_max,
                "mktcap_top_n": top_n,
                "universe_size": len(universe),
                "n_signals": len(all_signals),
                "n_closed": len(rets),
                "win_rate": (win / len(rets)) if rets else None,
                "avg_return": (sum(rets) / len(rets)) if rets else None,
                "sharpe28d": m.sharpe_28d,
            }
            rows.append(row)
            logger.info(
                "[sweep] PER≤%.0f, 시총상위%d → 유니버스%d종목, 신호%d건, 승률%s, 평균%s",
                per_max, top_n, len(universe), len(all_signals),
                _pct(row["win_rate"]), _pct(row["avg_return"]),
            )

    df_out = pd.DataFrame(rows)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("[sweep] 결과 저장: %s", out_path)

    # 표본 30건 이상인 조합만 정렬 비교(그 미만은 AND-1 스윕 사례처럼 노이즈일 위험 큼)
    filtered = cast(pd.DataFrame, df_out[df_out["n_signals"] >= 30])
    reliable = filtered.sort_values(by="avg_return", ascending=False)
    print(f"\n{'='*90}")
    print(f"SCENARIO2 필터 스윕 — 신호 30건 이상 조합만, 평균수익 내림차순 ({start}~{end})")
    print(f"{'='*90}")
    if reliable.empty:
        print("신호 30건 이상인 조합이 없습니다 — 전체 결과는 CSV 참고.")
    else:
        cols = ["per_max", "mktcap_top_n", "universe_size", "n_signals", "win_rate", "avg_return", "sharpe28d"]
        print(reliable[cols].to_string(index=False))
    print(f"{'='*90}")
    print("(참고) 문서 원안: PER≤15, 시총상위200")


if __name__ == "__main__":
    main()
