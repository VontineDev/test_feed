"""
run_quant_qvm_factor_ablation.py — QVM 3팩터(퀄리티/밸류/모멘텀) 분해 검증

2026-08-12: QVM 복합팩터(방법론4)가 SCENARIO5(PBR단독)를 상회했는데, 6단계에서
"필터를 더 결합한다고 좋아지는 게 아니라 어떤 단일 팩터냐가 중요"했던 선례가
있어 QVM 3팩터 중 실제로 엣지를 만드는 게 무엇인지 확인한다. compute_qvm_score의
factors 파라미터로 단일/2개조합/전체 7가지 조합을 전부 비교(유니버스는 시총
상위200∩top_pct고정 — 백테스트 결과에서 성과가 좋았던 상위20% 사용, 매매타이밍은
QVM 백테스트와 동일하게 2안 원안청산 RSI30/70/-7% 고정).

사용법:
    python scripts/run_quant_qvm_factor_ablation.py --start 2025-01-02 --end 2026-08-06
"""
from __future__ import annotations

import argparse
import itertools
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

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


def build_factor_combos() -> list[tuple[str, ...]]:
    """단일1~3개 조합 전부(순서 무관, 순수 함수)."""
    from analysis.fundamentals import QVM_FACTORS

    combos: list[tuple[str, ...]] = []
    for r in range(1, len(QVM_FACTORS) + 1):
        combos.extend(itertools.combinations(QVM_FACTORS, r))
    return combos


def main() -> None:
    parser = argparse.ArgumentParser(description="QVM 3팩터 분해 검증")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--top-pct", type=float, default=0.20,
                        help="QVM 상위 컷오프(기본 20%% — 본 백테스트에서 최고 성과 구간)")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--output", default="results/quant_qvm_factor_ablation.csv")
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
    from analysis.chart_screener import fetch_kind_sector_map, get_all_tickers
    from analysis.fundamentals import compute_qvm_score, compute_ratios, load_momentum, screen_qvm_top_pct
    from core.ohlcv_cache import batch_fetch_cached, load_listed_shares
    from run_quant_backtest import _pct, _select_universe
    from run_quant_qvm_backtest import _run_scenario2_with_mdd

    logger.info("[qvm-ablation] 종목 목록 조회 중...")
    sector_map = fetch_kind_sector_map()
    tickers = get_all_tickers(sector_map=sector_map if sector_map else None)
    logger.info("[qvm-ablation] 대상 티커 %d개", len(tickers))

    fetch_start = start - timedelta(days=400)
    ticker_pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t, _, _ in tickers]

    t0 = time.time()
    logger.info("[qvm-ablation] OHLCV 로드 중 (캐시 우선)...")
    ohlcv_map = batch_fetch_cached(ticker_pairs, fetch_start, end, args.workers, dsn, _fetch_single_ohlcv)
    logger.info("[qvm-ablation] OHLCV 로드 완료 — %.1f초, %d/%d 티커", time.time() - t0, len(ohlcv_map), len(tickers))

    listed_shares = load_listed_shares(dsn)
    universe_mktcap200 = _select_universe(ohlcv_map, listed_shares, start, end, "mktcap_top200")

    logger.info("[qvm-ablation] 펀더멘털 + 모멘텀 로드 중...")
    ratios_df = compute_ratios(dsn)
    momentum_df = load_momentum(dsn)

    rows = []
    for combo in build_factor_combos():
        qvm_df = compute_qvm_score(ratios_df, momentum_df, factors=combo)
        qvm_universe = screen_qvm_top_pct(qvm_df, args.top_pct)
        universe = universe_mktcap200 & qvm_universe
        name = "+".join(combo)
        logger.info("[qvm-ablation] %s 유니버스: QVM상위%d(%d종목 스코어계산가능) ∩ 시총200 = %d종목",
                    name, len(qvm_universe), len(qvm_df), len(universe))

        m = _run_scenario2_with_mdd(universe, ohlcv_map, tickers, start, end)
        logger.info("[qvm-ablation] %s 완료 — 신호 %d건, 승률 %s, 평균 %s, MDD %s",
                    name, m["n"], _pct(m["win_rate"]), _pct(m["avg_return"]), _pct(m["mdd"]))
        rows.append({"팩터조합": name, "유니버스종목수": len(universe), **m})

    df_out = pd.DataFrame(rows)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("[qvm-ablation] 결과 저장: %s", out_path)

    print(f"\n{'='*100}")
    print(f"QVM 3팩터 분해 검증 (시총상위200∩QVM상위{args.top_pct*100:.0f}%, {start}~{end})")
    print(f"{'='*100}")
    print(f"{'팩터조합':20s} {'종목수':>6s} {'신호':>6s} {'승률':>8s} {'평균수익':>10s} {'MDD':>8s}")
    for r in sorted(rows, key=lambda x: (x["avg_return"] is None, -(x["avg_return"] or 0))):
        print(f"{r['팩터조합']:20s} {r['유니버스종목수']:>6d} {r['n']:>6d} "
              f"{_pct(r['win_rate']):>8s} {_pct(r['avg_return']):>10s} {_pct(r['mdd']):>8s}")
    print(f"{'='*100}")


if __name__ == "__main__":
    main()
