"""
run_quant_qvm_walkforward.py — QVM(+모멘텀 단독) 폴드별 성과 일관성 검증

2026-08-12: 방법론5(run_quant_walkforward.py)는 SCENARIO2의 RSI청산/손절폭을
폴드별 train 구간에서 그리드서치해 "필터+청산 순차 최적화"의 과최적화를
검증했다. QVM(+팩터분해로 새로 발견한 momentum 단독)은 애초에 청산 파라미터를
전혀 건드리지 않은 원안(RSI30/70/-7%)만 썼으므로 같은 방식의 그리드서치
과최적화 위험은 없다 — 대신 확인해야 할 건 다른 문제: **QVM 유니버스 자체가
"현재" 재무/모멘텀 스냅샷으로 한 번만 계산돼 전체 백테스트 기간에 그대로
적용된다**(fundamentals.py 모듈독스트링의 lookahead 주의사항과 동일 — 과거
시점 재계산 불가)는 한계가 있다. 그래서 이 스크립트는 진짜 train/test
파라미터 최적화 검증이 아니라, **고정된 유니버스+고정된 청산으로 시간 구간을
나눠도 성과(승률·평균수익)가 어느 한 구간에만 몰려있지 않고 골고루 나오는지**
확인하는 기간별 일관성(sub-period consistency) 체크다 — run_quant_walkforward.py와
동일한 4개 폴드(test 구간만 사용, train에서 파라미터를 고르지 않음)를 재사용.

사용법:
    python scripts/run_quant_qvm_walkforward.py --start 2025-01-02 --end 2026-08-06
"""
from __future__ import annotations

import argparse
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


def build_variants() -> list[dict]:
    """검증 대상: 3팩터 QVM(top20/30%) + 팩터분해에서 발견한 모멘텀 단독(top20/30%)."""
    return [
        {"name": "QVM3factor_top20", "factors": ("quality", "value", "momentum"), "top_pct": 0.20},
        {"name": "QVM3factor_top30", "factors": ("quality", "value", "momentum"), "top_pct": 0.30},
        {"name": "momentum_only_top20", "factors": ("momentum",), "top_pct": 0.20},
        {"name": "momentum_only_top30", "factors": ("momentum",), "top_pct": 0.30},
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="QVM/모멘텀단독 폴드별 성과 일관성 검증")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD (전체구간 시작 — 유니버스 산정용)")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD (전체구간 끝)")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--output", default="results/quant_qvm_walkforward.csv")
    args = parser.parse_args()

    overall_start = date.fromisoformat(args.start)
    overall_end = date.fromisoformat(args.end)

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
    from run_quant_walkforward import build_folds

    logger.info("[qvm-wf] 종목 목록 조회 중...")
    sector_map = fetch_kind_sector_map()
    tickers = get_all_tickers(sector_map=sector_map if sector_map else None)
    logger.info("[qvm-wf] 대상 티커 %d개", len(tickers))

    fetch_start = overall_start - timedelta(days=400)
    ticker_pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t, _, _ in tickers]

    t0 = time.time()
    logger.info("[qvm-wf] OHLCV 로드 중 (캐시 우선)...")
    ohlcv_map = batch_fetch_cached(ticker_pairs, fetch_start, overall_end, args.workers, dsn, _fetch_single_ohlcv)
    logger.info("[qvm-wf] OHLCV 로드 완료 — %.1f초, %d/%d 티커", time.time() - t0, len(ohlcv_map), len(tickers))

    listed_shares = load_listed_shares(dsn)
    # 유니버스는 전체 구간(overall_start~overall_end) 시총 랭킹 기준으로 한 번만
    # 산정 — QVM 필터 자체가 "현재" 재무/모멘텀 스냅샷이라 폴드별로 다시 계산할
    # 방법이 없다(위 모듈 docstring 참고). 폴드마다 달라지는 건 test 기간뿐이다.
    universe_mktcap200 = _select_universe(ohlcv_map, listed_shares, overall_start, overall_end, "mktcap_top200")

    logger.info("[qvm-wf] 펀더멘털 + 모멘텀 로드 중...")
    ratios_df = compute_ratios(dsn)
    momentum_df = load_momentum(dsn)

    folds = build_folds()

    rows = []
    for v in build_variants():
        qvm_df = compute_qvm_score(ratios_df, momentum_df, factors=v["factors"])
        qvm_universe = screen_qvm_top_pct(qvm_df, v["top_pct"])
        universe = universe_mktcap200 & qvm_universe
        logger.info("[qvm-wf] === %s: 유니버스 %d종목(전체구간 고정) ===", v["name"], len(universe))

        for fold in folds:
            test_start, test_end = (date.fromisoformat(x) for x in fold["test"])
            m = _run_scenario2_with_mdd(universe, ohlcv_map, tickers, test_start, test_end)
            logger.info("[qvm-wf] %s / %s(test %s~%s): 신호%d 승률%s 평균%s MDD%s",
                        v["name"], fold["name"], test_start, test_end,
                        m["n"], _pct(m["win_rate"]), _pct(m["avg_return"]), _pct(m["mdd"]))
            rows.append({
                "변형": v["name"], "유니버스종목수": len(universe), "폴드": fold["name"],
                "test시작": test_start, "test종료": test_end, **m,
            })

    df_out = pd.DataFrame(rows)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("[qvm-wf] 결과 저장: %s", out_path)

    print(f"\n{'='*110}")
    print("QVM/모멘텀단독 폴드별 성과 일관성 (고정 유니버스+고정청산, 재최적화 없음)")
    print(f"{'='*110}")
    print(f"{'변형':22s}{'폴드':8s}{'신호':>6s}{'승률':>9s}{'평균수익':>10s}{'MDD':>9s}")
    for r in rows:
        print(f"{r['변형']:22s}{r['폴드']:8s}{r['n']:>6d}{_pct(r['win_rate']):>9s}"
              f"{_pct(r['avg_return']):>10s}{_pct(r['mdd']):>9s}")
    print(f"{'='*110}")


if __name__ == "__main__":
    main()
