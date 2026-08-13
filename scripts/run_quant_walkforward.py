"""
run_quant_walkforward.py — 방법론5: Walk-forward 검증 하네스

2026-08-11: TechnicalQuant.md 신규 방법론5. 기존 5·7단계(필터스윕→청산스윕
순차 최적화, +16.82%까지 도달)는 "이 특정 백테스트 구간(2025-01~2026-08)에
과최적화됐을 위험이 크다"는 자체 경고가 붙어 있었다 — 이 경고를 정량적으로
검증하기 위해 롤링 walk-forward를 도입한다.

방법: 전체 기간(2025-01-02~2026-08-06, 19개월)을 4개 폴드로 나눈다(train은
매 폴드 확장, test는 3~4개월 비중첩 구간 — 폴드4의 test에는 2026-06~08
급락장이 포함돼 상승장/급락장 양쪽에서 파라미터 안정성을 볼 수 있다).
각 폴드의 train 구간에서만 SCENARIO2 청산 그리드서치(RSI청산×손절폭, 진입
RSi30은 5·7단계와 동일하게 고정)를 축소판으로 돌려 최고 조합을 고르고,
그 조합을 그대로(재최적화 없이) test 구간에 적용해 성과가 유지되는지 확인한다.
필터(유니버스)는 5·7단계에서 이미 검증된 PER≤18 / PBR 0.2~1.0 2종을 그대로
쓴다(필터 자체까지 폴드마다 재탐색하면 탐색공간이 너무 커져 표본(폴드당
신호 수십 건)에 비해 자유도가 과도해진다 — 청산 파라미터만 walk-forward
대상으로 좁힌 것도 5·7단계와 동일한 "한 번에 한 축만" 원칙).

사용법:
    python scripts/run_quant_walkforward.py
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

# 축소 그리드 — 5·7단계 전체 그리드(RSI진입5×RSI청산5×손절4=100조합)를 폴드마다
# 돌리면 시간이 너무 오래 걸린다. 5·7단계에서 이미 확인된 "청산을 넓히면
# 개선된다"는 방향성 축(원안 70/-7% vs 최적화 80/-12%, 그 중간 75/-9%)만 본다.
RSI_OVERBOUGHT_GRID = [70.0, 75.0, 80.0]
HARD_STOP_GRID = [0.07, 0.09, 0.12]
MIN_SIGNALS_TRAIN = 15  # 폴드당 표본이 전체 구간(30건 기준)보다 작을 수밖에 없어 완화


def build_folds() -> list[dict]:
    """확장 train + 비중첩 test 폴드 4개(순수 함수 — DB 접근 없음)."""
    return [
        {"name": "Fold1", "train": ("2025-01-02", "2025-06-30"), "test": ("2025-07-01", "2025-09-30")},
        {"name": "Fold2", "train": ("2025-01-02", "2025-09-30"), "test": ("2025-10-01", "2025-12-31")},
        {"name": "Fold3", "train": ("2025-01-02", "2025-12-31"), "test": ("2026-01-01", "2026-03-31")},
        {"name": "Fold4", "train": ("2025-01-02", "2026-03-31"), "test": ("2026-04-01", "2026-08-06")},
    ]


def build_filters() -> dict:
    """5·7단계에서 검증된 두 유니버스 필터(순수 함수)."""
    from analysis.fundamentals import RatioThresholds
    return {
        "PER18": RatioThresholds(
            pbr_min=None, pbr_max=None, per_min=0.0, per_max=18.0,
            roe_min=None, debt_ratio_max=None, revenue_growth_min=None,
        ),
        "PBR": RatioThresholds(
            pbr_min=0.2, pbr_max=1.0, per_min=None, per_max=None,
            roe_min=None, debt_ratio_max=None, revenue_growth_min=None,
        ),
    }


def _run_signals(universe_ohlcv, name_of, start, end, rsi_overbought, hard_stop):
    from analysis.backtest.quant_signals import replay_quant

    all_signals = []
    for ticker, df in universe_ohlcv.items():
        market = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"
        try:
            sigs = replay_quant(
                ticker, name_of.get(ticker) or ticker, df, market, start, end,
                entry_key="SCENARIO2",
                hard_stop_pct=hard_stop, target_pct=None,
                use_ma20_exit=False, use_rsi70_exit=True,
                rsi_oversold=30.0, rsi_overbought=rsi_overbought,
            )
        except Exception as e:
            logger.debug("  %s 재현 실패(무시): %s", ticker, e)
            continue
        all_signals.extend(sigs)
    rets = [s.blended_return for s in all_signals if s.blended_return is not None]
    win = sum(1 for r in rets if r > 0)
    return {
        "n": len(all_signals),
        "win_rate": (win / len(rets)) if rets else None,
        "avg_return": (sum(rets) / len(rets)) if rets else None,
    }


def _pct(v, dp: int = 1) -> str:
    return f"{v * 100:+.{dp}f}%" if v is not None else "N/A"


def main() -> None:
    parser = argparse.ArgumentParser(description="방법론5: SCENARIO2 청산 파라미터 walk-forward 검증")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--output", default="results/quant_walkforward.csv")
    args = parser.parse_args()

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
    from analysis.fundamentals import compute_ratios, screen
    from core.ohlcv_cache import batch_fetch_cached, load_listed_shares
    from run_quant_filter_sweep import _rank_by_market_cap

    folds = build_folds()
    overall_start = date.fromisoformat(folds[0]["train"][0])
    overall_end = date.fromisoformat(folds[-1]["test"][1])

    logger.info("[wf] 종목 목록 조회 중...")
    sector_map = fetch_kind_sector_map()
    tickers = get_all_tickers(sector_map=sector_map if sector_map else None)
    name_of = {t: n for t, n, _ in tickers}
    logger.info("[wf] 대상 티커 %d개", len(tickers))

    fetch_start = overall_start - timedelta(days=400)
    ticker_pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t, _, _ in tickers]

    t0 = time.time()
    logger.info("[wf] OHLCV 로드 중 (캐시 우선, 전체 구간 %s~%s)...", fetch_start, overall_end)
    ohlcv_map = batch_fetch_cached(ticker_pairs, fetch_start, overall_end, args.workers, dsn, _fetch_single_ohlcv)
    logger.info("[wf] OHLCV 로드 완료 — %.1f초, %d/%d 티커", time.time() - t0, len(ohlcv_map), len(tickers))

    listed_shares = load_listed_shares(dsn)
    ratios_df = compute_ratios(dsn)
    filters = build_filters()

    rows = []
    for fold in folds:
        train_start, train_end = (date.fromisoformat(x) for x in fold["train"])
        test_start, test_end = (date.fromisoformat(x) for x in fold["test"])
        logger.info("[wf] === %s: train %s~%s / test %s~%s ===",
                    fold["name"], train_start, train_end, test_start, test_end)

        # 유니버스는 train 구간 정보만으로 산정(그 시점까지 알 수 있었던 정보로
        # 종목을 고른다는 walk-forward 원칙 — 5·7단계처럼 전체 구간 평균으로
        # 랭킹을 매기면 test 구간 정보가 유니버스 선정에 새어 들어간다).
        ranked_mktcap = _rank_by_market_cap(ohlcv_map, listed_shares, train_start, train_end)
        mktcap_universe = {t for t, _ in ranked_mktcap[:200]}

        for filter_name, thresholds in filters.items():
            fund_universe = screen(ratios_df, thresholds)
            universe = mktcap_universe & fund_universe
            universe_ohlcv = {t: ohlcv_map[t] for t in universe if t in ohlcv_map}
            logger.info("[wf] %s/%s 유니버스: %d종목", fold["name"], filter_name, len(universe))

            # train 그리드서치
            best = None
            for rsi_ob in RSI_OVERBOUGHT_GRID:
                for hard_stop in HARD_STOP_GRID:
                    m = _run_signals(universe_ohlcv, name_of, train_start, train_end, rsi_ob, hard_stop)
                    if m["n"] < MIN_SIGNALS_TRAIN or m["avg_return"] is None:
                        continue
                    if best is None or m["avg_return"] > best["avg_return"]:
                        best = {**m, "rsi_overbought": rsi_ob, "hard_stop_pct": hard_stop}

            if best is None:
                logger.warning("[wf] %s/%s: train 신호 %d건 미만 조합만 있어 최적화 불가 — 스킵",
                                fold["name"], filter_name, MIN_SIGNALS_TRAIN)
                rows.append({
                    "폴드": fold["name"], "필터": filter_name, "유니버스종목수": len(universe),
                    "최적RSI청산": None, "최적손절": None,
                    "train_신호": None, "train_승률": None, "train_평균수익": None,
                    "test_신호": None, "test_승률": None, "test_평균수익": None,
                    "성과유지": None,
                })
                continue

            # 같은 유니버스·같은 파라미터를 test 구간에 그대로(재최적화 없이) 적용
            test_m = _run_signals(universe_ohlcv, name_of, test_start, test_end,
                                   best["rsi_overbought"], best["hard_stop_pct"])

            degrade = None
            if best["avg_return"] is not None and test_m["avg_return"] is not None:
                degrade = test_m["avg_return"] - best["avg_return"]

            logger.info("[wf] %s/%s 최적(RSI청산%.0f/손절-%.0f%%): train 신호%d 승률%s 평균%s "
                        "→ test 신호%d 승률%s 평균%s",
                        fold["name"], filter_name, best["rsi_overbought"], best["hard_stop_pct"] * 100,
                        best["n"], _pct(best["win_rate"]), _pct(best["avg_return"]),
                        test_m["n"], _pct(test_m["win_rate"]), _pct(test_m["avg_return"]))

            rows.append({
                "폴드": fold["name"], "필터": filter_name, "유니버스종목수": len(universe),
                "최적RSI청산": best["rsi_overbought"], "최적손절": best["hard_stop_pct"],
                "train_신호": best["n"], "train_승률": best["win_rate"], "train_평균수익": best["avg_return"],
                "test_신호": test_m["n"], "test_승률": test_m["win_rate"], "test_평균수익": test_m["avg_return"],
                "성과변화(test-train)": degrade,
                "성과유지": (test_m["avg_return"] is not None and test_m["avg_return"] > 0),
            })

    df_out = pd.DataFrame(rows)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("[wf] 결과 저장: %s", out_path)

    print(f"\n{'='*120}")
    print("방법론5: SCENARIO2 청산 파라미터 walk-forward 검증 (train에서 찾은 최적 조합을 test에 그대로 적용)")
    print(f"{'='*120}")
    print(f"{'폴드':8s}{'필터':8s}{'RSI청산':>8s}{'손절':>6s}"
          f"{'train신호':>10s}{'train승률':>10s}{'train평균':>10s}"
          f"{'test신호':>9s}{'test승률':>9s}{'test평균':>10s}{'유지':>6s}")
    for r in rows:
        rsi_ob = f"{r['최적RSI청산']:.0f}" if r["최적RSI청산"] is not None else "N/A"
        stop = f"-{r['최적손절']*100:.0f}%" if r["최적손절"] is not None else "N/A"
        keep = "O" if r["성과유지"] else ("X" if r["성과유지"] is not None else "N/A")
        print(f"{r['폴드']:8s}{r['필터']:8s}{rsi_ob:>8s}{stop:>6s}"
              f"{str(r['train_신호']):>10s}{_pct(r['train_승률']):>10s}{_pct(r['train_평균수익']):>10s}"
              f"{str(r['test_신호']):>9s}{_pct(r['test_승률']):>9s}{_pct(r['test_평균수익']):>10s}{keep:>6s}")
    print(f"{'='*120}")


if __name__ == "__main__":
    main()
