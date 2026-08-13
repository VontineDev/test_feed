"""
run_qvm_universe_baseline.py — QVM 유니버스 buy&hold MDD 베이스라인

2026-08-12: QVM 전략(시총상위200∩QVM상위20~30%)의 MDD(-12.6~-18.4%)가 FUNNEL-1/
SCORE-1(-35.9~-39.5%)보다 훨씬 얕은데, 이게 "RSI 진입/청산 타이밍" 덕인지
아니면 "QVM이 원래 저변동성 대형우량주만 고른다" 덕인지 분리가 안 돼 있었다.
같은 유니버스를 그냥 동일비중 매수보유(일별 리밸런싱 근사)했을 때의 MDD를
계산해 두 효과를 나눈다 — (유니버스 자체 MDD) vs (전략 MDD)의 차이가 클수록
"타이밍/청산 로직이 낙폭을 방어했다"는 뜻이고, 비슷하면 "유니버스 선택 자체가
낙폭을 줄였다"는 뜻이다.

계산 방식: 유니버스 전 종목의 일별 수익률을 동일비중 평균(매일 리밸런싱한다는
근사 — 실제 buy&hold보다 약간 더 안정적으로 나올 수 있음, 그래도 전략의
포지션 사이징 방식과 일관되게 맞추기 위한 단순화)한 뒤 누적해 MDD 계산.

사용법:
    python scripts/run_qvm_universe_baseline.py --start 2025-01-02 --end 2026-08-06
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


def compute_buyhold_mdd(ohlcv_map: dict, universe: set[str], start: date, end: date):
    """유니버스를 동일비중 일별 리밸런싱했다고 가정한 buy&hold MDD."""
    from analysis.backtest.helpers import _compute_mdd

    frames = []
    for t in universe:
        df = ohlcv_map.get(t)
        if df is None or df.empty:
            continue
        window = df[(df.index.date >= start) & (df.index.date <= end)]  # type: ignore[attr-defined]
        if window.empty:
            continue
        rets = window["Close"].pct_change().dropna()
        rets.name = t
        frames.append(rets)
    if not frames:
        return None, 0

    combined = pd.concat(frames, axis=1)
    portfolio_daily = cast(pd.Series, combined.mean(axis=1, skipna=True))
    mdd = _compute_mdd(portfolio_daily.tolist())
    return mdd, len(frames)


def main() -> None:
    parser = argparse.ArgumentParser(description="QVM 유니버스 buy&hold MDD 베이스라인")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--workers", type=int, default=8)
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

    logger.info("[baseline] 종목 목록 조회 중...")
    sector_map = fetch_kind_sector_map()
    tickers = get_all_tickers(sector_map=sector_map if sector_map else None)

    fetch_start = start - timedelta(days=400)
    ticker_pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t, _, _ in tickers]

    t0 = time.time()
    logger.info("[baseline] OHLCV 로드 중 (캐시 우선)...")
    ohlcv_map = batch_fetch_cached(ticker_pairs, fetch_start, end, args.workers, dsn, _fetch_single_ohlcv)
    logger.info("[baseline] OHLCV 로드 완료 — %.1f초, %d/%d 티커", time.time() - t0, len(ohlcv_map), len(tickers))

    listed_shares = load_listed_shares(dsn)
    universe_mktcap200 = _select_universe(ohlcv_map, listed_shares, start, end, "mktcap_top200")

    logger.info("[baseline] 펀더멘털 + 모멘텀 로드 중...")
    ratios_df = compute_ratios(dsn)
    momentum_df = load_momentum(dsn)
    qvm_df = compute_qvm_score(ratios_df, momentum_df)

    rows = []
    for top_pct in (0.10, 0.20, 0.30):
        qvm_universe = screen_qvm_top_pct(qvm_df, top_pct)
        universe = universe_mktcap200 & qvm_universe
        mdd, n = compute_buyhold_mdd(ohlcv_map, universe, start, end)
        logger.info("[baseline] QVM상위%d%% 유니버스(%d종목) buy&hold MDD: %s",
                    int(top_pct * 100), n, _pct(mdd))
        rows.append({"유니버스": f"QVM상위{int(top_pct*100)}%∩시총200", "종목수": n, "buyhold_mdd": mdd})

    # 비교 기준: 시총상위200 전체(QVM 필터 없음)와 KOSPI200 자체도 같이 계산
    mdd_all200, n_all200 = compute_buyhold_mdd(ohlcv_map, universe_mktcap200, start, end)
    rows.append({"유니버스": "시총상위200(QVM필터없음)", "종목수": n_all200, "buyhold_mdd": mdd_all200})

    df_out = pd.DataFrame(rows)
    out_path = Path("results/qvm_universe_baseline_mdd.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("[baseline] 결과 저장: %s", out_path)

    print(f"\n{'='*80}")
    print(f"QVM 유니버스 buy&hold MDD 베이스라인 ({start}~{end})")
    print(f"{'='*80}")
    print(f"{'유니버스':30s} {'종목수':>6s} {'buy&hold MDD':>14s}")
    for r in rows:
        print(f"{r['유니버스']:30s} {r['종목수']:>6d} {_pct(r['buyhold_mdd']):>14s}")
    print(f"{'='*80}")
    print("비교: QVM 전략(RSI타이밍) MDD — 상위20% -12.6%, 상위30% -18.4%")
    print("      FUNNEL-1/SCORE-1(모의투자) MDD — -35.9% / -39.5%")


if __name__ == "__main__":
    main()
