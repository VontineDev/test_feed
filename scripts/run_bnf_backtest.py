"""
run_bnf_backtest.py — BNF_TradingModel.md(코테카와 타카시 역추세 모델) 백테스트 CLI

2026-08-26: 사용자 제공 문서(BNF_TradingModel.md)의 매매 규칙을 검증하기 위해
도입. analysis/backtest/quant_signals.py의 replay_bnf를 그대로 사용 —
로직 자체는 그쪽에, 이 스크립트는 유니버스 필터링 + 종목 순회 + 집계만 담당
(run_quant_backtest.py와 동일한 역할 분담).

문서의 종목선택 조건(12~16줄) 중 "뉴스/테마 집중 섹터"는 이 저장소에 종목별
테마·뉴스 매핑 데이터가 없어 제외 — 거래대금/변동성 두 기술적 조건만
--min-turnover/--min-atr-pct로 근사 적용한다(기본값 0 = 필터 없음, 문서
원안 그대로 전종목 대상 검증부터 시작하도록).

사용법:
    # 기본값(대형주/상승장 프리셋: 이격도 -22.5%)으로 전체 시장 검증
    python scripts/run_bnf_backtest.py --start 2025-01-02 --end 2026-08-06

    # 중소형주/하락장 프리셋(이격도 -32.5%)
    python scripts/run_bnf_backtest.py --start 2025-01-02 --end 2026-08-06 --preset small

    # 이격도 임계값 직접 지정 + 유니버스 필터(거래대금 5억 이상, ATR14 3% 이상)
    python scripts/run_bnf_backtest.py --start 2025-01-02 --end 2026-08-06 \
        --disc-threshold -0.28 --min-turnover 500000000 --min-atr-pct 0.03
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

# 문서 3줄: 대형우량주/상승장 -20%~-25%, 중소형주·가상자산/하락장 -30%~-35%.
# 프리셋은 각 구간의 중간값을 기본으로 쓴다(직접 --disc-threshold를 주면 우선).
PRESET_DISC_THRESHOLD = {"large": -0.225, "small": -0.325}


def _pct(v: float | None, dp: int = 1) -> str:
    return f"{v * 100:+.{dp}f}%" if v is not None else "N/A"


def _select_universe(
    ohlcv_map: dict[str, pd.DataFrame],
    start: date,
    end: date,
    min_avg_turnover: float,
    min_atr_pct: float,
) -> set[str] | None:
    """BNF 유니버스 필터(문서 12~16줄 中 거래량/변동성 2조건).
    거래대금(=Close×Volume 기간평균) min_avg_turnover 이상 AND
    (ATR14/Close 기간평균) min_atr_pct 이상인 종목만 남긴다.
    두 값 모두 0이면(기본값) 필터를 적용하지 않고 None을 반환 — 문서 원안이
    아직 검증 안 된 상태에서 유니버스부터 좁히면 신호가 너무 적어질 수 있어
    기본은 꺼둔다."""
    if min_avg_turnover <= 0 and min_atr_pct <= 0:
        return None

    from analysis.backtest.quant_signals import compute_indicators

    universe: set[str] = set()
    for ticker, df in ohlcv_map.items():
        window = df[(df.index.date >= start) & (df.index.date <= end)]  # type: ignore[attr-defined]
        if window.empty:
            continue
        avg_close = float(window["Close"].mean())
        avg_turnover = float(window["Volume"].mean()) * avg_close
        if min_avg_turnover > 0 and avg_turnover < min_avg_turnover:
            continue
        if min_atr_pct > 0:
            ind = compute_indicators(df)
            ind_window = ind[(ind.index.date >= start) & (ind.index.date <= end)]  # type: ignore[attr-defined]
            atr_pct = (ind_window["atr14"] / ind_window["Close"]).mean()
            if pd.isna(atr_pct) or float(atr_pct) < min_atr_pct:
                continue
        universe.add(ticker)
    return universe


def _metrics_from_signals(signals: list) -> dict:
    from analysis.backtest.helpers import _compute_group_metrics
    m = _compute_group_metrics(signals, rf_annual=0.03)
    rets = [s.blended_return for s in signals if s.blended_return is not None]
    win = sum(1 for r in rets if r > 0)
    reasons: dict[str, int] = {}
    for s in signals:
        if s.sell_reason:
            key = s.sell_reason.split(" ")[0].split("-")[0]  # "손절 -8%" → "손절"
            reasons[key] = reasons.get(key, 0) + 1
    return {
        "n": len(signals),
        "n_closed": len(rets),
        "win_rate": (win / len(rets)) if rets else None,
        "avg_return": (sum(rets) / len(rets)) if rets else None,
        "sharpe28d_proxy": m.sharpe_28d,
        "sell_reasons": reasons,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="BNF_TradingModel.md 역추세 모델 백테스트")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end",   required=True, help="YYYY-MM-DD")
    parser.add_argument("--market", default="ALL", choices=["KOSPI", "KOSDAQ", "ALL"])
    parser.add_argument("--max-tickers", type=int, default=0, help="0=전종목")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--output", default="results/bnf_backtest.csv")
    parser.add_argument("--preset", default="large", choices=["large", "small"],
                        help="이격도 임계값 프리셋: large=-22.5%%(대형우량주/상승장), "
                             "small=-32.5%%(중소형주·가상자산/하락장). --disc-threshold가 있으면 무시됨")
    parser.add_argument("--disc-threshold", type=float, default=None,
                        help="이격도 임계값 직접 지정 (예: -0.25). 지정 시 --preset 무시")
    parser.add_argument("--rsi-oversold", type=float, default=30.0)
    parser.add_argument("--lookback", type=int, default=10,
                        help="이격도/RSI 과매도 확인 lookback 윈도우(일)")
    parser.add_argument("--hard-stop-pct", type=float, default=0.08)
    parser.add_argument("--trail-up", type=float, default=0.15, help="상승추세 트레일링 스탑 폭")
    parser.add_argument("--trail-down", type=float, default=0.07, help="하락추세 트레일링 스탑 폭")
    parser.add_argument("--min-turnover", type=float, default=0.0,
                        help="유니버스 필터: 기간평균 거래대금(원) 최소치. 0=미적용")
    parser.add_argument("--min-atr-pct", type=float, default=0.0,
                        help="유니버스 필터: 기간평균 ATR14/Close 최소 비율. 0=미적용")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)
    if start >= end:
        sys.exit("--start 는 --end 보다 이전이어야 합니다")

    disc_threshold = args.disc_threshold if args.disc_threshold is not None else PRESET_DISC_THRESHOLD[args.preset]

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

    from analysis.chart_screener import get_all_tickers, fetch_kind_sector_map
    from analysis.backtest.fetch import _fetch_single_ohlcv
    from analysis.backtest.quant_signals import replay_bnf
    from core.ohlcv_cache import batch_fetch_cached

    logger.info("[bnf] 종목 목록 조회 중...")
    sector_map = fetch_kind_sector_map()
    all_tickers = get_all_tickers(sector_map=sector_map if sector_map else None)
    if args.market == "KOSPI":
        tickers = [(t, n, s) for t, n, s in all_tickers if t.endswith(".KS")]
    elif args.market == "KOSDAQ":
        tickers = [(t, n, s) for t, n, s in all_tickers if t.endswith(".KQ")]
    else:
        tickers = all_tickers
    if args.max_tickers > 0:
        tickers = tickers[:args.max_tickers]
    logger.info("[bnf] 대상 티커 %d개", len(tickers))

    # MA120 워밍업 여유 — 약 250 거래일 = 달력일 ~365일
    fetch_start = start - timedelta(days=400)
    ticker_pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t, _, _ in tickers]

    t0 = time.time()
    logger.info("[bnf] OHLCV 로드 중 (캐시 우선)...")
    ohlcv_map = batch_fetch_cached(ticker_pairs, fetch_start, end, args.workers, dsn, _fetch_single_ohlcv)
    logger.info("[bnf] OHLCV 로드 완료 — %.1f초, %d/%d 티커", time.time() - t0, len(ohlcv_map), len(tickers))

    universe = _select_universe(ohlcv_map, start, end, args.min_turnover, args.min_atr_pct)
    if universe is not None:
        logger.info("[bnf] 유니버스 필터(거래대금≥%.0f원, ATR14/Close≥%.1f%%) 적용: %d종목",
                    args.min_turnover, args.min_atr_pct * 100, len(universe))

    logger.info("[bnf] 진입/청산 파라미터: 이격도≤%s, RSI≤%.0f(lookback %d일), "
                "손절-%.0f%%, 트레일 상승%.0f%%/하락%.0f%%",
                _pct(disc_threshold), args.rsi_oversold, args.lookback,
                args.hard_stop_pct * 100, args.trail_up * 100, args.trail_down * 100)

    t1 = time.time()
    all_signals = []
    n_scanned = 0
    for ticker, name, _sector in tickers:
        if universe is not None and ticker not in universe:
            continue
        df = ohlcv_map.get(ticker)
        if df is None or df.empty:
            continue
        n_scanned += 1
        market = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"
        try:
            sigs = replay_bnf(
                ticker, name, df, market, start, end,
                disc_threshold=disc_threshold,
                rsi_oversold=args.rsi_oversold,
                lookback=args.lookback,
                hard_stop_pct=args.hard_stop_pct,
                trail_pct_uptrend=args.trail_up,
                trail_pct_downtrend=args.trail_down,
            )
        except Exception as e:
            logger.debug("  %s 재현 실패(무시): %s", ticker, e)
            continue
        all_signals.extend(sigs)
    logger.info("[bnf] 재현 완료 — %.1f초, %d종목 스캔, 신호 %d건",
                time.time() - t1, n_scanned, len(all_signals))

    m = _metrics_from_signals(all_signals)

    rows = [{
        "ticker": s.ticker, "name": s.name, "signal_date": s.signal_date,
        "entry_price": s.close_at_signal, "sell_date": s.sell_date,
        "sell_reason": s.sell_reason, "return": s.blended_return, "hold_days": s.hold_days,
    } for s in all_signals]
    df_out = pd.DataFrame(rows)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("[bnf] 결과 저장: %s", out_path)

    print(f"\n{'='*70}")
    print(f"BNF_TradingModel.md 역추세 모델 백테스트 ({start}~{end})")
    print(f"{'='*70}")
    print(f"이격도 임계값: {_pct(disc_threshold)}  |  신호 {m['n']}건  |  청산완료 {m['n_closed']}건")
    print(f"승률: {_pct(m['win_rate'])}  |  평균수익: {_pct(m['avg_return'])}  "
          f"|  Sharpe(28d proxy): {m['sharpe28d_proxy']}")
    if m["sell_reasons"]:
        print("청산 사유 분포:")
        for reason, cnt in sorted(m["sell_reasons"].items(), key=lambda kv: -kv[1]):
            print(f"  {reason:20s} {cnt:>6d}건")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
