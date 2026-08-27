"""D_new_high20(20일 신고가 돌파) / E_flow_streak(수급 3일연속) 진입 ×
compose 분할청산(cross/score1/funnel1) 교차조합.

**새 로직 없음** — 진입은 `scripts/run_quant_backtest.py::run_condition`과
동일하게 `quant_signals.replay_quant`를 전종목·펀더멘털 필터 없이(Tier E
baseline과 동일 조건) 돌리되 내부 청산은 비활성화(hard_stop=0.99 등)해
entry만 취하고, 청산은 `run_cross_combo_backtest.py::_apply_compose_exit`를
그대로 재사용한다.

지금까지 D/E는:
  - D_new_high20: 원안청산(quant_original류) 또는 ATR/Donchian(H, D 전용)만 시도됨
  - E_flow_streak: 원안청산만 시도됨(폐기 판정, 승률34.2%/평균+0.2%)
compose의 분할청산(TP1+트레일링, 최대보유 제한 있음)을 붙여본 적은 없음 —
2026-08-23 momentum/QVM×quant_optimized 조합이 "청산에 최대보유기간 제한이
없으면 소수 장기보유 종목의 미실현 평가익으로 지표가 왜곡된다"는 걸
보여준 뒤라, 이번엔 hold_days 분포·기간종료 비율·종목 중복도를 항상 같이
찍어 같은 함정에 빠졌는지 즉시 확인한다.

사용법:
    python scripts/run_de_entry_compose_exit_combo.py --start 2025-01-02 --end 2026-08-06
"""
from __future__ import annotations

import argparse
import logging
import os
import statistics as st
import sys
import time
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

from dotenv import load_dotenv

load_dotenv(os.path.join(Path(__file__).parent.parent, ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

from run_cross_combo_backtest import _apply_compose_exit  # noqa: E402

ENTRY_KEYS = ["D_new_high20", "E_flow_streak"]
COMPOSE_EXIT_VARIANTS = ["cross", "score1", "funnel1"]


def _generate_entries(entry_key: str, tickers, ohlcv_map, flow_lookup, start: date, end: date):
    """진입만 취하고 청산은 비활성화(hard_stop=0.99)해 SignalRecord 리스트 반환."""
    from analysis.backtest.quant_signals import replay_quant

    all_signals = []
    for ticker, name, _sector in tickers:
        df = ohlcv_map.get(ticker)
        if df is None or df.empty:
            continue
        market = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"
        try:
            sigs = replay_quant(
                ticker, name, df, market, start, end,
                entry_key=entry_key,
                hard_stop_pct=0.99, target_pct=None,
                use_ma20_exit=False, use_rsi70_exit=False,
                flow_lookup=flow_lookup if entry_key == "E_flow_streak" else None,
            )
        except Exception as e:
            logger.debug("  %s 재현 실패(무시): %s", ticker, e)
            continue
        for s in sigs:
            s.sell_date = None
            s.sell_reason = ""
            s.sell_return = None
            s.hold_days = None
            s.tp1_date = None
            s.tp1_ret = None
            s.final_exit_date = None
            s.final_exit_ret = None
            s.final_exit_type = ""
            s.blended_return = None
        all_signals.extend(sigs)
    return all_signals


def _pct(v, dp: int = 1) -> str:
    return f"{v * 100:+.{dp}f}%" if v is not None else "N/A"


def _diagnose(signals, label: str) -> dict:
    """hold_days 분포·기간종료 비율·종목 중복도 — momentum×quant_optimized 함정 재확인용."""
    closed = [s for s in signals if s.blended_return is not None]
    if not closed:
        return {"label": label, "n": 0}
    returns = [s.blended_return for s in closed]
    holds = [s.hold_days for s in closed if s.hold_days is not None]
    period_end = sum(1 for s in closed if s.final_exit_type == "period_end")
    tkr_counts = Counter(s.ticker for s in closed)
    return {
        "label": label,
        "n": len(closed),
        "win_rate": sum(1 for r in returns if r > 0) / len(returns),
        "avg_return": st.mean(returns),
        "median_return": st.median(returns),
        "avg_hold_days": st.mean(holds) if holds else None,
        "max_hold_days": max(holds) if holds else None,
        "period_end_pct": period_end / len(closed),
        "n_unique_tickers": len(tkr_counts),
        "top_ticker_count": tkr_counts.most_common(1)[0][1] if tkr_counts else 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="D/E 진입 × compose 분할청산 교차조합")
    parser.add_argument("--start", default="2025-01-02")
    parser.add_argument("--end", default="2026-08-06")
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
    from core.ohlcv_cache import batch_fetch_cached, load_flow_streaks

    logger.info("[quant] 종목 목록 조회 중...")
    sector_map = fetch_kind_sector_map()
    tickers = get_all_tickers(sector_map=sector_map if sector_map else None)
    logger.info("[quant] 대상 티커 %d개", len(tickers))

    fetch_start = start - timedelta(days=400)
    ticker_pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t, _, _ in tickers]

    t0 = time.time()
    ohlcv_map = batch_fetch_cached(ticker_pairs, fetch_start, end, args.workers, dsn, _fetch_single_ohlcv)
    logger.info("[quant] OHLCV 로드 완료 — %.1f초, %d/%d 티커", time.time() - t0, len(ohlcv_map), len(tickers))

    flow_lookup = load_flow_streaks(dsn, [t for t, _, _ in tickers], start, end)
    logger.info("[quant] 수급 streak %d건 로드", len(flow_lookup))

    rows = []
    diag_rows = []

    for entry_key in ENTRY_KEYS:
        signals_base = _generate_entries(entry_key, tickers, ohlcv_map, flow_lookup, start, end)
        if not signals_base:
            logger.warning("[%s] 신호 없음 — 스킵", entry_key)
            continue
        logger.info("[%s] 진입신호 %d건 생성", entry_key, len(signals_base))

        for exit_key in COMPOSE_EXIT_VARIANTS:
            import copy
            signals = copy.deepcopy(signals_base)
            _apply_compose_exit(signals, ohlcv_map, start, end, exit_key)

            label = f"{entry_key}매수 × {exit_key}분할청산"
            diag = _diagnose(signals, label)
            diag_rows.append(diag)
            if diag["n"] == 0:
                continue
            rows.append({
                "combo": label, "entry": entry_key, "exit": exit_key,
                "n_signals": diag["n"], "win_rate": diag["win_rate"],
                "avg_return": diag["avg_return"], "median_return": diag["median_return"],
            })
            logger.info(
                "  [%s] 신호%d 승률%s 평균%s 중앙값%s | 보유일평균%.0f/최대%s 기간종료%.0f%% 고유종목%d(최다%d회)",
                label, diag["n"], _pct(diag["win_rate"]), _pct(diag["avg_return"]), _pct(diag["median_return"]),
                diag["avg_hold_days"] or 0, diag["max_hold_days"], diag["period_end_pct"] * 100,
                diag["n_unique_tickers"], diag["top_ticker_count"],
            )

    elapsed = time.time() - t0
    logger.info("전체 완료 — %.0f초", elapsed)

    print("\n" + "=" * 115)
    print(f"{'조합':<32} {'신호':>6} {'승률':>8} {'평균수익':>10} {'중앙값':>10} {'보유일평균':>10} {'기간종료%':>10} {'고유종목':>8}")
    print("-" * 115)
    for r, d in zip(rows, [d for d in diag_rows if d["n"] > 0]):
        print(f"{r['combo']:<32} {r['n_signals']:>6} {_pct(r['win_rate']):>8} "
              f"{_pct(r['avg_return']):>10} {_pct(r['median_return']):>10} "
              f"{d['avg_hold_days']:>9.0f}일 {d['period_end_pct']*100:>9.1f}% {d['n_unique_tickers']:>8}")
    print("=" * 115)

    out_path = Path(__file__).parent.parent / "results" / "de_entry_compose_exit_combo.csv"
    out_path.parent.mkdir(exist_ok=True)
    import pandas as pd
    pd.DataFrame(rows).to_csv(out_path, index=False)
    logger.info("결과 저장: %s", out_path)


if __name__ == "__main__":
    main()
