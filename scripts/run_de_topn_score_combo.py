"""D_new_high20/E_flow_streak × SCORE-1 인프라 top-N 선별 — 실제 백테스트
(top-N 선별 설계 노트, TechnicalQuant.md 2026-08-25/26 "옵션 A" 결정 반영).

**새 백테스트 로직 최소화** — 진입 후보 생성만 새로 짜고(D/E 일별 신호를
주간 (ticker, week)로 집계, 이미 `run_de_topn_weekly_compression.py`가 확인한
대로 압축 손실 8.7%/4.4%뿐), 랭킹은 `strategy_compose.composite_score`
(SCORE-1이 쓰는 그 함수)를 그대로 재사용한다. 청산도 `run_cross_combo_
backtest._apply_compose_exit`(score1만 — cross/funnel1은 왜곡 확인됨, 이전
절 참고) 재사용.

랭킹 팩터:
  - D_new_high20: {"d_vol_ratio": 1.0, "stage": 1.0}
    (d_vol_ratio = 그 주 최대 거래량배율 당일/전일. **2026-08-26 1차 시도
    {"d_breakout_pct": 1.0, "stage": 1.0}는 기각** — 게이트만보다 승률·평균
    모두 악화, 중앙값이 score1 hard_stop_pct(10%)에 정확히 몰림. "그 주
    최대 돌파폭"이 건강한 추세 시작이 아니라 과열된 단기 급등(블로우오프)을
    역선택했을 가능성이 있어, 이번엔 가격 변동폭 대신 거래량 확인(volume
    confirmation — 돌파+거래량 동반 급증이 더 신뢰할 수 있다는 통상적
    기술적분석 논리)으로 팩터 자체를 바꿔 재시도.)
  - E_flow_streak: {"foreign_net_w": 1.0, "stage": 1.0}
    (streak 길이는 게이트로만 씀 — 95%가 임계값에 몰려 랭킹에 못 쓴다는
    2026-08-26 발견 반영. 랭킹은 SCORE-1과 동일한 순매수 "규모" 컬럼 재사용.
    이미 성공 확인됨 — 이번 실행은 D 재시도와 나란히 비교하기 위해 유지.)

두 컬럼 다 stage_classifications(폭넓은 커버리지)에서 오므로 chart_signals
(ichimoku 스크리너 통과 종목만 있는 좁은 테이블)는 소스에서 뺐다 — D/E
신호 종목 대부분이 애초에 ichimoku 통과 종목이 아니라 붙여도 커버리지가
낮았을 것.

사용법:
    python scripts/run_de_topn_score_combo.py --start 2025-01-02 --end 2026-08-06
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

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

from dotenv import load_dotenv

load_dotenv(os.path.join(Path(__file__).parent.parent, ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

from run_cross_combo_backtest import _apply_compose_exit  # noqa: E402
from run_de_topn_weekly_compression import _d_new_high20_signals, _e_flow_streak_signals  # noqa: E402

# 2026-08-23 절의 기존(랭킹 없는) D/E × score1 결과 — 비교 기준선.
BASELINE = {
    "D_new_high20": {"n": 29235, "win_rate": 0.510, "avg_return": 0.029, "median_return": 0.023},
    "E_flow_streak": {"n": 67686, "win_rate": 0.508, "avg_return": 0.024, "median_return": 0.016},
}


def _pct(v, dp: int = 1) -> str:
    return f"{v * 100:+.{dp}f}%" if v is not None else "N/A"


def _diagnose(signals, label: str) -> dict:
    closed = [s for s in signals if s.blended_return is not None]
    if not closed:
        return {"label": label, "n": 0}
    returns = [s.blended_return for s in closed]
    holds = [s.hold_days for s in closed if s.hold_days is not None]
    period_end = sum(1 for s in closed if s.final_exit_type == "period_end")
    tkr_counts = Counter(s.ticker for s in closed)
    return {
        "label": label, "n": len(closed),
        "win_rate": sum(1 for r in returns if r > 0) / len(returns),
        "avg_return": st.mean(returns), "median_return": st.median(returns),
        "avg_hold_days": st.mean(holds) if holds else None,
        "max_hold_days": max(holds) if holds else None,
        "period_end_pct": period_end / len(closed),
        "n_unique_tickers": len(tkr_counts),
        "top_ticker_count": tkr_counts.most_common(1)[0][1] if tkr_counts else 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="D/E × SCORE-1 인프라 top-N 선별 백테스트")
    parser.add_argument("--start", default="2025-01-02")
    parser.add_argument("--end", default="2026-08-06")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--top-n", type=int, default=20)
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

    from analysis import strategy_compose as sc
    from analysis.backtest.fetch import _fetch_single_ohlcv
    from analysis.backtest.helpers import _build_price_lookup, _entry_on_or_after
    from analysis.backtest.models import SignalRecord
    from analysis.chart_screener import fetch_kind_sector_map, get_all_tickers
    from core.ohlcv_cache import batch_fetch_cached, load_flow_streaks

    logger.info("[quant] 종목 목록 조회 중...")
    sector_map = fetch_kind_sector_map()
    tickers = get_all_tickers(sector_map=sector_map if sector_map else None)
    name_of = {t: n for t, n, _ in tickers}
    logger.info("[quant] 대상 티커 %d개", len(tickers))

    fetch_start = start - timedelta(days=400)
    ticker_pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t, _, _ in tickers]

    t0 = time.time()
    ohlcv_map = batch_fetch_cached(ticker_pairs, fetch_start, end, args.workers, dsn, _fetch_single_ohlcv)
    logger.info("[quant] OHLCV 로드 완료 — %.1f초, %d/%d 티커", time.time() - t0, len(ohlcv_map), len(tickers))

    flow_lookup = load_flow_streaks(dsn, [t for t, _, _ in tickers], start, end)
    logger.info("[quant] 수급 streak %d건 로드", len(flow_lookup))

    # ── 1. D/E 일별 신호 → 주간 (ticker, week) 게이트 프레임 ──────────
    d_daily: list[dict] = []
    e_daily: list[dict] = []
    for ticker, *_ in tickers:
        df = ohlcv_map.get(ticker)
        if df is None or df.empty:
            continue
        try:
            d_daily.extend(_d_new_high20_signals(ticker, df, start, end))
        except Exception as e:
            logger.debug("  %s D_new_high20 실패(무시): %s", ticker, e)
        try:
            e_daily.extend(_e_flow_streak_signals(ticker, df, flow_lookup, start, end))
        except Exception as e:
            logger.debug("  %s E_flow_streak 실패(무시): %s", ticker, e)

    d_weekly = (
        pd.DataFrame(d_daily).groupby(["ticker", "week"], as_index=False)
        .agg(d_breakout_pct=("breakout_pct", "max"), d_vol_ratio=("vol_ratio", "max"))
    ) if d_daily else pd.DataFrame(columns=pd.Index(["ticker", "week", "d_breakout_pct", "d_vol_ratio"]))
    e_weekly = (
        pd.DataFrame(e_daily)[["ticker", "week"]].drop_duplicates()
    ) if e_daily else pd.DataFrame(columns=pd.Index(["ticker", "week"]))
    logger.info("[게이트] D 주간후보 %d건, E 주간후보 %d건", len(d_weekly), len(e_weekly))

    # ── 2. SCORE-1과 동일한 팩터 소스(stage/flow) 로드 ────────────────
    factor_frame = sc.load_signal_frame(dsn, start, end, ["stage", "flow"])
    logger.info("[팩터] stage/flow 프레임 %d행", len(factor_frame))

    plans = [
        ("D_new_high20", d_weekly, {"d_vol_ratio": 1.0, "stage": 1.0}),
        ("E_flow_streak", e_weekly, {"foreign_net_w": 1.0, "stage": 1.0}),
    ]

    rows = []
    for entry_key, gate_frame, weights in plans:
        if gate_frame.empty:
            logger.warning("[%s] 게이트 후보 없음 — 스킵", entry_key)
            continue
        merged = gate_frame.merge(factor_frame, on=["ticker", "week"], how="left")
        picks = sc.composite_score(merged, weights, args.top_n)
        logger.info("[%s] top-%d 선별 후 %d건 (원본 게이트 %d건)", entry_key, args.top_n, len(picks), len(gate_frame))

        # ── 3. (ticker, week) → 금요일 이후 첫 거래일 진입가 (compose와 동일 관례) ──
        signals = []
        price_cache: dict[str, dict] = {}
        seen: set[tuple[str, date]] = set()
        for row in picks.itertuples(index=False):
            ticker = getattr(row, "ticker")
            week = getattr(row, "week")
            df = ohlcv_map.get(ticker)
            if df is None or df.empty:
                continue
            plook = price_cache.get(ticker)
            if plook is None:
                plook = _build_price_lookup(df)
                price_cache[ticker] = plook
            friday = sc.week_to_friday(week)
            entry = _entry_on_or_after(plook, friday)
            if entry is None:
                continue
            edate, eclose = entry
            if eclose <= 0 or (ticker, edate) in seen:
                continue
            seen.add((ticker, edate))
            mkt = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"
            signals.append(SignalRecord(
                ticker=ticker, name=name_of.get(ticker) or ticker, signal_date=edate,
                close_at_signal=eclose, mode="compose", market=mkt,
            ))
        if not signals:
            logger.warning("[%s] 진입가 산정 후 신호 0건", entry_key)
            continue

        _apply_compose_exit(signals, ohlcv_map, start, end, "score1")
        label = f"{entry_key}×top{args.top_n}선별 × score1분할청산"
        diag = _diagnose(signals, label)
        base = BASELINE[entry_key]
        rows.append((label, diag, base))
        logger.info(
            "  [%s] 신호%d 승률%s 평균%s 중앙값%s | 보유일평균%.0f/최대%s 기간종료%.0f%% 고유종목%d(최다%d회)",
            label, diag["n"], _pct(diag["win_rate"]), _pct(diag["avg_return"]), _pct(diag["median_return"]),
            diag["avg_hold_days"] or 0, diag["max_hold_days"], diag["period_end_pct"] * 100,
            diag["n_unique_tickers"], diag["top_ticker_count"],
        )

    elapsed = time.time() - t0
    logger.info("전체 완료 — %.0f초", elapsed)

    print("\n" + "=" * 120)
    print(f"{'조합':<38} {'신호':>7} {'승률':>8} {'평균수익':>10} {'중앙값':>10} {'기간종료%':>10} {'고유종목':>8}")
    print("-" * 120)
    for label, d, base in rows:
        print(f"{label:<38} {d['n']:>7} {_pct(d['win_rate']):>8} {_pct(d['avg_return']):>10} "
              f"{_pct(d['median_return']):>10} {d['period_end_pct']*100:>9.1f}% {d['n_unique_tickers']:>8}")
        print(f"{'  (비교) 게이트만/랭킹없음(기존 절)':<38} {base['n']:>7} {_pct(base['win_rate']):>8} "
              f"{_pct(base['avg_return']):>10} {_pct(base['median_return']):>10}")
    print("=" * 120)

    out_path = Path(__file__).parent.parent / "results" / "de_topn_score_combo.csv"
    out_path.parent.mkdir(exist_ok=True)
    pd.DataFrame([
        {"combo": label, **{k: v for k, v in d.items() if k != "label"}} for label, d, _ in rows
    ]).to_csv(out_path, index=False)
    logger.info("결과 저장: %s", out_path)


if __name__ == "__main__":
    main()
