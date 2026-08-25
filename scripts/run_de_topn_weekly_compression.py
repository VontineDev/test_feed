"""D_new_high20/E_flow_streak — top-N 선별 설계(TechnicalQuant.md "top-N 선별
설계 노트", 2026-08-25) 착수용 1단계 진단: 일별 신호를 주간(ticker, ISO week)
으로 묶으면 얼마나 압축되는지, 그리고 랭킹에 쓸 팩터 후보(D=돌파폭/거래대금,
E=streak길이)의 분포를 먼저 확인한다.

**새 백테스트 로직 없음** — quant_signals.compute_indicators/replay_quant의
E_flow_streak 판정 로직을 그대로 재사용해 신호 발생일만 다시 뽑고, 그 날의
raw 지표값(high20_prev/Volume/streak)을 추가로 읽어 요약 통계만 낸다.

사용법:
    python scripts/run_de_topn_weekly_compression.py --start 2025-01-02 --end 2026-08-06
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import cast

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

from dotenv import load_dotenv

load_dotenv(os.path.join(Path(__file__).parent.parent, ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

from analysis.backtest.quant_signals import compute_indicators
from analysis.strategy_compose import iso_week


def _d_new_high20_signals(ticker: str, df: pd.DataFrame, start: date, end: date) -> list[dict]:
    """_cond_new_high20과 동일한 전이 판정을 재현하되, 판정에 그치지 않고
    돌파폭((Close-high20_prev)/high20_prev)·거래량배율(Volume/vol_prev)·
    거래대금(Close*Volume)까지 함께 기록한다."""
    ind = compute_indicators(df)
    out = []
    for i in range(21, len(ind)):
        ts = ind.index[i]
        row_date = ts.date() if isinstance(ts, datetime) else cast(date, ts)
        if row_date < start or row_date > end:
            continue
        cur, prev = ind.iloc[i], ind.iloc[i - 1]
        if pd.isna(cur.get("high20_prev")) or pd.isna(cur["Close"]):
            continue
        today_breaks = float(cur["Close"]) >= float(cur["high20_prev"])
        if not today_breaks:
            continue
        if not pd.isna(prev.get("high20_prev")):
            prev_breaks = float(prev["Close"]) >= float(prev["high20_prev"])
            if prev_breaks:
                continue  # 전이 아님 — 이미 돌파 상태 유지 중
        high20_prev = float(cur["high20_prev"])
        breakout_pct = float(cur["Close"]) / high20_prev - 1.0
        vol_prev = cur.get("vol_prev")
        vol_ratio = float(cur["Volume"]) / float(vol_prev) if vol_prev and not pd.isna(vol_prev) and float(vol_prev) > 0 else None
        out.append({
            "ticker": ticker, "date": row_date, "week": iso_week(row_date),
            "breakout_pct": breakout_pct, "vol_ratio": vol_ratio,
            "txamt": float(cur["Close"]) * float(cur["Volume"]),
        })
    return out


def _e_flow_streak_signals(ticker: str, df: pd.DataFrame, flow_lookup, start: date, end: date, flow_streak_min: int = 3) -> list[dict]:
    """replay_quant의 E_flow_streak 전이 판정과 동일한 로직 — streak 길이
    (foreign_streak/inst_streak 중 더 큰 쪽)를 팩터 후보로 함께 기록."""
    out = []
    for i in range(1, len(df)):
        ts = df.index[i]
        row_date = ts.date() if isinstance(ts, datetime) else cast(date, ts)
        if row_date < start or row_date > end:
            continue
        prev_ts = df.index[i - 1]
        prev_date = prev_ts.date() if isinstance(prev_ts, datetime) else cast(date, prev_ts)
        streak = flow_lookup.get((ticker, row_date))
        prev_streak = flow_lookup.get((ticker, prev_date))
        if streak is None:
            continue
        f_str, i_str = streak
        triggered = ((f_str is not None and f_str >= flow_streak_min)
                     or (i_str is not None and i_str >= flow_streak_min))
        if prev_streak is not None:
            pf, pi = prev_streak
            was_triggered = ((pf is not None and pf >= flow_streak_min)
                              or (pi is not None and pi >= flow_streak_min))
            if was_triggered:
                triggered = False
        if not triggered:
            continue
        streak_len = max(f_str or 0, i_str or 0)
        out.append({"ticker": ticker, "date": row_date, "week": iso_week(row_date), "streak_len": streak_len})
    return out


def _compression_report(rows: list[dict], label: str) -> None:
    if not rows:
        logger.warning("[%s] 신호 없음", label)
        return
    df = pd.DataFrame(rows)
    raw_n = len(df)
    per_week = df.groupby(["ticker", "week"]).size()
    unique_n = len(per_week)
    compression = 1.0 - unique_n / raw_n

    print(f"\n--- {label} ---")
    print(f"일별 원시 신호: {raw_n}건")
    print(f"고유 (ticker, week) 조합: {unique_n}건")
    print(f"압축률(주간 집계 시 사라지는 비율): {compression*100:.1f}%")
    print(f"같은 (ticker, week)에 신호 2건 이상인 비율: {(per_week > 1).sum() / unique_n * 100:.1f}%"
          f" (최대 {per_week.max()}건/주)")

    if "breakout_pct" in df.columns:
        print(f"돌파폭 분포: p25={df['breakout_pct'].quantile(.25)*100:.2f}% "
              f"median={df['breakout_pct'].median()*100:.2f}% "
              f"p75={df['breakout_pct'].quantile(.75)*100:.2f}% "
              f"p95={df['breakout_pct'].quantile(.95)*100:.2f}%")
        vr = df["vol_ratio"].dropna()
        if len(vr):
            print(f"거래량배율 분포: median={vr.median():.2f}x p75={vr.quantile(.75):.2f}x p95={vr.quantile(.95):.2f}x")
    if "streak_len" in df.columns:
        print(f"streak 길이 분포: {Counter(df['streak_len'].tolist()).most_common()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="D/E 신호 주간 압축률 + 팩터 후보 분포 진단")
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

    d_rows: list[dict] = []
    e_rows: list[dict] = []
    for ticker, *_ in tickers:
        df = ohlcv_map.get(ticker)
        if df is None or df.empty:
            continue
        try:
            d_rows.extend(_d_new_high20_signals(ticker, df, start, end))
        except Exception as e:
            logger.debug("  %s D_new_high20 실패(무시): %s", ticker, e)
        try:
            e_rows.extend(_e_flow_streak_signals(ticker, df, flow_lookup, start, end))
        except Exception as e:
            logger.debug("  %s E_flow_streak 실패(무시): %s", ticker, e)

    logger.info("전체 완료 — %.0f초", time.time() - t0)
    _compression_report(d_rows, "D_new_high20")
    _compression_report(e_rows, "E_flow_streak")

    out_dir = Path(__file__).parent.parent / "results"
    out_dir.mkdir(exist_ok=True)
    if d_rows:
        pd.DataFrame(d_rows).to_csv(out_dir / "de_topn_d_signals_raw.csv", index=False)
    if e_rows:
        pd.DataFrame(e_rows).to_csv(out_dir / "de_topn_e_signals_raw.csv", index=False)
    logger.info("원시 신호 CSV 저장: results/de_topn_{d,e}_signals_raw.csv")


if __name__ == "__main__":
    main()
