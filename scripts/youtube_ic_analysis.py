"""YouTube Narrative IC Analysis

신호: youtube_attention_scores.attention_score (window_end 기준 rolling-5d)
수익률: window_end 종가 기준 T+1d / T+5d / T+20d close-to-close (yfinance)
IC:   Spearman(attention_score, ret_k) 날짜별 횡단면
ICIR: mean(IC) / std(IC)
t-stat: ICIR * sqrt(N_dates)

Usage:
    python scripts/youtube_ic_analysis.py [--output-dir docs]
    python scripts/youtube_ic_analysis.py --include-quintiles 2,3,4 --output-suffix _q2q4
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

import numpy as np
import pandas as pd
import psycopg2
import yfinance as yf
from dotenv import load_dotenv
from scipy import stats

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

HORIZONS = {"1d": 1, "5d": 5, "20d": 20}
PRICE_CACHE_PATH = Path(__file__).parent.parent / "data" / "youtube_ic_price_cache.pkl"


# ── DB ────────────────────────────────────────────────────────────────────────

def load_attention_scores(dsn: str) -> pd.DataFrame:
    conn = psycopg2.connect(dsn)
    try:
        df = pd.read_sql("""
            SELECT ticker, window_end::date AS signal_date, attention_score,
                   mention_count, sentiment_weighted
            FROM   youtube_attention_scores
            WHERE  attention_score > 0
            ORDER  BY signal_date, ticker
        """, conn, parse_dates=["signal_date"])
    finally:
        conn.close()
    logger.info("attention_scores: %d rows, %d tickers, %d dates",
                len(df), df["ticker"].nunique(), df["signal_date"].nunique())
    return df


# ── Price ──────────────────────────────────────────────────────────────────────

def _ks_symbol(ticker: str) -> str:
    return f"{ticker}.KS"


def fetch_prices(tickers: list[str], start: date, end: date) -> pd.DataFrame:
    """종목 리스트의 일별 종가 DataFrame (index=date, columns=ticker)."""
    symbols = [_ks_symbol(t) for t in tickers]
    start_str = (start - timedelta(days=5)).strftime("%Y-%m-%d")
    end_str   = (end   + timedelta(days=30)).strftime("%Y-%m-%d")
    logger.info("yfinance 다운로드: %d 종목 (%s ~ %s)", len(symbols), start_str, end_str)
    raw = yf.download(
        symbols, start=start_str, end=end_str,
        progress=False, auto_adjust=True, threads=True
    )
    if raw is None or raw.empty:
        return pd.DataFrame()
    if "Close" in raw.columns.get_level_values(0):
        close = cast(pd.DataFrame, raw["Close"]).copy()
    else:
        close = raw.copy()
    close.columns = [c.replace(".KS", "") for c in close.columns]
    close.index = cast(pd.DatetimeIndex, pd.to_datetime(close.index)).normalize()  # type: ignore[attr-defined]
    return close


def load_or_fetch_prices(tickers: list[str], start: date, end: date) -> pd.DataFrame:
    if PRICE_CACHE_PATH.exists():
        cached = cast(pd.DataFrame, pd.read_pickle(PRICE_CACHE_PATH))
        cached_tickers = set(cached.columns)
        need_tickers = set(tickers) - cached_tickers
        cache_end = cast(pd.Timestamp, cached.index.max()).date()
        if not need_tickers and cache_end >= end:
            logger.info("가격 캐시 사용: %d 종목", len(cached.columns))
            return cached
        logger.info("캐시 갱신 필요 (미수록 %d종목, 캐시 마지막일 %s)", len(need_tickers), cache_end)

    prices = fetch_prices(tickers, start, end)
    if not prices.empty:
        PRICE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        prices.to_pickle(PRICE_CACHE_PATH)
        logger.info("가격 캐시 저장: %s", PRICE_CACHE_PATH.name)
    return prices


# ── Forward returns ───────────────────────────────────────────────────────────

def compute_forward_returns(prices: pd.DataFrame, horizons: dict[str, int]) -> dict[str, pd.DataFrame]:
    """prices: (date × ticker) → {horizon: (date × ticker) forward return DataFrame}."""
    result = {}
    trading_dates = prices.index.sort_values()
    for label, k in horizons.items():
        fwd = prices.copy() * np.nan
        for i, dt in enumerate(trading_dates):
            future_idx = i + k
            if future_idx < len(trading_dates):
                future_dt = trading_dates[future_idx]
                fwd.loc[dt] = prices.loc[future_dt] / prices.loc[dt] - 1
        result[label] = fwd
    return result


# ── IC 계산 ────────────────────────────────────────────────────────────────────

def compute_ic_series(scores: pd.DataFrame, fwd_ret: pd.DataFrame,
                      include_quintiles: list[int] | None = None,
                      n_groups: int = 5) -> pd.Series:
    """날짜별 Spearman IC.

    include_quintiles: 포함할 분위 목록 (1-based). None이면 전체.
      예) [2,3,4] → 당일 횡단면에서 Q1·Q5 제외 후 IC 계산.
    """
    records = []
    for dt, grp in scores.groupby("signal_date"):
        if dt not in fwd_ret.index:
            continue
        ret_row = fwd_ret.loc[dt]
        merged = grp.set_index("ticker")[["attention_score"]].copy()
        merged["ret"] = ret_row.reindex(merged.index).values
        merged = merged.dropna()

        if include_quintiles is not None and len(merged) >= n_groups * 2:
            try:
                merged["q"] = pd.qcut(merged["attention_score"], n_groups,
                                       labels=False, duplicates="drop")
                # q는 0-based → include_quintiles는 1-based
                merged = merged[cast(pd.Series, merged["q"]).isin([q - 1 for q in include_quintiles])]
            except Exception:
                pass  # 분위 산출 실패 시 필터 없이 진행

        if len(merged) < 5:
            continue
        rho, _ = stats.spearmanr(merged["attention_score"], merged["ret"])
        records.append({"date": dt, "ic": rho, "n": len(merged)})

    if not records:
        return pd.Series(dtype=float)
    return cast(pd.Series, pd.DataFrame(records).set_index("date")["ic"])


# ── 보고 ───────────────────────────────────────────────────────────────────────

def print_ic_stats(ic_series: pd.Series, label: str, n_dates: int) -> None:
    if ic_series.empty:
        logger.warning("[%s] IC 시리즈 비어있음", label)
        return
    mean_ic = ic_series.mean()
    std_ic  = ic_series.std()
    icir    = mean_ic / std_ic if std_ic > 0 else np.nan
    t_stat  = icir * np.sqrt(len(ic_series)) if not np.isnan(icir) else np.nan
    pos_frac = (ic_series > 0).mean()
    print(f"  [{label}]  Mean IC={mean_ic:+.4f}  Std={std_ic:.4f}  "
          f"ICIR={icir:+.3f}  t={t_stat:+.2f}  "
          f"Pos%={pos_frac:.1%}  N={len(ic_series)}일")


def _set_korean_font() -> None:
    """Windows / macOS / Linux 순으로 한글 폰트 탐색 후 rcParams 적용."""
    import matplotlib.font_manager as fm
    candidates = [
        "Malgun Gothic",       # Windows 기본 한글 폰트
        "Apple SD Gothic Neo", # macOS
        "NanumGothic",         # Linux (nanum-gothic 패키지)
        "NanumBarunGothic",
        "Gulim",
    ]
    available = {f.name for f in fm.fontManager.ttflist}
    for name in candidates:
        if name in available:
            import matplotlib.pyplot as plt
            plt.rcParams["font.family"] = name
            plt.rcParams["axes.unicode_minus"] = False  # 음수 기호 깨짐 방지
            logger.debug("한글 폰트 적용: %s", name)
            return
    logger.warning("한글 폰트 없음 — 한글이 □로 표시될 수 있음")


def save_plots(ic_dict: dict[str, pd.Series], output_dir: Path, suffix: str = "",
               q_label: str = "전체") -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        logger.warning("matplotlib 미설치 — 차트 생성 건너뜀")
        return

    _set_korean_font()   # figure 생성 전에 폰트 설정
    output_dir.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(len(ic_dict), 2, figsize=(14, 4 * len(ic_dict)))
    if len(ic_dict) == 1:
        axes = [axes]

    colors = {"1d": "#2196F3", "5d": "#4CAF50", "20d": "#FF9800"}

    for ax_row, (label, ic) in zip(axes, ic_dict.items()):
        if ic.empty:
            continue
        color = colors.get(label, "steelblue")

        # 일별 IC 막대
        ax_bar = ax_row[0]
        ax_bar.bar(ic.index, ic.values, color=[color if v >= 0 else "#EF5350" for v in ic.values],
                   alpha=0.7, width=1.5)
        ax_bar.axhline(0, color="black", linewidth=0.8)
        ax_bar.axhline(ic.mean(), color=color, linewidth=1.5, linestyle="--",
                       label=f"Mean IC = {ic.mean():+.4f}")
        ax_bar.set_title(f"Daily IC  ({label})")
        ax_bar.legend(fontsize=8)
        ax_bar.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax_bar.xaxis.set_major_locator(mdates.MonthLocator())

        # 누적 IC
        ax_cum = ax_row[1]
        cum_ic = ic.cumsum()
        ax_cum.plot(cum_ic.index, cum_ic.values, color=color, linewidth=1.8)
        ax_cum.fill_between(cum_ic.index, 0, cum_ic.values,
                            where=cum_ic.values >= 0, alpha=0.15, color=color)
        ax_cum.fill_between(cum_ic.index, 0, cum_ic.values,
                            where=cum_ic.values < 0, alpha=0.15, color="#EF5350")
        ax_cum.axhline(0, color="black", linewidth=0.8)
        ax_cum.set_title(f"Cumulative IC  ({label})")
        ax_cum.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax_cum.xaxis.set_major_locator(mdates.MonthLocator())

    fig.suptitle(f"YouTube Narrative IC Analysis  (삼프로TV, 2026-01 ~ 2026-06)  [분위: {q_label}]",
                 fontsize=13, y=1.01)
    plt.tight_layout()

    out_path = output_dir / f"youtube_ic_analysis{suffix}.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    logger.info("차트 저장: %s", out_path)
    plt.close(fig)


def save_ic_csv(ic_dict: dict[str, pd.Series], output_dir: Path, suffix: str = "") -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    merged = pd.DataFrame(ic_dict)
    merged.index.name = "date"
    out_path = output_dir / f"youtube_ic_series{suffix}.csv"
    merged.to_csv(out_path)
    logger.info("IC 시리즈 CSV 저장: %s", out_path)


def decile_analysis(scores: pd.DataFrame, fwd_ret_dict: dict[str, pd.DataFrame],
                    n_groups: int = 5) -> None:
    """attention_score 분위별 평균 수익률 (5분위)."""
    print(f"\n{'='*60}")
    print(f"  분위별 평균 수익률  (Q1=저관심, Q{n_groups}=고관심)")
    print(f"{'='*60}")

    for label, fwd_ret in fwd_ret_dict.items():
        rows = []
        for dt, grp in scores.groupby("signal_date"):
            if dt not in fwd_ret.index:
                continue
            ret_row = fwd_ret.loc[dt]
            merged = grp.set_index("ticker")[["attention_score"]].copy()
            merged["ret"] = ret_row.reindex(merged.index).values
            merged = merged.dropna()
            if len(merged) < n_groups * 2:
                continue
            merged["q"] = pd.qcut(merged["attention_score"], n_groups,
                                   labels=False, duplicates="drop")
            rows.append(merged.groupby("q")["ret"].mean())

        if not rows:
            continue
        df_q = cast(pd.Series, pd.DataFrame(rows).mean())
        spread = df_q.iloc[-1] - df_q.iloc[0] if len(df_q) > 1 else np.nan
        parts = "  ".join(f"Q{int(cast(int, q))+1}={v:+.3%}" for q, v in df_q.items())
        print(f"  {label}:  {parts}   [Q{n_groups}-Q1 spread={spread:+.3%}]")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="YouTube Narrative IC Analysis")
    parser.add_argument("--min-tickers", type=int, default=5,
                        help="IC 계산에 필요한 최소 종목 수 (default: 5)")
    parser.add_argument("--output-dir", type=str, default="docs",
                        help="차트/CSV 출력 디렉토리 (default: docs)")
    parser.add_argument("--include-quintiles", type=str, default=None,
                        help="포함할 분위 (1-based, 콤마 구분). 예: 2,3,4 → Q1·Q5 제외")
    parser.add_argument("--output-suffix", type=str, default="",
                        help="출력 파일명 접미사. 예: _q2q4")
    parser.add_argument("--no-plot", action="store_true", help="차트 생성 건너뜀")
    args = parser.parse_args()

    include_quintiles: list[int] | None = None
    if args.include_quintiles:
        include_quintiles = [int(x.strip()) for x in args.include_quintiles.split(",")]
        logger.info("분위 필터 적용: Q%s", args.include_quintiles)

    dsn = (f"host={os.environ['DB_HOST']} port={os.environ['DB_PORT']} "
           f"dbname={os.environ['DB_NAME']} user={os.environ['DB_USER']} "
           f"password={os.environ['DB_PASSWORD']}")

    # 1. 신호 로드
    scores = load_attention_scores(dsn)
    tickers = scores["ticker"].unique().tolist()
    min_date = scores["signal_date"].min().date()
    max_date = scores["signal_date"].max().date()

    # 2. 가격 데이터
    prices = load_or_fetch_prices(tickers, min_date, max_date)
    if prices.empty:
        logger.error("가격 데이터 없음 — 종료")
        sys.exit(1)

    # 가격 인덱스를 날짜(KST)로 통일
    prices.index = cast(pd.DatetimeIndex, pd.to_datetime(prices.index)).normalize()  # type: ignore[attr-defined]
    prices = prices.sort_index()

    # 3. Forward returns 계산
    logger.info("Forward returns 계산 중...")
    fwd_returns = compute_forward_returns(prices, HORIZONS)

    # 4. IC 계산
    q_label = f"Q{args.include_quintiles}" if args.include_quintiles else "전체"
    print(f"\n{'='*60}")
    print("  YouTube Narrative IC Analysis  (삼프로TV 2026-01 ~ 2026-06)")
    print(f"  신호: attention_score  |  종목 수: {len(tickers)}  |  기간: {min_date} ~ {max_date}")
    print(f"  분위 필터: {q_label}")
    print(f"{'='*60}")

    ic_dict: dict[str, pd.Series] = {}
    for label in HORIZONS:
        ic = compute_ic_series(scores, fwd_returns[label],
                               include_quintiles=include_quintiles)
        ic_dict[label] = ic
        print_ic_stats(ic, label, n_dates=len(scores["signal_date"].unique()))

    # 5. 분위별 수익률
    decile_analysis(scores, fwd_returns)

    # 6. 월별 IC 요약
    print(f"\n{'='*60}")
    print("  월별 Mean IC")
    print(f"{'='*60}")
    for label, ic in ic_dict.items():
        if ic.empty:
            continue
        monthly = ic.groupby(pd.Grouper(freq="ME")).agg(["mean", "count"])
        parts = "  ".join(
            f"{cast(pd.Timestamp, idx).strftime('%Y-%m')}={row['mean']:+.3f}(N={int(row['count'])})"
            for idx, row in monthly.iterrows()
            if row["count"] > 0
        )
        print(f"  {label}: {parts}")

    # 7. 출력
    output_dir = Path(args.output_dir)
    sfx = args.output_suffix
    save_ic_csv(ic_dict, output_dir, suffix=sfx)
    if not args.no_plot:
        save_plots(ic_dict, output_dir, suffix=sfx, q_label=q_label)

    print(f"\n완료. 결과: {output_dir}/youtube_ic_analysis{sfx}.png, youtube_ic_series{sfx}.csv")


if __name__ == "__main__":
    main()
