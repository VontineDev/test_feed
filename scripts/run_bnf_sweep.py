"""
run_bnf_sweep.py — BNF 모델(analysis/backtest/quant_signals.py::replay_bnf)
이격도(진입) × 손절% × 트레일링%(상승/하락) 그리드서치.

배경: scripts/run_bnf_backtest.py로 돌린 전종목 풀기간(2020-2026) 백테스트
결과, 헤드라인 승률(53~70%)이 2020년 코로나 급락 후 V자 반등 구간(신호의
36~65%가 이 한 해에 집중)이 만든 착시였고, 그 해를 빼면 승률 30% 안팎·
평균수익 마이너스로 뒤집혔다(메모리 project_bnf_backtest_result.md). 이
그리드서치는 그 왜곡을 재현하지 않도록 **기본적으로 2020년을 학습/검증
구간 모두에서 제외**한다(--train-start 기본값 2021-01-01).

성능 최적화: 이번 스윕은 RSI 임계값·lookback을 고정하고 이격도(진입 조건)·
손절%·트레일링%(청산 조건)만 바꾼다. 이격도는 진입 신호 자체를 바꾸지만
손절/트레일링은 "이미 발생한 진입 신호를 어떻게 청산하느냐"만 바꾼다 —
그래서 이격도값 하나당 진입 신호(티커·인덱스·진입가)를 딱 한 번만 스캔해
캐싱해두고, 손절×트레일링 25개 조합은 그 캐시에 대해 청산 스캔만 재실행
한다. 이걸 안 하면 175개 조합마다 매번 전체 유니버스를 처음부터 다시
스캔해야 해서(scripts/run_bnf_backtest.py 실측: 전종목 240초/조합) 175개
조합이면 약 12시간이 걸린다 — 이 구조로는 이격도 7개 × 유니버스 스캔
1회씩만 하면 되므로 대폭 단축된다.

유니버스: 학습+검증 전체 기간 평균 거래대금 상위 --top-n(기본 500)으로
고정 — 원문서의 "거래량 폭증 종목" 조건을 근사(scripts/run_bnf_backtest.py
의 --min-turnover와 같은 취지, 여기서는 스윕 반복 비용을 감안해 랭킹
방식으로 고정).

사용법:
    python scripts/run_bnf_sweep.py
    # 기본: 이격도 7 × 손절 5 × 트레일링쌍 5 = 175조합, 학습 2021~2024 / 검증 2025~2026-08

    python scripts/run_bnf_sweep.py --top-n 1000 --workers 12
    python scripts/run_bnf_sweep.py --train-start 2020-01-01  # 2020 포함해서 비교하고 싶을 때
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import cast

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

# 문서 원문 구간(-20%~-35%)을 2.5%p 간격으로 세분화.
DISC_THRESHOLD_GRID = [-0.20, -0.225, -0.25, -0.275, -0.30, -0.325, -0.35]
HARD_STOP_GRID = [0.05, 0.07, 0.08, 0.10, 0.12]
# (상승추세 트레일링%, 하락추세 트레일링%) 쌍 — 상승>하락 관계(문서 "상승장은
# 여유, 하락장은 짧게")를 유지하는 조합만 넣는다. 기본값(0.15,0.07)은
# replay_bnf 기본 파라미터.
TRAIL_GRID = [(0.10, 0.05), (0.15, 0.07), (0.20, 0.07), (0.15, 0.10), (0.25, 0.10)]

RSI_OVERSOLD_FIXED = 30.0
LOOKBACK_FIXED = 10


def _pct(v: float | None, dp: int = 1) -> str:
    return f"{v * 100:+.{dp}f}%" if v is not None else "N/A"


def main() -> None:
    parser = argparse.ArgumentParser(description="BNF 이격도/손절/트레일링 그리드서치")
    parser.add_argument("--train-start", default="2021-01-01",
                        help="학습 구간 시작 (기본 2021-01-01 — 2020 코로나 반등 왜곡 제외, YYYY-MM-DD)")
    parser.add_argument("--train-end", default="2024-12-31")
    parser.add_argument("--val-start", default="2025-01-01")
    parser.add_argument("--val-end", default="2026-08-24", help="DB 최신 데이터 날짜")
    parser.add_argument("--market", default="ALL", choices=["KOSPI", "KOSDAQ", "ALL"])
    parser.add_argument("--top-n", type=int, default=500,
                        help="유니버스: 학습+검증 기간 평균 거래대금 상위 N종목 고정")
    parser.add_argument("--min-signals", type=int, default=20,
                        help="결과 표에 포함할 최소 검증기간 신호 수")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--output", default="results/bnf_sweep.csv")
    args = parser.parse_args()

    train_start = date.fromisoformat(args.train_start)
    train_end = date.fromisoformat(args.train_end)
    val_start = date.fromisoformat(args.val_start)
    val_end = date.fromisoformat(args.val_end)
    if train_start >= train_end:
        sys.exit("--train-start 는 --train-end 보다 이전이어야 합니다")
    if val_start >= val_end:
        sys.exit("--val-start 는 --val-end 보다 이전이어야 합니다")
    if val_start <= train_end:
        logger.warning("val-start(%s)가 train-end(%s)보다 이르거나 같습니다 — 구간 겹침!", val_start, train_end)
    combined_start, combined_end = min(train_start, val_start), max(train_end, val_end)

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

    from analysis.backtest.config import TX_COST_DEFAULT
    from analysis.backtest.fetch import _fetch_single_ohlcv
    from analysis.backtest.quant_signals import _cond_bnf_entry, _scan_exit_bnf, compute_bnf_indicators
    from analysis.chart_screener import fetch_kind_sector_map, get_all_tickers
    from core.ohlcv_cache import batch_fetch_cached

    logger.info("[bnf-sweep] 종목 목록 조회 중...")
    sector_map = fetch_kind_sector_map()
    all_tickers = get_all_tickers(sector_map=sector_map if sector_map else None)
    if args.market == "KOSPI":
        tickers = [(t, n, s) for t, n, s in all_tickers if t.endswith(".KS")]
    elif args.market == "KOSDAQ":
        tickers = [(t, n, s) for t, n, s in all_tickers if t.endswith(".KQ")]
    else:
        tickers = all_tickers
    logger.info("[bnf-sweep] 전체 티커 %d개", len(tickers))

    fetch_start = combined_start - timedelta(days=400)  # ma120 워밍업 여유
    ticker_pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t, _, _ in tickers]

    t0 = time.time()
    logger.info("[bnf-sweep] OHLCV 로드 중 (캐시 우선)...")
    ohlcv_map = batch_fetch_cached(ticker_pairs, fetch_start, combined_end, args.workers, dsn, _fetch_single_ohlcv)
    logger.info("[bnf-sweep] OHLCV 로드 완료 — %.1f초, %d/%d 티커", time.time() - t0, len(ohlcv_map), len(tickers))

    # 유니버스 고정: 학습+검증 기간 평균 거래대금 상위 top_n.
    turnover: dict[str, float] = {}
    for ticker, df in ohlcv_map.items():
        window = df[(df.index.date >= combined_start) & (df.index.date <= combined_end)]  # type: ignore[attr-defined]
        if window.empty:
            continue
        turnover[ticker] = float(window["Volume"].mean()) * float(window["Close"].mean())
    ranked = sorted(turnover.items(), key=lambda kv: kv[1], reverse=True)
    universe = {t for t, _ in ranked[:args.top_n]}
    logger.info("[bnf-sweep] 유니버스 고정(거래대금 상위 %d): %d종목", args.top_n, len(universe))

    # 지표는 이격도/손절/트레일링 그리드와 무관하게 동일 — 티커당 1회만 계산해 재사용.
    t1 = time.time()
    indicator_map: dict[str, pd.DataFrame] = {}
    for ticker in universe:
        df = ohlcv_map.get(ticker)
        if df is None or df.empty:
            continue
        indicator_map[ticker] = compute_bnf_indicators(df)
    logger.info("[bnf-sweep] 지표 사전계산 완료 — %.1f초, %d종목", time.time() - t1, len(indicator_map))

    rows = []
    for disc_idx, disc_threshold in enumerate(DISC_THRESHOLD_GRID):
        t2 = time.time()
        # 이 이격도값에서의 진입 신호(티커, 인덱스, 신호일, 진입가)를 한 번만 스캔.
        entries: list[tuple[str, int, date, float]] = []
        for ticker, df in indicator_map.items():
            min_start = 121  # ma120 워밍업 — replay_bnf 관례와 통일
            for i in range(min_start, len(df)):
                ts = df.index[i]
                row_date = ts.date() if isinstance(ts, datetime) else cast(date, ts)
                if row_date < combined_start or row_date > combined_end:
                    continue
                if pd.isna(df.iloc[i]["Close"]):
                    continue
                if not _cond_bnf_entry(df, i, disc_threshold, RSI_OVERSOLD_FIXED, LOOKBACK_FIXED):
                    continue
                entry_price = float(df.iloc[i]["Close"])
                if entry_price <= 0:
                    continue
                entries.append((ticker, i, row_date, entry_price))
        logger.info("[bnf-sweep] 이격도=%s 진입신호 %d건 스캔 완료 (%.1f초, %d/%d)",
                    _pct(disc_threshold), len(entries), time.time() - t2,
                    disc_idx + 1, len(DISC_THRESHOLD_GRID))

        for hard_stop in HARD_STOP_GRID:
            for trail_up, trail_down in TRAIL_GRID:
                train_rets: list[float] = []
                val_rets: list[float] = []
                for ticker, i, row_date, entry_price in entries:
                    df = indicator_map[ticker]
                    _sd, _reason, ret, _hd = _scan_exit_bnf(
                        df, i, entry_price, row_date,
                        hard_stop, trail_up, trail_down, TX_COST_DEFAULT,
                    )
                    if ret is None:
                        continue
                    if train_start <= row_date <= train_end:
                        train_rets.append(ret)
                    elif val_start <= row_date <= val_end:
                        val_rets.append(ret)

                def _metrics(rets: list[float]) -> tuple[int, float | None, float | None]:
                    if not rets:
                        return 0, None, None
                    win = sum(1 for r in rets if r > 0) / len(rets)
                    avg = sum(rets) / len(rets)
                    return len(rets), win, avg

                train_n, train_win, train_avg = _metrics(train_rets)
                val_n, val_win, val_avg = _metrics(val_rets)
                overfit_gap = (
                    (train_avg - val_avg) if train_avg is not None and val_avg is not None else None
                )
                rows.append({
                    "disc_threshold": disc_threshold, "hard_stop_pct": hard_stop,
                    "trail_up": trail_up, "trail_down": trail_down,
                    "train_n": train_n, "train_win_rate": train_win, "train_avg_return": train_avg,
                    "val_n": val_n, "val_win_rate": val_win, "val_avg_return": val_avg,
                    "overfit_gap": overfit_gap,
                })

    df_out = pd.DataFrame(rows)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("[bnf-sweep] 결과 저장: %s (%d 조합)", out_path, len(rows))

    filtered = cast(pd.DataFrame, df_out[df_out["val_n"] >= args.min_signals])
    reliable = filtered.sort_values(by="val_avg_return", ascending=False)
    print(f"\n{'='*110}")
    print(f"BNF 이격도×손절×트레일링 그리드서치 — 학습 {train_start}~{train_end} / "
          f"검증 {val_start}~{val_end} (거래대금 상위{args.top_n}종목, 검증신호 {args.min_signals}건 이상만)")
    print(f"{'='*110}")
    if reliable.empty:
        print("검증기간 신호가 충분한 조합이 없습니다 — 전체 결과는 CSV 참고.")
    else:
        cols = ["disc_threshold", "hard_stop_pct", "trail_up", "trail_down",
                "train_n", "train_win_rate", "train_avg_return",
                "val_n", "val_win_rate", "val_avg_return", "overfit_gap"]
        with pd.option_context("display.float_format", lambda v: f"{v:.4f}"):
            print(reliable[cols].head(20).to_string(index=False))
    print(f"{'='*110}")
    print("(참고) 문서 원안 기본값: 이격도 -22.5%~-32.5%(대형/중소형), 손절 미지정(replay_bnf 기본 -8%), "
          "트레일링 상승15%/하락7%")


if __name__ == "__main__":
    main()
