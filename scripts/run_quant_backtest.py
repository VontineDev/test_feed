"""
run_quant_backtest.py — TechnicalQuant.md 매매타이밍 조건 백테스트 CLI

2026-08-06: 사용자가 제공한 퀀트 전략 문서(종목선택 + 매매타이밍) 검증용.
1차 버전은 전체 시장 펀더멘털 데이터(PBR/PER/ROE/부채비율/매출증가율)가 없어
기술적 조건만 검증했으나, 이후 dart_fundamentals 백필(scripts/
dart_fundamentals_backfill.py) 완료로 --use-fundamentals 플래그를 추가해
SCENARIO1/2 각각에 문서가 명시한 고유 종목선택 조건(1안: PBR≤0.8·ROE≥10%·
부채비율≤100%, 2안: PER≤15)을 정확히 적용할 수 있다 — 두 시나리오가 서로
다른 숫자를 쓰므로 공통 범용 필터를 쓰면 안 된다(analysis/fundamentals.py의
SCENARIO1_THRESHOLDS/SCENARIO2_THRESHOLDS 참고).

사용법:
    # 개별 조건 5종 + 시나리오 2종 전체 비교 (펀더멘털 필터 없이)
    python scripts/run_quant_backtest.py --start 2025-01-02 --end 2026-08-06

    # 시나리오1/2에 실제 펀더멘털 필터까지 적용
    python scripts/run_quant_backtest.py --start 2025-01-02 --end 2026-08-06 --use-fundamentals

    # 특정 조건만
    python scripts/run_quant_backtest.py --condition A_ma20_breakout --start 2025-01-02 --end 2026-08-06

조건 목록:
    A_ma20_breakout    — 이평선 돌파 (주가>MA20 AND MA5>MA20)
    B_ma_alignment     — 정배열 진입 (MA5>MA20>MA60>MA120)
    C_rsi_macd_rebound — RSI(14) 30 상향돌파 OR MACD 골든크로스
    D_new_high20       — 신고가 돌파 (최근 20일 최고가 갱신)
    E_flow_streak      — 수급 결합 (외국인 또는 기관 3일 연속 순매수)
    SCENARIO1          — 문서 1안(밸류+추세추종, 펀더멘털 제외):
                         유니버스=거래대금 상위20% / 진입=MA20돌파+거래량200%↑ /
                         청산=손절-5%, 목표+20%, MA20이탈
    SCENARIO2          — 문서 2안(역발상 과매도반등, 펀더멘털 제외):
                         유니버스=시가총액 상위200 / 진입=RSI 30 상향돌파 /
                         청산=RSI70 익절, 손절-7%
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

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

INDIVIDUAL_CONDITIONS = [
    "A_ma20_breakout", "B_ma_alignment", "C_rsi_macd_rebound",
    "D_new_high20", "E_flow_streak",
]
ALL_CONDITIONS = INDIVIDUAL_CONDITIONS + ["SCENARIO1", "SCENARIO2"]


def _pct(v: Optional[float], dp: int = 1) -> str:
    return f"{v * 100:+.{dp}f}%" if v is not None else "N/A"


def _select_universe(
    ohlcv_map: dict[str, pd.DataFrame],
    listed_shares: dict[str, int],
    start: date,
    end: date,
    mode: str,
) -> set[str]:
    """SCENARIO1/2 전용 유니버스 필터. mode='txamt_top20' | 'mktcap_top200'.

    기간 평균 거래대금/시가총액으로 순위를 매긴다(일별 cross-sectional 순위
    대신 기간 평균을 쓰는 단순화 — 유니버스가 백테스트 기간 내내 크게 변하지
    않는다는 전제, 계산량을 크게 줄이기 위한 의도적 근사).
    """
    stats: dict[str, float] = {}
    for ticker, df in ohlcv_map.items():
        window = df[(df.index.date >= start) & (df.index.date <= end)]  # type: ignore[attr-defined]
        if window.empty:
            continue
        avg_close = float(window["Close"].mean())
        if mode == "txamt_top20":
            avg_vol = float(window["Volume"].mean())
            stats[ticker] = avg_vol * avg_close
        elif mode == "mktcap_top200":
            shares = listed_shares.get(ticker)
            if not shares:
                continue
            stats[ticker] = avg_close * shares
        else:
            raise ValueError(f"알 수 없는 유니버스 모드: {mode!r}")

    if not stats:
        return set()
    ranked = sorted(stats.items(), key=lambda kv: kv[1], reverse=True)
    if mode == "txamt_top20":
        cutoff = max(1, int(len(ranked) * 0.20))
        return {t for t, _ in ranked[:cutoff]}
    return {t for t, _ in ranked[:200]}


def _metrics_from_signals(signals: list) -> dict:
    from analysis.backtest.helpers import _compute_group_metrics
    m = _compute_group_metrics(signals, rf_annual=0.03)
    rets = [s.blended_return for s in signals if s.blended_return is not None]
    win = sum(1 for r in rets if r > 0)
    return {
        "n": len(signals),
        "n_closed": len(rets),
        "win_rate": (win / len(rets)) if rets else None,
        "avg_return": (sum(rets) / len(rets)) if rets else None,
        "sharpe28d_proxy": m.sharpe_28d,  # 참고용(고정기간 수익률 기반 아님 — n/a 될 수 있음)
    }


def run_condition(
    entry_key: str,
    ohlcv_map: dict[str, pd.DataFrame],
    tickers: list[tuple[str, str, str]],
    start: date,
    end: date,
    flow_streak_lookup: Optional[dict] = None,
    universe: Optional[set[str]] = None,
) -> dict:
    from analysis.backtest.quant_signals import replay_quant

    if entry_key == "SCENARIO1":
        hard_stop_pct, target_pct, use_ma20_exit, use_rsi70_exit = 0.05, 0.20, True, False
    elif entry_key == "SCENARIO2":
        hard_stop_pct, target_pct, use_ma20_exit, use_rsi70_exit = 0.07, None, False, True
    else:
        hard_stop_pct, target_pct, use_ma20_exit, use_rsi70_exit = 0.05, 0.15, True, False

    all_signals = []
    for ticker, name, _sector in tickers:
        if universe is not None and ticker not in universe:
            continue
        df = ohlcv_map.get(ticker)
        if df is None or df.empty:
            continue
        market = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"
        try:
            sigs = replay_quant(
                ticker, name, df, market, start, end,
                entry_key=entry_key,
                hard_stop_pct=hard_stop_pct, target_pct=target_pct,
                use_ma20_exit=use_ma20_exit, use_rsi70_exit=use_rsi70_exit,
                flow_lookup=flow_streak_lookup if entry_key == "E_flow_streak" else None,
            )
        except Exception as e:
            logger.debug("  %s 재현 실패(무시): %s", ticker, e)
            continue
        all_signals.extend(sigs)

    return _metrics_from_signals(all_signals)


def main() -> None:
    parser = argparse.ArgumentParser(description="TechnicalQuant.md 매매타이밍 조건 백테스트")
    parser.add_argument("--condition", default="ALL",
                        help=f"조건 이름 또는 ALL. 가능: {', '.join(ALL_CONDITIONS)}, ALL")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end",   required=True, help="YYYY-MM-DD")
    parser.add_argument("--market", default="ALL", choices=["KOSPI", "KOSDAQ", "ALL"])
    parser.add_argument("--max-tickers", type=int, default=0, help="0=전종목")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--output", default="results/quant_backtest.csv")
    parser.add_argument("--use-fundamentals", action="store_true",
                        help="SCENARIO1/2 유니버스에 문서 1안/2안 각각의 종목선택 조건을 "
                             "적용 — 1안: PBR≤0.8·ROE≥10%%·부채비율≤100%%, "
                             "2안: PER≤15. analysis/fundamentals.py + dart_fundamentals "
                             "필요 (scripts/dart_fundamentals_backfill.py로 먼저 백필)")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)
    if start >= end:
        sys.exit("--start 는 --end 보다 이전이어야 합니다")

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

    targets = ALL_CONDITIONS if args.condition.upper() == "ALL" else [args.condition]
    for c in targets:
        if c not in ALL_CONDITIONS:
            sys.exit(f"알 수 없는 조건: {c!r}. 가능: {', '.join(ALL_CONDITIONS)}, ALL")

    from analysis.chart_screener import get_all_tickers, fetch_kind_sector_map
    from analysis.backtest.fetch import _fetch_single_ohlcv
    from core.ohlcv_cache import batch_fetch_cached, load_flow_streaks, load_listed_shares

    logger.info("[quant] 종목 목록 조회 중...")
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
    logger.info("[quant] 대상 티커 %d개", len(tickers))

    # MA120/52주 워밍업 여유 — 약 250 거래일 = 달력일 ~365일
    fetch_start = start - timedelta(days=400)
    ticker_pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t, _, _ in tickers]

    t0 = time.time()
    logger.info("[quant] OHLCV 로드 중 (캐시 우선)...")
    ohlcv_map = batch_fetch_cached(ticker_pairs, fetch_start, end, args.workers, dsn, _fetch_single_ohlcv)
    logger.info("[quant] OHLCV 로드 완료 — %.1f초, %d/%d 티커", time.time() - t0, len(ohlcv_map), len(tickers))

    logger.info("[quant] 수급 streak 로드 중...")
    flow_streak_lookup = load_flow_streaks(dsn, [t for t, _, _ in tickers], start, end)
    logger.info("[quant] 수급 streak %d건 로드", len(flow_streak_lookup))

    listed_shares = load_listed_shares(dsn)

    fundamental_universe_s1 = None
    fundamental_universe_s2 = None
    if args.use_fundamentals:
        from analysis.fundamentals import (
            SCENARIO1_THRESHOLDS,
            SCENARIO2_THRESHOLDS,
            compute_ratios,
            screen,
        )
        logger.info("[quant] 펀더멘털 스크리닝 중 (dart_fundamentals)...")
        ratios_df = compute_ratios(dsn)
        # 문서 1안/2안은 각자 다른 종목선택 숫자를 쓴다(1안: PBR/ROE/부채비율,
        # 2안: PER만) — 동일한 범용 필터를 공유하면 두 시나리오 모두 문서와
        # 어긋나므로 시나리오별로 따로 스크리닝한다.
        fundamental_universe_s1 = screen(ratios_df, SCENARIO1_THRESHOLDS)
        fundamental_universe_s2 = screen(ratios_df, SCENARIO2_THRESHOLDS)
        logger.info("[quant] 1안 필터(PBR≤0.8, ROE≥10%%, 부채비율≤100%%) 통과: %d/%d종목",
                    len(fundamental_universe_s1), len(ratios_df))
        logger.info("[quant] 2안 필터(PER≤15) 통과: %d/%d종목",
                    len(fundamental_universe_s2), len(ratios_df))

    universe_txamt = None
    universe_mktcap = None
    if "SCENARIO1" in targets:
        universe_txamt = _select_universe(ohlcv_map, listed_shares, start, end, "txamt_top20")
        if fundamental_universe_s1 is not None:
            before = len(universe_txamt)
            universe_txamt &= fundamental_universe_s1
            logger.info("[quant] SCENARIO1 유니버스(거래대금 상위20%% ∩ 1안 펀더멘털): %d→%d종목",
                        before, len(universe_txamt))
        else:
            logger.info("[quant] SCENARIO1 유니버스(거래대금 상위20%%): %d종목", len(universe_txamt))
    if "SCENARIO2" in targets:
        universe_mktcap = _select_universe(ohlcv_map, listed_shares, start, end, "mktcap_top200")
        if fundamental_universe_s2 is not None:
            before = len(universe_mktcap)
            universe_mktcap &= fundamental_universe_s2
            logger.info("[quant] SCENARIO2 유니버스(시가총액 상위200 ∩ 2안 펀더멘털): %d→%d종목",
                        before, len(universe_mktcap))
        else:
            logger.info("[quant] SCENARIO2 유니버스(시가총액 상위200): %d종목", len(universe_mktcap))

    rows = []
    for cond in targets:
        universe = None
        if cond == "SCENARIO1":
            universe = universe_txamt
        elif cond == "SCENARIO2":
            universe = universe_mktcap

        logger.info("[quant] 조건 %s 실행 중...", cond)
        t1 = time.time()
        m = run_condition(
            cond, ohlcv_map, tickers, start, end,
            flow_streak_lookup=flow_streak_lookup, universe=universe,
        )
        logger.info("[quant] %s 완료 — %.1f초, 신호 %d건 승률 %s",
                    cond, time.time() - t1, m["n"], _pct(m["win_rate"]))
        rows.append({"조건": cond, **m})

    df_out = pd.DataFrame(rows)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("[quant] 결과 저장: %s", out_path)

    print(f"\n{'='*70}")
    print(f"TechnicalQuant.md 매매타이밍 백테스트 ({start}~{end})")
    print(f"{'='*70}")
    print(f"{'조건':22s} {'신호수':>8s} {'청산완료':>8s} {'승률':>8s} {'평균수익':>10s}")
    for r in rows:
        print(f"{r['조건']:22s} {r['n']:>8d} {r['n_closed']:>8d} "
              f"{_pct(r['win_rate']):>8s} {_pct(r['avg_return']):>10s}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
