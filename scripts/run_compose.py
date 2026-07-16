"""
run_compose.py — Tier-1/2 조합전략 백테스트 CLI

사용법:
    # 단일 전략
    python scripts/run_compose.py --strategy AND-1 --start 2026-04-13 --end 2026-06-14

    # 전체 Tier-1 비교 (AND-1/AND-2/SCORE-1/FUNNEL-1)
    python scripts/run_compose.py --strategy ALL --start 2026-04-13 --end 2026-06-14

    # 거래대금 조사 전략만 비교 (AND-1/AND-2/AND-3/AND-4)
    python scripts/run_compose.py --strategy TXAMT --start 2026-04-13 --end 2026-06-14

    # 옵션
    python scripts/run_compose.py --strategy SCORE-1 --start 2026-04-13 --end 2026-06-14 \\
        --market KOSPI --workers 8 --html results/score1.html

전략 목록:
    AND-1    — 이치모쿠 ∩ Stage2+ ∩ 수급 비매도 (기준선)
    AND-2    — AND-1 ∩ 거래량 주내 중앙값 이상
    AND-3    — AND-1 ∩ 거래대금 주내 중앙값 이상 (chart_signals.close×volume_w)
    AND-4    — AND-1 ∩ 거래대금 주내 상위 30%
    SCORE-1  — Stage·거래대금·수급 z-score 가중합 top-20 [권장]
    FUNNEL-1 — 수급 스크린 → 4주 내 이치모쿠 돌파

데이터 제약:
    - flow 의존 전략: daily_flow 기준 하한 2025-01-02
    - ichimoku 의존 전략: chart_signals 2025-W01 ~ 현재 (76주+, 계속 누적 중)
    - AND-*/SCORE-1/FUNNEL-1: --start 2025-01-02 이후 전 구간 실행 가능
    - 거래대금 플래그(AND-3/AND-4/SCORE-1): chart_signals.close 사용 — daily_ohlcv 불필요
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv(os.path.join(Path(__file__).parent.parent, ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

ALL_STRATEGIES   = ["AND-1", "AND-2", "AND-3", "AND-4", "AND-5", "AND-6", "SCORE-1", "FUNNEL-1"]
TXAMT_STRATEGIES = ["AND-1", "AND-2", "AND-3", "AND-4"]   # 거래대금 vs 거래량 비교셋
RELAX_STRATEGIES = ["AND-1", "AND-3", "AND-5", "AND-6"]   # stage2+ vs stage1+ 완화 비교셋


def _pct(v, dp=1):
    return f"{v * 100:+.{dp}f}%" if v is not None else "N/A"


def _val(v, dp=2):
    return f"{v:.{dp}f}" if v is not None else "N/A"


def _run_one(strategy: str, start: date, end: date, market: str, workers: int, dsn: str):
    from analysis.backtest.models import BacktestConfig
    from analysis.backtest.engine import run_backtest
    cfg = BacktestConfig(
        mode="compose",
        strategy=strategy,
        start=start,
        end=end,
        market=market,
        workers=workers,
        dsn=dsn,
    )
    return run_backtest(cfg)


def _print_single(res) -> None:
    m = res.overall
    cfg = res.config
    print(f"\n{'='*60}")
    print(f"전략: {cfg.strategy}  기간: {cfg.start}~{cfg.end}  시장: {cfg.market}")
    print(f"신호 수: {m.n}")
    print(f"  승률  7d/28d/91d : {_pct(m.win_rate_7d)} / {_pct(m.win_rate_28d)} / {_pct(m.win_rate_91d)}")
    print(f"  수익률 28d/91d   : {_pct(m.avg_return_28d)} / {_pct(m.avg_return_91d)}")
    print(f"  샤프  7d/28d/91d : {_val(m.sharpe_7d)} / {_val(m.sharpe_28d)} / {_val(m.sharpe_91d)}")
    print(f"  MDD             : {_pct(m.mdd)}")
    if m.win_rate_sell is not None:
        print(f"  매도신호 승률   : {_pct(m.win_rate_sell)}  평균수익: {_pct(m.avg_return_sell)}")
    if res.note:
        print(f"  비고: {res.note}")
    print(f"{'='*60}")


def _print_comparison(results: list) -> None:
    """전략별 지표를 샤프비율(28d) 기준 내림차순 비교표 출력."""
    rows = []
    for res in results:
        m = res.overall
        rows.append({
            "전략":       res.config.strategy,
            "신호수":      m.n,
            "승률28d":     _pct(m.win_rate_28d),
            "승률91d":     _pct(m.win_rate_91d),
            "수익28d":     _pct(m.avg_return_28d),
            "수익91d":     _pct(m.avg_return_91d),
            "샤프28d":     _val(m.sharpe_28d),
            "샤프91d":     _val(m.sharpe_91d),
            "MDD":         _pct(m.mdd),
            "매도승률":     _pct(m.win_rate_sell),
        })

    # 샤프28d 기준 정렬 (None은 맨 뒤)
    def _sort_key(r):
        v = r["샤프28d"]
        try:
            return (-float(v), r["전략"])
        except (ValueError, TypeError):
            return (float("inf"), r["전략"])

    rows.sort(key=_sort_key)

    # 컬럼 너비
    cols = list(rows[0].keys()) if rows else []
    widths = {c: max(len(c), max(len(str(r[c])) for r in rows)) for c in cols}

    sep = "  ".join("-" * widths[c] for c in cols)
    hdr = "  ".join(c.ljust(widths[c]) for c in cols)

    print(f"\n{'='*len(sep)}")
    print("Tier-1 전략 비교 (샤프28d 내림차순)")
    print(f"{'='*len(sep)}")
    print(hdr)
    print(sep)
    for r in rows:
        print("  ".join(str(r[c]).ljust(widths[c]) for c in cols))
    print(f"{'='*len(sep)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Tier-1 조합전략 백테스트")
    parser.add_argument("--strategy", required=True,
                        help=f"전략 이름 또는 ALL/TXAMT. 가능: {', '.join(ALL_STRATEGIES)}, ALL, TXAMT"
                             f" (TXAMT = AND-1/2/3/4 거래대금 비교)")
    parser.add_argument("--start", required=True, help="백테스트 시작일 (YYYY-MM-DD)")
    parser.add_argument("--end",   required=True, help="백테스트 종료일 (YYYY-MM-DD)")
    parser.add_argument("--market",  default="ALL", choices=["KOSPI", "KOSDAQ", "ALL"])
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--html", default="", help="단일 전략 HTML 리포트 저장 경로 (ALL 시 무시)")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)
    if start >= end:
        sys.exit("--start 는 --end 보다 이전이어야 합니다")

    dsn = os.environ.get("DATABASE_URL") or None
    if not dsn:
        u = os.environ.get("DB_USER", "")
        p = os.environ.get("DB_PASSWORD", "")
        h = os.environ.get("DB_HOST", "localhost")
        port = os.environ.get("DB_PORT", "5432")
        db = os.environ.get("DB_NAME", "news_db")
        if u and p:
            from urllib.parse import quote
            dsn = f"postgresql://{u}:{quote(p)}@{h}:{port}/{db}"
    if not dsn:
        sys.exit("DATABASE_URL (또는 DB_USER/DB_PASSWORD) 환경변수가 필요합니다")

    if args.strategy.upper() == "ALL":
        targets = ALL_STRATEGIES
    elif args.strategy.upper() == "TXAMT":
        targets = TXAMT_STRATEGIES
    elif args.strategy.upper() == "RELAX":
        targets = RELAX_STRATEGIES
    elif args.strategy in ALL_STRATEGIES:
        targets = [args.strategy]
    else:
        sys.exit(f"알 수 없는 전략: {args.strategy!r}. 가능: {', '.join(ALL_STRATEGIES)}, ALL, TXAMT")

    results = []
    for strat in targets:
        logger.info("[run_compose] 전략 %s 실행 중...", strat)
        try:
            res = _run_one(strat, start, end, args.market, args.workers, dsn)
            results.append(res)
            _print_single(res)

            # HTML 저장 (단일 전략 + --html 지정 시)
            if len(targets) == 1 and args.html:
                html_path = Path(args.html)
                html_path.parent.mkdir(parents=True, exist_ok=True)
                saved = res.to_html_report(str(html_path))
                logger.info("[run_compose] HTML 저장: %s", saved)
        except Exception as exc:
            logger.error("[run_compose] %s 실패: %s", strat, exc, exc_info=True)

    if len(results) > 1:
        _print_comparison(results)


if __name__ == "__main__":
    main()
