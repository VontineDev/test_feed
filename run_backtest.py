#!/usr/bin/env python3
"""
run_backtest.py - 백테스트 CLI 진입점

사용법:
  python run_backtest.py --mode ichimoku --start 2025-01-01 --end 2026-01-01
  python run_backtest.py --mode stage    --start 2025-01-01 --end 2026-01-01 --market KOSDAQ
  python run_backtest.py --mode cross    --start 2025-01-01 --end 2026-01-01 --max 100
  python run_backtest.py --mode ichimoku --start 2025-01-01 --end 2026-01-01 --tx-cost 0.003

옵션:
  --mode         ichimoku | stage | cross  (필수)
  --start        시작일 YYYY-MM-DD          (필수)
  --end          종료일 YYYY-MM-DD          (필수)
  --market       KOSPI | KOSDAQ | ALL       (기본: ALL)
  --max          최대 티커 수 (0=전종목)    (기본: 200)
  --tx-cost      왕복 거래비용 비율         (기본: KRX 실비 약 0.0021)
  --rf           무위험수익률 연율          (기본: 0.030)
  --workers      병렬 워커 수               (기본: 8)
"""

from __future__ import annotations

import argparse
import logging
from datetime import date

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _parse_date(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except ValueError:
        raise argparse.ArgumentTypeError(f"날짜 형식 오류 (YYYY-MM-DD): {s!r}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="통합 백테스트 엔진 - 이치모쿠 / 3단계(Stage 1) / 교차",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=None,
    )
    parser.add_argument(
        "--mode", choices=["ichimoku", "stage", "cross"], required=True,
        help="백테스트 모드",
    )
    parser.add_argument(
        "--start", type=_parse_date, required=True,
        metavar="YYYY-MM-DD", help="시작일",
    )
    parser.add_argument(
        "--end", type=_parse_date, required=True,
        metavar="YYYY-MM-DD", help="종료일",
    )
    parser.add_argument(
        "--market", choices=["KOSPI", "KOSDAQ", "ALL"], default="ALL",
        help="시장 필터 (기본: ALL)",
    )
    parser.add_argument(
        "--max", type=int, default=200, dest="max_tickers",
        metavar="N", help="최대 티커 수 (0=전종목, 기본: 200)",
    )
    parser.add_argument(
        "--tx-cost", type=float, default=None, dest="tx_cost",
        metavar="F", help="왕복 거래비용 비율 (예: 0.0021, 기본: KRX 실비)",
    )
    parser.add_argument(
        "--rf", type=float, default=0.030, dest="rf_rate",
        metavar="F", help="무위험수익률 연율 (기본: 0.030 = 3%%)",
    )
    parser.add_argument(
        "--workers", type=int, default=8,
        help="병렬 워커 수 (기본: 8)",
    )
    parser.add_argument(
        "--dsn", type=str, default=None,
        metavar="DSN",
        help="PostgreSQL DSN (설정 시 OHLCV 캐시 + 수급 조건 5 활성화). "
             "미설정 시 DATABASE_URL 환경변수에서 자동 로드.",
    )
    args = parser.parse_args()

    from backtest_engine import BacktestConfig, TX_COST_DEFAULT, run_backtest

    dsn = args.dsn
    if dsn is None:
        try:
            from db import get_dsn
            dsn = get_dsn()
        except Exception:
            dsn = None

    config = BacktestConfig(
        mode=args.mode,
        start=args.start,
        end=args.end,
        market=args.market,
        tx_cost_rt=args.tx_cost if args.tx_cost is not None else TX_COST_DEFAULT,
        max_tickers=args.max_tickers,
        rf_rate_annual=args.rf_rate,
        workers=args.workers,
        dsn=dsn,
    )

    print(f"\n백테스트 실행 중... (티커 수집 + 데이터 다운로드 수 분 소요)\n")
    result = run_backtest(config)
    print(result.to_text_report())


if __name__ == "__main__":
    main()
