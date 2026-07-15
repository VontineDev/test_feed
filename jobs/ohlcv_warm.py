"""
ohlcv_warm.py — daily_ohlcv 히스토리 백필 + 일배치 워밍 잡

KRX OpenAPI(get_daily_ohlcv_all)를 사용해 날짜별 전 종목 OHLCV를 채운다.
이미 채워진 날짜는 스킵 — 재실행 안전.

백필 (최초 1회):
  python jobs/ohlcv_warm.py --start 2025-01-02
  python jobs/ohlcv_warm.py --start 2025-01-02 --end 2026-06-14 --delay 0.1

일배치 (run_scheduler에서 호출):
  daily_ohlcv_warm_job(dsn)  →  전일 1건

필요 환경변수:
  DATABASE_URL   — Postgres DSN
  KRX_OPENAPI_KEY — KRX Open API 인증키 (미설정 시 skip & 경고)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, timedelta

# 프로젝트 루트를 sys.path에 추가
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

logger = logging.getLogger(__name__)


# ── 헬퍼 ─────────────────────────────────────────────────────

def _get_filled_dates(dsn: str, start: date, end: date) -> set[date]:
    """daily_ohlcv에 이미 저장된 날짜 집합"""
    import psycopg2
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT date FROM daily_ohlcv WHERE date BETWEEN %s AND %s",
                (start, end),
            )
            return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


def _weekdays(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


# ── 공개 함수 ─────────────────────────────────────────────────

def backfill_ohlcv(
    dsn: str,
    start: date,
    end: date,
    delay_s: float = 0.2,
) -> int:
    """start~end 기간 daily_ohlcv 백필.

    이미 데이터가 있는 날짜는 스킵하므로 중단 후 재실행 안전.
    KRX 휴장일은 API가 빈 배열을 반환 → 자동 스킵.

    반환: 저장된 총 행 수 (upsert 포함)
    """
    from core.ohlcv_cache import fill_daily_from_krx

    filled = _get_filled_dates(dsn, start, end)
    days = [d for d in _weekdays(start, end) if d not in filled]

    logger.info(
        "[ohlcv-warm] 백필 대상 %d일 (기존 %d일 스킵) %s ~ %s",
        len(days), len(filled), start, end,
    )

    total = 0
    for i, d in enumerate(days, 1):
        n = fill_daily_from_krx(dsn, d)
        total += n
        if n > 0:
            logger.info("[ohlcv-warm] %s %d종목 저장 (%d/%d)", d, n, i, len(days))
        else:
            logger.debug("[ohlcv-warm] %s 데이터 없음 (휴장일) (%d/%d)", d, i, len(days))
        if delay_s > 0:
            time.sleep(delay_s)

    logger.info("[ohlcv-warm] 백필 완료 — 총 %d행", total)
    return total


def daily_ohlcv_warm_job(dsn: str) -> int:
    """전일 daily_ohlcv 채우기. run_scheduler에서 호출.

    KRX_OPENAPI_KEY 미설정 시 fill_daily_from_krx 내부에서 경고 후 0 반환.
    """
    from core.ohlcv_cache import fill_daily_from_krx

    # TODO(core.dates): core.dates.last_trading_day와 의미가 다름 — 여기는
    # 주말 보정이 아니라 스킵. 월요일 실행 시 '어제'=일요일이라 금요일
    # 데이터를 채우는 회차가 없다(잠재 커버리지 갭). 보정 전환은 동작
    # 변경이므로 리팩토링 범위 밖 — 별도 fix로 판단 필요.
    yesterday = date.today() - timedelta(days=1)
    if yesterday.weekday() >= 5:
        logger.debug("[ohlcv-warm] 전일(%s) 주말 — 스킵", yesterday)
        return 0

    n = fill_daily_from_krx(dsn, yesterday)
    logger.info("[ohlcv-warm] 일배치 %s %d종목", yesterday, n)
    return n


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import os

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    parser = argparse.ArgumentParser(description="daily_ohlcv 히스토리 백필")
    parser.add_argument("--start", default="2025-01-02", help="시작일 YYYY-MM-DD")
    parser.add_argument(
        "--end",
        default=str(date.today() - timedelta(days=1)),
        help="종료일 YYYY-MM-DD (기본: 어제)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="API 호출 간격(초, 기본 0.2 = 5 req/s)",
    )
    args = parser.parse_args()

    try:
        from core.db import get_dsn as _get_dsn
        dsn = _get_dsn()
    except Exception as e:
        raise SystemExit(f"DSN 구성 실패: {e}")

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    if start > end:
        raise SystemExit(f"start({start}) > end({end})")

    total = backfill_ohlcv(dsn, start, end, delay_s=args.delay)
    print(f"완료: {total}행 저장 ({start} ~ {end})")
