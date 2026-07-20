"""
ohlcv_warm.py — daily_ohlcv 히스토리 백필 + 일배치 워밍 잡

KRX OpenAPI(get_daily_ohlcv_all)를 사용해 날짜별 전 종목 OHLCV를 채운다.
이미 채워진 날짜는 스킵 — 재실행 안전.

백필 (최초 1회):
  python jobs/ohlcv_warm.py --start 2025-01-02
  python jobs/ohlcv_warm.py --start 2025-01-02 --end 2026-06-14 --delay 0.1

일배치 (run_scheduler에서 호출):
  daily_ohlcv_warm_job(dsn)  →  최근 7일 캐치업 (기채움 날짜 스킵)

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

def _get_filled_dates(
    dsn: str, start: date, end: date, min_rows: int = 1
) -> set[date]:
    """daily_ohlcv에 이미 저장된 날짜 집합.

    min_rows: 이 행수 이상인 날짜만 '채워짐'으로 간주 — 부분 적재일
    (수집 중단 등)을 재수집 대상에 포함시키기 위한 임계값.
    """
    from core.db_sync import connect
    conn = connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT date FROM daily_ohlcv WHERE date BETWEEN %s AND %s"
                " GROUP BY date HAVING COUNT(*) >= %s",
                (start, end, min_rows),
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
    min_rows: int = 1,
) -> int:
    """start~end 기간 daily_ohlcv 백필.

    이미 데이터가 있는 날짜는 스킵하므로 중단 후 재실행 안전.
    KRX 휴장일은 API가 빈 배열을 반환 → 자동 스킵.
    min_rows: 행수가 이 값 미만인 날짜는 부분 적재로 보고 재수집.

    반환: 저장된 총 행 수 (upsert 포함)
    """
    from core.ohlcv_cache import fill_daily_from_krx

    filled = _get_filled_dates(dsn, start, end, min_rows=min_rows)
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


# 정상 거래일의 daily_ohlcv 행수는 전 종목 ~2,700행 — 이보다 크게 적으면
# 수집이 중간에 끊긴 부분 적재로 보고 재수집한다.
_DAILY_MIN_ROWS = 1000


def daily_ohlcv_warm_job(dsn: str) -> int:
    """최근 7일 daily_ohlcv 캐치업. run_scheduler에서 호출.

    '전일 1건 + 주말 스킵' 방식은 금요일분을 채우는 회차가 없었다
    (토요일 회차 부재, 월요일엔 전일=일요일이라 스킵). backfill_ohlcv
    재사용으로 전환 — 기채움 날짜는 스킵되므로 평시엔 전일 1건만
    수집되고, 주말 갭·최대 7일 장애 결손·부분 적재일은 자동 치유된다.

    KRX_OPENAPI_KEY 미설정 시 fill_daily_from_krx 내부에서 경고 후 0 반환.
    """
    today = date.today()
    n = backfill_ohlcv(
        dsn,
        start=today - timedelta(days=7),
        end=today - timedelta(days=1),
        min_rows=_DAILY_MIN_ROWS,
    )
    logger.info("[ohlcv-warm] 일배치 캐치업 완료 — %d행", n)
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
    parser.add_argument(
        "--min-rows",
        type=int,
        default=1,
        help="이 행수 미만인 날짜는 부분 적재로 보고 재수집 (기본 1 = 기존 동작)",
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

    total = backfill_ohlcv(dsn, start, end, delay_s=args.delay, min_rows=args.min_rows)
    print(f"완료: {total}행 저장 ({start} ~ {end})")
