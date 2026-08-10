"""
daily_ohlcv_backfill.py — daily_ohlcv 전종목 일별 히스토리 백필 (KRX OpenAPI, yfinance 미사용).

2026-08-10: 백테스트(scripts/run_*.py 전반)가 매번 yfinance로 재수집하는 걸
발견 — core/ohlcv_cache.py:batch_fetch_cached()는 daily_ohlcv를 캐시로 먼저
확인하지만, 실제 DB를 까보니 daily_ohlcv에 종목당 데이터가 2025-01-03부터
~64행(주 1회 정도의 스파스 스냅샷, 아마 _daily_market_snap_job이 하루 100종목씩만
찍어서)뿐이었다. 백테스트는 MA120 워밍업 때문에 start-760일(약 2023년)까지
필요한데 DB엔 그보다 훨씬 짧게만 있어 캐시 커버리지 조건(`fetch_start 이전부터
데이터 있어야 함`)을 절대 못 만족 — 그래서 매번 전종목을 yfinance로 다시
받았고, 이게 누적돼 이 세션에서 결국 rate-limit에 걸렸다(TechnicalQuant.md
"교차조합 전체 실행 결과" 참고).

이 스크립트는 core/ohlcv_cache.py:fill_daily_from_krx()를 거래일별로 반복
호출해 daily_ohlcv를 제대로 채운다. KRX OpenAPI(data-dbg.krx.co.kr)는
"하루치 전종목"을 API 호출 1회(KOSPI)+1회(KOSDAQ)로 반환하므로(종목별 개별
호출이 필요한 yfinance와 근본적으로 다름) — 전체 종목·전체 기간을 백필해도
API 호출 횟수는 "거래일수 × 2"뿐이라 rate-limit 위험이 사실상 없다(공식
문서 기준 10 req/s, 여기선 하루 2회씩만 씀). 백필 완료 후엔 같은 기간
백테스트를 다시 돌려도 daily_ohlcv 캐시가 히트해 yfinance를 아예 안 탄다.

사용법:
    # 기본: 2022-01-01 ~ 오늘, 이미 채워진 날짜는 건너뜀(재실행 안전)
    python scripts/daily_ohlcv_backfill.py

    # 범위 지정
    python scripts/daily_ohlcv_backfill.py --start 2023-01-01 --end 2026-08-06

    # 이미 채워진 날짜도 강제로 다시 받기(가격 정정 등)
    python scripts/daily_ohlcv_backfill.py --force
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
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

# 종목당 이 정도면 "그날은 이미 채워졌다"로 간주(휴장일엔 0, 정상 거래일엔 보통 2500+).
_FILLED_THRESHOLD = 1000


def _already_filled_dates(dsn: str, start: date, end: date) -> set[date]:
    """daily_ohlcv에 이미 전종목 수준으로 채워진 날짜 집합 (재실행 시 건너뛰기용)."""
    from core.db_sync import connect

    conn = connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT date, COUNT(*) AS n
                FROM daily_ohlcv
                WHERE date BETWEEN %s AND %s
                GROUP BY date
                HAVING COUNT(*) >= %s
                """,
                (start, end, _FILLED_THRESHOLD),
            )
            return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="daily_ohlcv 전종목 일별 히스토리 백필 (KRX OpenAPI)"
    )
    parser.add_argument("--start", default="2022-01-01", help="YYYY-MM-DD (기본: 2022-01-01)")
    parser.add_argument("--end", default=None, help="YYYY-MM-DD (기본: 어제)")
    parser.add_argument("--sleep", type=float, default=0.3,
                         help="거래일 처리 사이 대기(초). KRX rate limit 10req/s, "
                              "하루 2호출이라 여유 있지만 예의상 기본값 유지")
    parser.add_argument("--force", action="store_true",
                         help="이미 채워진 날짜도 다시 받아서 덮어쓰기(기본: 건너뜀)")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)
    if start > end:
        sys.exit(f"--start({start})이 --end({end})보다 늦습니다")

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

    appkey = os.environ.get("KRX_OPENAPI_KEY", "")
    if not appkey:
        sys.exit("KRX_OPENAPI_KEY 환경변수가 필요합니다 (openapi.krx.co.kr 가입 후 .env에 추가)")

    from core.ohlcv_cache import fill_daily_from_krx

    already: set[date] = set()
    if not args.force:
        logger.info("[backfill] 기존 커버리지 조회 중...")
        already = _already_filled_dates(dsn, start, end)
        logger.info("[backfill] 이미 채워진 날짜 %d일 — 건너뜀", len(already))

    total_days = (end - start).days + 1
    n_filled = n_skipped = n_empty = n_total_rows = 0
    t0 = time.time()

    d = start
    i = 0
    while d <= end:
        i += 1
        if d.weekday() >= 5:  # 토/일 — API 호출 없이 건너뜀
            d += timedelta(days=1)
            continue
        if d in already:
            n_skipped += 1
            d += timedelta(days=1)
            continue

        try:
            n_rows = fill_daily_from_krx(dsn, d, appkey=appkey)
        except Exception as e:
            logger.warning("[backfill] %s 실패(계속 진행): %s", d, e)
            n_rows = 0

        if n_rows > 0:
            n_filled += 1
            n_total_rows += n_rows
        else:
            n_empty += 1  # 휴장일이거나 API 응답 없음

        if i % 20 == 0 or d == end:
            elapsed = time.time() - t0
            logger.info(
                "[backfill] 진행 %s (%d/%d일) — 채움 %d 빈날 %d 스킵 %d, %.0f초 경과",
                d, i, total_days, n_filled, n_empty, n_skipped, elapsed,
            )

        time.sleep(args.sleep)
        d += timedelta(days=1)

    logger.info(
        "[backfill] 완료 — %s~%s, 채운 거래일 %d일(%d행), 빈날(휴장일 추정) %d일, "
        "기존 스킵 %d일, 총 %.0f초",
        start, end, n_filled, n_total_rows, n_empty, n_skipped, time.time() - t0,
    )
    print(
        f"\n다음 확인: SELECT symbol, MIN(date), MAX(date), COUNT(*) FROM daily_ohlcv "
        f"WHERE symbol = '005930.KS' GROUP BY symbol;"
    )


if __name__ == "__main__":
    main()
