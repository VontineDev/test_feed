"""
dart_fundamentals_backfill.py — 전체시장 핵심 재무 스냅샷 백필

2026-08-06: TechnicalQuant.md 펀더멘털 스크리닝(PBR/PER/ROE/부채비율/매출증가율)용.
dart_companies(corp_code↔stock_code)와 krx_listings를 매칭해 corp_code 목록을
만들고, DART fnlttSinglAcntAll API(회사당 1회 호출, 당기+전기 동시 반환)로
dart_fundamentals에 upsert한다.

사용법:
    python scripts/dart_fundamentals_backfill.py --year 2025
    python scripts/dart_fundamentals_backfill.py --year 2025 --max-tickers 50  # 테스트용
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
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


async def _run(year: str, max_tickers: int) -> None:
    import asyncpg
    from urllib.parse import quote

    from core.db_schema import init_db
    from data.dart_sync import sync_fundamentals

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        u, p = os.environ.get("DB_USER", ""), os.environ.get("DB_PASSWORD", "")
        h = os.environ.get("DB_HOST", "localhost")
        port = os.environ.get("DB_PORT", "5432")
        db = os.environ.get("DB_NAME", "news_db")
        if u and p:
            dsn = f"postgresql://{u}:{quote(p)}@{h}:{port}/{db}"
    if not dsn:
        sys.exit("DATABASE_URL (또는 DB_USER/DB_PASSWORD) 환경변수가 필요합니다")

    pool = await asyncpg.create_pool(dsn, min_size=2, max_size=5)
    try:
        logger.info("[fund-backfill] 스키마 확인/생성...")
        await init_db(pool)

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT k.short_code, dc.corp_code
                FROM krx_listings k
                JOIN dart_companies dc ON dc.stock_code = k.short_code
                ORDER BY k.short_code
                """
            )
        pairs = [(r["corp_code"], r["short_code"]) for r in rows]
        if max_tickers > 0:
            pairs = pairs[:max_tickers]
        logger.info("[fund-backfill] 대상 %d종목 (krx_listings ∩ dart_companies)", len(pairs))

        corp_stock_pairs = [(corp_code, stock_code) for corp_code, stock_code in pairs]
        n = await sync_fundamentals(pool, corp_stock_pairs, bsns_year=year, api_key=None)
        logger.info("[fund-backfill] 완료 — %d건 upsert (연도 %s)", n, year)
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="전체시장 핵심 재무 스냅샷 백필")
    parser.add_argument("--year", default=None, help="사업연도 YYYY (기본: 작년)")
    parser.add_argument("--max-tickers", type=int, default=0, help="0=전종목 (테스트용 제한)")
    args = parser.parse_args()

    from datetime import date
    year = args.year or str(date.today().year - 1)
    asyncio.run(_run(year, args.max_tickers))


if __name__ == "__main__":
    main()
