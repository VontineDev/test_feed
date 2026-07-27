"""
db_market.py  —  시세·수급 테이블 접근 함수
────────────────────────────────────────────────────────────
ticker_names / intraday_volumes / daily_ohlcv / daily_flow.
core.db facade를 통해 re-export됨.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)


# ── ticker_names: pykrx 개별 조회로 종목명 캐시 ─────────────────
async def upsert_ticker_names(pool: asyncpg.Pool, tickers: list[str]) -> int:
    """
    yfinance 심볼 목록(예: ['066570.KS', '035720.KQ'])에 대해
    pykrx get_market_ticker_name()으로 이름을 조회하고 ticker_names 테이블에 upsert.
    이미 존재하는 ticker는 건너뜀(updated_at 기준 7일 초과 시 갱신).
    반환: 신규/갱신된 행 수.
    """
    if not tickers:
        return 0

    async with pool.acquire() as conn:
        existing = await conn.fetch(
            """
            SELECT ticker FROM ticker_names
            WHERE ticker = ANY($1)
              AND updated_at > NOW() - INTERVAL '7 days'
            """,
            tickers,
        )
    skip = {r["ticker"] for r in existing}
    to_fetch = [t for t in tickers if t not in skip]
    if not to_fetch:
        return 0

    import asyncio as _aio
    from concurrent.futures import ThreadPoolExecutor as _TPE

    def _lookup(ticker: str) -> tuple[str, str] | None:
        code = ticker.split(".")[0]
        try:
            import pykrx.stock as _ps
            name = _ps.get_market_ticker_name(code)
            return (ticker, name) if name else None
        except Exception:
            return None

    loop = _aio.get_event_loop()
    with _TPE(max_workers=4) as ex:
        results = await loop.run_in_executor(
            ex,
            lambda: [r for r in map(_lookup, to_fetch) if r],
        )

    if not results:
        return 0

    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO ticker_names (ticker, name_ko, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (ticker) DO UPDATE
              SET name_ko = EXCLUDED.name_ko, updated_at = NOW()
            """,
            results,
        )
    logger.info("[ticker_names] %d/%d 종목명 upsert", len(results), len(to_fetch))
    return len(results)


# ── 분봉 거래량 저장 ─────────────────────────────────────────
async def save_intraday_volumes(
    pool: asyncpg.Pool,
    rows: list[dict],
) -> int:
    """
    분봉 거래량 데이터 일괄 저장. 중복(symbol+ts+interval) 시 건너뛴다.
    저장된 건수 반환.
    """
    if not rows:
        return 0
    inserted = 0
    try:
        async with pool.acquire() as conn:
            for r in rows:
                result = await conn.execute(
                    """
                    INSERT INTO intraday_volumes
                        (symbol, market, ts, interval,
                         open, high, low, close, volume,
                         is_extended, source)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                    ON CONFLICT (symbol, ts, interval) DO NOTHING
                    """,
                    r["symbol"],
                    r["market"],
                    r["ts"],
                    r["interval"],
                    r.get("open"),
                    r.get("high"),
                    r.get("low"),
                    r.get("close"),
                    r["volume"],
                    r.get("is_extended", False),
                    r["source"],
                )
                if result.endswith("1"):
                    inserted += 1
        logger.info("[분봉] %s 저장 %d/%d건", rows[0]["symbol"], inserted, len(rows))
    except Exception as e:
        logger.error("[분봉] 저장 실패: %s", e)
    return inserted


async def fetch_intraday_volumes(
    pool: asyncpg.Pool,
    symbol: str,
    interval: str = "5m",
    limit: int = 2000,
) -> list[dict]:
    """저장된 분봉 거래량 조회 (최신순)."""
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, market, ts, interval,
                       open, high, low, close, volume,
                       is_extended, source
                FROM   intraday_volumes
                WHERE  symbol = $1 AND interval = $2
                ORDER  BY ts DESC
                LIMIT  $3
                """,
                symbol,
                interval,
                limit,
            )
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("[분봉] 조회 실패: %s", e)
        return []


# ── 일봉 OHLCV 저장 ─────────────────────────────────────────
async def save_daily_ohlcv(
    pool: asyncpg.Pool,
    rows: list[dict],
) -> int:
    """
    일봉 OHLCV 데이터 일괄 저장. 중복(symbol+date) 시 최신 값으로 갱신.
    저장/갱신된 건수 반환.
    """
    if not rows:
        return 0
    upserted = 0
    try:
        async with pool.acquire() as conn:
            for r in rows:
                result = await conn.execute(
                    """
                    INSERT INTO daily_ohlcv
                        (symbol, market, date, open, high, low, close, volume, source)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                        open = EXCLUDED.open, high = EXCLUDED.high,
                        low = EXCLUDED.low, close = EXCLUDED.close,
                        volume = EXCLUDED.volume, fetched_at = now()
                    """,
                    r["symbol"],
                    r["market"],
                    r["date"],
                    r.get("open"),
                    r.get("high"),
                    r.get("low"),
                    r["close"],
                    r.get("volume"),
                    r["source"],
                )
                if result.endswith("1"):
                    upserted += 1
        logger.info("[일봉] %s 저장 %d/%d건", rows[0]["symbol"], upserted, len(rows))
    except Exception as e:
        logger.error("[일봉] 저장 실패: %s", e)
    return upserted


async def fetch_daily_ohlcv(
    pool: asyncpg.Pool,
    symbol: str,
    limit: int = 365,
) -> list[dict]:
    """저장된 일봉 OHLCV 조회 (최신순)."""
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, market, date, open, high, low, close, volume, source
                FROM   daily_ohlcv
                WHERE  symbol = $1
                ORDER  BY date DESC
                LIMIT  $2
                """,
                symbol,
                limit,
            )
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("[일봉] 조회 실패: %s", e)
        return []


async def get_daily_ohlcv_symbols(pool: asyncpg.Pool) -> list[dict]:
    """저장된 종목별 일봉 데이터 현황 조회."""
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, market, COUNT(*) AS cnt,
                       MIN(date) AS first_date, MAX(date) AS last_date
                FROM   daily_ohlcv
                GROUP  BY symbol, market
                ORDER  BY symbol
                """
            )
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("[일봉] 종목 현황 조회 실패: %s", e)
        return []


async def save_daily_flow(
    pool: asyncpg.Pool,
    ticker: str,
    trade_date: "date",
    foreign_net: Optional[int],
    inst_net: Optional[int],
    foreign_streak: Optional[int] = None,
    inst_streak: Optional[int] = None,
    personal_net: Optional[int] = None,
    personal_streak: Optional[int] = None,
) -> None:
    """Upsert one daily_flow row. trade_date: 'YYYY-MM-DD' string."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO daily_flow
                (ticker, trade_date, foreign_net, inst_net, foreign_streak, inst_streak,
                 personal_net, personal_streak)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (ticker, trade_date)
            DO UPDATE SET
                foreign_net     = EXCLUDED.foreign_net,
                inst_net        = EXCLUDED.inst_net,
                foreign_streak  = EXCLUDED.foreign_streak,
                inst_streak     = EXCLUDED.inst_streak,
                personal_net    = EXCLUDED.personal_net,
                personal_streak = EXCLUDED.personal_streak
            """,
            ticker,
            trade_date,
            foreign_net,
            inst_net,
            foreign_streak,
            inst_streak,
            personal_net,
            personal_streak,
        )


async def get_prev_streak(
    pool: asyncpg.Pool,
    ticker: str,
    trade_date: "date",
) -> tuple[Optional[int], Optional[int], Optional[int]]:
    """전일 foreign_streak, inst_streak, personal_streak 반환. 없으면 (None, None, None)."""
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT foreign_streak, inst_streak, personal_streak
                FROM   daily_flow
                WHERE  ticker = $1
                  AND  trade_date < $2
                ORDER  BY trade_date DESC
                LIMIT  1
                """,
                ticker,
                trade_date,
            )
        if row:
            return row["foreign_streak"], row["inst_streak"], row["personal_streak"]
    except Exception as e:
        logger.debug("[stage] get_prev_streak 실패 (%s): %s", ticker, e)
    return None, None, None
