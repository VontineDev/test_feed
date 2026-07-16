"""
db_stage.py  —  스테이지·워치리스트·섹터 테이블 접근 함수
────────────────────────────────────────────────────────────
stage_classifications / watchlist_vol_log / sector_daily_stats.
core.db facade를 통해 re-export됨.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)


# ── 3단계 분류 이력 조회 ─────────────────────────────────────
async def get_stage1_history(
    pool: asyncpg.Pool,
    tickers: list[str],
    since_date: "datetime | date",
) -> dict[str, list[dict]]:
    """
    stage_classifications WHERE stage=1 에서 Stage 1 이력 반환.

    D1 결정: chart_signals(Ichimoku 통과만) 대신 stage_classifications 조회.
    이유: 전 종목 Stage 1 일별 감지와 일관 — Ichimoku 통과 여부와 독립.
    Stage 2 lookback은 최소 5일이므로 당일 Stage 1과 충돌 없음 (D9).

    반환: {ticker: [{classified_date, s1_high, s1_volume, s1_txamt}, ...]}
    s1_high / s1_txamt이 NULL인 행은 해당 조건 스킵.
    """
    if not tickers:
        return {}
    if hasattr(since_date, "date"):
        since_date = since_date.date()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT ticker, classified_date, s1_high, s1_volume, s1_txamt
                FROM   stage_classifications
                WHERE  ticker = ANY($1)
                  AND  stage  = 1
                  AND  classified_date >= $2
                ORDER  BY classified_date DESC
                """,
                tickers,
                since_date,
            )
        result: dict[str, list[dict]] = {}
        for r in rows:
            result.setdefault(r["ticker"], []).append(dict(r))
        return result
    except Exception as e:
        logger.error("[stage] get_stage1_history 실패: %s", e)
        raise


async def get_stage2_history(
    pool: asyncpg.Pool,
    tickers: list[str],
    since_date: "datetime | date",
) -> dict[str, list[dict]]:
    """
    stage_classifications WHERE stage=2 에서 Stage 2 이력 반환.

    classify_stage_v15(_check_stage3_v12)의 Stage 3 전제 조건(직전 Stage 2 발동)
    체크에 사용. 존재 여부만 보므로 classified_date만 반환.

    반환: {ticker: [{classified_date}, ...]}
    """
    if not tickers:
        return {}
    if hasattr(since_date, "date"):
        since_date = since_date.date()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT ticker, classified_date
                FROM   stage_classifications
                WHERE  ticker = ANY($1)
                  AND  stage  = 2
                  AND  classified_date >= $2
                ORDER  BY classified_date DESC
                """,
                tickers,
                since_date,
            )
        result: dict[str, list[dict]] = {}
        for r in rows:
            result.setdefault(r["ticker"], []).append(dict(r))
        return result
    except Exception as e:
        logger.error("[stage] get_stage2_history 실패: %s", e)
        raise


async def save_stage_classifications(
    pool: asyncpg.Pool,
    rows: list[dict],
) -> int:
    """
    stage_classifications upsert.
    rows: list of {ticker, classified_date, stage, s1_entry_date, s1_high, s1_volume,
                   s1_txamt, peakout_flag, foreign_chg_14d_pct, flow_score}
    반환: 저장/갱신 건수.
    """
    if not rows:
        return 0
    count = 0
    try:
        async with pool.acquire() as conn:
            for r in rows:
                await conn.execute(
                    """
                    INSERT INTO stage_classifications
                        (ticker, classified_date, stage,
                         s1_entry_date, s1_high, s1_volume, s1_txamt, peakout_flag,
                         foreign_chg_14d_pct, flow_score)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    ON CONFLICT (ticker, classified_date) DO UPDATE SET
                        stage               = EXCLUDED.stage,
                        s1_entry_date       = EXCLUDED.s1_entry_date,
                        s1_high             = EXCLUDED.s1_high,
                        s1_volume           = EXCLUDED.s1_volume,
                        s1_txamt            = EXCLUDED.s1_txamt,
                        peakout_flag        = EXCLUDED.peakout_flag,
                        foreign_chg_14d_pct = EXCLUDED.foreign_chg_14d_pct,
                        flow_score          = EXCLUDED.flow_score
                    """,
                    r["ticker"],
                    r["classified_date"],
                    r["stage"],
                    r.get("s1_entry_date"),
                    r.get("s1_high"),
                    r.get("s1_volume"),
                    r.get("s1_txamt"),
                    r.get("peakout_flag", False),
                    r.get("foreign_chg_14d_pct"),
                    r.get("flow_score"),
                )
                count += 1
        logger.info("[stage] stage_classifications %d건 저장/갱신", count)
    except Exception as e:
        logger.error("[stage] save_stage_classifications 실패: %s", e)
    return count


async def upsert_sector_daily_stats(
    pool: asyncpg.Pool,
    rows: list[dict],
) -> int:
    """
    sector_daily_stats upsert.
    rows: list of {sector, trade_date, ticker_count, avg_return_pct,
                   foreign_net_sum, inst_net_sum, avg_flow_score,
                   stage1_count, stage2_count, stage3_count}
    반환: 저장/갱신 건수.
    """
    if not rows:
        return 0
    count = 0
    try:
        async with pool.acquire() as conn:
            for r in rows:
                await conn.execute(
                    """
                    INSERT INTO sector_daily_stats
                        (sector, trade_date, ticker_count, avg_return_pct,
                         foreign_net_sum, inst_net_sum, avg_flow_score,
                         stage1_count, stage2_count, stage3_count)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    ON CONFLICT (sector, trade_date) DO UPDATE SET
                        ticker_count    = EXCLUDED.ticker_count,
                        avg_return_pct  = EXCLUDED.avg_return_pct,
                        foreign_net_sum = EXCLUDED.foreign_net_sum,
                        inst_net_sum    = EXCLUDED.inst_net_sum,
                        avg_flow_score  = EXCLUDED.avg_flow_score,
                        stage1_count    = EXCLUDED.stage1_count,
                        stage2_count    = EXCLUDED.stage2_count,
                        stage3_count    = EXCLUDED.stage3_count
                    """,
                    r["sector"],
                    r["trade_date"],
                    r["ticker_count"],
                    r.get("avg_return_pct"),
                    r.get("foreign_net_sum"),
                    r.get("inst_net_sum"),
                    r.get("avg_flow_score"),
                    r.get("stage1_count", 0),
                    r.get("stage2_count", 0),
                    r.get("stage3_count", 0),
                )
                count += 1
        logger.info("[sector] sector_daily_stats %d건 저장/갱신", count)
    except Exception as e:
        logger.error("[sector] upsert_sector_daily_stats 실패: %s", e)
    return count


async def get_active_stage_tickers(
    pool: asyncpg.Pool,
    days: int = 7,
) -> set[str]:
    """최근 days일 이내에 Stage 1/2/3으로 분류된 종목 yfinance 심볼 set.

    뉴스 게이팅: 스크리너(주봉) 미통과 종목이라도 Stage 활성 중이면 신호 전달.
    """
    from datetime import date as _date, timedelta as _td
    cutoff = _date.today() - _td(days=days)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ticker
                FROM   stage_classifications
                WHERE  classified_date >= $1
                  AND  stage IS NOT NULL
                """,
                cutoff,
            )
        return {r["ticker"] for r in rows}
    except Exception as e:
        logger.warning("[stage_classifications] 활성 티커 조회 실패: %s", e)
        return set()


async def get_stage1_watchlist(
    pool: asyncpg.Pool,
    days: int = 14,
) -> list[dict]:
    """
    최근 `days` 캘린더일 이내에 Stage 1로 분류된 종목의 최신 기록 조회.
    종목당 가장 최근 Stage 1 날짜 1건만 반환.
    반환: [{ticker, s1_date, s1_volume, s1_txamt}]
    """
    from datetime import date as _date, timedelta as _td
    cutoff = _date.today() - _td(days=days)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker)
                       ticker,
                       classified_date AS s1_date,
                       s1_volume,
                       s1_txamt
                FROM   stage_classifications
                WHERE  stage = 1
                  AND  classified_date >= $1
                ORDER  BY ticker, classified_date DESC
                """,
                cutoff,
            )
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("[watchlist] get_stage1_watchlist 실패: %s", e)
        return []


async def upsert_watchlist_vol_log(
    pool: asyncpg.Pool,
    rows: list[dict],
) -> None:
    """Upsert daily vol_ratio for watchlist tickers.
    rows: [{ticker, trade_date, vol_ratio, s1_txamt, s1_vol(optional)}]
    vol_ratio = today_txamt / s1_txamt (거래대금 기준).
    """
    if not rows:
        return
    try:
        async with pool.acquire() as conn:
            for r in rows:
                await conn.execute(
                    """
                    INSERT INTO watchlist_vol_log (ticker, trade_date, vol_ratio, s1_vol, s1_txamt)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (ticker, trade_date)
                    DO UPDATE SET vol_ratio = EXCLUDED.vol_ratio,
                                  s1_vol    = EXCLUDED.s1_vol,
                                  s1_txamt  = EXCLUDED.s1_txamt
                    """,
                    r["ticker"],
                    r["trade_date"],
                    r.get("vol_ratio"),
                    r.get("s1_vol"),
                    r.get("s1_txamt"),
                )
    except Exception as e:
        logger.warning("[watchlist_vol_log] upsert 실패: %s", e)


async def get_watchlist_vol_log(
    pool: asyncpg.Pool,
    tickers: list[str],
    lookback: int = 3,
) -> dict[str, list[float]]:
    """
    Return last `lookback` vol_ratios per ticker (most-recent first).
    Used for rally death detection: if all lookback entries < 0.6, alert.
    """
    if not tickers:
        return {}
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker, rn)
                       ticker, vol_ratio
                FROM (
                    SELECT ticker, vol_ratio,
                           ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) AS rn
                    FROM   watchlist_vol_log
                    WHERE  ticker = ANY($1)
                ) sub
                WHERE rn <= $2
                ORDER BY ticker, rn
                """,
                tickers,
                lookback,
            )
        result: dict[str, list[float]] = {}
        for r in rows:
            result.setdefault(r["ticker"], []).append(r["vol_ratio"])
        return result
    except Exception as e:
        logger.warning("[watchlist_vol_log] 조회 실패: %s", e)
        return {}


async def get_stage3_peakout_map(
    pool,
    tickers: list[str],
    start: date,
    end: date,
    dsn: Optional[str] = None,
) -> dict[str, frozenset]:
    """Stage 3 peakout_flag=TRUE 날짜를 티커별 집합으로 반환.

    pool이 None이면 dsn으로 임시 연결을 생성한다.
    peakout_flag 컬럼이 없거나 DB 오류 시 빈 dict 반환 (use_stage3_peak=False 동작).

    반환: {ticker: frozenset({date, ...})}
    """
    async def _query(conn) -> dict[str, frozenset]:
        try:
            rows = await conn.fetch(
                """
                SELECT ticker, classified_date
                FROM   stage_classifications
                WHERE  ticker = ANY($1)
                  AND  classified_date BETWEEN $2 AND $3
                  AND  stage = 3
                  AND  peakout_flag = TRUE
                """,
                tickers, start, end,
            )
        except Exception:
            return {}
        result: dict[str, list] = {}
        for r in rows:
            result.setdefault(r["ticker"], []).append(r["classified_date"])
        return {t: frozenset(dates) for t, dates in result.items()}

    try:
        if pool is not None:
            async with pool.acquire() as conn:
                return await _query(conn)
        elif dsn:
            import asyncpg as _apg
            conn = await _apg.connect(dsn)
            try:
                return await _query(conn)
            finally:
                await conn.close()
        return {}
    except Exception as e:
        logger.warning("[db] get_stage3_peakout_map 실패: %s", e)
        return {}
