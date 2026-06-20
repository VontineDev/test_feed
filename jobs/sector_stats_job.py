"""섹터별 일별 수급·수익률 집계 잡.

sector_stats_job(db_pool) -> int
  daily_flow + krx_listings JOIN으로 섹터 집계 → sector_daily_stats upsert.
  당일(trade_date=today) 기준으로 실행. 이미 오늘 row 있으면 갱신.
"""

import logging
from datetime import date

import asyncpg

from core.db import upsert_sector_daily_stats

logger = logging.getLogger(__name__)


async def sector_stats_job(db_pool: asyncpg.Pool) -> int:
    """섹터 일별 통계 집계 및 upsert.

    daily_flow(오늘) + krx_listings(sector) + stage_classifications(오늘)를 JOIN.
    stage_classifications는 LEFT JOIN으로 미분류 종목도 포함.
    반환: upsert된 섹터 수.
    """
    if not db_pool:
        logger.warning("[섹터통계] DB 풀 없음 — 스킵")
        return 0

    today = date.today()
    logger.info("[섹터통계] 집계 시작 (trade_date=%s)", today)

    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    k.sector,
                    COUNT(DISTINCT f.ticker)                               AS ticker_count,
                    AVG(
                        CASE WHEN o_prev.close IS NOT NULL AND o_prev.close > 0
                             THEN (o_today.close / o_prev.close - 1.0)
                             ELSE NULL END
                    )                                                      AS avg_return_pct,
                    SUM(f.foreign_net)                                     AS foreign_net_sum,
                    SUM(f.inst_net)                                        AS inst_net_sum,
                    AVG(sc.flow_score)                                     AS avg_flow_score,
                    COUNT(CASE WHEN sc.stage = 1 THEN 1 END)              AS stage1_count,
                    COUNT(CASE WHEN sc.stage = 2 THEN 1 END)              AS stage2_count,
                    COUNT(CASE WHEN sc.stage = 3 THEN 1 END)              AS stage3_count
                FROM daily_flow f
                JOIN krx_listings k
                  ON k.yfinance_symbol = f.ticker
                 AND k.sector IS NOT NULL
                 AND k.sector <> ''
                LEFT JOIN daily_ohlcv o_today
                  ON o_today.ticker = f.ticker
                 AND o_today.trade_date = $1
                LEFT JOIN daily_ohlcv o_prev
                  ON o_prev.ticker = f.ticker
                 AND o_prev.trade_date = (
                     SELECT MAX(trade_date)
                     FROM daily_ohlcv
                     WHERE ticker = f.ticker
                       AND trade_date < $1
                 )
                LEFT JOIN stage_classifications sc
                  ON sc.ticker = f.ticker
                 AND sc.classified_date = $1
                WHERE f.trade_date = $1
                GROUP BY k.sector
                HAVING COUNT(DISTINCT f.ticker) > 0
                ORDER BY k.sector
                """,
                today,
            )
    except Exception as e:
        logger.error("[섹터통계] 집계 쿼리 실패: %s", e)
        return 0

    if not rows:
        logger.info("[섹터통계] 집계 결과 없음 (daily_flow 데이터 없거나 장 미개장)")
        return 0

    upsert_rows = [
        {
            "sector":          r["sector"],
            "trade_date":      today,
            "ticker_count":    r["ticker_count"],
            "avg_return_pct":  float(r["avg_return_pct"]) if r["avg_return_pct"] is not None else None,
            "foreign_net_sum": int(r["foreign_net_sum"]) if r["foreign_net_sum"] is not None else None,
            "inst_net_sum":    int(r["inst_net_sum"]) if r["inst_net_sum"] is not None else None,
            "avg_flow_score":  float(r["avg_flow_score"]) if r["avg_flow_score"] is not None else None,
            "stage1_count":    int(r["stage1_count"] or 0),
            "stage2_count":    int(r["stage2_count"] or 0),
            "stage3_count":    int(r["stage3_count"] or 0),
        }
        for r in rows
    ]

    logger.info("[섹터통계] 집계 완료 — %d섹터", len(upsert_rows))
    return await upsert_sector_daily_stats(db_pool, upsert_rows)
