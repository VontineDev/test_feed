"""섹터별 일별 수급·수익률 집계 잡.

sector_stats_job(db_pool) -> int
  daily_flow + krx_listings JOIN으로 섹터 집계 → sector_daily_stats upsert.
  전일 영업일(trade_date=T-1) 기준으로 실행. 이미 해당 날짜 row 있으면 갱신.

  T-1인 이유: daily_flow_sync_job(수급 동기화)이 trade_date를 항상 전일
  영업일로 저장하는 --incremental 기본 동작이라(jobs/infra_jobs.py 참고),
  daily_flow에는 "오늘" 날짜 행이 이 잡이 도는 시점(평일 20:30 KST)에
  존재한 적이 없다. trade_date=today()로 조회하면 항상 0건이라 이 테이블이
  영구히 비어있었다(2026-08-23 발견 — 2026-08-06 수정은 컬럼명 불일치로
  인한 쿼리 크래시만 고쳤을 뿐, 이 날짜 불일치는 그대로 남아있었음).
"""

import logging
from datetime import date

import asyncpg

from core.dates import last_trading_day
from core.db import upsert_sector_daily_stats

logger = logging.getLogger(__name__)


async def sector_stats_job(db_pool: asyncpg.Pool) -> int:
    """섹터 일별 통계 집계 및 upsert.

    daily_flow(전일)+ krx_listings(sector) + stage_classifications(전일)를 JOIN.
    stage_classifications는 LEFT JOIN으로 미분류 종목도 포함.
    반환: upsert된 섹터 수.
    """
    if not db_pool:
        logger.warning("[섹터통계] DB 풀 없음 — 스킵")
        return 0

    # daily_flow_sync_job(krx_flow_sync.py --incremental)이 last_trading_day()
    # 기준으로 trade_date를 저장하므로 여기도 동일 기준으로 맞춘다 — 단순
    # timedelta(days=1)은 월요일 실행 시 일요일(비거래일)을 가리켜 여전히
    # 0건이 나옴(2026-08-23 1차 수정 검증 중 발견).
    today = last_trading_day(date.today())
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
                  ON o_today.symbol = f.ticker
                 AND o_today.date = $1
                LEFT JOIN daily_ohlcv o_prev
                  ON o_prev.symbol = f.ticker
                 AND o_prev.date = (
                     SELECT MAX(date)
                     FROM daily_ohlcv
                     WHERE symbol = f.ticker
                       AND date < $1
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
