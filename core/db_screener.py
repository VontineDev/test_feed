"""
db_screener.py  —  chart_signals 테이블 접근 함수
────────────────────────────────────────────────────────────
주봉 차트 스크리닝 결과 저장·조회. core.db facade를 통해 re-export됨.
"""

from __future__ import annotations

import logging
from datetime import datetime

import asyncpg

logger = logging.getLogger(__name__)


# ── 차트 스크리닝 결과 저장 ──────────────────────────────────
async def save_chart_signals(
    pool: asyncpg.Pool,
    results: list,      # list[ScreenResult] — 순환 import 방지를 위해 타입 미지정
) -> int:
    """
    차트 스크리닝 결과를 chart_signals 테이블에 저장.
    동일 (ticker, week_of) 충돌 시 최신 데이터로 갱신.
    저장/갱신된 건수 반환.
    """
    if not results:
        return 0
    count = 0
    try:
        async with pool.acquire() as conn:
            for r in results:
                await conn.execute(
                    """
                    INSERT INTO chart_signals
                        (ticker, name, close, ma_20w, ma_60w, cloud_top,
                         is_enhanced, has_gapjum, week_of, screened_at,
                         sector, ma_120w, high_w, volume_w)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                    ON CONFLICT (ticker, week_of) DO UPDATE SET
                        close       = EXCLUDED.close,
                        ma_20w      = EXCLUDED.ma_20w,
                        ma_60w      = EXCLUDED.ma_60w,
                        cloud_top   = EXCLUDED.cloud_top,
                        is_enhanced = EXCLUDED.is_enhanced,
                        has_gapjum  = EXCLUDED.has_gapjum,
                        screened_at = EXCLUDED.screened_at,
                        sector      = EXCLUDED.sector,
                        ma_120w     = EXCLUDED.ma_120w,
                        high_w      = EXCLUDED.high_w,
                        volume_w    = EXCLUDED.volume_w
                    """,
                    r.ticker,
                    r.name,
                    r.close,
                    r.ma_20w,
                    r.ma_60w,
                    r.cloud_top,
                    r.is_enhanced,
                    r.has_gapjum,
                    r.week_of,
                    datetime.fromisoformat(r.screened_at),
                    r.sector,
                    r.ma_120w,
                    r.high_w,
                    r.volume_w,
                )
                count += 1
        logger.info("[차트스크리너] chart_signals %d건 저장/갱신", count)
    except Exception as e:
        logger.error("[차트스크리너] 저장 실패: %s", e)
    return count


async def load_chart_signals_latest(pool: asyncpg.Pool) -> tuple[str, list]:
    """
    가장 최근 week_of의 차트 스크리닝 결과 전체 반환.
    반환: (week_of, list[dict])  — 결과 없으면 ("", [])
    """
    try:
        async with pool.acquire() as conn:
            week = await conn.fetchval(
                "SELECT week_of FROM chart_signals ORDER BY screened_at DESC LIMIT 1"
            )
            if not week:
                return ("", [])
            rows = await conn.fetch(
                """
                SELECT ticker, name, close, ma_20w, ma_60w, cloud_top,
                       is_enhanced, has_gapjum, week_of, screened_at,
                       sector, ma_120w, high_w, volume_w
                FROM chart_signals
                WHERE week_of = $1
                ORDER BY has_gapjum DESC, close DESC
                """,
                week,
            )
        return (week, [dict(r) for r in rows])
    except Exception as e:
        logger.warning("[차트스크리너] 최근 결과 조회 실패: %s", e)
        return ("", [])


async def get_chart_signals_this_week(pool: asyncpg.Pool) -> set[str]:
    """
    이번 주 스크리닝 통과 종목의 yfinance 심볼 set 반환.
    run_scheduler.py에서 collect_job() 사이클마다 한 번 호출 (v2 HIGH CONFIDENCE용).
    """
    try:
        from analysis.chart_screener import current_week_of
        week = current_week_of()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT ticker FROM chart_signals WHERE week_of = $1",
                week,
            )
        return {r["ticker"] for r in rows}
    except Exception as e:
        logger.warning("[차트스크리너] 이번 주 신호 조회 실패: %s", e)
        return set()
