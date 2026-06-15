"""Compose 전략 주간 신호 -> paper_positions insert_pending.

일요일 21:15 KST 실행 (weekly_chart_screener 완료 후).
  FUNNEL-1  top-10 : 수급 스크린 후 이치모쿠 돌파 진입
  AND-1     top-5  : 이치모쿠 ∩ Stage2+ ∩ 수급 비매도
  SCORE-1   top-5  : Stage·거래대금·수급 z-score 가중합 (2026-06-16 추가)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta

from analysis.strategy_compose import STRATEGIES, derive_flags, iso_week, load_signal_frame
from data.kiwoom_paper_trader import MODEL_CONFIG, get_open_slot_count, insert_pending

logger = logging.getLogger(__name__)

# FUNNEL-1 max_weeks=4 윈도우 + 4주 선행 데이터 여유
_FUNNEL_LOOKBACK_WEEKS = 8

_STRATEGIES_CFG = [
    ("FUNNEL-1", "compose-funnel1", 10),
    ("AND-1",    "compose-and1",     5),
    ("SCORE-1",  "compose-score1",   5),
]


def _get_this_week_signals_sync(dsn: str, strategy: str, top_n: int) -> list[str]:
    """이번 주(ISO) 신호 ticker 목록 추출. 동기 — run_in_executor 전용."""
    today = date.today()
    start = today - timedelta(weeks=_FUNNEL_LOOKBACK_WEEKS)
    spec = STRATEGIES[strategy]
    frame = load_signal_frame(dsn, start, today, spec.sources)
    if frame.empty:
        logger.info("[compose-paper] %s: 신호 프레임 없음", strategy)
        return []
    frame = derive_flags(frame)
    signals = spec.run(frame)

    cur_week = iso_week(today)
    this_week = signals[signals["week"] == cur_week]
    # 일요일은 ISO 구 주(week-1)의 마지막 날 — cur_week로 조회 시 당일 데이터가
    # 아직 없을 수 있어 직전 주로 fallback. 단 signal_date는 date.today()로 삽입.
    if this_week.empty:
        prev_week = iso_week(today - timedelta(weeks=1))
        this_week = signals[signals["week"] == prev_week]
        if not this_week.empty:
            logger.info("[compose-paper] %s: cur_week 신호 없음, prev_week(%s) fallback", strategy, prev_week)

    if this_week.empty:
        return []

    if "score" in this_week.columns:
        this_week = this_week.nlargest(top_n, "score")
    else:
        this_week = this_week.head(top_n)

    return this_week["ticker"].tolist()


async def compose_paper_entry_job(dsn: str, pool) -> None:
    """FUNNEL-1 / AND-1 신호를 paper_positions pending에 적재."""
    if not dsn or pool is None:
        logger.warning("[compose-paper] DSN 또는 pool 없음 — 스킵")
        return

    loop = asyncio.get_running_loop()
    today = date.today()

    for strategy, model, top_n in _STRATEGIES_CFG:
        try:
            tickers = await asyncio.wait_for(
                loop.run_in_executor(
                    None, _get_this_week_signals_sync, dsn, strategy, top_n
                ),
                timeout=60.0,
            )
        except asyncio.TimeoutError:
            logger.error("[compose-paper] %s 신호 추출 60초 타임아웃", strategy)
            continue

        logger.info("[compose-paper] %s: 신호 %d개", strategy, len(tickers))
        if not tickers:
            logger.info("[compose-paper] %s: 이번 주 신호 없음 — 스킵", strategy)
            continue

        inserted = 0
        max_slots = MODEL_CONFIG[model]["max_slots"]

        for ticker in tickers:
            open_slots = await get_open_slot_count(pool, model)
            if open_slots >= max_slots:
                logger.debug("[compose-paper] %s 슬롯 만료(%d/%d) — 이하 전체 스킵", model, open_slots, max_slots)
                break

            # 같은 주에 이미 pending/open이면 중복 방지
            async with pool.acquire() as conn:
                exists = await conn.fetchval(
                    """SELECT 1 FROM paper_positions
                       WHERE model=$1 AND ticker=$2
                         AND signal_date >= date_trunc('week', CURRENT_DATE)
                         AND status IN ('pending', 'open')
                       LIMIT 1""",
                    model, ticker,
                )
            if exists:
                continue

            await insert_pending(
                pool, model, ticker, today,
                entry_theory=0.0,   # paper_open_entry_job이 실시간 시세로 채움
                tp1_pct=0.15,
                tp1_ratio=0.50,
                trail_pct=0.10,
                hard_stop_pct=0.10,
            )
            inserted += 1
            logger.info("[compose-paper] pending 추가: %s %s", model, ticker)

        logger.info("[compose-paper] %s: %d개 적재 완료", model, inserted)
