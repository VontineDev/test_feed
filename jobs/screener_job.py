"""주봉 차트 스크리너 잡.

weekly_screener_job(db_pool) -> set[str]
  전 종목 Ichimoku 스크리닝 → DB 저장 → Telegram 전송.
  반환값: 새 screener_tickers (호출자가 전역 캐시 갱신)
"""

import asyncio
import logging

import httpx

from core.db import save_chart_signals
from telegram.telegram_notify import send_weekly_screener as tg_send_weekly_screener

logger = logging.getLogger(__name__)


async def weekly_screener_job(db_pool) -> set[str]:
    loop = asyncio.get_running_loop()
    results = []
    try:
        from analysis.chart_screener import run_weekly_screen
        results = await loop.run_in_executor(None, run_weekly_screen)
        saved = await save_chart_signals(db_pool, results)
        logger.info("[차트스크리너] 완료 — 통과:%d 저장:%d", len(results), saved)
        new_tickers = {r.ticker for r in results}
        logger.info("[게이팅] 스크리너 캐시 갱신 — %d종목", len(new_tickers))
        async with httpx.AsyncClient() as http:
            await tg_send_weekly_screener(results, http=http)
    except Exception as e:
        logger.warning("[차트스크리너] 실행 실패: %s", e)
        return set()

    return new_tickers
