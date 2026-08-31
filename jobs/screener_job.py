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

# cron·대시보드 트리거(scheduler_triggers 폴링)·텔레그램(/run_screener,
# /run_all) 3개의 독립 경로가 이 잡을 부를 수 있다(2026-08-31
# jobs/infra_jobs.py::flow_sync_lock 참고 — 같은 3-경로 구조에서 텔레그램만
# 락이 있어 동시 실행되는 사고가 남). 텔레그램 경로(telegram/bot_handlers.py
# ::_run_screener_task)는 이 함수를 호출하지 않고 run_weekly_screen()을 직접
# 재구현해 쓰므로, 그쪽도 같은 락(bot._scan_lock — 이 객체를 re-export)으로
# 감싸 상호 배제한다.
screener_lock: asyncio.Lock = asyncio.Lock()


async def weekly_screener_job(db_pool) -> set[str]:
    if screener_lock.locked():
        logger.warning("[차트스크리너] 이미 다른 실행이 진행 중 — 이번 트리거는 건너뜀 (중복 실행 방지)")
        return set()

    async with screener_lock:
        return await _weekly_screener_job_impl(db_pool)


async def _weekly_screener_job_impl(db_pool) -> set[str]:
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
