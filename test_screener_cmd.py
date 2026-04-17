"""
test_screener_cmd.py — /screener 봇 명령어 실제 전송 테스트
DB에서 최신 스크리닝 결과를 로드하고 DM + 채널로 전송.
"""
import asyncio
import logging
import os

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
)
logger = logging.getLogger(__name__)


async def main():
    from db import create_pool, get_dsn, load_chart_signals_latest
    from chart_screener import ScreenResult
    from telegram_notify import send_weekly_screener, _get_token, _get_chat_id, _get_channel_id

    # ── 환경변수 확인 ──────────────────────────────────────────
    try:
        token = _get_token()
        chat_id = _get_chat_id()
    except ValueError as e:
        logger.error("환경변수 미설정: %s", e)
        return

    channel_id = _get_channel_id()
    logger.info("DM target   : %s", chat_id)
    logger.info("Channel     : %s", channel_id or "(미설정 — DM만 발송)")

    # ── DB 연결 ───────────────────────────────────────────────
    try:
        pool = await create_pool(get_dsn())
    except Exception as e:
        logger.error("DB 연결 실패: %s", e)
        return

    # ── 최신 스크리닝 결과 로드 (없으면 직접 실행 후 저장) ─────
    week, rows = await load_chart_signals_latest(pool)

    if not rows:
        logger.info("DB에 결과 없음 — 차트 스크리닝 직접 실행 중 (~14분)...")
        from chart_screener import run_weekly_screen
        from db import save_chart_signals
        loop = asyncio.get_running_loop()
        results_live = await loop.run_in_executor(None, run_weekly_screen)
        saved = await save_chart_signals(pool, results_live)
        logger.info("DB 저장 완료: %d건", saved)
        week, rows = await load_chart_signals_latest(pool)

    await pool.close()

    if not rows:
        logger.error("스크리닝 결과 없음 — 종료")
        return

    logger.info("DB 결과: %s주차 %d종목", week, len(rows))

    # ── ScreenResult 재구성 (/screener 핸들러와 동일 로직) ─────
    results = [
        ScreenResult(
            ticker=r["ticker"],
            name=r["name"] or r["ticker"],
            close=r["close"],
            ma_20w=r["ma_20w"],
            ma_60w=r["ma_60w"],
            cloud_top=r["cloud_top"],
            is_enhanced=r["is_enhanced"],
            has_gapjum=r["has_gapjum"],
            screened_at=r["screened_at"].isoformat() if r["screened_at"] else "",
            week_of=r["week_of"],
        )
        for r in rows
    ]

    # ── 전송 ──────────────────────────────────────────────────
    logger.info("전송 시작 — DM%s", " + 채널" if channel_id else "")
    ok = await send_weekly_screener(results)

    if ok:
        logger.info("✓ DM 전송 성공")
        if channel_id:
            logger.info("✓ 채널 전송 시도 완료 (채널 로그 확인)")
    else:
        logger.error("✗ DM 전송 실패 — 토큰/Chat ID 확인 필요")


if __name__ == "__main__":
    asyncio.run(main())
