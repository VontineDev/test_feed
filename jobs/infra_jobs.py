"""인프라 잡 — KRX 종목 갱신, 수급 증분 sync."""

import asyncio
import logging
import sys

logger = logging.getLogger(__name__)


async def daily_krx_refresh_job(db_pool) -> None:
    from data.krx_sync import sync_krx_listings
    from core.ticker_cache import ticker_cache
    try:
        n = await sync_krx_listings(db_pool)
        logger.info("[krx_sync] 일일 갱신 완료: %d행", n)
    except Exception as e:
        logger.error("[krx_sync] 일일 갱신 실패: %s — 기존 DB 데이터로 캐시 재로드", e)
    finally:
        try:
            await ticker_cache.load(db_pool)
        except Exception as _cache_e:
            logger.warning("[ticker_cache] 캐시 재로드 실패: %s", _cache_e)


async def daily_flow_sync_job() -> None:
    logger.info("[flow-sync] krx_flow_sync --incremental 시작")
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "krx_flow_sync.py", "--incremental",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await proc.communicate()
        if proc.returncode == 0:
            logger.info("[flow-sync] 완료 (exit=0)")
        else:
            logger.warning("[flow-sync] 비정상 종료 (exit=%d)", proc.returncode)
        if out:
            for line in out.decode("utf-8", errors="replace").splitlines():
                if line.strip():
                    logger.debug("[flow-sync] %s", line)
    except Exception as e:
        logger.warning("[flow-sync] 실행 실패: %s", e)
