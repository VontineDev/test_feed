"""jobs/scheduler_wrappers.py — run_scheduler.py의 순수 위임 잡 래퍼.

전역 상태를 재할당(global 재바인딩)하지 않는 잡만 여기 있음 — 재할당하거나
다른 잡을 체이닝하는 _daily_stage_job/_weekly_screener_job, 오케스트레이션인
_trigger_watcher_job, 전역 상태 R/W인 collect_job/summary_worker는
run_scheduler.py에 그대로 남는다.

_db_pool/_paper_trader를 읽는 함수는 `import run_scheduler` 후
`run_scheduler._db_pool` 형태로 접근한다 — main()이 run_scheduler.py
자신의 모듈 전역을 나중에 재할당해도, 속성 접근은 매번 그 시점의 최신 값을
읽으므로(모듈은 싱글턴) 정상 반영된다. `from run_scheduler import _db_pool`
같은 이름 import는 값을 import 시점에 스냅샷해버려 이후 재할당을 못 보므로
사용하지 않는다.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


# ── 잡 래퍼 (핵심 로직은 jobs/ 패키지에 위치) ───────────────

async def _build_watchlist_entries(pool) -> dict:
    """워치리스트 데이터 조회·조합 — jobs/watchlist_job.py 위임."""
    from jobs.watchlist_job import build_watchlist_entries
    return await build_watchlist_entries(pool)


async def _watchlist_brief_job() -> None:
    """거래대금 워치리스트 일보 — jobs/watchlist_job.py 위임."""
    import run_scheduler
    from jobs.watchlist_job import watchlist_brief_job
    await watchlist_brief_job(run_scheduler._db_pool)


# ── 인프라 잡 ────────────────────────────────────────────────

async def _daily_krx_refresh_job():
    import run_scheduler
    if not run_scheduler._db_pool:
        return
    from jobs.infra_jobs import daily_krx_refresh_job
    await daily_krx_refresh_job(run_scheduler._db_pool)


async def _youtube_narrative_sync_job():
    from jobs.infra_jobs import youtube_narrative_sync_job
    await youtube_narrative_sync_job()


async def _youtube_attention_score_job():
    from jobs.infra_jobs import youtube_attention_score_job
    await youtube_attention_score_job()


async def _youtube_forward_return_job():
    from jobs.infra_jobs import youtube_forward_return_job
    await youtube_forward_return_job()


async def _daily_market_snap_job():
    from jobs.infra_jobs import daily_market_snap_job
    await daily_market_snap_job()


async def _daily_aftermarket_sync_job():
    from jobs.infra_jobs import daily_aftermarket_sync_job
    await daily_aftermarket_sync_job()


async def _daily_flow_sync_job():
    from jobs.infra_jobs import daily_flow_sync_job
    await daily_flow_sync_job()


async def _daily_ohlcv_warm_job():
    from jobs.infra_jobs import daily_ohlcv_warm_job
    await daily_ohlcv_warm_job()


async def _sector_stats_job():
    import run_scheduler
    from jobs.sector_stats_job import sector_stats_job
    await sector_stats_job(run_scheduler._db_pool)


async def _daily_dart_disclosure_job():
    import run_scheduler
    if not run_scheduler._db_pool:
        return
    from jobs.infra_jobs import daily_dart_disclosure_job
    await daily_dart_disclosure_job(run_scheduler._db_pool)


async def _monthly_dart_xbrl_job():
    import run_scheduler
    if not run_scheduler._db_pool:
        return
    from jobs.infra_jobs import monthly_dart_xbrl_job
    await monthly_dart_xbrl_job(run_scheduler._db_pool)


async def _annual_dart_extractor_job():
    import run_scheduler
    if not run_scheduler._db_pool:
        return
    from jobs.infra_jobs import annual_dart_extractor_job
    await annual_dart_extractor_job(run_scheduler._db_pool)


async def _dart_screened_sync_job():
    """스크리닝 종목 DART 동기화 — 스크리너/Stage 잡 이후 또는 독립 실행."""
    import run_scheduler
    if not run_scheduler._db_pool:
        return
    from jobs.infra_jobs import dart_screened_sync_job
    await dart_screened_sync_job(run_scheduler._db_pool, days=30, limit=30)


# ── 모의투자 잡 래퍼 ─────────────────────────────────────────

async def _paper_exit_checker_job() -> None:
    import run_scheduler
    if not run_scheduler._db_pool or not run_scheduler._paper_trader:
        return
    from jobs.paper_jobs import paper_exit_checker_job
    await paper_exit_checker_job(run_scheduler._db_pool, run_scheduler._paper_trader)


async def _paper_eod_sampler_job() -> None:
    import run_scheduler
    if not run_scheduler._db_pool or not run_scheduler._paper_trader:
        logger.debug("[paper-sampler] 미초기화 — 스킵")
        return
    from jobs.paper_jobs import paper_eod_sampler_job
    await paper_eod_sampler_job(run_scheduler._db_pool, run_scheduler._paper_trader)


async def _paper_open_entry_job() -> None:
    import run_scheduler
    if not run_scheduler._db_pool or not run_scheduler._paper_trader:
        return
    from jobs.paper_jobs import paper_open_entry_job
    await paper_open_entry_job(run_scheduler._db_pool, run_scheduler._paper_trader)


async def _compose_paper_entry_job() -> None:
    import run_scheduler
    if not run_scheduler._db_pool:
        return
    from core.db import get_dsn as _get_dsn
    from jobs.compose_paper_job import compose_paper_entry_job
    dsn = _get_dsn()
    if not dsn:
        logger.warning("[compose-paper] DSN 없음 — 스킵")
        return
    await compose_paper_entry_job(dsn, run_scheduler._db_pool)
