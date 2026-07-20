"""jobs/scheduler_wrappers.py — run_scheduler.py의 순수 위임 잡 래퍼.

풀·트레이더 등 공유 상태는 jobs/scheduler_state.py가 소유한다 —
`state.db_pool` 속성 접근은 매번 그 시점의 최신 바인딩을 읽으므로
run_scheduler.main()의 재할당이 정상 반영된다(모듈은 싱글턴).
`from jobs.scheduler_state import db_pool` 같은 이름 import는 값을
스냅샷해 이후 재할당을 못 보므로 사용하지 않는다.
"""

from __future__ import annotations

import logging

from jobs import scheduler_state as state

logger = logging.getLogger(__name__)


# ── 잡 래퍼 (핵심 로직은 jobs/ 패키지에 위치) ───────────────

async def _build_watchlist_entries(pool) -> dict:
    """워치리스트 데이터 조회·조합 — jobs/watchlist_job.py 위임."""
    from jobs.watchlist_job import build_watchlist_entries
    return await build_watchlist_entries(pool)


async def _watchlist_brief_job() -> None:
    """거래대금 워치리스트 일보 — jobs/watchlist_job.py 위임."""
    from jobs.watchlist_job import watchlist_brief_job
    await watchlist_brief_job(state.db_pool)


# ── 인프라 잡 ────────────────────────────────────────────────

async def _daily_krx_refresh_job():
    if not state.db_pool:
        return
    from jobs.infra_jobs import daily_krx_refresh_job
    await daily_krx_refresh_job(state.db_pool)


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
    from jobs.sector_stats_job import sector_stats_job
    await sector_stats_job(state.db_pool)


async def _daily_dart_disclosure_job():
    if not state.db_pool:
        return
    from jobs.infra_jobs import daily_dart_disclosure_job
    await daily_dart_disclosure_job(state.db_pool)


async def _monthly_dart_xbrl_job():
    if not state.db_pool:
        return
    from jobs.infra_jobs import monthly_dart_xbrl_job
    await monthly_dart_xbrl_job(state.db_pool)


async def _annual_dart_extractor_job():
    if not state.db_pool:
        return
    from jobs.infra_jobs import annual_dart_extractor_job
    await annual_dart_extractor_job(state.db_pool)


async def _dart_screened_sync_job():
    """스크리닝 종목 DART 동기화 — 스크리너/Stage 잡 이후 또는 독립 실행."""
    if not state.db_pool:
        return
    from jobs.infra_jobs import dart_screened_sync_job
    await dart_screened_sync_job(state.db_pool, days=30, limit=30)


# ── 모의투자 잡 래퍼 ─────────────────────────────────────────

async def _paper_exit_checker_job() -> None:
    if not state.db_pool or not state.paper_trader:
        return
    from jobs.paper_jobs import paper_exit_checker_job
    await paper_exit_checker_job(state.db_pool, state.paper_trader)


async def _paper_eod_sampler_job() -> None:
    if not state.db_pool or not state.paper_trader:
        logger.debug("[paper-sampler] 미초기화 — 스킵")
        return
    from jobs.paper_jobs import paper_eod_sampler_job
    await paper_eod_sampler_job(state.db_pool, state.paper_trader)


async def _paper_open_entry_job() -> None:
    if not state.db_pool or not state.paper_trader:
        return
    from jobs.paper_jobs import paper_open_entry_job
    await paper_open_entry_job(state.db_pool, state.paper_trader)


async def _compose_paper_entry_job() -> None:
    if not state.db_pool:
        return
    from core.db import get_dsn as _get_dsn
    from jobs.compose_paper_job import compose_paper_entry_job
    dsn = _get_dsn()
    if not dsn:
        logger.warning("[compose-paper] DSN 없음 — 스킵")
        return
    await compose_paper_entry_job(dsn, state.db_pool)
