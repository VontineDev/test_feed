"""인프라 잡 — KRX 종목 갱신, 수급 증분 sync."""

import asyncio
import logging
import os
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


async def daily_dart_disclosure_job(db_pool) -> None:
    """평일 09:00 KST — 전일 Top 20 기업 공시 이벤트 수집.

    스케줄러 재시작 등으로 수집이 끊겼을 경우, 마지막 수집일 다음날부터
    오늘까지 자동 백필한다 (최대 90일).
    """
    from datetime import date, timedelta
    from data.dart_sync import get_top20_corp_codes, sync_disclosures

    try:
        corp_codes = await get_top20_corp_codes(db_pool)
        if not corp_codes:
            logger.warning("[dart] Top 20 corp_code 없음 — seed-companies 선행 필요")
            return

        # 마지막 수집일 확인 (rcept_dt 기준)
        async with db_pool.acquire() as conn:
            last_dt = await conn.fetchval(
                "SELECT MAX(rcept_dt) FROM dart_disclosures"
            )

        today = date.today()
        yesterday = today - timedelta(days=1)

        if last_dt and (yesterday - last_dt).days > 1:
            # 갭 감지: 마지막 수집일 다음날부터 어제까지 백필 (최대 90일)
            gap_start = last_dt + timedelta(days=1)
            if (yesterday - gap_start).days > 90:
                gap_start = yesterday - timedelta(days=89)
            bgn_de = gap_start.strftime("%Y%m%d")
            end_de = yesterday.strftime("%Y%m%d")
            logger.info("[dart] 공시 갭 감지 (%s ~ %s) — 백필 시작", bgn_de, end_de)
            n = await sync_disclosures(db_pool, corp_codes, bgn_de, end_de)
            logger.info("[dart] 공시 백필 완료: %d건 (%s ~ %s)", n, bgn_de, end_de)
        else:
            # 정상: 전일 하루치만 수집
            n = await sync_disclosures(db_pool, corp_codes)
            logger.info("[dart] 일별 공시 수집 완료: %d건", n)

    except Exception as e:
        logger.error("[dart] 일별 공시 수집 실패: %s", e)


async def annual_dart_segments_job(db_pool) -> None:
    """매년 5월 1일 03:00 KST — Top 20 사업보고서 II-2/II-4 Ollama 파싱."""
    from data.dart_sync import get_top20_corp_codes, sync_segments
    try:
        corp_codes = await get_top20_corp_codes(db_pool)
        if not corp_codes:
            logger.warning("[dart] Top 20 corp_code 없음 — seed-companies 선행 필요")
            return
        from datetime import date
        bsns_year = str(date.today().year - 1)
        n = await sync_segments(db_pool, corp_codes, bsns_year)
        logger.info("[dart] 연간 세그먼트 파싱 완료: %d건 (%s)", n, bsns_year)
    except Exception as e:
        logger.error("[dart] 연간 세그먼트 파싱 실패: %s", e)


async def annual_dart_extractor_job(db_pool) -> None:
    """매년 5월 1일 03:00 KST — Top20 사업보고서 로컬 XML Ollama 추출 → dart_extractions."""
    from data.dart_extractor import extract_all
    try:
        n = await extract_all(db_pool)
        logger.info("[dart-extractor] 완료: %d건", n)
    except Exception as e:
        logger.error("[dart-extractor] 실패: %s", e)


async def monthly_dart_xbrl_job(db_pool) -> None:
    """매월 1일 02:00 KST — Top 20 기업 전년도 XBRL 재무수치 갱신."""
    from data.dart_sync import get_top20_corp_codes, sync_xbrl
    try:
        corp_codes = await get_top20_corp_codes(db_pool)
        if not corp_codes:
            logger.warning("[dart] Top 20 corp_code 없음 — seed-companies 선행 필요")
            return
        n = await sync_xbrl(db_pool, corp_codes)
        logger.info("[dart] 월별 XBRL 갱신 완료: %d건", n)
    except Exception as e:
        logger.error("[dart] 월별 XBRL 갱신 실패: %s", e)


async def daily_market_snap_job() -> None:
    """ka10032 당일 누적 스냅샷 → daily_market_snap upsert.

    1차: 평일 16:10 KST — NXT 단일가 종료 후 중간 스냅샷 (히트맵/TOP 즉시 반영용)
    2차: 평일 20:10 KST — NXT 애프터마켓 종료 후 완전한 KRX+NXT 최종값으로 덮어씀

    stex_tp=3 (KRX+NXT 합산) top100. (ticker, trade_date) upsert라 중복 없음.
    """
    from datetime import date as _date
    from core.db import get_dsn as _get_dsn
    from data.kiwoom_aftermarket_sync import (
        _build_client, run_daily_snap,
    )
    logger.info("[daily-snap] 시작")
    try:
        dsn = _get_dsn()
        client = _build_client()
        saved = run_daily_snap(dsn, client, _date.today())
        if saved == 0:
            logger.warning("[daily-snap] 저장된 데이터 없음")
    except Exception as e:
        logger.error("[daily-snap] 실패: %s", e)


async def daily_aftermarket_sync_job() -> None:
    """평일 16:05 KST — NXT 시간외 단일가 + ka10032 합산 거래대금 수집.

    kiwoom_aftermarket_sync.py --incremental 을 서브프로세스로 실행.
    trade_date = 전일 영업일로 저장 (--incremental 기본 동작).
    """
    logger.info("[aftermarket-sync] kiwoom_aftermarket_sync --incremental 시작")
    try:
        import os as _os
        cwd = _os.path.join(_os.path.dirname(__file__), "..", "data")
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "kiwoom_aftermarket_sync.py", "--incremental",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=cwd,
        )
        out, _ = await proc.communicate()
        if proc.returncode == 0:
            logger.info("[aftermarket-sync] 완료 (exit=0)")
        else:
            logger.warning("[aftermarket-sync] 비정상 종료 (exit=%d)", proc.returncode)
        if out:
            for line in out.decode("utf-8", errors="replace").splitlines():
                if line.strip():
                    logger.debug("[aftermarket-sync] %s", line)
    except Exception as e:
        logger.warning("[aftermarket-sync] 실행 실패: %s", e)


async def youtube_narrative_sync_job() -> None:
    """평일 09:05 KST — 전일 삼프로TV 업로드 수집 + LLM 추출 → youtube_mention_raw."""
    import os
    from datetime import date as _date, timedelta as _td
    from core.db import get_dsn as _get_dsn
    from data.youtube_narrative_sync import run_sync, ensure_tables
    logger.info("[yt-sync] 운영 수집 시작")
    try:
        dsn        = _get_dsn()
        api_key = os.environ.get("YOUTUBE_API_KEY", "")
        if not api_key:
            logger.warning("[yt-sync] YOUTUBE_API_KEY 미설정 — 건너뜀")
            return
        ensure_tables(dsn)
        yesterday = _date.today() - _td(days=1)
        n = await asyncio.to_thread(run_sync, dsn, api_key, yesterday, yesterday)
        logger.info("[yt-sync] 완료: %d건", n)
    except Exception as e:
        logger.error("[yt-sync] 실패: %s", e)


async def youtube_attention_score_job() -> None:
    """평일 09:10 KST — rolling 5영업일 attention_score 집계."""
    from datetime import date as _date
    from core.db import get_dsn as _get_dsn
    from data.youtube_narrative_sync import compute_attention_scores
    logger.info("[yt-attn] 집계 시작")
    try:
        n = await asyncio.to_thread(compute_attention_scores, _get_dsn())
        logger.info("[yt-attn] 완료: %d종목", n)
    except Exception as e:
        logger.error("[yt-attn] 실패: %s", e)


async def youtube_forward_return_job() -> None:
    """평일 15:40 KST — 당일 종가로 forward return 백필."""
    from core.db import get_dsn as _get_dsn
    from data.youtube_narrative_sync import fill_forward_returns
    logger.info("[yt-ret] forward return 채우기 시작")
    try:
        n = await asyncio.to_thread(fill_forward_returns, _get_dsn())
        logger.info("[yt-ret] 완료: %d건", n)
    except Exception as e:
        logger.error("[yt-ret] 실패: %s", e)


async def dart_screened_sync_job(
    db_pool,
    days:  int  = 30,
    limit: int  = 30,
    force: bool = False,
) -> None:
    """스크리닝 후 자동 DART 분석 — 최근 days일 Stage/스크리너 종목 대상.

    screener_job / stage_job 완료 후 호출하거나 독립 스케줄로 실행.
    Ollama 미응답 시 조용히 종료 (스케줄러 크래시 방지).
    """
    try:
        from scripts.dart_screened_sync import dart_screened_sync_job as _impl
        stats = await _impl(db_pool, days=days, limit=limit, force=force)
        logger.info(
            "[dart-screened] 잡 완료 — 추출:%d 스킵:%d 실패:%d",
            stats["extracted"], stats["skipped"], stats["failed"],
        )
    except Exception as e:
        logger.error("[dart-screened] 잡 실패: %s", e)


async def daily_ohlcv_warm_job() -> None:
    """평일 18:30 KST — 전일 전 종목 OHLCV를 daily_ohlcv에 채우기.

    KRX OpenAPI(get_daily_ohlcv_all) 사용. KRX_OPENAPI_KEY 미설정 시 경고 후 skip.
    daily_flow_sync(18:00) 완료 후 30분 뒤 실행.
    """
    from core.db import get_dsn as _get_dsn
    from jobs.ohlcv_warm import daily_ohlcv_warm_job as _impl
    try:
        dsn = _get_dsn()
        if not dsn:
            logger.warning("[ohlcv-warm] DSN 미설정 — 스킵")
            return
        import asyncio
        loop = asyncio.get_running_loop()
        n = await loop.run_in_executor(None, _impl, dsn)
        logger.info("[ohlcv-warm] 일배치 완료: %d종목", n)
    except Exception as e:
        logger.error("[ohlcv-warm] 일배치 실패: %s", e)


async def daily_flow_sync_job() -> None:
    logger.info("[flow-sync] krx_flow_sync --incremental --backend kiwoom 시작")
    try:
        _root   = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
        _script = os.path.join(_root, "data", "krx_flow_sync.py")
        _env    = {**os.environ, "PYTHONPATH": _root}
        proc = await asyncio.create_subprocess_exec(
            sys.executable, _script, "--incremental", "--backend", "kiwoom",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=_env,
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


async def weekly_flow_personal_backfill_job() -> None:
    """일요일 19:00 KST — 지난 한 주를 krx-direct로 재수집해 personal_net 채움.

    daily_flow_sync_job(평일 18:00, --backend kiwoom)은 ka10045 특성상
    personal_net을 채우지 못한다(TODOS.md P2). data.krx.co.kr은 개인 순매수까지
    제공하므로, 주 1회 krx-direct --force로 지난 7일을 다시 적재해 메꾼다.
    foreign_net/inst_net도 krx-direct 값으로 같이 덮어쓴다 — 공식 원천이라
    kiwoom과 값이 다르더라도 더 신뢰할 수 있는 쪽으로 수렴시키는 것이 의도.
    KRX_SESSION 쿠키가 만료되면 이 잡만 조용히 실패하고(로그만 남김),
    다음 평일 daily_flow_sync_job(kiwoom)은 영향받지 않는다.
    """
    from datetime import date, timedelta
    today = date.today()
    end = today - timedelta(days=1)          # 토요일 (직전 거래일 근처)
    start = end - timedelta(days=6)           # 지난 월요일부터 7일 범위
    logger.info("[flow-personal-backfill] krx_flow_sync --backend krx-direct --force %s~%s 시작",
                start, end)
    try:
        _root   = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
        _script = os.path.join(_root, "data", "krx_flow_sync.py")
        _env    = {**os.environ, "PYTHONPATH": _root}
        proc = await asyncio.create_subprocess_exec(
            sys.executable, _script,
            "--backend", "krx-direct",
            "--start", start.isoformat(), "--end", end.isoformat(),
            "--force",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=_env,
        )
        out, _ = await proc.communicate()
        if proc.returncode == 0:
            logger.info("[flow-personal-backfill] 완료 (exit=0)")
        else:
            logger.warning(
                "[flow-personal-backfill] 비정상 종료 (exit=%d) — KRX_SESSION 만료 의심, "
                "다음 주 재시도", proc.returncode,
            )
        if out:
            for line in out.decode("utf-8", errors="replace").splitlines():
                if line.strip():
                    logger.debug("[flow-personal-backfill] %s", line)
    except Exception as e:
        logger.warning("[flow-personal-backfill] 실행 실패: %s", e)
