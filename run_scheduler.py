"""
run_scheduler.py  —  수집/요약 분리 구조
────────────────────────────────────────────────────────────
[수집 잡]  1분마다 피드 수집 → 신규 기사를 Queue에 적재 (즉시 종료)
[요약 워커] Queue를 상시 대기 → LM Studio(Ollama fallback)로 순차 요약

수집 잡이 요약을 기다리지 않으므로 잡 스킵이 발생하지 않습니다.

실행:
    python run_scheduler.py              # 기본 7분 간격
    python run_scheduler.py --interval 1 # 1분 간격 (빠른 테스트용)
    python run_scheduler.py --no-summary # 요약 없이 수집만

Ctrl+C 로 종료.
"""

import argparse
import asyncio
import calendar
import hashlib
import logging
import re
from logging.handlers import TimedRotatingFileHandler
import os
import sys
import time

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import feedparser
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ── .env 파일 자동 로드 ──────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv 미설치 시 환경변수 직접 설정으로 동작

from reports.summarizer import summarize, Backend
from core.db import (
    create_pool, get_dsn, init_db, save_article, save_signal,
    load_seen_hashes, save_chart_signals, load_chart_signals_latest,
    get_stage1_history, save_stage_classifications,
    get_stage1_watchlist, upsert_watchlist_vol_log, get_watchlist_vol_log,
)
from telegram.telegram_notify import (
    send_signal as tg_send_signal,
    send_weekly_screener as tg_send_weekly_screener,
    send_screener_comparison as tg_send_screener_comparison,
    send_watchlist_brief as tg_send_watchlist_brief,
)
from analysis.signal_detector import detect_signal
from core.article_fetcher import fetch_article_body
from telegram.telegram_bot import bot_polling_loop, init_bot
from data.market_data import MacroContext, get_macro_context, get_resolution_miss_report

# ── 로깅 설정 ────────────────────────────────────────────────
_LOG_DIR = Path(__file__).parent / "logs"
_LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        TimedRotatingFileHandler(
            str(_LOG_DIR / "news_crawler.log"),
            when="midnight",
            interval=1,
            backupCount=14,
            encoding="utf-8",
        ),
    ],
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ── 스크리닝·Stage 캐시 (뉴스 게이팅용) ─────────────────────
# _screener_tickers: 주봉 스크리닝 통과 종목 (일요일 갱신)
# _active_stage_tickers: 최근 7일 이내 Stage 1/2/3 분류 종목 (일봉 분류기 갱신)
# 둘 다 비어 있으면 게이팅 비활성 (초기 실행 방어).
_screener_tickers: set[str]      = set()
_active_stage_tickers: set[str]  = set()


# ── 피드 목록 ────────────────────────────────────────────────
FEEDS = [
    # Reuters → Google News 우회 (RSS 수집 정상)
    {
        "source": "reuters", "category": "markets",
        "url": "https://news.google.com/rss/search?q=when:24h+allinurl:reuters.com+markets&ceid=US:en&hl=en-US&gl=US",
    },
    {
        "source": "reuters", "category": "macro",
        "url": "https://news.google.com/rss/search?q=when:24h+allinurl:reuters.com+economy+fed&ceid=US:en&hl=en-US&gl=US",
    },
    # Yahoo Finance — Investing.com 대체 (Akamai WAF 우회, 본문 수집 가능)
    {
        "source": "yahoo", "category": "markets",
        "url": "https://finance.yahoo.com/rss/topstories",
    },
    {
        "source": "yahoo", "category": "macro",
        "url": "https://finance.yahoo.com/rss/2.0/headline?s=%5EGSPC&region=US&lang=en-US",
    },
    {
        "source": "yahoo", "category": "korea",
        "url": "https://finance.yahoo.com/rss/2.0/headline?s=%5EKS11&region=US&lang=en-US",
    },
    # Bloomberg — MarketWatch 대체 (RSS 공개 피드)
    {
        "source": "bloomberg", "category": "markets",
        "url": "https://feeds.bloomberg.com/markets/news.rss",
    },
    {
        "source": "bloomberg", "category": "macro",
        "url": "https://feeds.bloomberg.com/economics/news.rss",
    },
    # CNBC — RSS 피드는 유지 (본문은 403이지만 RSS summary 활용)
    {
        "source": "cnbc", "category": "korea",
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100727362",
    },
    # 연합뉴스 — 공식 경제·마켓 RSS
    {
        "source": "yonhap", "category": "korea",
        "url": "https://www.yna.co.kr/rss/economy.xml",
    },
    {
        "source": "yonhap", "category": "korea",
        "url": "https://www.yna.co.kr/rss/market.xml",
    },
    # 한국경제 — 경제·시장 RSS
    {
        "source": "hankyung", "category": "korea",
        "url": "https://www.hankyung.com/feed/economy",
    },
    {
        "source": "hankyung", "category": "korea",
        "url": "https://www.hankyung.com/feed/finance",
    },
    # 매일경제 — 경제·증권 RSS
    {
        "source": "mk", "category": "korea",
        "url": "https://www.mk.co.kr/rss/30100041/",  # 경제
    },
    {
        "source": "mk", "category": "korea",
        "url": "https://www.mk.co.kr/rss/50200011/",  # 증권
    },
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── 공유 상태 ─────────────────────────────────────────────────
_seen_hashes: set[str] = set()       # 중복 방지 (인메모리)
_summary_queue: asyncio.Queue = None # 수집 → 요약 워커 전달용
_db_pool = None                      # asyncpg 커넥션 풀
_paper_trader = None                 # KiwoomPaperTrader (모의투자, 선택적)

# ── 매크로 컨텍스트 TTL 캐시 (5분) ──────────────────────────
_macro_cache: Optional[MacroContext] = None
_macro_cache_ts: float = 0.0
MACRO_CACHE_TTL = 300.0  # 5 minutes — one yfinance call per batch, not per article


async def _get_macro() -> Optional[MacroContext]:
    """Return cached MacroContext, refreshing if stale. Never raises."""
    global _macro_cache, _macro_cache_ts
    try:
        if _macro_cache is None or time.monotonic() - _macro_cache_ts > MACRO_CACHE_TTL:
            _macro_cache = await get_macro_context()
            _macro_cache_ts = time.monotonic()
    except Exception as e:
        logger.warning("[매크로] 컨텍스트 조회 실패 (무시): %s", e)
    return _macro_cache

MAX_AGE_HOURS = 24  # 이 시간보다 오래된 기사는 수집 제외
MIN_INPUT_LEN = 50   # 이 글자 수 미만이면 LLM 요약 스킵 — 제목 보강 후 기준


# ── 유틸 ─────────────────────────────────────────────────────
def _url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]

def _parse_dt(entry) -> datetime | None:
    """RSS entry → timezone-aware datetime. 파싱 실패 시 None."""
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            ts = calendar.timegm(entry.published_parsed)
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            pass
    return None

def _fmt_date(dt: datetime | None) -> str:
    return dt.strftime("%m-%d %H:%M") if dt else "??-?? ??:??"

def _is_fresh(dt: datetime | None) -> bool:
    """published_at이 None이면 통과(날짜 없는 피드 허용), 있으면 MAX_AGE_HOURS 이내만 허용."""
    if dt is None:
        return True
    age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
    return age_hours <= MAX_AGE_HOURS


FETCH_RETRY_COUNT = 3      # 최대 재시도 횟수
FETCH_RETRY_DELAY = 2.0    # 초기 대기 시간 (초) — 지수 백오프

# ── 단일 피드 수집 (재시도 포함) ─────────────────────────────
async def fetch_feed(http: httpx.AsyncClient, cfg: dict) -> list[dict]:
    last_error = None
    for attempt in range(1, FETCH_RETRY_COUNT + 1):
        try:
            r = await http.get(cfg["url"], timeout=15)
            r.raise_for_status()
            parsed = feedparser.parse(r.text)

            articles = []
            skipped = 0
            for e in parsed.entries:
                if not getattr(e, "link", None):
                    continue
                dt = _parse_dt(e)
                if not _is_fresh(dt):
                    skipped += 1
                    continue
                url = getattr(e, "link", "")
                articles.append({
                    "source":       cfg["source"],
                    "category":     cfg["category"],
                    "title":        getattr(e, "title", ""),
                    "url":          url,
                    "url_hash":     _url_hash(url),
                    "summary":      (getattr(e, "summary", "") or "")[:200],
                    "published":    _fmt_date(dt),
                    "published_dt": dt,
                })

            if skipped:
                logger.debug(
                    "  [%s/%s] 낡은 기사 %d건 제외 (24시간 초과)",
                    cfg["source"], cfg["category"], skipped,
                )
            return articles

        except Exception as e:
            last_error = e
            if attempt < FETCH_RETRY_COUNT:
                delay = FETCH_RETRY_DELAY * (2 ** (attempt - 1))  # 2s → 4s → 8s
                logger.warning(
                    "  [%s/%s] 수집 실패 (%d/%d회) — %.0f초 후 재시도: %s",
                    cfg["source"], cfg["category"],
                    attempt, FETCH_RETRY_COUNT, delay, e,
                )
                await asyncio.sleep(delay)
            else:
                logger.warning(
                    "  [%s/%s] 수집 최종 실패 (%d회 시도): %s",
                    cfg["source"], cfg["category"],
                    FETCH_RETRY_COUNT, last_error,
                )
    return []


# ──────────────────────────────────────────────────────────────
# [수집 잡] 스케줄러가 주기적으로 호출
# 피드 수집 → 중복 필터 → Queue 적재만 하고 즉시 반환
# 요약을 기다리지 않으므로 절대 스킵되지 않음
# ──────────────────────────────────────────────────────────────
async def collect_job() -> None:
    run_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    logger.info("━" * 55)
    logger.info("▶ [수집] 시작  %s", run_at)

    # ── DB 해시 선로딩 (재시작 후 첫 실행 시에만) ────────────
    if not _seen_hashes and _db_pool is not None:
        loaded = await load_seen_hashes(_db_pool)
        _seen_hashes.update(loaded)
        logger.info("  [중복방지] DB에서 %d건 해시 로드 완료", len(loaded))

    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as http:
        tasks = [fetch_feed(http, cfg) for cfg in FEEDS]
        results = await asyncio.gather(*tasks)

    queued = 0
    for feed_articles in results:
        for art in feed_articles:
            h = _url_hash(art["url"])
            if h in _seen_hashes:
                continue
            _seen_hashes.add(h)

            if _summary_queue is not None:
                # 요약 워커가 활성화된 경우 Queue에 적재
                await _summary_queue.put(art)
            else:
                # --no-summary 모드: 바로 출력
                logger.info(
                    "  [NEW] [%s/%s] (%s) %s",
                    art["source"], art["category"],
                    art["published"], art["title"][:65],
                )
            queued += 1

    if queued == 0:
        logger.info("  (신규 기사 없음 — 24시간 초과 기사 제외 또는 전부 중복)")
    else:
        queue_size = _summary_queue.qsize() if _summary_queue else 0
        logger.info(
            "▶ [수집] 완료 — 신규 %d건 적재  (누적 %d건, 요약 대기 %d건)",
            queued, len(_seen_hashes), queue_size,
        )

    logger.info("[진단] %s", get_resolution_miss_report(10))


# ──────────────────────────────────────────────────────────────
# [요약 워커] 별도 asyncio 태스크로 상시 실행
# Queue에서 기사를 꺼내 LLM 요약 → DB 저장
# 수집 잡과 완전히 분리되어 독립적으로 동작
# ──────────────────────────────────────────────────────────────
async def summary_worker() -> None:
    logger.info("[요약 워커] 시작 — Queue 대기 중")

    async with httpx.AsyncClient() as http:
        while True:
            try:
                art = await _summary_queue.get()
            except asyncio.CancelledError:
                remaining = _summary_queue.qsize()
                if remaining:
                    logger.info("[요약 워커] 종료 — 미처리 %d건 남음", remaining)
                break

            try:
                # ── 1. 본문 크롤링 (RSS 요약 보강) ───────────────
                body = await fetch_article_body(
                    url=art["url"],
                    source=art["source"],
                    http=http,
                )
                # 본문 크롤링 성공 시 사용, 실패 시 RSS 요약으로 fallback, 둘 다 짧으면 제목으로 보강
                if len(body) > MIN_INPUT_LEN:
                    input_text = body
                    logger.debug("  [본문] %d자 — %s", len(body), art["title"][:40])
                elif len(art["summary"].strip()) >= MIN_INPUT_LEN:
                    input_text = art["summary"]
                else:
                    # 본문·RSS 요약 모두 짧아도 제목으로 요약 시도 (Yahoo 429 등 대응)
                    input_text = art["title"]
                    logger.debug("  [제목요약] 본문/RSS 없음 — 제목으로 요약: %s", art["title"][:60])

                # ── 2. 한글 요약 ─────────────────────────────
                res = await summarize(
                    title=art["title"],
                    body=input_text,
                    http=http,
                )

                summary_ko  = res.text if res.success else ""
                llm_backend = res.backend.value

                if res.success:
                    logger.info(
                        "  [요약] [%s] [%s/%s] (%s) %s",
                        llm_backend,
                        art["source"], art["category"],
                        art["published"], art["title"][:55],
                    )
                    logger.info("         ▷ %s", summary_ko[:120])
                else:
                    logger.info(
                        "  [요약실패] [%s/%s] (%s) %s",
                        art["source"], art["category"],
                        art["published"], art["title"][:65],
                    )

                # ── 2. DB 저장 ────────────────────────────────
                article_id = None
                if _db_pool is not None:
                    saved = await save_article(
                        _db_pool,
                        url_hash    = art["url_hash"],
                        url         = art["url"],
                        source      = art["source"],
                        category    = art["category"],
                        title_en    = art["title"],
                        summary_en  = art["summary"],
                        summary_ko  = summary_ko,
                        llm_backend = llm_backend,
                        published_at= art.get("published_dt"),
                    )
                    if saved:
                        async with _db_pool.acquire() as conn:
                            row = await conn.fetchrow(
                                "SELECT id FROM news_articles WHERE url_hash = $1",
                                art["url_hash"],
                            )
                        article_id = row["id"] if row else None
                        logger.debug("  [DB] 저장 완료 id=%s", article_id)

                # ── 3. 매매 신호 감지 ─────────────────────────
                signal = None
                if summary_ko:
                    macro = await _get_macro()
                    signal = await detect_signal(
                        title=art["title"],
                        summary_ko=summary_ko,
                        http=http,
                        macro=macro,
                    )
                    if signal.is_actionable:
                        icon = {"BUY": "🟢", "SELL": "🔴", "WATCH": "🟡"}.get(signal.direction, "")
                        logger.info(
                            "  [신호] %s %s 강도:%d/5 | %s",
                            icon, signal.direction, signal.strength, signal.reason[:60],
                        )
                        if signal.tickers:
                            logger.info("         관련종목: %s", ", ".join(signal.tickers))

                        if _db_pool and article_id:
                            await save_signal(
                                _db_pool,
                                article_id      = article_id,
                                direction       = signal.direction,
                                strength        = signal.strength,
                                reason          = signal.reason,
                                tickers         = signal.tickers,
                                llm_backend     = signal.backend.value,
                                macro_usd_krw   = macro.usd_krw if macro else None,
                                macro_base_rate = macro.korea_base_rate if macro else None,
                                article_type    = signal.article_type,
                            )

                # ── 4. Telegram 전송 ──────────────────────────
                if signal and signal.is_actionable:
                    # 게이팅: Ichimoku 스크리너 OR 최근 7일 활성 Stage 종목만 전달
                    signal_syms  = set(signal.ticker_symbols.values())
                    in_screener  = bool(signal_syms & _screener_tickers)
                    in_stage     = bool(signal_syms & _active_stage_tickers)
                    has_any_gate = bool(_screener_tickers or _active_stage_tickers)

                    if has_any_gate and signal.ticker_symbols and not (in_screener or in_stage):
                        logger.info(
                            "  [게이팅] 스크리너·Stage 미등록 종목 신호 억제: %s",
                            ", ".join(list(signal.ticker_symbols.keys())[:3]),
                        )
                    else:
                        if in_screener:
                            signal.confidence = "HIGH"
                            logger.info(
                                "  [HIGH CONFIDENCE] 스크리너 교차 종목: %s",
                                ", ".join(signal_syms & _screener_tickers),
                            )
                        elif in_stage:
                            logger.info(
                                "  [Stage 통과] 최근 7일 활성 종목: %s",
                                ", ".join(signal_syms & _active_stage_tickers),
                            )
                        await tg_send_signal(art, summary_ko, signal, http=http)

            except Exception as e:
                logger.warning("[요약 워커] 처리 오류: %s", e)
            finally:
                _summary_queue.task_done()


# ── 잡 래퍼 (핵심 로직은 jobs/ 패키지에 위치) ───────────────

async def _daily_stage_job() -> None:
    """일봉 3단계 분류기 — jobs/stage_job.py 위임."""
    global _active_stage_tickers
    from jobs.stage_job import daily_stage_job as _impl
    _active_stage_tickers = await _impl(_db_pool)
    await _dart_screened_sync_job()


async def _build_watchlist_entries(pool) -> dict:
    """워치리스트 데이터 조회·조합 — jobs/watchlist_job.py 위임."""
    from jobs.watchlist_job import build_watchlist_entries
    return await build_watchlist_entries(pool)


async def _watchlist_brief_job() -> None:
    """거래대금 워치리스트 일보 — jobs/watchlist_job.py 위임."""
    from jobs.watchlist_job import watchlist_brief_job
    await watchlist_brief_job(_db_pool)


# ── 인프라 잡 ────────────────────────────────────────────────

async def _daily_krx_refresh_job():
    if not _db_pool:
        return
    from jobs.infra_jobs import daily_krx_refresh_job
    await daily_krx_refresh_job(_db_pool)


async def _weekly_screener_job():
    global _screener_tickers
    if not _db_pool:
        logger.warning("[차트스크리너] DB 풀 없음 — 스크리닝 건너뜀")
        return
    from jobs.screener_job import weekly_screener_job
    _screener_tickers = await weekly_screener_job(_db_pool)
    # 스크리닝 완료 후 신규 종목 DART 분석 자동 실행
    await _dart_screened_sync_job()


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


async def _daily_dart_disclosure_job():
    if not _db_pool:
        return
    from jobs.infra_jobs import daily_dart_disclosure_job
    await daily_dart_disclosure_job(_db_pool)


async def _monthly_dart_xbrl_job():
    if not _db_pool:
        return
    from jobs.infra_jobs import monthly_dart_xbrl_job
    await monthly_dart_xbrl_job(_db_pool)


async def _annual_dart_extractor_job():
    if not _db_pool:
        return
    from jobs.infra_jobs import annual_dart_extractor_job
    await annual_dart_extractor_job(_db_pool)


async def _dart_screened_sync_job():
    """스크리닝 종목 DART 동기화 — 스크리너/Stage 잡 이후 또는 독립 실행."""
    if not _db_pool:
        return
    from jobs.infra_jobs import dart_screened_sync_job
    await dart_screened_sync_job(_db_pool, days=30, limit=30)


# ── 모의투자 잡 래퍼 ─────────────────────────────────────────

async def _paper_exit_checker_job() -> None:
    if not _db_pool or not _paper_trader:
        return
    from jobs.paper_jobs import paper_exit_checker_job
    await paper_exit_checker_job(_db_pool, _paper_trader)


async def _paper_eod_sampler_job() -> None:
    if not _db_pool or not _paper_trader:
        logger.debug("[paper-sampler] 미초기화 — 스킵")
        return
    from jobs.paper_jobs import paper_eod_sampler_job
    await paper_eod_sampler_job(_db_pool, _paper_trader)


async def _paper_open_entry_job() -> None:
    if not _db_pool or not _paper_trader:
        return
    from jobs.paper_jobs import paper_open_entry_job
    await paper_open_entry_job(_db_pool, _paper_trader)


# ── 대시보드 → 스케줄러 트리거 폴러 ─────────────────────────
# dashboard POST /api/scheduler/trigger → scheduler_triggers INSERT
# 이 잡이 30초마다 pending 행을 1개씩 꺼내 실행하고 status='done'으로 갱신.
# FOR UPDATE SKIP LOCKED: 동시 실행 방지 (max_instances=1로도 충분하나 DB 레벨 보장)
async def _trigger_watcher_job():
    if not _db_pool:
        return
    try:
        async with _db_pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT id, job_name FROM scheduler_triggers"
                    " WHERE status = 'pending'"
                    " ORDER BY requested_at ASC LIMIT 1"
                    " FOR UPDATE SKIP LOCKED"
                )
                if not row:
                    return
                trig_id = row["id"]
                job_name = row["job_name"]
                await conn.execute(
                    "UPDATE scheduler_triggers"
                    " SET status='running', executed_at=NOW()"
                    " WHERE id=$1", trig_id
                )
        logger.info("[trigger] 대시보드 요청 잡 실행: %s", job_name)
        try:
            if job_name == "stage":
                await _daily_stage_job()
            elif job_name == "screener":
                await _weekly_screener_job()
            elif job_name == "dart_screened":
                await _dart_screened_sync_job()
            elif job_name == "paper_sample":
                await _paper_eod_sampler_job()
            else:
                logger.warning("[trigger] 알 수 없는 잡: %s", job_name)
        finally:
            async with _db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE scheduler_triggers SET status='done' WHERE id=$1",
                    trig_id
                )
    except Exception as e:
        logger.warning("[trigger] 폴링 실패: %s", e)


# ── 스케줄러 진입점 ───────────────────────────────────────────
async def main(interval: int, enable_summary: bool) -> None:
    global _summary_queue, _db_pool, _paper_trader, _screener_tickers, _active_stage_tickers

    logger.info("뉴스 크롤러 시작 — 수집 %d분 간격", interval)
    logger.info("구조: [수집 잡] → Queue → [요약 워커] (완전 분리)")
    logger.info("한글 요약: %s | 피드 %d개 | Ctrl+C 로 종료\n",
                "ON (Ollama→LM Studio)" if enable_summary else "OFF", len(FEEDS))

    # ── KOREA_BASE_RATE 신선도 체크 ───────────────────────────
    _env_path = Path(".env")
    if not _env_path.exists():
        logger.info(
            "KOREA_BASE_RATE 신선도 체크 생략 — .env 파일 없음 (컨테이너/플랫폼 환경변수로 주입된 경우 수동 확인 필요)"
        )
    if _env_path.exists():
        age_days = (time.time() - _env_path.stat().st_mtime) / 86400
        _base_rate_str = os.getenv("KOREA_BASE_RATE", "2.5")
        try:
            logger.info("KOREA_BASE_RATE=%.2f%% (env loaded)", float(_base_rate_str))
        except ValueError:
            logger.warning("KOREA_BASE_RATE 값이 숫자가 아닙니다: %r — 2.5%%로 기본값 사용", _base_rate_str)
        if age_days > 90:
            logger.warning(
                "KOREA_BASE_RATE may be stale — .env last modified %d days ago. "
                "Check BOK rate at https://www.bok.or.kr/eng/main/contents.do?menuNo=400652",
                int(age_days),
            )

    # ── DB 초기화 ─────────────────────────────────────────────
    try:
        _db_pool = await create_pool()
        await init_db(_db_pool)
    except Exception as e:
        logger.error("DB 연결 실패: %s", e)
        logger.error("DB 없이 계속 실행합니다 (콘솔 출력만)")
        _db_pool = None

    # ── KRX 종목 캐시 초기화 ─────────────────────────────────────
    if _db_pool:
        from data.krx_sync import sync_krx_listings
        from core.ticker_cache import ticker_cache as _ticker_cache
        try:
            await sync_krx_listings(_db_pool)
        except Exception as _krx_e:
            logger.warning("[krx_sync] 초기 동기화 실패: %s — DB에 기존 데이터로 캐시 로드", _krx_e)
        finally:
            try:
                await asyncio.wait_for(_ticker_cache.load(_db_pool), timeout=60.0)
            except asyncio.TimeoutError:
                logger.warning("[ticker_cache] 캐시 로드 타임아웃 (60s) — 정적 맵으로 운영")
            except Exception as _cache_e:
                logger.warning("[ticker_cache] 캐시 로드 실패: %s — 정적 맵으로 운영", _cache_e)

    # ── 스크리너 게이팅 캐시 초기화 (DB에서 이번 주 종목 로드) ──
    if _db_pool:
        try:
            from core.db import get_chart_signals_this_week
            _screener_tickers = await get_chart_signals_this_week(_db_pool)
            logger.info("[게이팅] 스크리너 캐시 로드 — %d종목", len(_screener_tickers))
        except Exception as _gt_e:
            logger.warning("[게이팅] 스크리너 캐시 로드 실패: %s", _gt_e)
        try:
            from core.db import get_active_stage_tickers as _get_active_stage
            _active_stage_tickers = await _get_active_stage(_db_pool, days=7)
            logger.info("[게이팅] 활성 Stage 캐시 로드 — %d종목", len(_active_stage_tickers))
        except Exception as _st_e:
            logger.warning("[게이팅] 활성 Stage 캐시 로드 실패: %s", _st_e)

    # ── 봇 초기화 ─────────────────────────────────────────────
    init_bot(_seen_hashes)
    bot_task = asyncio.create_task(bot_polling_loop(_db_pool))
    logger.info("Telegram 봇 시작 — /status /signals /today /help")

    # ── 키움 모의투자 클라이언트 초기화 (선택적) ─────────────────
    if _db_pool and os.environ.get("KIWOOM_MOCK_APPKEY"):
        try:
            from data.kiwoom_paper_trader import KiwoomPaperTrader, init_paper_positions
            _paper_trader = KiwoomPaperTrader()
            await init_paper_positions(_db_pool)
            logger.info("[paper] 모의투자 클라이언트 초기화 완료")
        except Exception as _pe:
            logger.warning("[paper] 모의투자 초기화 실패 (스킵): %s", _pe)
            _paper_trader = None
    else:
        logger.info("[paper] KIWOOM_MOCK_APPKEY 미설정 — 모의투자 비활성")

    # ── 요약 워커 초기화 ──────────────────────────────────────
    worker_task = None
    if enable_summary:
        _summary_queue = asyncio.Queue()
        worker_task = asyncio.create_task(summary_worker())

    # ── APScheduler 잡스토어 설정 ─────────────────────────────
    # Build jobstores — fall back to MemoryJobStore if Postgres is unreachable
    # at startup (e.g. container cold-start race) so the scheduler doesn't crash.
    try:
        from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
        # Normalize both postgresql:// and postgres:// (e.g. Heroku/Render DATABASE_URL)
        # to the explicit psycopg2 dialect so SQLAlchemy 2.x doesn't emit SAWarning.
        _dsn = re.sub(r"^postgres(ql)?://", "postgresql+psycopg2://", get_dsn(), count=1)
        jobstores = {"default": SQLAlchemyJobStore(url=_dsn)}
        logger.info("[스케줄러] APScheduler jobstore: Postgres (%s)", _dsn.split("@")[-1])
    except Exception as _jse:
        from apscheduler.jobstores.memory import MemoryJobStore
        jobstores = {"default": MemoryJobStore()}
        logger.warning("[스케줄러] APScheduler jobstore: MemoryJobStore (Postgres 연결 실패: %s)", _jse)
    scheduler = AsyncIOScheduler(timezone="UTC", jobstores=jobstores)

    # ── 잡 등록 ──────────────────────────────────────────────
    scheduler.add_job(
        collect_job,
        trigger="interval",
        minutes=interval,
        id="news_collect",
        next_run_time=datetime.now(timezone.utc) + timedelta(seconds=3),  # 스케줄러 시작 후 3초 뒤 첫 실행
        max_instances=1,                            # 중복 실행 방지
        coalesce=True,                              # 밀린 잡 합치기
        replace_existing=True,
    )
    scheduler.add_job(
        _daily_krx_refresh_job,
        CronTrigger(hour=20, minute=0, timezone="Asia/Seoul"),
        id="krx_daily_refresh",
        max_instances=1,
        misfire_grace_time=3600,
        replace_existing=True,
    )
    scheduler.add_job(
        _weekly_screener_job,
        CronTrigger(day_of_week="sun", hour=20, minute=30, timezone="Asia/Seoul"),
        id="weekly_chart_screener",
        max_instances=1,
        misfire_grace_time=3600,
        replace_existing=True,
    )
    scheduler.add_job(
        _daily_stage_job,
        CronTrigger(day_of_week="mon-fri", hour=7, minute=30, timezone="UTC"),  # = 16:30 KST
        id="daily_stage_classifier",
        max_instances=1,
        misfire_grace_time=3600,
        replace_existing=True,
    )
    scheduler.add_job(
        _watchlist_brief_job,
        CronTrigger(day_of_week="mon-fri", hour=8, minute=0, timezone="UTC"),
        id="daily_watchlist_brief",
        max_instances=1,
        misfire_grace_time=3600,
        replace_existing=True,
    )
    # ── YouTube 내러티브 수집: 평일 09:05 KST (00:05 UTC) ──────────
    # 전일 삼프로TV 업로드 → LLM 추출 → youtube_mention_raw
    # YOUTUBE_API_KEY / GEMINI_API_KEY 미설정 시 잡이 자동으로 건너뜀
    scheduler.add_job(
        _youtube_narrative_sync_job,
        CronTrigger(day_of_week="mon-fri", hour=0, minute=5, timezone="UTC"),  # = 09:05 KST
        id="youtube_narrative_sync",
        max_instances=1,
        misfire_grace_time=3600,
        replace_existing=True,
    )
    # ── YouTube attention_score 집계: 평일 09:35 KST (00:35 UTC) ───
    # sync 잡(09:05) 대비 30분 여유 — 삼프로TV 15개 영상 × 4s/Gemini ≈ 60~120s
    scheduler.add_job(
        _youtube_attention_score_job,
        CronTrigger(day_of_week="mon-fri", hour=0, minute=35, timezone="UTC"),  # = 09:35 KST
        id="youtube_attention_score",
        max_instances=1,
        misfire_grace_time=3600,
        replace_existing=True,
    )
    # ── YouTube forward return 채우기: 평일 15:40 KST (06:40 UTC) ──
    scheduler.add_job(
        _youtube_forward_return_job,
        CronTrigger(day_of_week="mon-fri", hour=6, minute=40, timezone="UTC"),  # = 15:40 KST
        id="youtube_forward_return",
        max_instances=1,
        misfire_grace_time=1800,
        replace_existing=True,
    )
    # ── 당일 최종 스냅샷: 평일 16:10 KST (07:10 UTC) ─────────────
    # ka10032 top100(KRX+NXT 합산) → daily_market_snap
    # NXT 종료(16:00) 후 10분 여유. 히트맵/TOP 장마감 데이터 소스.
    scheduler.add_job(
        _daily_market_snap_job,
        CronTrigger(day_of_week="mon-fri", hour=7, minute=10, timezone="UTC"),  # = 16:10 KST
        id="daily_market_snap",
        max_instances=1,
        misfire_grace_time=1800,
        replace_existing=True,
    )
    # ── NXT 시간외 수집: 평일 16:05 KST (07:05 UTC) ──────────────
    # kiwoom_aftermarket_sync.py --incremental
    # NXT 시간외 단일가(15:40~16:00) 종료 후 5분 여유 확보.
    # reg_value(ka10032 KRX+NXT 합산)도 동시에 갱신.
    scheduler.add_job(
        _daily_aftermarket_sync_job,
        CronTrigger(day_of_week="mon-fri", hour=7, minute=5, timezone="UTC"),  # = 16:05 KST
        id="daily_aftermarket_sync",
        max_instances=1,
        misfire_grace_time=1800,
        replace_existing=True,
    )
    # ── 수급 증분 sync: 평일 18:00 KST (09:00 UTC) ───────────────
    # krx_flow_sync.py --incremental (전일 전 종목 수급 → daily_flow)
    # 장 마감(15:30) + KRX 데이터 게시 여유(~2h) 확보.
    # stage_classifier(16:30 KST)는 DB에 이미 적재된 전일 데이터를 읽으므로 순서 무관.
    scheduler.add_job(
        _daily_flow_sync_job,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=0, timezone="UTC"),  # = 18:00 KST
        id="daily_flow_sync",
        max_instances=1,
        misfire_grace_time=3600,
        replace_existing=True,
    )

    if _paper_trader:
        scheduler.add_job(
            _paper_exit_checker_job,
            CronTrigger(day_of_week="mon-fri", hour=7, minute=10, timezone="UTC"),  # 16:10 KST
            id="paper_exit_checker",
            max_instances=1,
            misfire_grace_time=1800,
            replace_existing=True,
        )
        logger.info("[paper] Exit Checker 등록 완료 (16:10 KST)")
        scheduler.add_job(
            _paper_eod_sampler_job,
            CronTrigger(day_of_week="mon-fri", hour=7, minute=40, timezone="UTC"),  # 16:40 KST
            id="paper_eod_sampler",
            max_instances=1,
            misfire_grace_time=1800,
            replace_existing=True,
        )
        logger.info("[paper] EOD 샘플러 등록 완료 (16:40 KST)")
        scheduler.add_job(
            _paper_open_entry_job,
            CronTrigger(day_of_week="mon-fri", hour=0, minute=5, timezone="UTC"),  # 09:05 KST
            id="paper_open_entry",
            max_instances=1,
            misfire_grace_time=900,
            replace_existing=True,
        )
        logger.info("[paper] T+1 진입 잡 등록 완료 (09:05 KST)")

    # ── OpenDART 공시 수집: 평일 09:00 KST (00:00 UTC) ──────────
    # 전일 Top 20 기업 공시 이벤트 (실적발표·유상증자 등) → dart_disclosures
    # DART_API_KEY 미설정 시 내부에서 경고 후 skip — 스케줄러 크래시 없음.
    scheduler.add_job(
        _daily_dart_disclosure_job,
        CronTrigger(day_of_week="mon-fri", hour=0, minute=0, timezone="UTC"),  # = 09:00 KST
        id="daily_dart_disclosures",
        max_instances=1,
        misfire_grace_time=3600,
        replace_existing=True,
    )
    # ── OpenDART XBRL 갱신: 매월 1일 17:00 UTC (02:00 KST+1) ───
    # Top 20 기업 전년도 사업보고서 XBRL 재무수치 → dart_xbrl
    scheduler.add_job(
        _monthly_dart_xbrl_job,
        CronTrigger(day=1, hour=17, minute=0, timezone="UTC"),  # = 02:00 KST 다음날
        id="monthly_dart_xbrl",
        max_instances=1,
        misfire_grace_time=86400,
        replace_existing=True,
    )
    # ── OpenDART XML Ollama 추출: 연 3회 → dart_extractions ──────────────────
    # 봄(5/20):  사업보고서(12월결산, 3월말 제출) + 1분기(5월중 제출) 완료 후
    # 가을(9/1): 반기보고서(6월결산, 8월중 제출) 완료 후
    # 겨울(11/20): 3분기보고서(9월결산, 11월중 제출) 완료 후
    for _dart_month, _dart_day, _dart_id_suffix in [
        (5,  20, "spring"),   # 봄: 사업보고서 + 1분기
        (9,   1, "autumn"),   # 가을: 반기보고서
        (11, 20, "winter"),   # 겨울: 3분기보고서
    ]:
        scheduler.add_job(
            _annual_dart_extractor_job,
            CronTrigger(month=_dart_month, day=_dart_day, hour=18, minute=0, timezone="UTC"),  # = 03:00 KST
            id=f"dart_extractions_{_dart_id_suffix}",
            max_instances=1,
            misfire_grace_time=86400,
            replace_existing=True,
        )

    scheduler.add_job(
        _trigger_watcher_job,
        trigger="interval",
        seconds=30,
        id="trigger_watcher",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )

    scheduler.start()

    try:
        while True:
            await asyncio.sleep(30)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        scheduler.shutdown(wait=False)
        if worker_task:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
        if bot_task:
            bot_task.cancel()
            try:
                await bot_task
            except asyncio.CancelledError:
                pass
        if _db_pool:
            await _db_pool.close()
            logger.info("DB 풀 종료")
        logger.info("종료 — 누적 수집 %d건", len(_seen_hashes))


async def _run_once_watchlist() -> None:
    """--once watchlist: DB 연결 후 _watchlist_brief_job() 즉시 실행."""
    global _db_pool
    try:
        _db_pool = await create_pool()
        await init_db(_db_pool)
    except Exception as e:
        logger.error("DB 연결 실패: %s", e)
        return
    try:
        await _watchlist_brief_job()
    finally:
        if _db_pool:
            await _db_pool.close()


async def _run_once_stage() -> None:
    """--once stage: DB + 티커 캐시 초기화 후 _daily_stage_job() 즉시 실행."""
    global _db_pool
    try:
        _db_pool = await create_pool()
        await init_db(_db_pool)
    except Exception as e:
        logger.error("DB 연결 실패: %s", e)
        return
    try:
        from data.krx_sync import sync_krx_listings
        from core.ticker_cache import ticker_cache as _ticker_cache
        try:
            await sync_krx_listings(_db_pool)
        except Exception as _e:
            logger.warning("[stage] krx_sync 실패: %s — DB 캐시로 진행", _e)
        try:
            await asyncio.wait_for(_ticker_cache.load(_db_pool), timeout=60.0)
        except Exception as _e:
            logger.warning("[stage] ticker_cache 로드 실패: %s — 정적 맵으로 진행", _e)
        await _daily_stage_job()
    finally:
        if _db_pool:
            await _db_pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="뉴스 크롤러 스케줄러")
    parser.add_argument(
        "--interval", type=int, default=7,
        help="수집 간격 (분, 기본값: 7)"
    )
    parser.add_argument(
        "--no-summary", action="store_true",
        help="한글 요약 비활성화 (수집만)"
    )
    parser.add_argument(
        "--once", type=str, default=None, metavar="JOB",
        help="즉시 실행 후 종료 (watchlist | stage)"
    )
    args = parser.parse_args()

    if args.once == "watchlist":
        asyncio.run(_run_once_watchlist())
    elif args.once == "stage":
        asyncio.run(_run_once_stage())
    else:
        asyncio.run(main(args.interval, enable_summary=not args.no_summary))
