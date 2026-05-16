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

from summarizer import summarize, Backend
from db import (
    create_pool, get_dsn, init_db, save_article, save_signal, save_cross_analysis,
    load_seen_hashes, save_chart_signals, load_chart_signals_latest,
    get_stage1_history, save_stage_classifications,
    get_stage1_watchlist, upsert_watchlist_vol_log, get_watchlist_vol_log,
)
from telegram_notify import (
    send_signal as tg_send_signal,
    send_weekly_screener as tg_send_weekly_screener,
    send_screener_comparison as tg_send_screener_comparison,
    send_watchlist_brief as tg_send_watchlist_brief,
)
from signal_detector import detect_signal
from article_fetcher import fetch_article_body
from telegram_bot import bot_polling_loop, init_bot
from market_data import cross_analyze, CrossAnalysis, MacroContext, get_macro_context, get_resolution_miss_report

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

# ── 스크리닝 종목 캐시 (뉴스 게이팅용) ───────────────────────
# 주봉 스크리닝 완료 시 갱신. 비어 있으면 게이팅 비활성 (초기 실행 방어).
_screener_tickers: set[str] = set()


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
                cross  = None
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

                        # ── 3-1. 시세 교차 분석 ───────────────
                        if signal.tickers:
                            try:
                                cross = await asyncio.get_running_loop().run_in_executor(
                                    None,
                                    cross_analyze,
                                    signal.direction,
                                    signal.strength,
                                    signal.tickers,
                                    signal.ticker_symbols,
                                )
                                verdict_icon = {
                                    "CONFIRM": "✅", "CAUTION": "⚠️",
                                    "FILTER": "🚫", "NEUTRAL": "➖",
                                }.get(cross.verdict, "")
                                logger.info(
                                    "  [교차] %s %s 점수:%d/10 | %s",
                                    verdict_icon, cross.verdict,
                                    cross.score, cross.summary[:60],
                                )
                            except Exception as e:
                                logger.warning("[교차] 분석 실패: %s", e)
                                cross = None

                        if _db_pool and article_id:
                            signal_id = await save_signal(
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
                            if signal_id and cross:
                                try:
                                    await save_cross_analysis(_db_pool, signal_id, cross)
                                except Exception as e:
                                    logger.warning("[백테스트] 교차분석 저장 실패: %s", e)

                # ── 4. Telegram 전송 ──────────────────────────
                # 신호 감지 시에만 전송 (한국·외신 모두 동일)
                if signal and signal.is_actionable:
                    # FILTER 판정이면 신호 알림 억제
                    if cross and cross.verdict == "FILTER":
                        logger.info("  [교차] 역방향 시세로 신호 알림 억제")
                    # 스크리너 게이팅: 관련 종목이 스크리닝 통과 종목에 없으면 억제
                    elif _screener_tickers and signal.ticker_symbols and not (
                        set(signal.ticker_symbols) & _screener_tickers
                    ):
                        logger.info(
                            "  [게이팅] 스크리너 미통과 종목 신호 억제: %s",
                            ", ".join(signal.ticker_symbols[:3]),
                        )
                    else:
                        await tg_send_signal(art, summary_ko, signal, http=http, cross=cross)

            except Exception as e:
                logger.warning("[요약 워커] 처리 오류: %s", e)
            finally:
                _summary_queue.task_done()


# ── 3단계 분류기: 일별 16:30 KST (UTC 07:30) ─────────────────
async def _daily_stage_job() -> None:
    """일봉 3단계 분류기 — Stage 1/2/3 분류 + daily_flow streak + Ichimoku 비교 전송."""
    if not _db_pool:
        logger.warning("[3단계] DB 풀 없음 — 스킵")
        return

    loop = asyncio.get_running_loop()
    logger.info("[3단계] 일별 분류 시작")

    # 1. Ichimoku 비교용 결과 (D4: load_chart_signals_latest 사용 — 종목명/has_gapjum 포함)
    _week, ichimoku_rows = await load_chart_signals_latest(_db_pool)
    logger.info("[3단계] Ichimoku 비교 풀: %d종목 (week_of=%s)", len(ichimoku_rows), _week)

    # 2. 전 종목 티커 목록 (KOSPI + KOSDAQ, ~2770개)
    from chart_screener import get_all_tickers as _get_all_tickers
    all_tickers: list[tuple[str, str, str]] = await loop.run_in_executor(
        None, _get_all_tickers, None
    )
    logger.info("[3단계] 분류 대상: %d종목 / SCREENER_WORKERS=%s",
                len(all_tickers), os.environ.get("SCREENER_WORKERS", "1"))

    from datetime import date as _date

    today = _date.today()

    # 2b. daily OHLCV (60일, yfinance, 병렬)
    import yfinance as _yf

    def _fetch_daily_ohlcv(ticker: str):
        try:
            tkr = _yf.Ticker(ticker)
            df = tkr.history(period="60d", interval="1d", auto_adjust=True)
            if df.empty or len(df) < 21:
                return None
            if isinstance(df.columns, __import__("pandas").MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
            df.index = __import__("pandas").to_datetime(df.index, utc=True)
            return df
        except Exception:
            return None

    workers = int(os.environ.get("SCREENER_WORKERS", "1"))
    from concurrent.futures import ThreadPoolExecutor as _TPE, as_completed as _as_completed

    price_map: dict[str, object] = {}
    with _TPE(max_workers=workers) as pool_ex:
        futures = {pool_ex.submit(_fetch_daily_ohlcv, t): t for t, _, _ in all_tickers}
        for fut in _as_completed(futures):
            tk = futures[fut]
            try:
                df = fut.result()
                if df is not None:
                    price_map[tk] = df
            except Exception:
                pass
    logger.info("[3단계] OHLCV 수집: %d/%d종목", len(price_map), len(all_tickers))

    # 3. daily_flow 20일 배치 로드
    from datetime import timedelta as _td
    since_20d = today - _td(days=20)
    flow_map: dict[str, object] = {}
    import pandas as _pd
    try:
        async with _db_pool.acquire() as conn:
            rows_flow = await conn.fetch(
                """
                SELECT ticker, trade_date, foreign_net, inst_net,
                       foreign_streak, inst_streak
                FROM   daily_flow
                WHERE  trade_date >= $1
                ORDER  BY trade_date ASC
                """,
                since_20d,
            )
        for row in rows_flow:
            t = row["ticker"]
            if t not in flow_map:
                flow_map[t] = []
            flow_map[t].append(dict(row))
        for t, rows_list in flow_map.items():
            df_f = _pd.DataFrame(rows_list)
            df_f = df_f.set_index("trade_date")
            df_f.index = _pd.to_datetime(df_f.index)
            flow_map[t] = df_f
    except Exception as e:
        logger.warning("[3단계] flow_map 로드 실패: %s", e)

    # 4. s1_history 배치 조회
    all_ticker_list = [t for t, _, _ in all_tickers]
    since_14d = today - _td(days=14)
    s1_history = await get_stage1_history(_db_pool, all_ticker_list, since_14d)
    logger.info("[3단계] s1_history: %d종목 이력", len(s1_history))

    # 5. classify_stage() 병렬 실행
    from stage_classifier import classify_stage as _classify, check_peakout as _peakout

    market_map = {t: ("KOSDAQ" if s.endswith(".KQ") else "KOSPI") for t, _, s in all_tickers}

    def _classify_one(ticker: str) -> tuple[str, Optional[int], bool]:
        price_df = price_map.get(ticker)
        flow_df  = flow_map.get(ticker, _pd.DataFrame())
        if price_df is None:
            return ticker, None, False
        market = market_map.get(ticker, "KOSPI")
        stage = _classify(ticker, price_df, flow_df, s1_history, market)
        peakout = _peakout(ticker, flow_df, price_df) if stage == 3 else False
        return ticker, stage, peakout

    stage_results: dict[str, int] = {}
    peakout_flags: set[str] = set()
    upsert_rows: list[dict] = []

    with _TPE(max_workers=workers) as pool_ex2:
        futures2 = {pool_ex2.submit(_classify_one, t): t for t in all_ticker_list}
        for fut in _as_completed(futures2):
            try:
                ticker, stage, peakout = fut.result()
                if stage is not None:
                    stage_results[ticker] = stage
                    if peakout:
                        peakout_flags.add(ticker)

                    s1_high = s1_vol = None
                    if stage == 1:
                        pdf = price_map.get(ticker)
                        if pdf is not None and not pdf.empty:
                            last_row = pdf.iloc[-1]
                            s1_high = float(last_row.get("High") or last_row.get("Close") or 0) or None
                            s1_vol  = int(last_row.get("Volume") or 0) or None

                    upsert_rows.append({
                        "ticker":          ticker,
                        "classified_date": today,
                        "stage":           stage,
                        "s1_entry_date":   today if stage == 1 else None,
                        "s1_high":         s1_high,
                        "s1_volume":       s1_vol,
                        "peakout_flag":    peakout,
                    })
            except Exception as e:
                logger.debug("[3단계] 분류 오류: %s", e)

    logger.info("[3단계] 분류 완료 — Stage1:%d Stage2:%d Stage3:%d 피크아웃:%d",
                sum(1 for s in stage_results.values() if s == 1),
                sum(1 for s in stage_results.values() if s == 2),
                sum(1 for s in stage_results.values() if s == 3),
                len(peakout_flags))

    # 6. stage_classifications upsert
    await save_stage_classifications(_db_pool, upsert_rows)

    # 7. 텔레그램 비교 메세지 전송
    try:
        await tg_send_screener_comparison(
            ichimoku_rows=ichimoku_rows,
            stage_results=stage_results,
            peakout_flags=peakout_flags,
        )
    except Exception as e:
        logger.warning("[3단계] 텔레그램 전송 실패: %s", e)

    logger.info("[3단계] 일별 분류 완료")


# ── 거래대금 워치리스트 일보 (평일 17:00 KST = 08:00 UTC) ────
async def _watchlist_brief_job() -> None:
    """Stage 1 진입 후 추적 중인 종목의 거래대금 건강도·수급 스트릭·Ichimoku를 요약해 전송.

    확장 기능:
    - Stage 1→2 전환 감지 시 별도 알림 전송
    - vol_ratio < 0.6 3거래일 연속 시 랠리 소멸 경고 전송
    - 확신도 순 정렬 (vol_ratio × 스트릭 composite)
    """
    if not _db_pool:
        logger.warning("[워치리스트] DB 풀 없음 — 스킵")
        return

    import yfinance as _yf
    from datetime import date as _date
    from concurrent.futures import ThreadPoolExecutor as _TPE, as_completed as _as_completed

    logger.info("[워치리스트] 거래대금 워치리스트 일보 시작")
    today = _date.today()
    loop  = asyncio.get_running_loop()

    # Step 1: Stage 1 워치리스트 조회 (최근 14 캘린더일)
    watchlist = await get_stage1_watchlist(_db_pool, days=14)
    if not watchlist:
        logger.info("[워치리스트] 워치리스트 없음")
        async with httpx.AsyncClient() as http:
            await tg_send_watchlist_brief([], http=http)
        return

    logger.info("[워치리스트] %d종목 추적", len(watchlist))

    tickers     = [r["ticker"]   for r in watchlist]
    s1_vol_map  = {r["ticker"]: r["s1_volume"]   for r in watchlist}
    s1_date_map = {r["ticker"]: r["s1_date"]     for r in watchlist}

    # Step 2: 5개 벌크 쿼리 (1 connection acquire)
    flow_map: dict[str, dict]         = {}
    ichimoku_set: set[str]            = set()
    stage_map: dict[str, int]         = {}
    yesterday_stage_map: dict[str, int] = {}
    latest_week: Optional[str]        = None

    try:
        from datetime import timedelta as _td
        yesterday = today - _td(days=1)
        async with _db_pool.acquire() as conn:
            latest_week = await conn.fetchval(
                "SELECT week_of FROM chart_signals ORDER BY screened_at DESC LIMIT 1"
            )
            flow_rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker) ticker, foreign_streak, inst_streak
                FROM   daily_flow
                WHERE  ticker = ANY($1) AND trade_date <= $2
                ORDER  BY ticker, trade_date DESC
                """,
                tickers, today,
            )
            for r in flow_rows:
                flow_map[r["ticker"]] = {"f_streak": r["foreign_streak"], "i_streak": r["inst_streak"]}

            if latest_week:
                ichi_rows = await conn.fetch(
                    "SELECT ticker FROM chart_signals WHERE ticker = ANY($1) AND week_of = $2",
                    tickers, latest_week,
                )
                ichimoku_set = {r["ticker"] for r in ichi_rows}

            stage_rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker) ticker, stage
                FROM   stage_classifications
                WHERE  ticker = ANY($1)
                ORDER  BY ticker, classified_date DESC
                """,
                tickers,
            )
            for r in stage_rows:
                stage_map[r["ticker"]] = r["stage"]

            # 전일 Stage (Stage 1→2 전환 감지용)
            prev_stage_rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker) ticker, stage
                FROM   stage_classifications
                WHERE  ticker = ANY($1) AND classified_date <= $2
                ORDER  BY ticker, classified_date DESC
                """,
                tickers, yesterday,
            )
            for r in prev_stage_rows:
                yesterday_stage_map[r["ticker"]] = r["stage"]
    except Exception as e:
        logger.warning("[워치리스트] 벌크 DB 조회 실패: %s", e)

    # Step 3: 이전 vol_ratio 이력 조회 (랠리 소멸 3거래일 감지)
    vol_log_map: dict[str, list[float]] = await get_watchlist_vol_log(_db_pool, tickers, lookback=3)

    # Step 4: yfinance 병렬 거래량 fetch
    def _fetch_vol(t: str) -> tuple[str, Optional[float]]:
        try:
            df = _yf.Ticker(t).history(period="2d", interval="1d", auto_adjust=True)
            return t, float(df["Volume"].iloc[-1]) if not df.empty else None
        except Exception:
            return t, None

    vol_today: dict[str, Optional[float]] = {}
    with _TPE(max_workers=4) as pool_ex:
        futures = {pool_ex.submit(_fetch_vol, t): t for t in tickers if s1_vol_map.get(t)}
        for fut in _as_completed(futures):
            t, v = fut.result()
            vol_today[t] = v

    # Step 5: 결과 조합
    entries: list[dict] = []
    vol_log_rows: list[dict] = []

    for ticker in tickers:
        s1_vol    = s1_vol_map.get(ticker)
        s1_date   = s1_date_map[ticker]
        days_since = (today - s1_date).days

        today_vol  = vol_today.get(ticker)
        vol_ratio  = (today_vol / s1_vol) if (s1_vol and s1_vol > 0 and today_vol is not None) else None

        flow       = flow_map.get(ticker, {})
        ichimoku_ok = (ticker in ichimoku_set) if latest_week else None
        current_stage = stage_map.get(ticker)

        logger.debug(
            "[워치리스트] %s vol_ratio=%.2f stage=%s streak=(%s,%s)",
            ticker, vol_ratio or 0, current_stage,
            flow.get("f_streak"), flow.get("i_streak"),
        )

        entries.append({
            "ticker":        ticker,
            "days_since":    days_since,
            "vol_ratio":     vol_ratio,
            "f_streak":      flow.get("f_streak"),
            "i_streak":      flow.get("i_streak"),
            "ichimoku_ok":   ichimoku_ok,
            "current_stage": current_stage,
        })

        if vol_ratio is not None:
            vol_log_rows.append({"ticker": ticker, "trade_date": today, "vol_ratio": vol_ratio, "s1_vol": s1_vol})

    # Step 6: vol_ratio 로그 upsert (향후 랠리 소멸 / vol delta 추적용)
    await upsert_watchlist_vol_log(_db_pool, vol_log_rows)

    # Step 7: 확신도 순 정렬 (vol_ratio × 스트릭 composite — 높을수록 먼저)
    def _conviction(e: dict) -> float:
        vr = e.get("vol_ratio") or 0.0
        fs = e.get("f_streak") or 0
        is_ = e.get("i_streak") or 0
        streak_bonus = (1 if fs > 0 else 0) + (1 if is_ > 0 else 0)
        return vr + streak_bonus * 0.1

    entries.sort(key=_conviction, reverse=True)

    # Step 8: 단독 알림 — Stage 2 전환 감지
    stage2_alerts: list[str] = []
    for e in entries:
        ticker = e["ticker"]
        today_stage = e.get("current_stage")
        prev_stage  = yesterday_stage_map.get(ticker)
        if today_stage == 2 and prev_stage == 1:
            stage2_alerts.append(ticker)

    if stage2_alerts:
        try:
            from datetime import date as _d
            today_str = _d.today().strftime("%Y-%m-%d")
            alert_lines = [f"🟢 Stage 2 전환 확인 ({today_str})", ""]
            for t in stage2_alerts:
                code = t.split(".")[0]
                days_d = (today - s1_date_map[t]).days
                alert_lines.append(f"  {code} — D+{days_d} → Stage 2 진입")
            alert_msg = "\n".join(alert_lines)
            async with httpx.AsyncClient() as http:
                from telegram_notify import _get_token, _get_chat_id, _post_message
                token   = _get_token()
                chat_id = _get_chat_id()
                await _post_message(http, token, chat_id, alert_msg, label="Stage2알림", parse_mode=None)
            logger.info("[워치리스트] Stage 2 전환 알림 — %d종목", len(stage2_alerts))
        except Exception as e:
            logger.warning("[워치리스트] Stage 2 알림 전송 실패: %s", e)

    # Step 9: 단독 알림 — 랠리 소멸 감지 (최근 3거래일 연속 vol_ratio < 0.6)
    rally_death_alerts: list[str] = []
    for e in entries:
        ticker    = e["ticker"]
        vol_ratio = e.get("vol_ratio")
        history   = vol_log_map.get(ticker, [])
        all_three = ([vol_ratio] + history)[:3]
        if len(all_three) == 3 and all(r is not None and r < 0.6 for r in all_three):
            rally_death_alerts.append(ticker)

    if rally_death_alerts:
        try:
            from datetime import date as _d
            today_str = _d.today().strftime("%Y-%m-%d")
            death_lines = [f"❌ 랠리 소멸 경고 ({today_str})", "3거래일 연속 진입비 -40% 이상", ""]
            for t in rally_death_alerts:
                code  = t.split(".")[0]
                ratio = next((e["vol_ratio"] for e in entries if e["ticker"] == t), None)
                pct   = f"{(ratio - 1) * 100:.0f}%" if ratio is not None else "N/A"
                death_lines.append(f"  {code} 오늘 비율 {pct}")
            death_msg = "\n".join(death_lines)
            async with httpx.AsyncClient() as http:
                from telegram_notify import _get_token, _get_chat_id, _post_message
                token   = _get_token()
                chat_id = _get_chat_id()
                await _post_message(http, token, chat_id, death_msg, label="랠리소멸알림", parse_mode=None)
            logger.info("[워치리스트] 랠리 소멸 경고 — %d종목", len(rally_death_alerts))
        except Exception as e:
            logger.warning("[워치리스트] 랠리 소멸 알림 전송 실패: %s", e)

    # Step 10: 일보 전송
    try:
        async with httpx.AsyncClient() as http:
            await tg_send_watchlist_brief(entries, http=http)
        logger.info("[워치리스트] 일보 전송 완료 — %d종목", len(entries))
    except Exception as e:
        logger.warning("[워치리스트] 전송 실패: %s", e)


# ── 스케줄러 진입점 ───────────────────────────────────────────
async def main(interval: int, enable_summary: bool) -> None:
    global _summary_queue, _db_pool, _screener_tickers

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
        from krx_sync import sync_krx_listings
        from ticker_cache import ticker_cache as _ticker_cache
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
            from db import get_chart_signals_this_week
            _screener_tickers = await get_chart_signals_this_week(_db_pool)
            logger.info("[게이팅] 스크리너 캐시 로드 — %d종목", len(_screener_tickers))
        except Exception as _gt_e:
            logger.warning("[게이팅] 스크리너 캐시 로드 실패: %s", _gt_e)

    # ── 봇 초기화 ─────────────────────────────────────────────
    init_bot(_seen_hashes)
    bot_task = asyncio.create_task(bot_polling_loop(_db_pool))
    logger.info("Telegram 봇 시작 — /status /signals /today /help")

    # ── 펀더멘털 캐시 예열 ─────────────────────────────────────
    from market_data import prewarm_fundamentals, YFINANCE_MAP
    _ks_symbols = [v for v in YFINANCE_MAP.values() if v.endswith((".KS", ".KQ"))]
    if _ks_symbols:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, prewarm_fundamentals, _ks_symbols)

    # 요약 워커 초기화
    worker_task = None
    if enable_summary:
        _summary_queue = asyncio.Queue()
        worker_task = asyncio.create_task(summary_worker())

    # 수집 스케줄러 등록
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

    # ── 백테스팅: 가격 체크포인트 트래커 (30분 간격) ──────────
    async def _track_outcomes_job():
        if not _db_pool:
            return
        try:
            from backtest import track_outcomes
            result = await track_outcomes(_db_pool)
            if result["filled"]:
                logger.info("[트래커] 체크포인트 %d개 채움", result["filled"])
        except Exception as e:
            logger.warning("[트래커] 실행 실패: %s", e)

    scheduler.add_job(
        _track_outcomes_job,
        trigger="interval",
        minutes=30,
        id="price_tracker",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )

    # ── 백테스팅: 주간 리포트 (일요일 20:00 KST) ───────────────
    async def _weekly_backtest_report_job():
        if not _db_pool:
            return
        try:
            from backtest import backtest_report_telegram
            from telegram_notify import _get_token, _get_chat_id
            import httpx as _httpx
            report = await backtest_report_telegram(_db_pool)
            token = _get_token()
            chat_id = _get_chat_id()
            if not token or not chat_id:
                logger.warning("[주간리포트] TELEGRAM_BOT_TOKEN 또는 TELEGRAM_CHAT_ID 미설정")
                return
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            async with _httpx.AsyncClient() as http:
                await http.post(url, json={
                    "chat_id": chat_id,
                    "text": report,
                    "parse_mode": "MarkdownV2",
                }, timeout=30)
            logger.info("[주간리포트] 백테스팅 주간 리포트 전송 완료")
        except Exception as e:
            logger.warning("[주간리포트] 실행 실패: %s", e)

    scheduler.add_job(
        _weekly_backtest_report_job,
        CronTrigger(day_of_week="sun", hour=20, minute=0, timezone="Asia/Seoul"),
        id="weekly_backtest",
        max_instances=1,
        misfire_grace_time=3600,
        replace_existing=True,
    )

    # ── KRX 종목 리스트 일일 갱신 (매일 20:00 KST — 장 마감 후) ──
    async def _daily_krx_refresh_job():
        if not _db_pool:
            return
        from krx_sync import sync_krx_listings
        from ticker_cache import ticker_cache as _ticker_cache
        try:
            n = await sync_krx_listings(_db_pool)
            logger.info("[krx_sync] 일일 갱신 완료: %d행", n)
        except Exception as e:
            logger.error("[krx_sync] 일일 갱신 실패: %s — 기존 DB 데이터로 캐시 재로드", e)
        finally:
            try:
                await _ticker_cache.load(_db_pool)
            except Exception as _cache_e:
                logger.warning("[ticker_cache] 캐시 재로드 실패: %s", _cache_e)

    scheduler.add_job(
        _daily_krx_refresh_job,
        CronTrigger(hour=20, minute=0, timezone="Asia/Seoul"),
        id="krx_daily_refresh",
        max_instances=1,
        misfire_grace_time=3600,
        replace_existing=True,
    )

    # ── 차트 스크리닝: 주봉 스크리닝 (일요일 20:30 KST) ──────────
    async def _weekly_screener_job():
        global _screener_tickers
        loop = asyncio.get_running_loop()
        if not _db_pool:
            logger.warning("[차트스크리너] DB 풀 없음 — 스크리닝 건너뜀")
            return
        try:
            from chart_screener import run_weekly_screen
            results = await loop.run_in_executor(None, run_weekly_screen)
            saved = await save_chart_signals(_db_pool, results)
            logger.info("[차트스크리너] 완료 — 통과:%d 저장:%d", len(results), saved)
            # 뉴스 게이팅 캐시 갱신
            _screener_tickers = {r.ticker for r in results}
            logger.info("[게이팅] 스크리너 캐시 갱신 — %d종목", len(_screener_tickers))
            async with httpx.AsyncClient() as http:
                await tg_send_weekly_screener(results, http=http)
        except Exception as e:
            logger.warning("[차트스크리너] 실행 실패: %s", e)
            return

        try:
            from generate_html_report import generate_html
            from pathlib import Path as _Path
            html_str = generate_html(results)
            _html_dir = _Path("reports/screener")
            _html_dir.mkdir(parents=True, exist_ok=True)
            from datetime import datetime as _dt
            from zoneinfo import ZoneInfo as _ZI
            _html_path = _html_dir / f"screener_{_dt.now(_ZI('Asia/Seoul')).strftime('%Y%m%d_%H%M')}.html"
            _html_path.write_text(html_str, encoding="utf-8")
            logger.info("[차트스크리너] HTML 리포트: %s", _html_path)
        except Exception as _html_e:
            logger.warning("[차트스크리너] HTML 생성 실패 (비중요): %s", _html_e)


    scheduler.add_job(
        _weekly_screener_job,
        CronTrigger(day_of_week="sun", hour=20, minute=30, timezone="Asia/Seoul"),
        id="weekly_chart_screener",
        max_instances=1,
        misfire_grace_time=3600,
        replace_existing=True,
    )

    # ── 3단계 분류기: 일별 16:30 KST (UTC 07:30) ─────────────────
    scheduler.add_job(
        _daily_stage_job,
        CronTrigger(day_of_week="mon-fri", hour=7, minute=30, timezone="UTC"),  # = 16:30 KST
        id="daily_stage_classifier",
        max_instances=1,
        misfire_grace_time=3600,
        replace_existing=True,
    )

    # ── 거래량 분석: 일일 배치 리포트 (평일 15:40 KST) ──────────
    async def _daily_volume_report_job():
        try:
            from batch_run import run_batch
            from telegram_notify import _get_token, _get_chat_id
            import httpx as _httpx

            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, run_batch)

            summaries = result["summaries"]
            success = result["success"]
            total = result["total"]

            def _fmt_pct(pct: float) -> str:
                return f"+{pct:.2f}%" if pct > 0 else f"{pct:.2f}%"

            up = sorted(
                [s for s in summaries if s.get("has_data") and s["pct"] > 0],
                key=lambda x: -x["pct"],
            )[:3]
            down = sorted(
                [s for s in summaries if s.get("has_data") and s["pct"] < 0],
                key=lambda x: x["pct"],
            )[:3]

            up_lines   = "\n".join(f"  📈 {s['name']} {_fmt_pct(s['pct'])}" for s in up)   or "  없음"
            down_lines = "\n".join(f"  📉 {s['name']} {_fmt_pct(s['pct'])}" for s in down) or "  없음"

            msg = (
                f"📊 오늘의 거래량 패턴 리포트\n"
                f"분석 완료: {success}/{total}종목\n\n"
                f"상승 상위 3:\n{up_lines}\n\n"
                f"하락 상위 3:\n{down_lines}"
            )

            token = _get_token()
            chat_id = _get_chat_id()
            if not token or not chat_id:
                logger.warning("[볼륨리포트] TELEGRAM_TOKEN 또는 TELEGRAM_CHAT_ID 미설정")
                return
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            async with _httpx.AsyncClient() as http:
                await http.post(url, json={"chat_id": chat_id, "text": msg}, timeout=30)
            logger.info("[볼륨리포트] 일일 배치 리포트 전송 완료 (%d/%d)", success, total)
        except Exception as e:
            logger.warning("[볼륨리포트] 실행 실패: %s", e)

    scheduler.add_job(
        _daily_volume_report_job,
        CronTrigger(day_of_week="mon-fri", hour=15, minute=40, timezone="Asia/Seoul"),
        id="daily_volume_report",
        max_instances=1,
        misfire_grace_time=3600,
        replace_existing=True,
    )

    # ── 거래대금 워치리스트 일보 (평일 17:00 KST = 08:00 UTC) ──
    scheduler.add_job(
        _watchlist_brief_job,
        CronTrigger(day_of_week="mon-fri", hour=8, minute=0, timezone="UTC"),
        id="daily_watchlist_brief",
        max_instances=1,
        misfire_grace_time=3600,
        replace_existing=True,
    )

    # ── 수급 증분 sync: 평일 18:00 KST (09:00 UTC) ───────────────
    # krx_flow_sync.py --incremental (전일 전 종목 수급 → daily_flow)
    # 장 마감(15:30) + KRX 데이터 게시 여유(~2h) 확보.
    # stage_classifier(16:30 KST)는 DB에 이미 적재된 전일 데이터를 읽으므로 순서 무관.
    async def _daily_flow_sync_job():
        import sys as _sys
        logger.info("[flow-sync] krx_flow_sync --incremental 시작")
        try:
            proc = await asyncio.create_subprocess_exec(
                _sys.executable, "krx_flow_sync.py", "--incremental",
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

    scheduler.add_job(
        _daily_flow_sync_job,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=0, timezone="UTC"),  # = 18:00 KST
        id="daily_flow_sync",
        max_instances=1,
        misfire_grace_time=3600,
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
        from krx_sync import sync_krx_listings
        from ticker_cache import ticker_cache as _ticker_cache
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
