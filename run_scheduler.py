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
from logging.handlers import TimedRotatingFileHandler
import sys
from datetime import datetime, timezone, timedelta

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
from db import create_pool, init_db, save_article, save_signal, save_cross_analysis, load_seen_hashes
from telegram_notify import send_signal as tg_send_signal
from signal_detector import detect_signal
from article_fetcher import fetch_article_body
from telegram_bot import bot_polling_loop, init_bot
from market_data import cross_analyze, CrossAnalysis

# ── 로깅 설정 ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        TimedRotatingFileHandler(
            "news_crawler.log",
            when="midnight",
            interval=1,
            backupCount=14,
            encoding="utf-8",
        ),
    ],
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)



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
                    signal = await detect_signal(
                        title=art["title"],
                        summary_ko=summary_ko,
                        http=http,
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
                                cross = await asyncio.get_event_loop().run_in_executor(
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
                                article_id  = article_id,
                                direction   = signal.direction,
                                strength    = signal.strength,
                                reason      = signal.reason,
                                tickers     = signal.tickers,
                                llm_backend = signal.backend.value,
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
                    else:
                        await tg_send_signal(art, summary_ko, signal, http=http, cross=cross)

            except Exception as e:
                logger.warning("[요약 워커] 처리 오류: %s", e)
            finally:
                _summary_queue.task_done()


# ── 스케줄러 진입점 ───────────────────────────────────────────
async def main(interval: int, enable_summary: bool) -> None:
    global _summary_queue, _db_pool

    logger.info("뉴스 크롤러 시작 — 수집 %d분 간격", interval)
    logger.info("구조: [수집 잡] → Queue → [요약 워커] (완전 분리)")
    logger.info("한글 요약: %s | 피드 %d개 | Ctrl+C 로 종료\n",
                "ON (Ollama→LM Studio)" if enable_summary else "OFF", len(FEEDS))

    # ── DB 초기화 ─────────────────────────────────────────────
    try:
        _db_pool = await create_pool()
        await init_db(_db_pool)
    except Exception as e:
        logger.error("DB 연결 실패: %s", e)
        logger.error("DB 없이 계속 실행합니다 (콘솔 출력만)")
        _db_pool = None

    # ── 봇 초기화 ─────────────────────────────────────────────
    init_bot(_seen_hashes)
    bot_task = asyncio.create_task(bot_polling_loop(_db_pool))
    logger.info("Telegram 봇 시작 — /status /signals /today /help")

    # 요약 워커 초기화
    worker_task = None
    if enable_summary:
        _summary_queue = asyncio.Queue()
        worker_task = asyncio.create_task(summary_worker())

    # 수집 스케줄러 등록
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        collect_job,
        trigger="interval",
        minutes=interval,
        id="news_collect",
        next_run_time=datetime.now(timezone.utc) + timedelta(seconds=3),  # 스케줄러 시작 후 3초 뒤 첫 실행
        max_instances=1,                            # 중복 실행 방지
        coalesce=True,                              # 밀린 잡 합치기
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
    args = parser.parse_args()
    asyncio.run(main(args.interval, enable_summary=not args.no_summary))
