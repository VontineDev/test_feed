"""
telegram_bot.py  —  Telegram 봇 명령어 처리 모듈
────────────────────────────────────────────────────────────
Long polling 방식으로 명령어를 수신하고 DB 조회 결과를 응답.

지원 명령어:
    /status   — 크롤러 현재 상태 (수집 건수, 마지막 수집 시각 등)
    /signals  — 최근 매매 신호 10건 (BUY/SELL/WATCH)
    /today    — 오늘 수집된 기사 요약 (카테고리별 건수 + 최신 5건)
    /backtest — 교차분석 백테스팅 리포트 (판정별/종목별 적중률)
    /help     — 명령어 목록
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import os
logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"

# 마지막으로 처리한 update_id (중복 처리 방지)
_last_update_id: int = 0
# 크롤러 시작 시각 (uptime 계산용)
_start_time: datetime = datetime.now(timezone.utc)
# 누적 수집 건수 참조 (run_scheduler에서 주입)
_seen_hashes_ref: Optional[set] = None


def init_bot(seen_hashes: set) -> None:
    """run_scheduler에서 _seen_hashes 참조를 주입"""
    global _seen_hashes_ref, _start_time
    _seen_hashes_ref = seen_hashes
    _start_time = datetime.now(timezone.utc)


# ── 공통 유틸 ────────────────────────────────────────────────

def _get_token() -> str:
    token = os.environ.get("TELEGRAM_TOKEN", "")
    if not token:
        raise ValueError("TELEGRAM_TOKEN 미설정")
    return token

def _get_chat_id() -> str:
    return os.environ.get("TELEGRAM_CHAT_ID", "")

def _get_allowed_ids() -> set[str]:
    """ALLOWED_CHAT_IDS (콤마 구분) → set. 미설정 시 TELEGRAM_CHAT_ID 단일 허용.
    둘 다 미설정 시 RuntimeError — 무인증 접근을 허용하지 않기 위함.
    """
    raw = os.environ.get("ALLOWED_CHAT_IDS", "").strip()
    if raw:
        return {cid.strip() for cid in raw.split(",") if cid.strip()}
    single = _get_chat_id()
    if single:
        return {single}
    raise RuntimeError(
        "ALLOWED_CHAT_IDS 또는 TELEGRAM_CHAT_ID 환경변수 중 하나는 반드시 설정해야 합니다. "
        "미설정 시 봇 명령어가 모든 사용자에게 열립니다."
    )

def esc(text: str) -> str:
    """MarkdownV2 이스케이프"""
    for ch in r"\_*[]()~>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text

def _fmt_kst(dt: Optional[datetime]) -> str:
    if not dt:
        return "\\-"
    kst = dt + timedelta(hours=9)
    return esc(kst.strftime("%m-%d %H:%M"))


# ── 메시지 전송 ───────────────────────────────────────────────

async def _send(http: httpx.AsyncClient, chat_id: str, text: str) -> None:
    token = _get_token()
    url = TELEGRAM_API.format(token=token, method="sendMessage")
    try:
        resp = await http.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "MarkdownV2",
            "disable_web_page_preview": True,
        }, timeout=10)
        if not resp.json().get("ok"):
            logger.warning("[봇] 메시지 전송 실패: %s", resp.json().get("description"))
    except Exception as e:
        logger.warning("[봇] 전송 오류: %s", e)


async def _send_plain(http: httpx.AsyncClient, chat_id: str, text: str) -> None:
    """MarkdownV2 없이 일반 텍스트 전송 (박스 문자·그래프 포함 메시지용)"""
    token = _get_token()
    url = TELEGRAM_API.format(token=token, method="sendMessage")
    try:
        resp = await http.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }, timeout=10)
        if not resp.json().get("ok"):
            logger.warning("[봇] 메시지 전송 실패: %s", resp.json().get("description"))
    except Exception as e:
        logger.warning("[봇] 전송 오류: %s", e)


# ── 명령어 핸들러 ─────────────────────────────────────────────

async def _handle_status(http: httpx.AsyncClient, chat_id: str, pool) -> None:
    """/status — 크롤러 현재 상태"""
    now = datetime.now(timezone.utc)
    uptime = now - _start_time
    hours, rem = divmod(int(uptime.total_seconds()), 3600)
    minutes = rem // 60
    collected = len(_seen_hashes_ref) if _seen_hashes_ref else 0

    # DB에서 오늘 수집 건수
    today_count = 0
    signal_count = 0
    if pool:
        async with pool.acquire() as conn:
            today_count = await conn.fetchval(
                "SELECT COUNT(*) FROM news_articles WHERE fetched_at >= NOW() - INTERVAL '24 hours'"
            )
            signal_count = await conn.fetchval(
                "SELECT COUNT(*) FROM trade_signals WHERE detected_at >= NOW() - INTERVAL '24 hours'"
            )

    lines = [
        "📡 *크롤러 상태*",
        "",
        f"🕐 업타임: {esc(f'{hours}시간 {minutes}분')}",
        f"📰 누적 수집: {esc(str(collected))}건",
        f"📊 최근 24h 수집: {esc(str(today_count))}건",
        f"🎯 최근 24h 신호: {esc(str(signal_count))}건",
        f"🌐 피드: Reuters \\+ Investing \\+ CNBC",
    ]
    await _send(http, chat_id, "\n".join(lines))


async def _handle_signals(http: httpx.AsyncClient, chat_id: str, pool, direction_filter: str = "") -> None:
    """/signals [buy|sell|watch] — 최근 매매 신호 10건 (방향 필터 선택)"""
    if not pool:
        await _send(http, chat_id, "DB 미연결 상태입니다\\.")
        return

    dir_upper = direction_filter.upper()
    valid_dirs = ("BUY", "SELL", "WATCH")
    if dir_upper and dir_upper not in valid_dirs:
        await _send(http, chat_id, "사용법: /signals \\[buy\\|sell\\|watch\\]")
        return

    if dir_upper:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT s.direction, s.strength, s.reason, s.tickers,
                       s.detected_at, a.title_en, a.source
                FROM trade_signals s
                JOIN news_articles a ON a.id = s.article_id
                WHERE s.direction = $1
                ORDER BY s.detected_at DESC
                LIMIT 10
            """, dir_upper)
    else:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT s.direction, s.strength, s.reason, s.tickers,
                       s.detected_at, a.title_en, a.source
                FROM trade_signals s
                JOIN news_articles a ON a.id = s.article_id
                ORDER BY s.detected_at DESC
                LIMIT 10
            """)

    if not rows:
        label = f" \\({esc(dir_upper)}\\)" if dir_upper else ""
        await _send(http, chat_id, f"최근 감지된 신호가 없습니다{label}\\.")
        return

    icon_map = {"BUY": "🟢", "SELL": "🔴", "WATCH": "🟡"}
    header = f"🎯 *최근 {esc(dir_upper)} 신호 10건*" if dir_upper else "🎯 *최근 매매 신호 10건*"
    lines = [header, ""]
    for r in rows:
        icon = icon_map.get(r["direction"], "⚪")
        bar  = "⬛" * r["strength"] + "⬜" * (5 - r["strength"])
        kst  = _fmt_kst(r["detected_at"])
        lines += [
            f"{icon} *{esc(r['direction'])}* {bar}",
            f"   {esc(r['title_en'][:55])}",
            f"   💬 {esc(r['reason'][:60]) if r['reason'] else '\\-'}",
            f"   🕐 {kst}",
            "",
        ]
    await _send(http, chat_id, "\n".join(lines))


async def _handle_today(http: httpx.AsyncClient, chat_id: str, pool) -> None:
    """/today — 오늘 수집 현황 + 최신 기사 5건"""
    if not pool:
        await _send(http, chat_id, "DB 미연결 상태입니다\\.")
        return

    async with pool.acquire() as conn:
        # 카테고리별 건수
        cat_rows = await conn.fetch("""
            SELECT category, COUNT(*) as cnt
            FROM news_articles
            WHERE fetched_at >= NOW() - INTERVAL '24 hours'
            GROUP BY category ORDER BY cnt DESC
        """)
        # 최신 기사 5건
        art_rows = await conn.fetch("""
            SELECT source, category, title_en, summary_ko, fetched_at
            FROM news_articles
            WHERE fetched_at >= NOW() - INTERVAL '24 hours'
            ORDER BY fetched_at DESC
            LIMIT 5
        """)

    cat_emoji = {"markets": "📈", "macro": "🏦", "korea": "🇰🇷"}
    lines = ["📅 *오늘 수집 현황*", ""]

    # 카테고리 통계
    for r in cat_rows:
        em = cat_emoji.get(r["category"], "📰")
        lines.append(f"{em} {esc(r['category'])}: {esc(str(r['cnt']))}건")

    lines += ["", "📰 *최신 기사 5건*", ""]

    for r in art_rows:
        em  = cat_emoji.get(r["category"], "📰")
        kst = _fmt_kst(r["fetched_at"])
        ko  = r["summary_ko"] or ""
        lines += [
            f"{em} *{esc(r['title_en'][:55])}*",
            f"   {esc(ko[:80]) if ko else '\\(요약 없음\\)'}",
            f"   🕐 {kst}",
            "",
        ]
    await _send(http, chat_id, "\n".join(lines))


async def _handle_backtest(http: httpx.AsyncClient, chat_id: str, pool) -> None:
    """/backtest — 교차분석 백테스팅 리포트"""
    from backtest import backtest_report_telegram
    report = await backtest_report_telegram(pool)
    await _send(http, chat_id, report)


async def _handle_volume(http: httpx.AsyncClient, chat_id: str, args: list[str]) -> None:
    """/volume <종목명|티커> — 시간대별 거래량 패턴 분석"""
    import functools
    from volume_pattern import resolve_ticker, fetch_data, build_report

    if not args:
        await _send_plain(http, chat_id,
            "사용법: /volume <종목명 또는 티커>\n"
            "예) /volume 삼성전자\n"
            "    /volume AAPL\n"
            "    /volume 005930"
        )
        return

    raw_input = " ".join(args)
    await _send_plain(http, chat_id, f"📊 {raw_input} 거래량 패턴 조회 중...")

    try:
        ticker, display_name, market = resolve_ticker(raw_input)

        loop = asyncio.get_running_loop()
        df, full_name, data_source = await loop.run_in_executor(
            None,
            functools.partial(fetch_data, ticker, market),
        )

        if df is None or df.empty:
            await _send_plain(http, chat_id,
                f"'{raw_input}'에 대한 데이터를 가져올 수 없습니다.\n"
                "티커/종목명을 확인해 주세요."
            )
            return

        report = build_report(df, ticker, display_name, full_name, market, data_source)
        _suffix = "\n...(생략)"
        if len(report) > 4096 - len(_suffix):
            report = report[:4096 - len(_suffix)] + _suffix

        await _send_plain(http, chat_id, report)
    except Exception as e:
        logger.warning("[봇] /volume 오류: %s", e)
        await _send_plain(http, chat_id, f"오류가 발생했습니다: {e}")


async def _handle_help(http: httpx.AsyncClient, chat_id: str) -> None:
    """/help — 명령어 목록"""
    lines = [
        "📋 *사용 가능한 명령어*",
        "",
        "/status — 크롤러 상태 \\(업타임, 수집 건수\\)",
        "/signals — 최근 매매 신호 10건",
        "/signals buy — BUY 신호만 조회",
        "/signals sell — SELL 신호만 조회",
        "/signals watch — WATCH 신호만 조회",
        "/today — 오늘 수집 현황 \\+ 최신 기사",
        "/backtest — 교차분석 백테스팅 리포트",
        "/volume <종목명\\|티커> — 시간대별 거래량 패턴 분석",
        "/help — 이 도움말",
    ]
    await _send(http, chat_id, "\n".join(lines))


# ── 업데이트 수신 및 라우팅 ───────────────────────────────────

async def _get_updates(http: httpx.AsyncClient, offset: int) -> list[dict]:
    """Long polling으로 업데이트 수신 (최대 30초 대기)"""
    token = _get_token()
    url = TELEGRAM_API.format(token=token, method="getUpdates")
    try:
        resp = await http.get(url, params={
            "offset": offset,
            "timeout": 30,
            "allowed_updates": ["message"],
        }, timeout=35)
        data = resp.json()
        return data.get("result", []) if data.get("ok") else []
    except Exception:
        return []


async def _process_update(http: httpx.AsyncClient, update: dict, pool) -> None:
    """단일 업데이트 처리"""
    msg = update.get("message", {})
    text = msg.get("text", "").strip()
    chat_id = str(msg.get("chat", {}).get("id", ""))

    # 화이트리스트 Chat ID만 허용
    allowed = _get_allowed_ids()
    if allowed and chat_id not in allowed:
        logger.debug("[봇] 허용되지 않은 chat_id: %s", chat_id)
        return

    if not text.startswith("/"):
        return

    parts = text.split()
    cmd = parts[0].lower().split("@")[0]  # /status@botname → /status
    args = parts[1:]
    logger.info("[봇] 명령어 수신: %s (chat_id: %s)", cmd, chat_id)

    if cmd == "/status":
        await _handle_status(http, chat_id, pool)
    elif cmd == "/signals":
        direction_filter = args[0] if args else ""
        await _handle_signals(http, chat_id, pool, direction_filter)
    elif cmd == "/today":
        await _handle_today(http, chat_id, pool)
    elif cmd == "/backtest":
        await _handle_backtest(http, chat_id, pool)
    elif cmd == "/volume":
        await _handle_volume(http, chat_id, args)
    elif cmd in ("/help", "/start"):
        await _handle_help(http, chat_id)
    else:
        await _send(http, chat_id, f"알 수 없는 명령어입니다\\. /help 를 입력해보세요\\.")


# ── 봇 폴링 루프 (별도 asyncio 태스크로 실행) ────────────────

async def bot_polling_loop(pool) -> None:
    """
    run_scheduler의 main()에서 asyncio.create_task()로 실행.
    예) bot_task = asyncio.create_task(bot_polling_loop(db_pool))
    """
    global _last_update_id
    logger.info("[봇] 명령어 수신 시작 (/status /signals /today /help)")

    async with httpx.AsyncClient() as http:
        while True:
            try:
                updates = await _get_updates(http, _last_update_id + 1)
                for update in updates:
                    _last_update_id = max(_last_update_id, update["update_id"])
                    await _process_update(http, update, pool)
            except asyncio.CancelledError:
                logger.info("[봇] 폴링 종료")
                break
            except Exception as e:
                logger.warning("[봇] 폴링 오류: %s", e)
                await asyncio.sleep(5)
