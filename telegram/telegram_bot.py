"""
telegram_bot.py  —  Telegram 봇 명령어 처리 모듈
────────────────────────────────────────────────────────────
Long polling 방식으로 명령어를 수신하고 DB 조회 결과를 응답.

지원 명령어:
    /status   — 크롤러 현재 상태 (수집 건수, 마지막 수집 시각 등)
    /signals  — 최근 매매 신호 10건 (BUY/SELL/WATCH)
    /today    — 오늘 수집된 기사 요약 (카테고리별 건수 + 최신 5건)
    /backtest  — 통합 백테스트 (ichimoku / stage / cross / compose 모드)
    /screener     — 최신 강세 후보 발굴 결과 (DB 조회, 명령어 발신자에게만 전송)
    /run_screener — 강세 후보 즉시 스캔 (전 종목 실시간 스캔, 결과 저장 후 발신자에게 전송)
    /buy          — 진입 기록 (거래 저널)
    /sell      — 청산 기록 (FIFO)
    /port      — 보유 현황 + 미실현 P&L
    /pnl       — 실현 P&L 요약
    /help      — 명령어 목록
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
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
# 스크리너 중복 실행 방지 락
_scan_lock: asyncio.Lock = asyncio.Lock()
# 파이프라인 수동 트리거 락
# _flow_lock: jobs/infra_jobs.py::daily_flow_sync_job()이 실제로 쓰는 락을
# 그대로 re-export한다(같은 객체) — cron/대시보드트리거/텔레그램 3개 진입
# 경로가 서로 몰라 동시 실행되던 사고(2026-08-31) 이후, 락을 자원을 쓰는
# 함수 자체에 두고 여기선 같은 락을 "훑어보기"(.locked())용으로만 참조한다.
# 텔레그램 쪽에서 또 async with로 감싸면 자기 자신을 기다리며 데드락 나니
# 감싸면 안 됨 — _run_flow_task()가 daily_flow_sync_job()을 직접 호출하는
# 이유가 이것.
from jobs.infra_jobs import flow_sync_lock as _flow_lock  # noqa: E402,F401 — bot._flow_lock로 외부 참조
_stage_lock: asyncio.Lock = asyncio.Lock()
_youtube_lock: asyncio.Lock = asyncio.Lock()
# /backtest 중복 실행 방지 락
_backtest_lock: asyncio.Lock = asyncio.Lock()


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


# ── 명령어 핸들러 (bot_handlers.py로 분리) ────────────────────
# _process_update 라우팅과 기존 `from telegram.telegram_bot import X` 경로
# (테스트 4파일)를 보존하는 top-level 재수출. bot_handlers는 telegram_bot을
# top-level에서 import하지 않고 함수 본문에서 지연 import하므로 순환 없음.
from telegram.bot_handlers import (  # noqa: F401
    MODEL_ICON, esc, _fmt_kst,
    _handle_status, _handle_signals, _handle_today,
    _handle_backtest_compose, _handle_backtest, _handle_top,
    _handle_screener, _run_screener_task,
    _run_flow_task, _run_stage_task, _run_youtube_task,
    _handle_run_flow, _handle_run_stage, _handle_run_screener,
    _handle_run_youtube, _handle_run_all,
    _handle_paper, _handle_paper_perf, _handle_paper_exit,
    _handle_watchlist, _handle_help,
)


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
        await _handle_backtest(http, chat_id, args)
    elif cmd == "/top":
        await _handle_top(http, chat_id, args)
    elif cmd == "/screener":
        await _handle_screener(http, chat_id, pool)
    elif cmd == "/buy":
        from telegram.telegram_trade import handle_buy
        await handle_buy(http, _get_token(), chat_id, args, pool)
    elif cmd == "/sell":
        from telegram.telegram_trade import handle_sell
        await handle_sell(http, _get_token(), chat_id, args, pool)
    elif cmd == "/port":
        from telegram.telegram_trade import handle_port
        await handle_port(http, _get_token(), chat_id, pool)
    elif cmd == "/pnl":
        from telegram.telegram_trade import handle_pnl
        await handle_pnl(http, _get_token(), chat_id, args, pool)
    elif cmd == "/paper":
        await _handle_paper(http, chat_id, pool)
    elif cmd == "/paper_perf":
        await _handle_paper_perf(http, chat_id, pool)
    elif cmd == "/paper_exit":
        await _handle_paper_exit(http, chat_id, pool, args)
    elif cmd == "/watchlist":
        await _handle_watchlist(http, chat_id, pool)
    elif cmd == "/run_flow":
        await _handle_run_flow(http, chat_id)
    elif cmd == "/run_stage":
        await _handle_run_stage(http, chat_id, pool)
    elif cmd == "/run_screener":
        await _handle_run_screener(http, chat_id, pool)
    elif cmd == "/run_youtube":
        await _handle_run_youtube(http, chat_id)
    elif cmd == "/run_all":
        await _handle_run_all(http, chat_id, pool)
    elif cmd in ("/help", "/start"):
        await _handle_help(http, chat_id)
    else:
        await _send(http, chat_id, "알 수 없는 명령어입니다\\. /help 를 입력해보세요\\.")


# ── 봇 폴링 루프 (별도 asyncio 태스크로 실행) ────────────────

async def _register_commands(http: httpx.AsyncClient) -> None:
    """Telegram에 봇 명령어 목록을 등록 (/ 입력 시 자동완성에 표시됨)."""
    token = _get_token()
    url = TELEGRAM_API.format(token=token, method="setMyCommands")
    commands = [
        {"command": "status",   "description": "크롤러 상태 (업타임, 수집 건수)"},
        {"command": "signals",  "description": "최근 매매 신호 10건"},
        {"command": "today",    "description": "오늘 수집 현황 + 최신 기사"},
        {"command": "backtest", "description": "백테스트 — /backtest ichimoku|stage|cross|compose <start> <end>  compose예) /backtest compose FUNNEL-1 2025-01-01 2026-06-14"},
        {"command": "top",       "description": "당일 거래금액 상위 10 (KOSPI+KOSDAQ)"},
        {"command": "screener", "description": "최신 강세 후보 발굴 결과 (DB 조회)"},
        {"command": "buy",      "description": "진입 기록 — /buy 005930 70000 100 [YYYYMMDD]"},
        {"command": "sell",     "description": "청산 기록 (FIFO) — /sell 005930 73500"},
        {"command": "port",     "description": "보유 현황 + 미실현 P&L"},
        {"command": "pnl",         "description": "실현 P&L 요약 — /pnl [week|month|all]"},
        {"command": "paper",       "description": "모의투자 오픈 포지션 현황"},
        {"command": "paper_perf",  "description": "모의투자 누적 성과 (승률·수익·슬리피지)"},
        {"command": "paper_exit",  "description": "수동 강제 청산 — /paper_exit 005930"},
        {"command": "watchlist",      "description": "거래대금 워치리스트 즉시 조회 (온디맨드)"},
        {"command": "run_flow",       "description": "수급 수집 즉시 실행 (KRX, 40~60분)"},
        {"command": "run_stage",      "description": "스테이지 분류 즉시 실행 (10~20분)"},
        {"command": "run_screener",   "description": "스크리너 즉시 실행 (전 종목, 10~20분)"},
        {"command": "run_youtube",    "description": "유튜브 수집+어텐션 점수 즉시 실행 (5~10분)"},
        {"command": "run_all",        "description": "4개 파이프라인 동시 실행"},
        {"command": "help",           "description": "명령어 목록"},
    ]
    try:
        resp = await http.post(url, json={"commands": commands}, timeout=10)
        if resp.json().get("ok"):
            logger.info("[봇] 명령어 목록 등록 완료 (%d개)", len(commands))
        else:
            logger.warning("[봇] 명령어 등록 실패: %s", resp.json().get("description"))
    except Exception as e:
        logger.warning("[봇] 명령어 등록 오류: %s", e)


async def bot_polling_loop(pool) -> None:
    """
    run_scheduler의 main()에서 asyncio.create_task()로 실행.
    예) bot_task = asyncio.create_task(bot_polling_loop(db_pool))
    """
    global _last_update_id
    logger.info("[봇] 명령어 수신 시작 (/status /signals /today /help)")

    _telegram_proxy = os.environ.get("TELEGRAM_PROXY", "")
    if _telegram_proxy:
        logger.info("[봇] TELEGRAM_PROXY 적용: %s", _telegram_proxy)

    async with (
        httpx.AsyncClient(proxy=_telegram_proxy) if _telegram_proxy
        else httpx.AsyncClient()
    ) as http:
        await _register_commands(http)
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
