"""
telegram_notify.py  —  Telegram 알림 모듈
────────────────────────────────────────────────────────────
신규 기사 수집 시 Telegram 봇으로 즉시 알림 전송.

환경변수:
    TELEGRAM_TOKEN   : BotFather에서 발급받은 토큰
    TELEGRAM_CHAT_ID : 메시지를 받을 Chat ID
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

import httpx

_SEND_MAX_RETRIES = 3       # 최대 재시도 횟수
_SEND_RETRY_BASE  = 2.0     # 초기 대기 시간(초) — 지수 백오프

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"

# 카테고리별 이모지
CATEGORY_EMOJI = {
    "markets": "📈",
    "macro":   "🏦",
    "korea":   "🇰🇷",
}

SOURCE_LABEL = {
    "reuters":     "Reuters",
    "investing":   "Investing",
    "cnbc":        "CNBC",
    "yahoo":       "Yahoo Finance",
    "marketwatch": "MarketWatch",
    "bloomberg":   "Bloomberg",
}


def _get_token() -> str:
    token = os.environ.get("TELEGRAM_TOKEN", "")
    if not token:
        raise ValueError("환경변수 TELEGRAM_TOKEN 이 설정되지 않았습니다.")
    return token

def _get_chat_id() -> str:
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not chat_id:
        raise ValueError("환경변수 TELEGRAM_CHAT_ID 가 설정되지 않았습니다.")
    return chat_id

def _get_channel_id() -> str:
    """TELEGRAM_CHANNEL_ID 환경변수. 미설정 시 빈 문자열 (채널 발송 건너뜀)."""
    return os.environ.get("TELEGRAM_CHANNEL_ID", "").strip()


def _build_message(art: dict, summary_ko: str) -> str:
    """기사 정보 → Telegram 메시지 문자열 생성 (MarkdownV2)"""
    emoji    = CATEGORY_EMOJI.get(art["category"], "📰")
    source   = SOURCE_LABEL.get(art["source"], art["source"].upper())
    category = art["category"].upper()
    title    = art["title"]
    pub      = art["published"]
    url      = art["url"]

    # MarkdownV2 특수문자 이스케이프
    def esc(text: str) -> str:
        for ch in r"\_*[]()~`>#+-=|{}.!":
            text = text.replace(ch, f"\\{ch}")
        return text

    lines = [
        f"{emoji} *\\[{esc(source)}\\/{esc(category)}\\]*",
        f"*{esc(title)}*",
    ]
    if summary_ko:
        lines.append(f"\n{esc(summary_ko)}")
    lines.append(f"\n🕐 {esc(pub)}  \\|  [원문 보기]({url})")

    return "\n".join(lines)


async def _post_message(
    http: httpx.AsyncClient,
    token: str,
    chat_id: str,
    text: str,
    label: str = "",
) -> bool:
    """단일 대상(chat_id 또는 channel_id)에 메시지 발송. 재시도 포함."""
    url = TELEGRAM_API.format(token=token, method="sendMessage")
    payload = {
        "chat_id":    chat_id,
        "text":       text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }
    for attempt in range(1, _SEND_MAX_RETRIES + 1):
        try:
            resp = await http.post(url, json=payload, timeout=10)
            data = resp.json()
            if data.get("ok"):
                logger.debug("[Telegram] %s 전송 완료 → %s", label, chat_id)
                return True
            if resp.status_code == 429:
                retry_after = data.get("parameters", {}).get("retry_after", _SEND_RETRY_BASE * attempt)
                logger.warning("[Telegram] 429 Rate Limit — %s초 후 재시도 (%d/%d)", retry_after, attempt, _SEND_MAX_RETRIES)
                await asyncio.sleep(retry_after)
                continue
            logger.warning("[Telegram] %s 전송 실패 → %s: %s", label, chat_id, data.get("description", ""))
            return False
        except Exception as e:
            if attempt < _SEND_MAX_RETRIES:
                delay = _SEND_RETRY_BASE * (2 ** (attempt - 1))
                logger.warning("[Telegram] %s 요청 오류 (%d/%d) — %.0f초 후 재시도: %s", label, attempt, _SEND_MAX_RETRIES, delay, e)
                await asyncio.sleep(delay)
            else:
                logger.warning("[Telegram] %s 요청 오류 (최종 실패): %s", label, e)
                return False
    return False


async def send_article(
    art: dict,
    summary_ko: str,
    http: Optional[httpx.AsyncClient] = None,
) -> bool:
    """
    단일 기사를 Telegram으로 전송.
    개인 DM + 채널(설정 시) 모두 발송. DM 성공 여부를 반환.
    """
    try:
        token   = _get_token()
        chat_id = _get_chat_id()
    except ValueError as e:
        logger.warning("[Telegram] 설정 오류: %s", e)
        return False

    message    = _build_message(art, summary_ko)
    channel_id = _get_channel_id()

    _own_client = http is None
    if _own_client:
        http = httpx.AsyncClient()

    try:
        # 채널 설정 시 채널로만 발송, 미설정 시 개인 DM으로 발송
        if channel_id:
            ok = await _post_message(http, token, channel_id, message, label="기사(채널)")
        else:
            ok = await _post_message(http, token, chat_id, message, label="기사")
        return ok
    finally:
        if _own_client:
            await http.aclose()


# ── 단독 테스트 ───────────────────────────────────────────────
if __name__ == "__main__":
    import asyncio, sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    TEST_ART = {
        "source":   "cnbc",
        "category": "macro",
        "title":    "Federal Reserve holds rates steady, signals two cuts in 2026",
        "url":      "https://www.cnbc.com/test",
        "published": "03-21 14:00",
    }
    TEST_SUMMARY = "연준이 금리를 동결하고 2026년 두 차례 인하를 시사했습니다. 파월 의장은 인플레이션이 목표치에 근접하고 있다고 밝혔습니다."

    async def test():
        print("Telegram 알림 테스트 전송 중...")
        ok = await send_article(TEST_ART, TEST_SUMMARY)
        print("✓ 전송 성공" if ok else "✗ 전송 실패 — 로그 확인")

    asyncio.run(test())


# ── 매매 신호 전용 알림 ───────────────────────────────────────
async def send_signal(
    art: dict,
    summary_ko: str,
    signal,                          # TradeSignal
    http=None,
    cross=None,                      # CrossAnalysis (optional)
) -> bool:
    """
    매매 신호 감지 시 별도 강조 메시지 전송.
    cross 교차 분석 결과가 있으면 시세 컨텍스트도 포함.
    """
    try:
        token   = _get_token()
        chat_id = _get_chat_id()
    except ValueError as e:
        logger.warning("[Telegram] 설정 오류: %s", e)
        return False

    icon = {"BUY": "🟢", "SELL": "🔴", "WATCH": "🟡"}.get(signal.direction, "📊")
    bar  = "⬛" * signal.strength + "⬜" * (5 - signal.strength)

    def esc(text: str) -> str:
        for ch in r"\_*[]()~>#+-=|{}.!":
            text = text.replace(ch, f"\\{ch}")
        return text

    tickers_str = " ".join(f"`{t}`" for t in signal.tickers) if signal.tickers else "\\-"
    source  = SOURCE_LABEL.get(art["source"], art["source"].upper())

    # 교차 분석 점수 표시
    score_line = ""
    price_lines = []
    if cross:
        verdict_icon = {
            "CONFIRM": "✅", "CAUTION": "⚠️",
            "FILTER": "🚫", "NEUTRAL": "➖",
        }.get(cross.verdict, "")
        score_line = f"📊 교차분석: {verdict_icon} {esc(cross.verdict)} {cross.score}/10"
        for ctx in cross.price_contexts[:3]:
            sign = "▲" if ctx.change_pct >= 0 else "▼"
            rsi_str = f" RSI\\:{esc(str(ctx.rsi))}" if ctx.rsi else ""
            price_lines.append(
                f"  {sign} {esc(ctx.ticker)} {esc(str(abs(ctx.change_pct)))}%{rsi_str}"
            )

    lines = [
        f"{icon} *매매 신호 감지 \\- {esc(signal.direction)}*",
        f"강도: {bar} {signal.strength}/5",
    ]
    if score_line:
        lines.append(score_line)
    lines += ["", f"📰 {esc(art['title'][:70])}", ""]
    if price_lines:
        lines += ["💹 시세:"] + price_lines + [""]
    lines += [
        f"💬 {esc(signal.reason)}",
        f"🏷 종목: {tickers_str}",
        f"📡 출처: {esc(source)} \\| {esc(art['published'])}",
        f"[원문 보기]({art['url']})",
    ]

    message    = "\n".join(lines)
    channel_id = _get_channel_id()

    _own_client = http is None
    if _own_client:
        http = httpx.AsyncClient()
    try:
        # 채널 설정 시 채널로만 발송, 미설정 시 개인 DM으로 발송
        if channel_id:
            ok = await _post_message(http, token, channel_id, message, label="신호(채널)")
        else:
            ok = await _post_message(http, token, chat_id, message, label="신호")
        return ok
    finally:
        if _own_client:
            await http.aclose()
