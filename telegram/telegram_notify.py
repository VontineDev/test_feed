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

TYPE_BADGE = {
    "earnings":   "📊",
    "ma":         "🤝",
    "management": "👤",
    "analyst":    "🔍",
    "regulatory": "⚖️",
    "product":    "🚀",
    "macro":      "🌐",
    "other":      "📰",   # internal reference only — never rendered in messages
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
    parse_mode: Optional[str] = "MarkdownV2",
) -> bool:
    """단일 대상(chat_id 또는 channel_id)에 메시지 발송. 재시도 포함."""
    url = TELEGRAM_API.format(token=token, method="sendMessage")
    payload: dict = {
        "chat_id":    chat_id,
        "text":       text,
        "disable_web_page_preview": True,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
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


async def send_admin_alert(text: str, http: Optional[httpx.AsyncClient] = None) -> bool:
    """스케줄러 잡 실패 등 운영 알림을 관리자 개인 DM으로 전송.

    기사/신호 알림과 달리 채널 발송하지 않고 TELEGRAM_CHAT_ID로만 보낸다.
    TELEGRAM_TOKEN/CHAT_ID 미설정 시 조용히 False 반환 (알림도 실패해도
    잡 자체를 죽이지 않기 위함 — 호출부에서 반환값을 기다리지 않는다).
    """
    try:
        token   = _get_token()
        chat_id = _get_chat_id()
    except ValueError as e:
        logger.warning("[Telegram] 관리자 알림 설정 오류: %s", e)
        return False

    def esc(s: str) -> str:
        for ch in r"\_*[]()~`>#+-=|{}.!":
            s = s.replace(ch, f"\\{ch}")
        return s

    message = f"⚠️ *운영 알림*\n{esc(text)}"

    _own_client = http is None
    if _own_client:
        http = httpx.AsyncClient()
    try:
        return await _post_message(http, token, chat_id, message, label="관리자 알림")
    finally:
        if _own_client:
            await http.aclose()


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
) -> bool:
    """매매 신호 감지 시 별도 강조 메시지 전송."""
    try:
        token   = _get_token()
        chat_id = _get_chat_id()
    except ValueError as e:
        logger.warning("[Telegram] 설정 오류: %s", e)
        return False

    icon = {"BUY": "🟢", "SELL": "🔴", "WATCH": "🟡"}.get(signal.direction, "📊")
    bar  = "⬛" * signal.strength + "⬜" * (5 - signal.strength)

    def esc(text: str) -> str:
        for ch in r"\_*[]()~`>#+-=|{}.!":
            text = text.replace(ch, f"\\{ch}")
        return text

    # ticker_symbols: {LLM추출명 → yfinance 심볼} — 심볼이 있으면 한글명+코드 표시
    from core.ticker_cache import ticker_cache as _tc
    def _fmt_ticker(name: str) -> str:
        sym = getattr(signal, "ticker_symbols", {}).get(name, "")
        ko  = _tc.get_name(sym) if sym else ""
        code = sym.split(".")[0] if sym else ""
        if ko and ko != code:
            return f"{ko}\\(`{esc(code)}`\\)" if code else esc(ko)
        return f"`{esc(name)}`"
    tickers_str = " ".join(_fmt_ticker(t) for t in signal.tickers) if signal.tickers else "\\-"
    source  = SOURCE_LABEL.get(art["source"], art["source"].upper())

    article_type = getattr(signal, "article_type", "other") or "other"
    type_badge = TYPE_BADGE.get(article_type, "")
    badge_prefix = f"{type_badge} " if article_type != "other" and type_badge else ""

    confidence = getattr(signal, "confidence", "NORMAL")
    conf_line = "🔥 *HIGH CONFIDENCE \\- 스크리너 교차 종목*\n" if confidence == "HIGH" else ""

    lines = [
        f"{conf_line}{badge_prefix}{icon} *매매 신호 감지 \\- {esc(signal.direction)}*",
        f"강도: {bar} {signal.strength}/5",
        "",
        f"📰 {esc(art['title'][:70])}",
        "",
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


# ── 주봉 차트 스크리닝 결과 알림 ─────────────────────────────
async def send_weekly_screener(
    results: list,                          # list[ScreenResult]
    http: Optional[httpx.AsyncClient] = None,
    target_chat_id: Optional[str] = None,  # 명령어 응답 시 지정; 없으면 env TELEGRAM_CHAT_ID + 채널
) -> bool:
    """
    주봉 차트 스크리닝 결과를 Telegram으로 전송.
    통과 종목이 없어도 '이번 주 없음' 메시지 발송.
    target_chat_id가 지정되면 그 대화에만 발송하고 채널 브로드캐스트를 건너뜀.
    """
    try:
        token   = _get_token()
        chat_id = target_chat_id or _get_chat_id()
    except ValueError as e:
        logger.warning("[Telegram] 설정 오류: %s", e)
        return False

    def esc(text: str) -> str:
        for ch in r"\_*[]()~`>#+-=|{}.!":
            text = text.replace(ch, f"\\{ch}")
        return text

    def esc_code(text: str) -> str:
        """MarkdownV2 인라인 코드 내부 — 백틱과 백슬래시만 이스케이프."""
        return text.replace("\\", "\\\\").replace("`", "\\`")

    from analysis.chart_screener import current_week_of
    week = current_week_of()

    if not results:
        message = (
            f"📊 *강세 후보 발굴 \\({esc(week)}\\)*\n\n"
            f"이번 주 조건 통과 종목 없음"
        )
    else:
        from collections import defaultdict
        # 정배열(has_gapjum) 우선, 그 다음 종가 내림차순
        sorted_r = sorted(results, key=lambda r: (not r.has_gapjum, -r.close))

        # 섹터별 그룹핑
        by_sector: dict[str, list] = defaultdict(list)
        for r in sorted_r:
            short_sector = (r.sector.split(",")[0][:20].strip()) or "기타"
            by_sector[short_sector].append(r)

        # 종목 수 기준 상위 5개 섹터
        top_sectors = sorted(by_sector.items(), key=lambda x: -len(x[1]))[:5]

        from analysis.screener_filters import filter_summary
        f_sum = filter_summary(results)
        lines = [
            f"📊 *강세 후보 발굴 \\({esc(week)}\\)*",
            f"통과: {len(results)}개 \\(정배열 {sum(1 for r in results if r.has_gapjum)}개\\)",
            f"🏷 저평가 {f_sum.get('저평가_우량주', 0)}개 \\| 성장 {f_sum.get('성장주', 0)}개 \\| 배당 {f_sum.get('배당주', 0)}개\n",
        ]

        # KIND 데이터 없음 경고
        if results and not any(r.sector for r in results):
            lines.insert(1, "⚠️ *KIND 섹터 데이터 없음 \\— 전체 기타 그룹*")

        for sector_name, sector_stocks in top_sectors:
            # 섹터 내 정배열 우선, 종가 내림차순 top-3
            top3 = sorted(sector_stocks, key=lambda r: (not r.has_gapjum, -r.close))[:3]
            lines.append(f"*{esc(sector_name)}* \\({len(sector_stocks)}종목\\)")
            for r in top3:
                star = " ★" if r.has_gapjum else ""
                lines.append(
                    f"• {esc(r.name)}{esc(star)} `{esc_code(r.ticker.split('.')[0])}`  {r.close:,.0f}"
                )
            lines.append("")

        message = "\n".join(lines)

    # target_chat_id가 지정된 경우 채널 브로드캐스트 건너뜀 (봇 명령어 응답 전용)
    channel_id = "" if target_chat_id else _get_channel_id()

    _own_client = http is None
    if _own_client:
        http = httpx.AsyncClient()
    try:
        ok = await _post_message(http, token, chat_id, message, label="차트스크리닝(DM)")
        if channel_id:
            await _post_message(http, token, channel_id, message, label="차트스크리닝(채널)")
        return ok
    finally:
        if _own_client:
            await http.aclose()


async def send_screener_comparison(
    ichimoku_rows: list[dict],           # load_chart_signals_latest() 반환값 (D4)
    stage_results: dict[str, int],       # ticker → stage (1/2/3), 전 종목 분류 결과
    peakout_flags: set[str],             # Stage 3 피크아웃 ticker 집합
    http: Optional[httpx.AsyncClient] = None,
) -> bool:
    """
    독립 실행된 주봉 Ichimoku와 일봉 3단계 분류기 결과를 사후 교차해 Telegram 전송.

    두 시스템은 완전히 독립 — 전 종목 대상 실행 후 결과를 교차함.
    포맷: 겹치는 종목 우선(양쪽 통과) → Ichimoku만/Stage만 카운트 → 피크아웃 경고.
    """
    try:
        token   = _get_token()
        chat_id = _get_chat_id()
    except ValueError as e:
        logger.warning("[Telegram] 설정 오류: %s", e)
        return False

    def esc(text: str) -> str:
        for ch in r"\_*[]()~`>#+-=|{}.!":
            text = text.replace(ch, f"\\{ch}")
        return text

    def esc_code(text: str) -> str:
        return text.replace("\\", "\\\\").replace("`", "\\`")

    from datetime import date as _date
    today_str = esc(_date.today().strftime("%Y-%m-%d"))

    ichimoku_tickers = {r["ticker"] for r in ichimoku_rows}
    # 종목 이름/정배열 조회용 맵
    ichimoku_info: dict[str, dict] = {r["ticker"]: r for r in ichimoku_rows}

    overlap = ichimoku_tickers & set(stage_results.keys())
    ichimoku_only = ichimoku_tickers - set(stage_results.keys())
    stage_only    = set(stage_results.keys()) - ichimoku_tickers

    lines: list[str] = [
        f"📊 *스크리너 교차 확인 \\({today_str}\\)*",
        "",
    ]

    if overlap:
        # Stage 내림차순(3→1), 정배열 우선 정렬
        def _sort_key(t: str) -> tuple:
            info  = ichimoku_info.get(t, {})
            stage = stage_results.get(t, 0)
            gapjum = bool(info.get("has_gapjum", False))
            return (-stage, not gapjum)

        sorted_overlap = sorted(overlap, key=_sort_key)[:10]  # 최대 10개 표시
        lines.append(f"✅ *양쪽 통과 \\({esc(str(len(overlap)))}종목\\) — 최우선 감시*")
        for ticker in sorted_overlap:
            info    = ichimoku_info.get(ticker, {})
            name    = esc(str(info.get("name", ticker.split(".")[0])))
            code    = esc_code(ticker.split(".")[0])
            stage   = stage_results.get(ticker, 0)
            star    = " ★정배열" if info.get("has_gapjum") else ""
            lines.append(f"• {name} `{code}` Stage{stage}{esc(star)}")
        if len(overlap) > 10:
            lines.append(f"  _\\(외 {esc(str(len(overlap) - 10))}종목\\)_")
        lines.append("")
    else:
        lines.append("✅ 양쪽 공통 통과 종목 없음")
        lines.append("")

    lines.append(
        f"📌 Ichimoku만 통과: {esc(str(len(ichimoku_only)))}종목"
    )
    lines.append(
        f"📌 Stage만 해당: {esc(str(len(stage_only)))}종목"
    )

    # 피크아웃 경고
    if peakout_flags:
        lines.append("")
        lines.append("⚠️ *Stage3 피크아웃 주의*")
        for ticker in list(peakout_flags)[:5]:
            info = ichimoku_info.get(ticker, {})
            name = esc(str(info.get("name", ticker.split(".")[0])))
            code = esc_code(ticker.split(".")[0])
            lines.append(f"• {name} `{code}`")

    message = "\n".join(lines)

    channel_id = _get_channel_id()
    _own_client = http is None
    if _own_client:
        http = httpx.AsyncClient()
    try:
        ok = await _post_message(http, token, chat_id, message, label="교차비교(DM)")
        if channel_id:
            await _post_message(http, token, channel_id, message, label="교차비교(채널)")
        return ok
    finally:
        if _own_client:
            await http.aclose()


async def send_watchlist_brief(
    entries: list[dict],
    http: Optional[httpx.AsyncClient] = None,
    target_chat_id: Optional[str] = None,
) -> bool:
    """
    거래대금 워치리스트 일보 전송 (plain text).
    entries: list of {ticker, days_since, vol_ratio, vol_ratio_delta,
                      retiring, f_streak, i_streak, ichimoku_ok, current_stage}
    target_chat_id: 지정 시 해당 채팅에 전송 (미지정 시 TELEGRAM_CHAT_ID)
    """
    try:
        token   = _get_token()
        chat_id = target_chat_id or _get_chat_id()
    except ValueError as e:
        logger.warning("[Telegram] 설정 오류: %s", e)
        return False

    from datetime import date as _date
    today_str = _date.today().strftime("%Y-%m-%d")

    from core.ticker_cache import ticker_cache as _tc

    if not entries:
        message = f"📊 거래대금 워치리스트 ({today_str}, 장 마감)\n\n워치리스트 없음"
    else:
        lines = [f"📊 거래대금 워치리스트 ({today_str}, 장 마감)", ""]
        for e in entries:
            ticker  = e["ticker"]
            code    = ticker.split(".")[0]
            ko_name = _tc.get_name(ticker)
            display = f"{ko_name}({code})" if ko_name != code else code
            stage   = e.get("current_stage") or "?"
            days_d  = e.get("days_since", 0)

            # D+10 이상: 마지막 추적일 표시
            retiring    = e.get("retiring", False)
            retire_mark = " [마지막 추적일]" if retiring else ""

            vol_ratio = e.get("vol_ratio")
            if vol_ratio is None:
                vol_line = "  거래대금 비율 N/A"
            elif vol_ratio >= 1.0:
                vol_line = f"  거래대금 비율 +{(vol_ratio - 1) * 100:.0f}% ✅"
            elif vol_ratio >= 0.6:
                vol_line = f"  거래대금 비율 {(vol_ratio - 1) * 100:.0f}% ⚠️"
            else:
                vol_line = f"  거래대금 비율 {(vol_ratio - 1) * 100:.0f}% 소멸 ❌"

            # 전일 대비 변화 표시
            delta = e.get("vol_ratio_delta")
            if delta is not None:
                arrow     = "▲" if delta >= 0 else "▼"
                delta_str = f"  전일 대비 {delta * 100:+.0f}% {arrow}"
            else:
                delta_str = None

            f_streak = e.get("f_streak")
            i_streak = e.get("i_streak")

            if f_streak is None:
                f_part = "❓ 외국인 N/A"
            else:
                f_icon = "🔵" if f_streak > 0 else "🔴"
                f_part = f"{f_icon} 외국인 {f_streak:+d}일"

            if i_streak is None:
                i_part = "❓ 기관 N/A"
            else:
                i_icon = "🔵" if i_streak > 0 else "🔴"
                i_part = f"{i_icon} 기관 {i_streak:+d}일"

            ichimoku_ok = e.get("ichimoku_ok")
            if ichimoku_ok is True:
                ichi_line = "  ☁️ 일목: 구름 상단 ✅"
            elif ichimoku_ok is False:
                ichi_line = "  ☁️ 일목: 통과 ❌"
            else:
                ichi_line = "  ☁️ 일목: N/A"

            lines.append(f"{display} — Stage {stage}, D+{days_d}{retire_mark}")
            lines.append(vol_line)
            if delta_str:
                lines.append(delta_str)
            lines.append(f"  {f_part} / {i_part}")
            lines.append(ichi_line)
            lines.append("")

        message = "\n".join(lines)

    _own_client = http is None
    if _own_client:
        http = httpx.AsyncClient()
    try:
        return await _post_message(
            http, token, chat_id, message,
            label="워치리스트", parse_mode=None,
        )
    finally:
        if _own_client:
            await http.aclose()
