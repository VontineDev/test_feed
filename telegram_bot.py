"""
telegram_bot.py  —  Telegram 봇 명령어 처리 모듈
────────────────────────────────────────────────────────────
Long polling 방식으로 명령어를 수신하고 DB 조회 결과를 응답.

지원 명령어:
    /status   — 크롤러 현재 상태 (수집 건수, 마지막 수집 시각 등)
    /signals  — 최근 매매 신호 10건 (BUY/SELL/WATCH)
    /today    — 오늘 수집된 기사 요약 (카테고리별 건수 + 최신 5건)
    /backtest  — 통합 백테스트 (ichimoku / stage / cross 모드)
    /screener  — 최신 주봉 차트 스크리닝 결과 (DB 조회, 명령어 발신자에게만 전송)
    /scan      — 주봉 스크리닝 즉시 실행 (전 종목 실시간 스캔, 결과 저장 후 발신자에게 전송)
    /buy       — 진입 기록 (거래 저널)
    /sell      — 청산 기록 (FIFO)
    /port      — 보유 현황 + 미실현 P&L
    /pnl       — 실현 P&L 요약
    /help      — 명령어 목록
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
# /scan 중복 실행 방지 락
_scan_lock: asyncio.Lock = asyncio.Lock()


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


# /backtest 중복 실행 방지 락
_backtest_lock: asyncio.Lock = asyncio.Lock()


async def _handle_backtest(
    http: httpx.AsyncClient, chat_id: str, args: list[str]
) -> None:
    """/backtest <mode> <start> <end> [market] [--max N] [--tx-cost F]

    예:
      /backtest ichimoku 2025-01-01 2026-01-01
      /backtest stage    2025-01-01 2026-01-01 KOSDAQ
      /backtest cross    2025-01-01 2026-01-01 ALL --max 50
    """
    if _backtest_lock.locked():
        await _send_plain(http, chat_id,
            "⏳ 백테스트가 이미 실행 중입니다. 완료 후 결과가 전송됩니다.")
        return

    # ── 인수 파싱 ──────────────────────────────────────────────
    USAGE = (
        "사용법: /backtest <mode> <start> <end> [market] [--max N] [--tx-cost F]\n"
        "              [--tp1 F] [--tp1-ratio F] [--trail F] [--stop F]\n"
        "  mode        : ichimoku | stage | stage2 | cross\n"
        "  start/end   : YYYY-MM-DD\n"
        "  market      : KOSPI | KOSDAQ | ALL (기본: ALL)\n"
        "  --max N     : 최대 티커 수 (기본 200)\n"
        "  --tx-cost F : 왕복 거래비용 (기본 ~0.0021)\n"
        "  --tp1 F     : 1차 익절 목표 (예: 0.25 = +25%)\n"
        "  --tp1-ratio F: 1차 익절 비율 (기본 0.5)\n"
        "  --trail F   : 트레일링 스탑 (고점 대비, 예: 0.10)\n"
        "  --stop F    : 하드 손절 (기본 0.08)\n\n"
        "권장 파라미터:\n"
        "  Stage/KOSPI : --tp1 0.25 --trail 0.10 --stop 0.10\n"
        "  Stage/KOSDAQ: --tp1 0.25 --trail 0.15 --stop 0.10\n"
        "  Cross       : --tp1 0.15 --trail 0.10 --stop 0.10\n\n"
        "예) /backtest stage 2024-01-01 2026-01-01 KOSPI --tp1 0.25 --trail 0.10 --stop 0.10\n"
        "    /backtest cross 2024-01-01 2026-01-01 ALL --tp1 0.15 --trail 0.10 --stop 0.10"
    )

    if len(args) < 3:
        await _send_plain(http, chat_id, USAGE)
        return

    mode  = args[0].lower()
    start_str = args[1]
    end_str   = args[2]

    if mode not in ("ichimoku", "stage", "stage2", "cross"):
        await _send_plain(http, chat_id, f"mode는 ichimoku|stage|stage2|cross 중 하나여야 합니다.\n\n{USAGE}")
        return

    from datetime import date as _date
    try:
        start = _date.fromisoformat(start_str)
        end   = _date.fromisoformat(end_str)
    except ValueError:
        await _send_plain(http, chat_id, f"날짜 형식 오류 (YYYY-MM-DD): {start_str}, {end_str}")
        return

    if start >= end:
        await _send_plain(http, chat_id, "start는 end보다 이전이어야 합니다.")
        return

    if (end - start).days > 10 * 365:
        await _send_plain(http, chat_id, "백테스트 기간은 최대 10년입니다.")
        return

    # 선택적 인수 파싱
    remaining   = args[3:]
    market      = "ALL"
    max_tickers = 200
    tx_cost     = None
    tp1_pct     = 0.0
    tp1_ratio   = 0.5
    trail_pct   = 0.0
    hard_stop   = 0.08

    i = 0
    while i < len(remaining):
        tok = remaining[i]
        if tok.upper() in ("KOSPI", "KOSDAQ", "ALL"):
            market = tok.upper()
        elif tok == "--max" and i + 1 < len(remaining):
            try:
                max_tickers = int(remaining[i + 1]); i += 1
            except ValueError:
                pass
        elif tok == "--tx-cost" and i + 1 < len(remaining):
            try:
                v = float(remaining[i + 1])
                tx_cost = max(0.0, min(v, 0.10)); i += 1
            except ValueError:
                pass
        elif tok == "--tp1" and i + 1 < len(remaining):
            try:
                tp1_pct = max(0.0, min(float(remaining[i + 1]), 1.0)); i += 1
            except ValueError:
                pass
        elif tok == "--tp1-ratio" and i + 1 < len(remaining):
            try:
                tp1_ratio = max(0.0, min(float(remaining[i + 1]), 1.0)); i += 1
            except ValueError:
                pass
        elif tok == "--trail" and i + 1 < len(remaining):
            try:
                trail_pct = max(0.0, min(float(remaining[i + 1]), 1.0)); i += 1
            except ValueError:
                pass
        elif tok == "--stop" and i + 1 < len(remaining):
            try:
                hard_stop = max(0.01, min(float(remaining[i + 1]), 0.5)); i += 1
            except ValueError:
                pass
        i += 1

    from backtest_engine import BacktestConfig, TX_COST_DEFAULT, MODE_KOR, run_backtest

    try:
        from db import get_dsn as _get_dsn
        _dsn: str | None = _get_dsn()
    except Exception:
        _dsn = None

    config = BacktestConfig(
        mode=mode,
        start=start,
        end=end,
        market=market,
        tx_cost_rt=tx_cost if tx_cost is not None else TX_COST_DEFAULT,
        max_tickers=max_tickers,
        dsn=_dsn,
        tp1_pct=tp1_pct,
        tp1_ratio=tp1_ratio,
        trail_pct=trail_pct,
        hard_stop_pct=hard_stop,
    )

    mode_kor   = MODE_KOR.get(mode, mode)
    eta_min    = max(2, max_tickers // 50) if max_tickers > 0 else 20
    exit_label = (
        f"tp1={tp1_pct*100:.0f}% trail={trail_pct*100:.0f}% stop={hard_stop*100:.0f}%"
        if tp1_pct > 0 or trail_pct > 0
        else f"MA20/손절 {hard_stop*100:.0f}%"
    )
    await _send_plain(http, chat_id,
        f"📊 백테스트 시작 — {mode_kor}\n"
        f"📅 {start} ~ {end}  {market}  티커 최대 {max_tickers or '전종목'}개\n"
        f"📐 청산: {exit_label}\n"
        f"⏳ 예상 소요: {eta_min}~{eta_min * 3}분 (데이터 다운로드 포함)\n"
        "완료되면 결과를 전송합니다."
    )

    async with _backtest_lock:
        try:
            loop   = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, run_backtest, config)

            # HTML 리포트 저장
            from datetime import datetime as _dt
            from zoneinfo import ZoneInfo as _ZI
            _ts = _dt.now(_ZI("Asia/Seoul")).strftime("%Y%m%d_%H%M%S")
            _html_path = f"reports/backtest/backtest_{mode}_{_ts}.html"
            result.to_html_report(_html_path)

            report = result.to_telegram_report()
            top_bottom = result.top_bottom_telegram_text(n=5)
            if top_bottom:
                report = report + top_bottom
            report += f"\n\n📁 {_html_path}"

            # Telegram 4096자 제한 처리
            if len(report) > 4090:
                report = report[:4087] + "..."
            await _send_plain(http, chat_id, report)
        except Exception as e:
            logger.warning("[봇/backtest] 실행 실패: %s", e)
            await _send_plain(http, chat_id, f"백테스트 실행 중 오류: {e}")


async def _handle_top(http: httpx.AsyncClient, chat_id: str, args: list[str]) -> None:
    """/top — 당일 거래금액 상위 10개 종목 (KOSPI+KOSDAQ 합산)"""
    import pandas as _pd
    import FinanceDataReader as _fdr
    from datetime import date as _date

    await _send_plain(http, chat_id, "📊 당일 거래금액 상위 10 조회 중...")

    def _fetch() -> str:
        kospi  = _fdr.StockListing("KOSPI")
        kosdaq = _fdr.StockListing("KOSDAQ")
        df = _pd.concat([kospi, kosdaq], ignore_index=True)
        df["Amount"]      = _pd.to_numeric(df["Amount"],      errors="coerce").fillna(0)
        df["Close"]       = _pd.to_numeric(df["Close"],       errors="coerce").fillna(0)
        df["ChagesRatio"] = _pd.to_numeric(df["ChagesRatio"], errors="coerce").fillna(0)

        top = df.nlargest(10, "Amount").reset_index(drop=True)

        def _fmt_amount(v: float) -> str:
            if v >= 1e12:
                return f"{v/1e12:.1f}조"
            if v >= 1e8:
                return f"{v/1e8:.0f}억"
            return f"{v:,.0f}원"

        def _fmt_pct(v: float) -> str:
            return f"+{v:.2f}%" if v > 0 else f"{v:.2f}%"

        today = _date.today().strftime("%m/%d")
        lines = [f"📊 거래금액 상위 10  ({today}  KOSPI+KOSDAQ)\n"]
        for i, row in top.iterrows():
            pct = _fmt_pct(row["ChagesRatio"])
            amt = _fmt_amount(row["Amount"])
            price = f"{int(row['Close']):,}"
            lines.append(
                f"{i+1:2d}. {row['Name']:<10}  {price:>10}  {pct:>8}  {amt}"
            )
        return "\n".join(lines)

    try:
        loop = asyncio.get_running_loop()
        msg = await loop.run_in_executor(None, _fetch)
        await _send_plain(http, chat_id, msg)
    except Exception as e:
        logger.warning("[봇] /top 오류: %s", e)
        await _send_plain(http, chat_id, f"오류: {e}")


async def _handle_screener(http: httpx.AsyncClient, chat_id: str, pool) -> None:
    """/screener — 최신 주봉 차트 스크리닝 결과 (DB 조회 후 DM + 채널 동시 발송)"""
    if not pool:
        await _send(http, chat_id, "DB 미연결 상태입니다\\.")
        return

    from db import load_chart_signals_latest
    from chart_screener import ScreenResult

    week, rows = await load_chart_signals_latest(pool)
    if not rows:
        await _send(http, chat_id,
            "스크리닝 결과가 없습니다\\.\n매주 일요일 20:30에 업데이트됩니다\\.")
        return

    results = [
        ScreenResult(
            ticker=r["ticker"],
            name=r["name"] or r["ticker"],
            close=r["close"],
            ma_20w=r["ma_20w"],
            ma_60w=r["ma_60w"],
            cloud_top=r["cloud_top"],
            is_enhanced=r["is_enhanced"],
            has_gapjum=r["has_gapjum"],
            screened_at=r["screened_at"].isoformat() if r["screened_at"] else "",
            week_of=r["week_of"],
            sector=r.get("sector") or "",
            ma_120w=r.get("ma_120w"),
        )
        for r in rows
    ]

    from telegram_notify import send_weekly_screener
    await send_weekly_screener(results, http=http, target_chat_id=chat_id)


async def _handle_scan(http: httpx.AsyncClient, chat_id: str, pool) -> None:
    """/scan — 주봉 스크리닝 즉시 실행 (전 종목 실시간 스캔, 결과 저장 후 발신자에게 전송)"""
    if not pool:
        await _send(http, chat_id, "DB 미연결 상태입니다\\.")
        return

    if _scan_lock.locked():
        await _send(http, chat_id, "⏳ 스크리닝이 이미 실행 중입니다\\. 완료 후 결과가 전송됩니다\\.")
        return

    async with _scan_lock:
        await _send(http, chat_id,
            "🔍 주봉 스크리닝 시작\\.\\.\\.\n전 종목 실시간 스캔 중 \\(약 10\\~20분 소요\\)\\.")
        try:
            from chart_screener import run_weekly_screen
            from db import save_chart_signals
            loop = asyncio.get_running_loop()
            results = await loop.run_in_executor(None, run_weekly_screen)
            saved = await save_chart_signals(pool, results)
            logger.info("[봇/scan] 완료 — 통과:%d 저장:%d", len(results), saved)
            from telegram_notify import send_weekly_screener
            await send_weekly_screener(results, http=http, target_chat_id=chat_id)
        except Exception as e:
            logger.warning("[봇/scan] 실행 실패: %s", e)
            await _send(http, chat_id, f"스크리닝 실행 중 오류가 발생했습니다\\: {esc(str(e))}")


async def _handle_paper(http: httpx.AsyncClient, chat_id: str, pool) -> None:
    """/paper — 모의투자 오픈 포지션 현황"""
    if not pool:
        await _send(http, chat_id, "DB 미연결 상태입니다\\.")
        return

    rows = await pool.fetch(
        """
        SELECT model, ticker, signal_date, entry_theory, entry_actual,
               slippage_pct, qty, tp1_date, watermark, status, created_at
        FROM paper_positions
        WHERE status IN ('pending','open')
        ORDER BY model, signal_date
        """
    )
    if not rows:
        await _send(http, chat_id, "📭 현재 오픈 포지션이 없습니다\\.")
        return

    from datetime import date as _date
    import asyncio as _asyncio

    today = _date.today()

    # 현재가 일괄 조회 (KiwoomPaperTrader 또는 yfinance fallback)
    tickers = list({r["ticker"] for r in rows if r["status"] == "open"})
    price_map: dict[str, int] = {}

    try:
        from kiwoom_paper_trader import KiwoomPaperTrader
        _trader = KiwoomPaperTrader()
        loop = _asyncio.get_running_loop()
        for _t in tickers:
            _p = await loop.run_in_executor(None, _trader.get_current_price, _t)
            if _p:
                price_map[_t] = _p
    except Exception:
        try:
            import yfinance as yf
            loop = _asyncio.get_running_loop()
            def _fetch_prices():
                _map = {}
                for _t in tickers:
                    try:
                        _df = yf.Ticker(_t).history(period="2d", interval="1d")
                        if not _df.empty:
                            _map[_t] = int(_df["Close"].iloc[-1])
                    except Exception:
                        pass
                return _map
            price_map = await loop.run_in_executor(None, _fetch_prices)
        except Exception:
            pass

    # 모델별 그룹
    from collections import defaultdict as _dd
    by_model: dict[str, list] = _dd(list)
    for r in rows:
        by_model[r["model"]].append(r)

    MODEL_ICON = {"stage": "🔵", "kosdaq": "🟣", "cross": "⭐", "ichimoku": "🌊"}
    lines = ["📊 *모의투자 포지션 현황*", ""]

    for model, pos_list in sorted(by_model.items()):
        icon = MODEL_ICON.get(model, "•")
        lines.append(f"{icon} *{esc(model.upper())}* \\({len(pos_list)}건\\)")
        for r in pos_list:
            ticker_code = r["ticker"].split(".")[0]
            days = (today - r["signal_date"]).days
            entry = r["entry_actual"] or r["entry_theory"] or 0
            cur   = price_map.get(r["ticker"])
            status_tag = "⏳" if r["status"] == "pending" else ""

            if cur and entry:
                ret_pct = (cur - entry) / entry * 100
                ret_str = f"{ret_pct:+.1f}%"
                cur_str = f"{cur:,}"
            else:
                ret_str = "\\-"
                cur_str = "\\-"

            tp1_tag = " ✅TP1" if r["tp1_date"] else ""
            lines.append(
                f"  {status_tag}`{esc(ticker_code)}` D\\+{days} "
                f"진입={int(entry):,} 현재={cur_str} *{esc(ret_str)}*{esc(tp1_tag)}"
            )
        lines.append("")

    await _send(http, chat_id, "\n".join(lines))


async def _handle_paper_perf(http: httpx.AsyncClient, chat_id: str, pool) -> None:
    """/paper_perf — 모의투자 누적 성과 (이론 vs 실전)"""
    if not pool:
        await _send(http, chat_id, "DB 미연결 상태입니다\\.")
        return

    # 모델별 집계
    model_stats = await pool.fetch(
        """
        SELECT
            model,
            COUNT(*) FILTER (WHERE status='closed')                          AS closed,
            COUNT(*) FILTER (WHERE status='open')                            AS open_cnt,
            COUNT(*) FILTER (WHERE status='pending')                         AS pending_cnt,
            AVG(blended_return) FILTER (WHERE status='closed')               AS avg_ret,
            AVG(CASE WHEN blended_return > 0 AND status='closed' THEN 1.0
                     WHEN status='closed' THEN 0.0 END)                      AS win_rate,
            AVG(slippage_pct) FILTER (WHERE status IN ('open','closed'))     AS avg_slip,
            COUNT(*) FILTER (WHERE tp1_date IS NOT NULL)                     AS tp1_hits
        FROM paper_positions
        GROUP BY model
        ORDER BY model
        """
    )

    if not model_stats or all(r["closed"] == 0 for r in model_stats):
        # 전체 건수만 표시
        total = await pool.fetchval("SELECT COUNT(*) FROM paper_positions")
        pending = await pool.fetchval("SELECT COUNT(*) FROM paper_positions WHERE status='pending'")
        open_cnt = await pool.fetchval("SELECT COUNT(*) FROM paper_positions WHERE status='open'")
        await _send(
            http, chat_id,
            f"📈 *모의투자 성과*\n\n"
            f"총 {total}건 \\| pending {pending} \\| open {open_cnt}\n"
            f"아직 청산된 포지션이 없습니다\\."
        )
        return

    # 전체 합계
    total_closed = sum(r["closed"] for r in model_stats)
    total_open   = sum(r["open_cnt"] for r in model_stats)
    total_pend   = sum(r["pending_cnt"] for r in model_stats)

    MODEL_ICON = {"stage": "🔵", "kosdaq": "🟣", "cross": "⭐", "ichimoku": "🌊"}

    lines = [
        "📈 *모의투자 누적 성과*",
        f"청산 {total_closed}건 \\| 보유 {total_open}건 \\| 대기 {total_pend}건",
        "",
        "```",
        f"{'모델':<10} {'청산':>4} {'승률':>6} {'평균수익':>8} {'슬리피지':>8}",
        "─" * 42,
    ]

    for r in model_stats:
        if r["closed"] == 0:
            continue
        model     = r["model"]
        win_rate  = (r["win_rate"] or 0) * 100
        avg_ret   = (r["avg_ret"]  or 0) * 100
        avg_slip  = (r["avg_slip"] or 0) * 100
        lines.append(
            f"{model:<10} {r['closed']:>4} {win_rate:>5.1f}% {avg_ret:>+7.2f}% {avg_slip:>+7.2f}%"
        )

    lines.append("─" * 42)

    # 전체 평균 (청산된 것만)
    all_closed = await pool.fetch(
        "SELECT blended_return, slippage_pct FROM paper_positions WHERE status='closed'"
    )
    if all_closed:
        _rets  = [r["blended_return"] for r in all_closed if r["blended_return"] is not None]
        _slips = [r["slippage_pct"]   for r in all_closed if r["slippage_pct"] is not None]
        _wins  = sum(1 for x in _rets if x > 0)
        avg_r  = (sum(_rets) / len(_rets)   * 100) if _rets  else 0
        avg_s  = (sum(_slips) / len(_slips) * 100) if _slips else 0
        wr_all = (_wins / len(_rets) * 100) if _rets else 0
        lines.append(
            f"{'전체':<10} {len(_rets):>4} {wr_all:>5.1f}% {avg_r:>+7.2f}% {avg_s:>+7.2f}%"
        )

    lines += ["```", ""]

    # 슬리피지 해석
    if all_closed and _slips:
        avg_slip_pct = avg_s
        if abs(avg_slip_pct) <= 0.5:
            slip_comment = "슬리피지 정상 범위 \\(\\-0\\.5%\\~\\+0\\.5%\\)"
        elif avg_slip_pct > 0.5:
            slip_comment = "⚠️ 슬리피지 과다 — 진입 불리"
        else:
            slip_comment = "✅ 슬리피지 유리 — 갭업 진입"
        lines.append(slip_comment)

    await _send(http, chat_id, "\n".join(lines))


async def _handle_paper_exit(http: httpx.AsyncClient, chat_id: str, pool, args: list[str]) -> None:
    """/paper_exit <ticker> — 특정 종목 수동 강제 청산"""
    if not pool:
        await _send(http, chat_id, "DB 미연결 상태입니다\\.")
        return
    if not args:
        await _send(http, chat_id, "사용법: /paper\\_exit \\<종목코드\\>\n예\\) /paper\\_exit 005930")
        return

    raw = args[0].upper().replace("-", ".")
    # 6자리 숫자면 .KS/.KQ 자동 추론 (DB에서 확인)
    row = await pool.fetchrow(
        "SELECT id, ticker, qty, entry_actual, model FROM paper_positions "
        "WHERE status='open' AND (ticker=$1 OR ticker LIKE $2 OR ticker LIKE $3) LIMIT 1",
        raw, f"{raw}.KS", f"{raw}.KQ",
    )
    if not row:
        await _send(http, chat_id, f"`{esc(raw)}` 오픈 포지션을 찾을 수 없습니다\\.")
        return

    import asyncio as _asyncio
    loop = _asyncio.get_running_loop()
    sell_ord = ""

    try:
        from kiwoom_paper_trader import KiwoomPaperTrader, update_to_closed
        _trader = KiwoomPaperTrader()
        qty = row["qty"] or 1
        sell_ord = await loop.run_in_executor(None, _trader.place_sell, row["ticker"], qty)
    except Exception as _e:
        sell_ord = f"MANUAL:{_e}"

    # 현재가 조회 (blended_return 계산)
    cur_price = None
    try:
        import yfinance as yf
        _df = await loop.run_in_executor(
            None, lambda: yf.Ticker(row["ticker"]).history(period="2d", interval="1d")
        )
        if not _df.empty:
            cur_price = float(_df["Close"].iloc[-1])
    except Exception:
        pass

    entry = row["entry_actual"] or 0
    blended = (cur_price - entry) / entry if (cur_price and entry) else None

    from kiwoom_paper_trader import update_to_closed as _utc
    await _utc(pool, row["id"], cur_price or entry, "manual", sell_ord, blended)

    ret_str = f"{blended*100:+.2f}%" if blended is not None else "N/A"
    await _send(
        http, chat_id,
        f"✅ *{esc(row['ticker'])}* 수동 청산 완료\n"
        f"수익률: {esc(ret_str)} \\| 주문번호: `{esc(sell_ord)}`"
    )


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
        "/backtest \\<mode\\> \\<start\\> \\<end\\> — 통합 백테스트 \\(이치모쿠/3단계/교차\\)",
        "  예\\) /backtest ichimoku 2025\\-01\\-01 2026\\-01\\-01",
        "  모드\\: ichimoku \\| stage \\| cross",
        "/top — 당일 거래금액 상위 10 \\(KOSPI\\+KOSDAQ\\)",
        "/screener — 최신 주봉 차트 스크리닝 결과 \\(DB 조회\\)",
        "/scan — 주봉 스크리닝 즉시 실행 \\(전 종목 실시간 스캔, 약 10\\~20분\\)",
        "/paper — 모의투자 오픈 포지션 현황",
        "/paper\\_perf — 모의투자 누적 성과 \\(승률·수익·슬리피지\\)",
        "/paper\\_exit \\<종목코드\\> — 수동 강제 청산",
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
        await _handle_backtest(http, chat_id, args)
    elif cmd == "/top":
        await _handle_top(http, chat_id, args)
    elif cmd == "/screener":
        await _handle_screener(http, chat_id, pool)
    elif cmd == "/scan":
        await _handle_scan(http, chat_id, pool)
    elif cmd == "/buy":
        from telegram_trade import handle_buy
        await handle_buy(http, _get_token(), chat_id, args, pool)
    elif cmd == "/sell":
        from telegram_trade import handle_sell
        await handle_sell(http, _get_token(), chat_id, args, pool)
    elif cmd == "/port":
        from telegram_trade import handle_port
        await handle_port(http, _get_token(), chat_id, pool)
    elif cmd == "/pnl":
        from telegram_trade import handle_pnl
        await handle_pnl(http, _get_token(), chat_id, args, pool)
    elif cmd == "/paper":
        await _handle_paper(http, chat_id, pool)
    elif cmd == "/paper_perf":
        await _handle_paper_perf(http, chat_id, pool)
    elif cmd == "/paper_exit":
        await _handle_paper_exit(http, chat_id, pool, args)
    elif cmd in ("/help", "/start"):
        await _handle_help(http, chat_id)
    else:
        await _send(http, chat_id, f"알 수 없는 명령어입니다\\. /help 를 입력해보세요\\.")


# ── 봇 폴링 루프 (별도 asyncio 태스크로 실행) ────────────────

async def _register_commands(http: httpx.AsyncClient) -> None:
    """Telegram에 봇 명령어 목록을 등록 (/ 입력 시 자동완성에 표시됨)."""
    token = _get_token()
    url = TELEGRAM_API.format(token=token, method="setMyCommands")
    commands = [
        {"command": "status",   "description": "크롤러 상태 (업타임, 수집 건수)"},
        {"command": "signals",  "description": "최근 매매 신호 10건"},
        {"command": "today",    "description": "오늘 수집 현황 + 최신 기사"},
        {"command": "backtest", "description": "통합 백테스트 (이치모쿠/3단계/교차) — /backtest ichimoku 2025-01-01 2026-01-01"},
        {"command": "top",       "description": "당일 거래금액 상위 10 (KOSPI+KOSDAQ)"},
        {"command": "screener", "description": "최신 주봉 차트 스크리닝 결과 (DB 조회)"},
        {"command": "scan",     "description": "주봉 스크리닝 즉시 실행 (전 종목 실시간 스캔)"},
        {"command": "buy",      "description": "진입 기록 — /buy 005930 70000 100 [YYYYMMDD]"},
        {"command": "sell",     "description": "청산 기록 (FIFO) — /sell 005930 73500"},
        {"command": "port",     "description": "보유 현황 + 미실현 P&L"},
        {"command": "pnl",         "description": "실현 P&L 요약 — /pnl [week|month|all]"},
        {"command": "paper",       "description": "모의투자 오픈 포지션 현황"},
        {"command": "paper_perf",  "description": "모의투자 누적 성과 (승률·수익·슬리피지)"},
        {"command": "paper_exit",  "description": "수동 강제 청산 — /paper_exit 005930"},
        {"command": "help",        "description": "명령어 목록"},
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

    async with httpx.AsyncClient() as http:
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
