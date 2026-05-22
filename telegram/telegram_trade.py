"""
telegram_trade.py — 거래 저널 Telegram 명령 핸들러

지원 명령:
    /buy  <ticker> <price> <qty> [YYYYMMDD]  — 진입 기록
    /sell <ticker> <price>                   — FIFO 청산
    /port                                    — 미청산 포지션 현황
    /pnl  [week|month|all]                   — 실현 P&L 요약
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Optional

import httpx

from core.db import (
    save_trade,
    close_position,
    get_open_positions,
    get_pnl_summary,
)

logger = logging.getLogger(__name__)

# ── 헬퍼 ─────────────────────────────────────────────────────

def _to_ticker(code: str) -> str:
    """
    6자리 종목코드 → yfinance 심볼.
    숫자 6자리면 KOSPI(.KS) / KOSDAQ(.KQ) 구분 없이 .KS 기본.
    이미 .KS/.KQ 붙은 경우 그대로.
    """
    code = code.strip().upper()
    if code.endswith(".KS") or code.endswith(".KQ"):
        return code
    digits = code.zfill(6)
    return f"{digits}.KS"


def _parse_date(s: str) -> Optional[date]:
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except ValueError:
        return None


def _fmt_won(n: int) -> str:
    if abs(n) >= 100_000_000:
        return f"{n / 100_000_000:.1f}억"
    if abs(n) >= 10_000:
        return f"{n / 10_000:.0f}만"
    return f"{n:,}"


async def _send(http: httpx.AsyncClient, token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        await http.post(url, json={"chat_id": chat_id, "text": text,
                                   "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        logger.warning("[trade] 메시지 전송 실패: %s", e)


# ── /buy ──────────────────────────────────────────────────────

async def handle_buy(
    http: httpx.AsyncClient,
    token: str,
    chat_id: str,
    args: list[str],
    pool,
) -> None:
    """
    /buy 005930 70000 100
    /buy 005930 70000 100 20260501
    """
    if len(args) < 3:
        await _send(http, token, chat_id,
                    "사용법: /buy &lt;종목코드&gt; &lt;가격&gt; &lt;수량&gt; [YYYYMMDD]")
        return

    ticker_raw, price_raw, qty_raw = args[0], args[1], args[2]
    date_raw = args[3] if len(args) >= 4 else None

    try:
        entry_price = int(price_raw.replace(",", ""))
        qty = int(qty_raw.replace(",", ""))
    except ValueError:
        await _send(http, token, chat_id,
                    "가격/수량은 숫자여야 합니다. 예) /buy 005930 70000 100")
        return

    if entry_price <= 0 or qty <= 0:
        await _send(http, token, chat_id, "가격과 수량은 양수여야 합니다.")
        return

    entry_date: date
    if date_raw:
        parsed = _parse_date(date_raw)
        if not parsed:
            await _send(http, token, chat_id,
                        f"날짜 형식 오류: {date_raw}. YYYYMMDD 형식으로 입력하세요.")
            return
        entry_date = parsed
    else:
        entry_date = date.today()

    ticker = _to_ticker(ticker_raw)

    trade_id = await save_trade(
        pool,
        ticker=ticker,
        entry_date=entry_date,       # date 객체 — asyncpg 요구사항
        entry_price=entry_price,
        qty=qty,
    )

    if trade_id is None:
        await _send(http, token, chat_id, "⚠️ 거래 저장 실패. 로그를 확인하세요.")
        return

    cost = entry_price * qty
    await _send(
        http, token, chat_id,
        f"✅ <b>진입 기록 완료</b> (#{trade_id})\n"
        f"종목: {ticker}\n"
        f"날짜: {entry_date}\n"
        f"가격: {entry_price:,}원 × {qty:,}주 = {_fmt_won(cost)}원\n"
        f"(신호/Stage 정보는 DB에서 자동 조회됨)"
    )


# ── /sell ─────────────────────────────────────────────────────

async def handle_sell(
    http: httpx.AsyncClient,
    token: str,
    chat_id: str,
    args: list[str],
    pool,
) -> None:
    """
    /sell 005930 73500
    FIFO: entry_date 가장 오래된 미청산 포지션 전량 청산.
    """
    if len(args) < 2:
        await _send(http, token, chat_id,
                    "사용법: /sell &lt;종목코드&gt; &lt;가격&gt;")
        return

    ticker_raw, price_raw = args[0], args[1]
    try:
        exit_price = int(price_raw.replace(",", ""))
    except ValueError:
        await _send(http, token, chat_id, "가격은 숫자여야 합니다.")
        return

    if exit_price <= 0:
        await _send(http, token, chat_id, "가격은 양수여야 합니다.")
        return

    ticker = _to_ticker(ticker_raw)
    exit_date = date.today()

    result = await close_position(
        pool,
        ticker=ticker,
        exit_date=exit_date,
        exit_price=exit_price,
    )

    if result is None:
        await _send(http, token, chat_id,
                    f"⚠️ {ticker} 미청산 포지션이 없습니다.")
        return

    pnl = result.get("pnl")
    pnl_pct = result.get("pnl_pct")
    entry_price = result.get("entry_price", 0)
    qty = result.get("qty", 0)

    sign = "🟢" if (pnl or 0) >= 0 else "🔴"
    pnl_str = f"{sign} {_fmt_won(int(pnl))}원 ({pnl_pct:+.2f}%)" if pnl is not None else "계산 중"

    await _send(
        http, token, chat_id,
        f"✅ <b>청산 완료</b>\n"
        f"종목: {ticker}\n"
        f"날짜: {exit_date}\n"
        f"매도가: {exit_price:,}원 × {qty:,}주\n"
        f"P&L: {pnl_str}"
    )


# ── /port ─────────────────────────────────────────────────────

async def handle_port(
    http: httpx.AsyncClient,
    token: str,
    chat_id: str,
    pool,
) -> None:
    """미청산 포지션 현황 + 미실현 P&L (yfinance — asyncio.to_thread 로 이벤트 루프 차단 방지)."""
    positions = await get_open_positions(pool)

    if not positions:
        await _send(http, token, chat_id, "📭 보유 종목 없음")
        return

    # yfinance 현재가 배치 조회 (동기 → 스레드풀)
    tickers = [p["ticker"] for p in positions]
    prices: dict[str, Optional[int]] = {}
    as_of = ""

    def _fetch_prices():
        import yfinance as yf
        from datetime import datetime as _dt
        nonlocal as_of
        try:
            data = yf.download(tickers, period="1d", auto_adjust=True,
                               progress=False)
            close = data["Close"] if "Close" in data.columns else data
            latest = close.iloc[-1]
            as_of = _dt.now().strftime("%Y-%m-%d %H:%M")
            return {t: int(latest[t]) if t in latest and latest[t] == latest[t] else None
                    for t in tickers}
        except Exception as e:
            logger.warning("[trade] yfinance 조회 실패: %s", e)
            return {t: None for t in tickers}

    prices = await asyncio.to_thread(_fetch_prices)

    lines = [f"📊 <b>보유 현황</b> ({as_of or '조회 실패'})"]
    for p in positions:
        t = p["ticker"]
        cur = prices.get(t)
        entry = int(p["entry_price"])
        qty = int(p["qty"])
        cost = entry * qty

        if cur is not None:
            unreal = (cur - entry) * qty
            sign = "🟢" if unreal >= 0 else "🔴"
            unreal_str = f"{sign} {_fmt_won(unreal)}원"
        else:
            unreal_str = "가격 조회 불가"

        stage_str = f" (S{p['stage_at_entry']})" if p.get("stage_at_entry") else ""
        lines.append(
            f"\n<b>{t}</b>{stage_str}\n"
            f"  진입: {entry:,}원 × {qty:,}주 ({p['entry_date']})\n"
            f"  미실현: {unreal_str}"
        )

    await _send(http, token, chat_id, "\n".join(lines))


# ── /pnl ──────────────────────────────────────────────────────

async def handle_pnl(
    http: httpx.AsyncClient,
    token: str,
    chat_id: str,
    args: list[str],
    pool,
) -> None:
    """
    /pnl [week|month|all]
    실현 P&L 요약 리포트.
    """
    period = args[0].lower() if args else "all"
    if period not in ("week", "month", "all"):
        period = "all"

    label = {"week": "직전 주", "month": "이번 달", "all": "전체"}[period]
    summary = await get_pnl_summary(pool, period=period)

    cnt = summary["trade_cnt"]
    if cnt == 0:
        await _send(http, token, chat_id,
                    f"📭 {label} 실현 거래 없음")
        return

    win = summary["win_cnt"]
    lose = cnt - win
    win_rate = round(win / cnt * 100) if cnt else 0
    total = summary["total_pnl"]
    sign = "🟢" if total >= 0 else "🔴"

    stage_lines = []
    for s, d in sorted(summary["by_stage"].items()):
        wr = round(d["wins"] / d["total"] * 100) if d["total"] else 0
        stage_lines.append(f"  Stage {s}: {d['total']}건 → 승률 {wr}%")

    text = (
        f"📈 <b>P&L 요약</b> ({label})\n\n"
        f"거래: {cnt}건  승 {win} / 패 {lose}  (승률 {win_rate}%)\n"
        f"총 P&L: {sign} {_fmt_won(total)}원\n"
        f"평균 수익: {_fmt_won(summary['avg_win'])}원\n"
        f"평균 손실: {_fmt_won(summary['avg_loss'])}원"
    )
    if stage_lines:
        text += "\n\nStage별:\n" + "\n".join(stage_lines)

    await _send(http, token, chat_id, text)
