"""
bot_handlers.py  —  Telegram 봇 명령어 핸들러 모음
────────────────────────────────────────────────────────────
telegram_bot.py(폴링 루프·라우팅·전송 계층)에서 분리한 명령어 핸들러와
수동 트리거 태스크. 라우팅(_process_update)이 참조할 수 있도록
telegram_bot.py가 이 모듈의 이름들을 top-level에서 재수출한다(facade).

전송/락/전역 상태는 telegram_bot이 소유한다 — 테스트가
mock.patch("telegram.telegram_bot._send_plain") 등으로 telegram_bot 모듈
속성을 교체하거나(_backtest_lock, _scan_lock 재바인딩 포함) init_bot이
전역을 재할당하므로, 여기서는 각 함수 본문에서 `import telegram.telegram_bot
as bot` 지연 import 후 속성 접근으로 항상 최신 바인딩을 읽는다
(jobs/scheduler_wrappers.py와 동일한 패턴). `from telegram.telegram_bot
import _send_plain` 같은 이름 import는 값을 스냅샷해 패치/재할당을 못 본다.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, cast

import httpx

logger = logging.getLogger(__name__)

# 모의투자 모델별 아이콘 (/paper, /paper_perf 공용)
MODEL_ICON = {"stage": "🔵", "kosdaq": "🟣", "cross": "⭐", "ichimoku": "🌊"}


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


# ── 명령어 핸들러 ─────────────────────────────────────────────

async def _handle_status(http: httpx.AsyncClient, chat_id: str, pool) -> None:
    """/status — 크롤러 현재 상태"""
    import telegram.telegram_bot as bot
    now = datetime.now(timezone.utc)
    uptime = now - bot._start_time
    hours, rem = divmod(int(uptime.total_seconds()), 3600)
    minutes = rem // 60
    collected = len(bot._seen_hashes_ref) if bot._seen_hashes_ref else 0

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

    # 시장 현황 (KRX 직접 호출 — 대시보드 독립적)
    market_line = ""
    try:
        from datetime import datetime as _dt
        from zoneinfo import ZoneInfo as _ZI
        from data.krx_openapi import get_client as _krx_client
        _kst = _ZI("Asia/Seoul")
        _bas = _dt.now(_kst).strftime("%Y%m%d")
        _client = _krx_client()
        _ks = _client.get_kospi_index_ohlcv(_bas)
        _kq = _client.get_kosdaq_index_ohlcv(_bas)

        def _fmt(label: str, d: dict | None) -> str:
            if not d or not d.get("close") or not d.get("prev_close"):
                return ""
            pct = (d["close"] - d["prev_close"]) / d["prev_close"] * 100
            arrow = "▲" if pct > 0 else "▼" if pct < 0 else "–"
            return f"{label} {arrow}{abs(pct):.2f}%"

        parts = [s for s in (_fmt("코스피", _ks), _fmt("코스닥", _kq)) if s]
        if parts:
            market_line = f"📈 시장: {esc(' / '.join(parts))}"
    except Exception as _e:
        logger.debug("[status] 시장 현황 조회 실패: %s", _e)

    lines = [
        "📡 *크롤러 상태*",
        "",
        f"🕐 업타임: {esc(f'{hours}시간 {minutes}분')}",
        f"📰 누적 수집: {esc(str(collected))}건",
        f"📊 최근 24h 수집: {esc(str(today_count))}건",
        f"🎯 최근 24h 신호: {esc(str(signal_count))}건",
        "🌐 피드: Reuters \\+ Investing \\+ CNBC",
    ]
    if market_line:
        lines.insert(2, market_line)
    await bot._send(http, chat_id, "\n".join(lines))


async def _handle_signals(http: httpx.AsyncClient, chat_id: str, pool, direction_filter: str = "") -> None:
    """/signals [buy|sell|watch] — 최근 매매 신호 10건 (방향 필터 선택)"""
    import telegram.telegram_bot as bot
    if not pool:
        await bot._send(http, chat_id, "DB 미연결 상태입니다\\.")
        return

    dir_upper = direction_filter.upper()
    valid_dirs = ("BUY", "SELL", "WATCH")
    if dir_upper and dir_upper not in valid_dirs:
        await bot._send(http, chat_id, "사용법: /signals \\[buy\\|sell\\|watch\\]")
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
        await bot._send(http, chat_id, f"최근 감지된 신호가 없습니다{label}\\.")
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
    await bot._send(http, chat_id, "\n".join(lines))


async def _handle_today(http: httpx.AsyncClient, chat_id: str, pool) -> None:
    """/today — 오늘 수집 현황 + 최신 기사 5건"""
    import telegram.telegram_bot as bot
    if not pool:
        await bot._send(http, chat_id, "DB 미연결 상태입니다\\.")
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
    await bot._send(http, chat_id, "\n".join(lines))


async def _handle_backtest_compose(
    http: httpx.AsyncClient, chat_id: str, args: list[str]
) -> None:
    """/backtest compose <strategy> <start> <end> [market]

    예:
      /backtest compose FUNNEL-1 2025-01-01 2026-06-14
      /backtest compose ALL 2025-01-01 2026-06-14 KOSPI
    """
    import telegram.telegram_bot as bot
    VALID_STRATEGIES = ("AND-1", "AND-2", "SCORE-1", "FUNNEL-1", "ALL")
    USAGE_COMPOSE = (
        "사용법: /backtest compose <strategy> <start> <end> [market]\n"
        "  strategy: AND-1 | AND-2 | SCORE-1 | FUNNEL-1 | ALL\n"
        "  start/end: YYYY-MM-DD\n"
        "  market: KOSPI | KOSDAQ | ALL (기본: ALL)\n\n"
        "예) /backtest compose FUNNEL-1 2025-01-01 2026-06-14\n"
        "    /backtest compose ALL 2025-01-01 2026-06-14"
    )

    if len(args) < 3:
        await bot._send_plain(http, chat_id, USAGE_COMPOSE)
        return

    strategy = args[0].upper()
    start_str = args[1]
    end_str = args[2]

    if strategy not in VALID_STRATEGIES:
        await bot._send_plain(http, chat_id,
            f"strategy는 {' | '.join(VALID_STRATEGIES)} 중 하나여야 합니다.\n\n{USAGE_COMPOSE}")
        return

    from datetime import date as _date
    try:
        start = _date.fromisoformat(start_str)
        end = _date.fromisoformat(end_str)
    except ValueError:
        await bot._send_plain(http, chat_id,
            f"날짜 형식 오류 (YYYY-MM-DD): {start_str}, {end_str}")
        return

    if start >= end:
        await bot._send_plain(http, chat_id, "start는 end보다 이전이어야 합니다.")
        return

    market = "ALL"
    if len(args) >= 4 and args[3].upper() in ("KOSPI", "KOSDAQ", "ALL"):
        market = args[3].upper()

    try:
        from core.db import get_dsn as _get_dsn
        _dsn: str | None = _get_dsn()
    except Exception:
        _dsn = None

    if not _dsn:
        await bot._send_plain(http, chat_id,
            "compose 백테스트는 DB 연결이 필요합니다 (DSN 미설정).")
        return

    if bot._backtest_lock.locked():
        await bot._send_plain(http, chat_id,
            "⏳ 백테스트가 이미 실행 중입니다. 완료 후 결과가 전송됩니다.")
        return

    label = "Tier-1 전체 비교" if strategy == "ALL" else strategy
    await bot._send_plain(http, chat_id,
        f"📊 조합전략 백테스트 시작 — {label}\n"
        f"📅 {start} ~ {end}  {market}\n"
        "⏳ 예상 소요: 1~3분\n"
        "완료되면 결과를 전송합니다."
    )

    from analysis.backtest.models import BacktestConfig
    from analysis.backtest.engine import run_backtest
    from analysis.strategy_compose import STRATEGIES as _STRATS

    async with bot._backtest_lock:
        try:
            loop = asyncio.get_running_loop()
            from datetime import datetime as _dt
            from zoneinfo import ZoneInfo as _ZI
            _ts = _dt.now(_ZI("Asia/Seoul")).strftime("%Y%m%d_%H%M%S")

            if strategy == "ALL":
                results = {}
                for s in _STRATS:
                    cfg = BacktestConfig(
                        mode="compose", strategy=s,
                        start=start, end=end, market=market, dsn=_dsn,
                    )
                    results[s] = await loop.run_in_executor(None, run_backtest, cfg)
                    _html = f"reports/backtest/compose_{s.lower().replace('-', '')}_{_ts}.html"
                    results[s].to_html_report(_html)

                def _pct(v):
                    return f"{v*100:.1f}%" if v is not None else "—"
                def _val(v):
                    return f"{v:.2f}" if v is not None else "—"

                lines = [
                    f"📊 Tier-1 조합전략 비교  {start}~{end}  {market}",
                    "",
                    f"{'전략':<10} {'신호':>5} {'승률28d':>7} {'수익28d':>8} {'샤프28d':>7} {'MDD':>7}",
                    "─" * 52,
                ]
                for s, r in sorted(
                    results.items(),
                    key=lambda kv: kv[1].overall.sharpe_28d or -999,
                    reverse=True,
                ):
                    m = r.overall
                    lines.append(
                        f"{s:<10} {m.n:>5} {_pct(m.win_rate_28d):>7} "
                        f"{_pct(m.avg_return_28d):>8} {_val(m.sharpe_28d):>7} "
                        f"{_pct(m.mdd):>7}"
                    )

                msg = "```\n" + "\n".join(lines) + "\n```"
                if len(msg) > 4090:
                    msg = msg[:4087] + "..."
                await bot._send_plain(http, chat_id, msg)

            else:
                cfg = BacktestConfig(
                    mode="compose", strategy=strategy,
                    start=start, end=end, market=market, dsn=_dsn,
                )
                result = await loop.run_in_executor(None, run_backtest, cfg)
                _html_path = (
                    f"reports/backtest/compose_{strategy.lower().replace('-', '')}_{_ts}.html"
                )
                result.to_html_report(_html_path)

                report = result.to_telegram_report()
                report += f"\n\n📁 {_html_path}"
                if len(report) > 4090:
                    report = report[:4087] + "..."
                await bot._send_plain(http, chat_id, report)

        except Exception as e:
            logger.warning("[봇/compose] 실행 실패: %s", e)
            await bot._send_plain(http, chat_id, f"조합전략 백테스트 실행 중 오류: {e}")


async def _handle_backtest(
    http: httpx.AsyncClient, chat_id: str, args: list[str]
) -> None:
    """/backtest <mode> <start> <end> [market] [--max N] [--tx-cost F]

    예:
      /backtest ichimoku 2025-01-01 2026-01-01
      /backtest stage    2025-01-01 2026-01-01 KOSDAQ
      /backtest cross    2025-01-01 2026-01-01 ALL --max 50
      /backtest compose  FUNNEL-1 2025-01-01 2026-06-14
    """
    import telegram.telegram_bot as bot
    if bot._backtest_lock.locked():
        await bot._send_plain(http, chat_id,
            "⏳ 백테스트가 이미 실행 중입니다. 완료 후 결과가 전송됩니다.")
        return

    # ── 인수 파싱 ──────────────────────────────────────────────
    USAGE = (
        "사용법: /backtest <mode> <start> <end> [market] [--max N] [--tx-cost F]\n"
        "              [--tp1 F] [--tp1-ratio F] [--trail F] [--stop F]\n"
        "  mode        : ichimoku | stage | stage2 | cross | compose\n"
        "  start/end   : YYYY-MM-DD\n"
        "  market      : KOSPI | KOSDAQ | ALL (기본: ALL)\n"
        "  --max N     : 최대 티커 수 (기본 200)\n"
        "  --tx-cost F : 왕복 거래비용 (기본 ~0.0021)\n"
        "  --tp1 F     : 1차 익절 목표 (예: 0.25 = +25%)\n"
        "  --tp1-ratio F: 1차 익절 비율 (기본 0.5)\n"
        "  --trail F   : 트레일링 스탑 (고점 대비, 예: 0.10)\n"
        "  --stop F    : 하드 손절 (기본 0.08)\n\n"
        "compose 모드:\n"
        "  /backtest compose <strategy> <start> <end> [market]\n"
        "  strategy: AND-1 | AND-2 | SCORE-1 | FUNNEL-1 | ALL\n\n"
        "권장 파라미터:\n"
        "  Stage/KOSPI : --tp1 0.25 --trail 0.10 --stop 0.10\n"
        "  Stage/KOSDAQ: --tp1 0.25 --trail 0.15 --stop 0.10\n"
        "  Cross       : --tp1 0.15 --trail 0.10 --stop 0.10\n\n"
        "예) /backtest stage 2024-01-01 2026-01-01 KOSPI --tp1 0.25 --trail 0.10 --stop 0.10\n"
        "    /backtest compose FUNNEL-1 2025-01-01 2026-06-14"
    )

    if len(args) < 3:
        await bot._send_plain(http, chat_id, USAGE)
        return

    mode  = args[0].lower()
    start_str = args[1]
    end_str   = args[2]

    if mode == "compose":
        await bot._handle_backtest_compose(http, chat_id, args[1:])
        return

    if mode not in ("ichimoku", "stage", "stage2", "cross"):
        await bot._send_plain(http, chat_id,
            f"mode는 ichimoku|stage|stage2|cross|compose 중 하나여야 합니다.\n\n{USAGE}")
        return

    from datetime import date as _date
    try:
        start = _date.fromisoformat(start_str)
        end   = _date.fromisoformat(end_str)
    except ValueError:
        await bot._send_plain(http, chat_id, f"날짜 형식 오류 (YYYY-MM-DD): {start_str}, {end_str}")
        return

    if start >= end:
        await bot._send_plain(http, chat_id, "start는 end보다 이전이어야 합니다.")
        return

    if (end - start).days > 10 * 365:
        await bot._send_plain(http, chat_id, "백테스트 기간은 최대 10년입니다.")
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

    from analysis.backtest.models import BacktestConfig
    from analysis.backtest.config import TX_COST_DEFAULT, MODE_KOR
    from analysis.backtest.engine import run_backtest

    try:
        from core.db import get_dsn as _get_dsn
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
    await bot._send_plain(http, chat_id,
        f"📊 백테스트 시작 — {mode_kor}\n"
        f"📅 {start} ~ {end}  {market}  티커 최대 {max_tickers or '전종목'}개\n"
        f"📐 청산: {exit_label}\n"
        f"⏳ 예상 소요: {eta_min}~{eta_min * 3}분 (데이터 다운로드 포함)\n"
        "완료되면 결과를 전송합니다."
    )

    async with bot._backtest_lock:
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
            await bot._send_plain(http, chat_id, report)
        except Exception as e:
            logger.warning("[봇/backtest] 실행 실패: %s", e)
            await bot._send_plain(http, chat_id, f"백테스트 실행 중 오류: {e}")


async def _handle_top(http: httpx.AsyncClient, chat_id: str, args: list[str]) -> None:
    """/top — 당일 거래금액 상위 10개 종목 (KOSPI+KOSDAQ 합산)"""
    import telegram.telegram_bot as bot
    import pandas as _pd
    import FinanceDataReader as _fdr
    from datetime import date as _date

    await bot._send_plain(http, chat_id, "📊 당일 거래금액 상위 10 조회 중...")

    def _fetch() -> str:
        kospi  = _fdr.StockListing("KOSPI")
        kosdaq = _fdr.StockListing("KOSDAQ")
        df = _pd.concat([kospi, kosdaq], ignore_index=True)
        df["Amount"]      = cast(_pd.Series, _pd.to_numeric(df["Amount"],      errors="coerce")).fillna(0)
        df["Close"]       = cast(_pd.Series, _pd.to_numeric(df["Close"],       errors="coerce")).fillna(0)
        df["ChagesRatio"] = cast(_pd.Series, _pd.to_numeric(df["ChagesRatio"], errors="coerce")).fillna(0)

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
            pct = _fmt_pct(float(row["ChagesRatio"]))
            amt = _fmt_amount(float(row["Amount"]))
            price = f"{int(row['Close']):,}"
            lines.append(
                f"{int(cast(int, i))+1:2d}. {row['Name']:<10}  {price:>10}  {pct:>8}  {amt}"
            )
        return "\n".join(lines)

    try:
        loop = asyncio.get_running_loop()
        msg = await loop.run_in_executor(None, _fetch)
        await bot._send_plain(http, chat_id, msg)
    except Exception as e:
        logger.warning("[봇] /top 오류: %s", e)
        await bot._send_plain(http, chat_id, f"오류: {e}")


async def _handle_screener(http: httpx.AsyncClient, chat_id: str, pool) -> None:
    """/screener — 최신 강세 후보 발굴 결과 (DB 조회 후 DM + 채널 동시 발송)"""
    import telegram.telegram_bot as bot
    if not pool:
        await bot._send(http, chat_id, "DB 미연결 상태입니다\\.")
        return

    from core.db import load_chart_signals_latest
    from analysis.chart_screener import ScreenResult

    week, rows = await load_chart_signals_latest(pool)
    if not rows:
        await bot._send(http, chat_id,
            "강세 후보 결과가 없습니다\\.\n매주 일요일 20:30에 업데이트됩니다\\.")
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

    from telegram.telegram_notify import send_weekly_screener
    await send_weekly_screener(results, http=http, target_chat_id=chat_id)


async def _run_screener_task(http: httpx.AsyncClient, chat_id: str, pool) -> None:
    import telegram.telegram_bot as bot
    async with bot._scan_lock:
        try:
            from analysis.chart_screener import run_weekly_screen
            from core.db import save_chart_signals
            loop = asyncio.get_running_loop()
            results = await loop.run_in_executor(None, run_weekly_screen)
            saved = await save_chart_signals(pool, results)
            logger.info("[봇/screener] 완료 — 통과:%d 저장:%d", len(results), saved)
            from telegram.telegram_notify import send_weekly_screener
            await send_weekly_screener(results, http=http, target_chat_id=chat_id)
        except Exception as e:
            logger.warning("[봇/screener] 실패: %s", e)
            await bot._send_plain(http, chat_id, f"스크리너 실행 중 오류: {e}")


# ── 파이프라인 수동 트리거 내부 태스크 ───────────────────────────

async def _run_flow_task(http: httpx.AsyncClient, chat_id: str) -> None:
    # bot._flow_lock은 daily_flow_sync_job() 내부에서 쓰는 락과 같은 객체다
    # (jobs/infra_jobs.py::flow_sync_lock을 re-export) — 여기서 또 async with로
    # 감싸면 같은 락을 자기 자신이 쥔 채로 다시 기다려 데드락 난다. 함수
    # 자체가 이미 락을 잡고, 이미 실행 중이면 조용히 스킵하므로 그냥 호출만
    # 한다(cron/대시보드 트리거와 동일하게).
    import telegram.telegram_bot as bot
    try:
        from jobs.infra_jobs import daily_flow_sync_job
        await daily_flow_sync_job()
        await bot._send_plain(http, chat_id, "✅ 수급 수집 완료.")
    except Exception as e:
        logger.warning("[봇/run_flow] 실패: %s", e)
        await bot._send_plain(http, chat_id, f"수급 수집 실패: {e}")


async def _run_stage_task(http: httpx.AsyncClient, chat_id: str, pool) -> None:
    # bot._stage_lock은 daily_stage_job() 내부(jobs/stage_job.py::stage_job_lock)와
    # 같은 객체다 — 여기서 또 async with로 감싸면 자기 자신을 기다리며
    # 데드락 나므로(flow_sync_lock과 동일 사고) 그냥 호출만 한다.
    import telegram.telegram_bot as bot
    try:
        from jobs.stage_job import daily_stage_job
        new_active = await daily_stage_job(pool)
        await bot._send_plain(http, chat_id, f"✅ 스테이지 분류 완료 — 활성 {len(new_active)}종목.")
    except Exception as e:
        logger.warning("[봇/run_stage] 실패: %s", e)
        await bot._send_plain(http, chat_id, f"스테이지 분류 실패: {e}")


async def _run_youtube_task(http: httpx.AsyncClient, chat_id: str) -> None:
    # bot._youtube_lock은 youtube_narrative_sync_job() 내부(jobs/infra_jobs.py
    # ::youtube_sync_lock)와 같은 객체다 — 여기서 또 async with로 감싸면
    # 자기 자신을 기다리며 데드락 나므로(flow_sync_lock과 동일 사고) 그냥
    # 호출만 한다. youtube_attention_score_job()은 그 락 대상이 아니라서
    # (로컬 DB 집계, 경합 위험 낮음) 원래처럼 감싸지 않고 이어서 부른다.
    import telegram.telegram_bot as bot
    try:
        from jobs.infra_jobs import youtube_narrative_sync_job, youtube_attention_score_job
        await youtube_narrative_sync_job()
        await youtube_attention_score_job()
        await bot._send_plain(http, chat_id, "✅ 유튜브 수집 + 어텐션 점수 완료.")
    except Exception as e:
        logger.warning("[봇/run_youtube] 실패: %s", e)
        await bot._send_plain(http, chat_id, f"유튜브 수집 실패: {e}")


# ── 파이프라인 수동 트리거 핸들러 ─────────────────────────────────

async def _handle_run_flow(http: httpx.AsyncClient, chat_id: str) -> None:
    """/run_flow — 수급 파이프라인 즉시 실행"""
    import telegram.telegram_bot as bot
    if bot._flow_lock.locked():
        await bot._send_plain(http, chat_id, "⏳ 수급 수집이 이미 실행 중입니다. 완료 후 알림 전송.")
        return
    await bot._send_plain(http, chat_id, "📥 수급 수집 시작 — 완료 시 알림 전송 (약 40~60분 소요).")
    asyncio.create_task(_run_flow_task(http, chat_id))


async def _handle_run_stage(http: httpx.AsyncClient, chat_id: str, pool) -> None:
    """/run_stage — 스테이지 분류 즉시 실행"""
    import telegram.telegram_bot as bot
    if not pool:
        await bot._send_plain(http, chat_id, "DB 미연결 상태입니다.")
        return
    if bot._stage_lock.locked():
        await bot._send_plain(http, chat_id, "⏳ 스테이지 분류가 이미 실행 중입니다. 완료 후 알림 전송.")
        return
    await bot._send_plain(http, chat_id, "🔵 스테이지 분류 시작 — 완료 시 알림 전송 (약 10~20분 소요).")
    asyncio.create_task(_run_stage_task(http, chat_id, pool))


async def _handle_run_screener(http: httpx.AsyncClient, chat_id: str, pool) -> None:
    """/run_screener — 스크리너 즉시 실행"""
    import telegram.telegram_bot as bot
    if not pool:
        await bot._send_plain(http, chat_id, "DB 미연결 상태입니다.")
        return
    if bot._scan_lock.locked():
        await bot._send_plain(http, chat_id, "⏳ 스크리너가 이미 실행 중입니다. 완료 후 알림 전송.")
        return
    await bot._send_plain(http, chat_id, "🔍 스크리너 시작 — 완료 시 알림 전송 (약 10~20분 소요).")
    asyncio.create_task(_run_screener_task(http, chat_id, pool))


async def _handle_run_youtube(http: httpx.AsyncClient, chat_id: str) -> None:
    """/run_youtube — 유튜브 수집 + 어텐션 점수 즉시 실행"""
    import telegram.telegram_bot as bot
    if bot._youtube_lock.locked():
        await bot._send_plain(http, chat_id, "⏳ 유튜브 수집이 이미 실행 중입니다. 완료 후 알림 전송.")
        return
    await bot._send_plain(http, chat_id, "▶️ 유튜브 수집 시작 — 완료 시 알림 전송 (약 5~10분 소요).")
    asyncio.create_task(_run_youtube_task(http, chat_id))


async def _handle_run_all(http: httpx.AsyncClient, chat_id: str, pool) -> None:
    """/run_all — 4개 파이프라인 동시 실행"""
    import telegram.telegram_bot as bot
    statuses = {
        "수급":    bot._flow_lock.locked(),
        "스테이지": bot._stage_lock.locked(),
        "스크리너": bot._scan_lock.locked(),
        "유튜브":  bot._youtube_lock.locked(),
    }
    already = [k for k, v in statuses.items() if v]
    to_run  = [k for k, v in statuses.items() if not v]

    if not to_run:
        await bot._send_plain(http, chat_id, "모든 파이프라인이 이미 실행 중입니다.")
        return

    msg_parts = [f"🚀 파이프라인 시작: {', '.join(to_run)}"]
    if already:
        msg_parts.append(f"⏳ 이미 실행 중 (건너뜀): {', '.join(already)}")
    msg_parts.append("각 파이프라인 완료 시 개별 알림 전송.")
    await bot._send_plain(http, chat_id, "\n".join(msg_parts))

    if "수급" in to_run:
        asyncio.create_task(_run_flow_task(http, chat_id))
    if "스테이지" in to_run and pool:
        asyncio.create_task(_run_stage_task(http, chat_id, pool))
    if "스크리너" in to_run and pool:
        asyncio.create_task(_run_screener_task(http, chat_id, pool))
    if "유튜브" in to_run:
        asyncio.create_task(_run_youtube_task(http, chat_id))


async def _handle_paper(http: httpx.AsyncClient, chat_id: str, pool) -> None:
    """/paper — 모의투자 오픈 포지션 현황"""
    import telegram.telegram_bot as bot
    if not pool:
        await bot._send(http, chat_id, "DB 미연결 상태입니다\\.")
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
        await bot._send(http, chat_id, "📭 현재 오픈 포지션이 없습니다\\.")
        return

    from datetime import date as _date
    import asyncio as _asyncio

    today = _date.today()

    # 현재가 일괄 조회 (KiwoomPaperTrader 또는 yfinance fallback)
    tickers = list({r["ticker"] for r in rows if r["status"] == "open"})
    price_map: dict[str, int] = {}

    try:
        from data.kiwoom_paper_trader import KiwoomPaperTrader
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

    await bot._send(http, chat_id, "\n".join(lines))


async def _handle_paper_perf(http: httpx.AsyncClient, chat_id: str, pool) -> None:
    """/paper_perf — 모의투자 누적 성과 (이론 vs 실전)"""
    import telegram.telegram_bot as bot
    if not pool:
        await bot._send(http, chat_id, "DB 미연결 상태입니다\\.")
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
        await bot._send(
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

    await bot._send(http, chat_id, "\n".join(lines))


async def _handle_paper_exit(http: httpx.AsyncClient, chat_id: str, pool, args: list[str]) -> None:
    """/paper_exit <ticker> — 특정 종목 수동 강제 청산"""
    import telegram.telegram_bot as bot
    if not pool:
        await bot._send(http, chat_id, "DB 미연결 상태입니다\\.")
        return
    if not args:
        await bot._send(http, chat_id, "사용법: /paper\\_exit \\<종목코드\\>\n예\\) /paper\\_exit 005930")
        return

    raw = args[0].upper().replace("-", ".")
    # 6자리 숫자면 .KS/.KQ 자동 추론 (DB에서 확인)
    row = await pool.fetchrow(
        "SELECT id, ticker, qty, entry_actual, model FROM paper_positions "
        "WHERE status='open' AND (ticker=$1 OR ticker LIKE $2 OR ticker LIKE $3) LIMIT 1",
        raw, f"{raw}.KS", f"{raw}.KQ",
    )
    if not row:
        await bot._send(http, chat_id, f"`{esc(raw)}` 오픈 포지션을 찾을 수 없습니다\\.")
        return

    import asyncio as _asyncio
    loop = _asyncio.get_running_loop()
    sell_ord = ""

    try:
        from data.kiwoom_paper_trader import KiwoomPaperTrader
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

    from data.kiwoom_paper_trader import update_to_closed as _utc
    await _utc(pool, row["id"], cur_price or entry, "manual", sell_ord, blended)

    ret_str = f"{blended*100:+.2f}%" if blended is not None else "N/A"
    await bot._send(
        http, chat_id,
        f"✅ *{esc(row['ticker'])}* 수동 청산 완료\n"
        f"수익률: {esc(ret_str)} \\| 주문번호: `{esc(sell_ord)}`"
    )


async def _handle_watchlist(http: httpx.AsyncClient, chat_id: str, pool) -> None:
    """/watchlist — 거래대금 워치리스트 즉시 조회 (스케줄 없이 온디맨드)"""
    import telegram.telegram_bot as bot
    if not pool:
        await bot._send_plain(http, chat_id, "DB 미연결 상태입니다.")
        return
    try:
        from run_scheduler import _build_watchlist_entries
        from telegram.telegram_notify import send_watchlist_brief as _send_brief
        data = await _build_watchlist_entries(pool)
        await _send_brief(data["entries"], http=http, target_chat_id=chat_id)
    except Exception as e:
        logger.warning("[봇] /watchlist 오류: %s", e)
        await bot._send_plain(http, chat_id, f"워치리스트 조회 실패: {e}")


async def _handle_help(http: httpx.AsyncClient, chat_id: str) -> None:
    """/help — 명령어 목록"""
    import telegram.telegram_bot as bot
    lines = [
        "📋 *사용 가능한 명령어*",
        "",
        "/status — 크롤러 상태 \\(업타임, 수집 건수\\)",
        "/signals — 최근 매매 신호 10건",
        "/signals buy — BUY 신호만 조회",
        "/signals sell — SELL 신호만 조회",
        "/signals watch — WATCH 신호만 조회",
        "/today — 오늘 수집 현황 \\+ 최신 기사",
        "/backtest \\<mode\\> \\<start\\> \\<end\\> — 통합 백테스트 \\(이치모쿠/3단계/교차/조합\\)",
        "  예\\) /backtest ichimoku 2025\\-01\\-01 2026\\-01\\-01",
        "  예\\) /backtest compose FUNNEL\\-1 2025\\-01\\-01 2026\\-06\\-14",
        "  모드\\: ichimoku \\| stage \\| cross \\| compose",
        "  compose 전략\\: AND\\-1 \\| AND\\-2 \\| SCORE\\-1 \\| FUNNEL\\-1 \\| ALL",
        "/top — 당일 거래금액 상위 10 \\(KOSPI\\+KOSDAQ\\)",
        "/screener — 최신 강세 후보 발굴 결과 \\(DB 조회\\)",
        "/watchlist — 거래대금 워치리스트 즉시 조회",
        "/paper — 모의투자 오픈 포지션 현황",
        "/paper\\_perf — 모의투자 누적 성과 \\(승률·수익·슬리피지\\)",
        "/paper\\_exit \\<종목코드\\> — 수동 강제 청산",
        "",
        "📡 *파이프라인 수동 실행*",
        "/run\\_flow — 수급 수집 즉시 실행 \\(KRX, 40\\~60분\\)",
        "/run\\_stage — 스테이지 분류 즉시 실행 \\(10\\~20분\\)",
        "/run\\_screener — 스크리너 즉시 실행 \\(전 종목 실시간 스캔, 10\\~20분\\)",
        "/run\\_youtube — 유튜브 수집\\+어텐션 점수 즉시 실행 \\(5\\~10분\\)",
        "/run\\_all — 4개 파이프라인 동시 실행",
        "/help — 이 도움말",
    ]
    await bot._send(http, chat_id, "\n".join(lines))
