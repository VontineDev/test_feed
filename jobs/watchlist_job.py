"""워치리스트 일보 잡.

build_watchlist_entries(pool) -> dict
  워치리스트 데이터 조회·조합 헬퍼 (스케줄러 + /watchlist 봇 커맨드 공유).

watchlist_brief_job(pool) -> None
  Stage 1 진입 후 추적 중인 종목의 거래대금 건강도·수급 스트릭·Ichimoku를 요약해 전송.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Optional

import httpx
import yfinance as yf

from core.db import (
    get_stage1_watchlist,
    get_watchlist_vol_log,
    upsert_watchlist_vol_log,
)
from telegram.telegram_notify import (
    send_watchlist_brief as tg_send_watchlist_brief,
    _get_token,
    _get_chat_id,
    _post_message,
)

logger = logging.getLogger(__name__)


async def build_watchlist_entries(pool) -> dict:
    """워치리스트 데이터 조회·조합 헬퍼.

    vol_log upsert는 스케줄러만 수행 — 이 함수에서는 하지 않음.

    반환 dict:
      entries:             확신도순 정렬된 entry list (send_watchlist_brief 입력)
      vol_log_rows:        오늘 vol_ratio upsert 대기 행
      vol_log_map:         {ticker: [최신, ...]} lookback=3
      s1_date_map:         {ticker: date}
      yesterday_stage_map: {ticker: stage}
    """
    today = date.today()

    # Step 1: Stage 1 워치리스트 조회 (최근 14 캘린더일)
    watchlist = await get_stage1_watchlist(pool, days=14)
    if not watchlist:
        return {
            "entries": [], "vol_log_rows": [], "vol_log_map": {},
            "s1_date_map": {}, "yesterday_stage_map": {},
        }

    tickers      = [r["ticker"]   for r in watchlist]
    s1_txamt_map = {r["ticker"]: r.get("s1_txamt") or (
        # 구버전 DB 행 fallback: s1_volume × s1_high 추정값 사용
        int(r["s1_volume"] * r["s1_high"])
        if r.get("s1_volume") and r.get("s1_high") else None
    ) for r in watchlist}
    s1_date_map  = {r["ticker"]: r["s1_date"]   for r in watchlist}

    # Step 2: 5개 벌크 쿼리
    flow_map: dict[str, dict]           = {}
    ichimoku_set: set[str]              = set()
    stage_map: dict[str, int]           = {}
    yesterday_stage_map: dict[str, int] = {}
    latest_week: Optional[str]          = None

    try:
        yesterday = today - timedelta(days=1)
        async with pool.acquire() as conn:
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

    # Step 3: vol_ratio 이력 (lookback=3, 최신순). index 0 = 어제 (upsert 전이므로 오늘 없음)
    vol_log_map: dict[str, list[float]] = await get_watchlist_vol_log(pool, tickers, lookback=3)

    # Step 4: yfinance 병렬 거래대금 fetch (Volume × Close)
    def _fetch_txamt(t: str) -> tuple[str, Optional[float]]:
        try:
            df = yf.Ticker(t).history(period="2d", interval="1d", auto_adjust=True)
            if df.empty:
                return t, None
            last = df.iloc[-1]
            vol   = float(last["Volume"])
            close = float(last["Close"])
            txamt = vol * close
            return t, txamt if txamt > 0 else None
        except Exception:
            return t, None

    txamt_today: dict[str, Optional[float]] = {}
    with ThreadPoolExecutor(max_workers=4) as pool_ex:
        futures = {pool_ex.submit(_fetch_txamt, t): t for t in tickers if s1_txamt_map.get(t)}
        for fut in as_completed(futures):
            t, v = fut.result()
            txamt_today[t] = v

    # Step 5: 결과 조합 (vol_ratio_delta, retiring 포함)
    entries: list[dict] = []
    vol_log_rows: list[dict] = []

    for ticker in tickers:
        s1_txamt   = s1_txamt_map.get(ticker)
        s1_date    = s1_date_map[ticker]
        days_since = (today - s1_date).days
        today_txamt = txamt_today.get(ticker)
        vol_ratio  = (today_txamt / s1_txamt) if (s1_txamt and s1_txamt > 0 and today_txamt is not None) else None

        # 전일 대비 vol_ratio 변화 (vol_log_map index 0 = 어제)
        hist            = vol_log_map.get(ticker) or []
        yesterday_ratio = hist[0] if hist else None
        vol_ratio_delta = (
            (vol_ratio - yesterday_ratio)
            if (vol_ratio is not None and yesterday_ratio is not None)
            else None
        )

        flow          = flow_map.get(ticker, {})
        ichimoku_ok   = (ticker in ichimoku_set) if latest_week else None
        current_stage = stage_map.get(ticker)
        retiring      = days_since >= 10  # D+10부터 마지막 추적일 표시

        logger.debug(
            "[워치리스트] %s vol_ratio=%.2f delta=%s stage=%s streak=(%s,%s)",
            ticker, vol_ratio or 0,
            f"{vol_ratio_delta:+.2f}" if vol_ratio_delta is not None else "N/A",
            current_stage, flow.get("f_streak"), flow.get("i_streak"),
        )

        entries.append({
            "ticker":          ticker,
            "days_since":      days_since,
            "vol_ratio":       vol_ratio,
            "vol_ratio_delta": vol_ratio_delta,
            "retiring":        retiring,
            "f_streak":        flow.get("f_streak"),
            "i_streak":        flow.get("i_streak"),
            "ichimoku_ok":     ichimoku_ok,
            "current_stage":   current_stage,
        })

        if vol_ratio is not None:
            vol_log_rows.append({
                "ticker":     ticker,
                "trade_date": today,
                "vol_ratio":  vol_ratio,
                "s1_txamt":   s1_txamt,
                "s1_vol":     None,   # 구버전 호환용 컬럼, txamt 기준 전환 후 미사용
            })

    # Step 7: 확신도 순 정렬
    def _conviction(e: dict) -> float:
        vr  = e.get("vol_ratio") or 0.0
        fs  = e.get("f_streak") or 0
        is_ = e.get("i_streak") or 0
        return vr + ((1 if fs > 0 else 0) + (1 if is_ > 0 else 0)) * 0.1

    entries.sort(key=_conviction, reverse=True)

    return {
        "entries":             entries,
        "vol_log_rows":        vol_log_rows,
        "vol_log_map":         vol_log_map,
        "s1_date_map":         s1_date_map,
        "yesterday_stage_map": yesterday_stage_map,
    }


async def watchlist_brief_job(pool) -> None:
    """Stage 1 진입 후 추적 중인 종목의 거래대금 건강도·수급 스트릭·Ichimoku를 요약해 전송."""
    if not pool:
        logger.warning("[워치리스트] DB 풀 없음 — 스킵")
        return

    logger.info("[워치리스트] 거래대금 워치리스트 일보 시작")

    data                = await build_watchlist_entries(pool)
    entries             = data["entries"]
    vol_log_rows        = data["vol_log_rows"]
    vol_log_map         = data["vol_log_map"]
    s1_date_map         = data["s1_date_map"]
    yesterday_stage_map = data["yesterday_stage_map"]

    if not entries:
        logger.info("[워치리스트] 워치리스트 없음")
        async with httpx.AsyncClient() as http:
            await tg_send_watchlist_brief([], http=http)
        return

    logger.info("[워치리스트] %d종목 추적", len(entries))

    # Step 6: vol_ratio 로그 upsert
    await upsert_watchlist_vol_log(pool, vol_log_rows)

    today = date.today()

    # Step 8: 단독 알림 — Stage 2 전환 감지
    stage2_alerts: list[str] = []
    for e in entries:
        ticker = e["ticker"]
        if e.get("current_stage") == 2 and yesterday_stage_map.get(ticker) == 1:
            stage2_alerts.append(ticker)

    if stage2_alerts:
        try:
            today_str = today.strftime("%Y-%m-%d")
            from core.ticker_cache import ticker_cache as _tc
            alert_lines = [f"Stage 2 전환 확인 ({today_str})", ""]
            for t in stage2_alerts:
                code  = t.split(".")[0]
                ko    = _tc.get_name(t)
                label = f"{ko}({code})" if ko != code else code
                days_d = (today - s1_date_map[t]).days
                alert_lines.append(f"  {label} — D+{days_d} → Stage 2 진입")
            alert_msg = "\n".join(alert_lines)
            async with httpx.AsyncClient() as http:
                await _post_message(http, _get_token(), _get_chat_id(), alert_msg, label="Stage2알림", parse_mode=None)
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
            today_str = today.strftime("%Y-%m-%d")
            from core.ticker_cache import ticker_cache as _tc
            death_lines = [f"랠리 소멸 경고 ({today_str})", "3거래일 연속 진입비 -40% 이상", ""]
            for t in rally_death_alerts:
                code  = t.split(".")[0]
                ko    = _tc.get_name(t)
                label = f"{ko}({code})" if ko != code else code
                ratio = next((e["vol_ratio"] for e in entries if e["ticker"] == t), None)
                pct   = f"{(ratio - 1) * 100:.0f}%" if ratio is not None else "N/A"
                death_lines.append(f"  {label} 오늘 비율 {pct}")
            death_msg = "\n".join(death_lines)
            async with httpx.AsyncClient() as http:
                await _post_message(http, _get_token(), _get_chat_id(), death_msg, label="랠리소멸알림", parse_mode=None)
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
