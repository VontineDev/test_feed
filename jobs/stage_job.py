"""일봉 3단계 분류기 잡.

daily_stage_job(db_pool) -> set[str]
  Stage 1/2/3 분류 + daily_flow streak + Ichimoku 비교 전송.
  반환값: 새 active_stage_tickers (호출자가 전역 캐시 갱신)
"""

import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import yfinance as yf

from analysis.chart_screener import get_all_tickers
from analysis.stage_classifier import (
    classify_stage, check_peakout,
    compute_foreign_chg_pct, compute_flow_score,
)
from core.db import (
    load_chart_signals_latest,
    get_stage1_history,
    save_stage_classifications,
    get_active_stage_tickers,
)
from telegram.telegram_notify import send_screener_comparison as tg_send_screener_comparison

logger = logging.getLogger(__name__)


def _fetch_daily_ohlcv(ticker: str):
    try:
        df = yf.Ticker(ticker).history(period="60d", interval="1d", auto_adjust=True)
        if df.empty or len(df) < 21:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        df.index = pd.to_datetime(df.index, utc=True)
        return df
    except Exception:
        return None


async def daily_stage_job(db_pool) -> set[str]:
    if not db_pool:
        logger.warning("[3단계] DB 풀 없음 — 스킵")
        return set()

    loop = asyncio.get_running_loop()
    logger.info("[3단계] 일별 분류 시작")

    # 1. Ichimoku 비교용 결과 (D4: load_chart_signals_latest 사용 — 종목명/has_gapjum 포함)
    _week, ichimoku_rows = await load_chart_signals_latest(db_pool)
    logger.info("[3단계] Ichimoku 비교 풀: %d종목 (week_of=%s)", len(ichimoku_rows), _week)

    # 2. 전 종목 티커 목록 (KOSPI + KOSDAQ, ~2770개)
    all_tickers: list[tuple[str, str, str]] = await loop.run_in_executor(
        None, get_all_tickers, None
    )
    cap = int(os.environ.get("DAILY_CLASSIFIER_TICKERS", "150"))
    if len(all_tickers) > cap:
        _ichi_syms = {r["ticker"] for r in ichimoku_rows}
        _priority  = [(t, n, s) for t, n, s in all_tickers if t in _ichi_syms]
        _others    = [(t, n, s) for t, n, s in all_tickers if t not in _ichi_syms]
        _n_fill    = max(0, cap - len(_priority))
        all_tickers = _priority + _others[:_n_fill]
        logger.info("[3단계] 티커 캡 %d 적용 (Ichimoku 우선 %d + 기타 %d)",
                    cap, len(_priority), _n_fill)

    logger.info("[3단계] 분류 대상: %d종목 / SCREENER_WORKERS=%s",
                len(all_tickers), os.environ.get("SCREENER_WORKERS", "1"))

    today = date.today()
    workers = int(os.environ.get("SCREENER_WORKERS", "1"))

    # 2b. daily OHLCV (60일, yfinance, 병렬)
    price_map: dict[str, object] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool_ex:
        futures = {pool_ex.submit(_fetch_daily_ohlcv, t): t for t, _, _ in all_tickers}
        for fut in as_completed(futures):
            tk = futures[fut]
            try:
                df = fut.result()
                if df is not None:
                    price_map[tk] = df
            except Exception:
                pass
    logger.info("[3단계] OHLCV 수집: %d/%d종목", len(price_map), len(all_tickers))

    # 3. daily_flow 20일 배치 로드
    since_20d = today - timedelta(days=20)
    flow_map: dict[str, object] = {}
    try:
        async with db_pool.acquire() as conn:
            rows_flow = await conn.fetch(
                """
                SELECT ticker, trade_date, foreign_net, inst_net,
                       foreign_streak, inst_streak, personal_net
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
            df_f = pd.DataFrame(rows_list)
            df_f = df_f.set_index("trade_date")
            df_f.index = pd.to_datetime(df_f.index)
            flow_map[t] = df_f
    except Exception as e:
        logger.warning("[3단계] flow_map 로드 실패: %s", e)

    # 3b. listed_shares 배치 로드 (krx_listings, yfinance_symbol 기준)
    listed_shares_map: dict[str, int] = {}
    try:
        async with db_pool.acquire() as conn:
            share_rows = await conn.fetch(
                "SELECT yfinance_symbol, listed_shares FROM krx_listings WHERE listed_shares IS NOT NULL"
            )
        for r in share_rows:
            listed_shares_map[r["yfinance_symbol"]] = r["listed_shares"]
        logger.info("[3단계] listed_shares 로드: %d종목", len(listed_shares_map))
    except Exception as e:
        logger.warning("[3단계] listed_shares 로드 실패: %s", e)

    # 4. s1_history 배치 조회
    all_ticker_list = [t for t, _, _ in all_tickers]
    since_14d = today - timedelta(days=14)
    s1_history = await get_stage1_history(db_pool, all_ticker_list, since_14d)
    logger.info("[3단계] s1_history: %d종목 이력", len(s1_history))

    # 5. classify_stage() 병렬 실행
    market_map = {t: ("KOSDAQ" if s.endswith(".KQ") else "KOSPI") for t, _, s in all_tickers}

    def _classify_one(ticker: str) -> tuple[str, Optional[int], bool]:
        price_df = price_map.get(ticker)
        flow_df  = flow_map.get(ticker, pd.DataFrame())
        if price_df is None:
            return ticker, None, False
        market = market_map.get(ticker, "KOSPI")
        stage = classify_stage(ticker, price_df, flow_df, s1_history, market)
        peakout = check_peakout(ticker, flow_df, price_df) if stage == 3 else False
        return ticker, stage, peakout

    stage_results: dict[str, int] = {}
    peakout_flags: set[str] = set()
    upsert_rows: list[dict] = []

    with ThreadPoolExecutor(max_workers=workers) as pool_ex2:
        futures2 = {pool_ex2.submit(_classify_one, t): t for t in all_ticker_list}
        for fut in as_completed(futures2):
            try:
                ticker, stage, peakout = fut.result()
                if stage is not None:
                    stage_results[ticker] = stage
                    if peakout:
                        peakout_flags.add(ticker)

                    s1_high = s1_vol = s1_txamt = None
                    if stage == 1:
                        pdf = price_map.get(ticker)
                        if pdf is not None and not pdf.empty:
                            last_row = pdf.iloc[-1]
                            s1_high  = float(last_row.get("High") or last_row.get("Close") or 0) or None
                            _vol     = int(last_row.get("Volume") or 0)
                            _close   = float(last_row.get("Close") or 0)
                            s1_vol   = _vol or None
                            s1_txamt = int(_vol * _close) or None  # 거래대금 = Volume × Close

                    flow_df_for_score = flow_map.get(ticker, pd.DataFrame())
                    price_df_for_score = price_map.get(ticker)
                    lshares = listed_shares_map.get(ticker)

                    upsert_rows.append({
                        "ticker":               ticker,
                        "classified_date":      today,
                        "stage":                stage,
                        "s1_entry_date":        today if stage == 1 else None,
                        "s1_high":              s1_high,
                        "s1_volume":            s1_vol,
                        "s1_txamt":             s1_txamt,
                        "peakout_flag":         peakout,
                        "foreign_chg_14d_pct":  compute_foreign_chg_pct(flow_df_for_score, lshares),
                        "flow_score":           compute_flow_score(flow_df_for_score, price_df_for_score),
                    })
            except Exception as e:
                logger.debug("[3단계] 분류 오류: %s", e)

    logger.info("[3단계] 분류 완료 — Stage1:%d Stage2:%d Stage3:%d 피크아웃:%d",
                sum(1 for s in stage_results.values() if s == 1),
                sum(1 for s in stage_results.values() if s == 2),
                sum(1 for s in stage_results.values() if s == 3),
                len(peakout_flags))

    # 6. stage_classifications upsert
    await save_stage_classifications(db_pool, upsert_rows)

    # 6b. 분류된 종목 이름 캐시 갱신 (ticker_names)
    try:
        from core.db import upsert_ticker_names
        await upsert_ticker_names(db_pool, list(stage_results.keys()))
    except Exception as e:
        logger.warning("[3단계] ticker_names 업데이트 실패: %s", e)

    # 7. 텔레그램 비교 메세지 전송
    try:
        await tg_send_screener_comparison(
            ichimoku_rows=ichimoku_rows,
            stage_results=stage_results,
            peakout_flags=peakout_flags,
        )
    except Exception as e:
        logger.warning("[3단계] 텔레그램 전송 실패: %s", e)

    # 8. 활성 Stage 캐시 조회 후 반환 (호출자가 전역 갱신)
    new_active: set[str] = set()
    try:
        new_active = await get_active_stage_tickers(db_pool, days=7)
        logger.info("[3단계] 활성 Stage 캐시 갱신 — %d종목", len(new_active))
    except Exception as e:
        logger.warning("[3단계] 활성 Stage 캐시 갱신 실패: %s", e)

    logger.info("[3단계] 일별 분류 완료")
    return new_active
