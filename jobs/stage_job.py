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
from analysis.stage_classifier import classify_stage_v15, check_peakout
from core.db import (
    load_chart_signals_latest,
    get_stage1_history,
    get_stage2_history,
    save_stage_classifications,
    get_active_stage_tickers,
)
from jobs.stage_shared import normalize_ohlcv, load_listed_shares, build_row, load_flow_range
from telegram.telegram_notify import send_screener_comparison as tg_send_screener_comparison

logger = logging.getLogger(__name__)


def _fetch_daily_ohlcv(ticker: str):
    try:
        df = yf.Ticker(ticker).history(period="60d", interval="1d", auto_adjust=True)
        if df.empty or len(df) < 21:
            return None
        return normalize_ohlcv(df)
    except Exception:
        return None


# cron(평일)·대시보드 트리거(scheduler_triggers 폴링)·텔레그램(/run_stage,
# /run_all) 3개의 독립 경로가 daily_stage_job()을 부를 수 있다 — 2026-08-31
# daily_flow_sync_job에서 이 3-경로 구조가 텔레그램 쪽에만 락이 있어 동시
# 실행되는 사고가 난 걸 발견(jobs/infra_jobs.py::flow_sync_lock 참고), 같은
# 패턴을 가진 stage/screener/youtube도 예방적으로 고친다. 호출부가 아니라
# 자원을 실제로 쓰는 함수 자체에 락을 둬서 어느 경로로 불러도 자동으로
# 보호받게 한다.
stage_job_lock: asyncio.Lock = asyncio.Lock()


async def daily_stage_job(db_pool) -> set[str]:
    if stage_job_lock.locked():
        logger.warning("[3단계] 이미 다른 실행이 진행 중 — 이번 트리거는 건너뜀 (중복 실행 방지)")
        return set()
    async with stage_job_lock:
        return await _daily_stage_job_impl(db_pool)


async def _daily_stage_job_impl(db_pool) -> set[str]:
    if not db_pool:
        logger.warning("[3단계] DB 풀 없음 — 스킵")
        return set()

    loop = asyncio.get_running_loop()
    logger.info("[3단계] 일별 분류 시작")

    # 1. Ichimoku 비교용 결과 (D4: load_chart_signals_latest 사용 — 종목명/has_gapjum 포함)
    _week, ichimoku_rows = await load_chart_signals_latest(db_pool)
    logger.info("[3단계] Ichimoku 비교 풀: %d종목 (week_of=%s)", len(ichimoku_rows), _week)

    # 2. 전 종목 티커 목록 (KOSPI + KOSDAQ, ~2770개) — 캡 없이 스크리너와 동일하게 전종목 스캔.
    # 스크리너(analysis/chart_screener.py)가 이미 동일한 yfinance 개별 호출 패턴으로 매일
    # 전종목(2763개)을 SCREENER_WORKERS만으로 문제없이 처리하는 게 실증돼 있어, 예전에 있던
    # 150종목 캡/KOSPI-KOSDAQ 순환 로직(2026-04 도입, 실측 없는 추정 기반)을 제거했다.
    all_tickers: list[tuple[str, str, str]] = await loop.run_in_executor(
        None, get_all_tickers, None
    )
    logger.info("[3단계] 분류 대상: %d종목 / SCREENER_WORKERS=%s",
                len(all_tickers), os.environ.get("SCREENER_WORKERS", "1"))

    today = date.today()
    workers = int(os.environ.get("SCREENER_WORKERS", "1"))

    # 2b. daily OHLCV (60일, yfinance, 병렬)
    price_map: dict[str, pd.DataFrame] = {}
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

    # 3. daily_flow 20일 배치 로드 (실패해도 빈 map으로 계속 — 라이브 잡 생존 우선)
    since_20d = today - timedelta(days=20)
    flow_map: dict[str, pd.DataFrame] = {}
    try:
        flow_map = await load_flow_range(db_pool, since_20d, today)
    except Exception as e:
        logger.warning("[3단계] flow_map 로드 실패: %s", e)

    # 3b. listed_shares 배치 로드 (krx_listings, yfinance_symbol 기준)
    listed_shares_map: dict[str, int] = {}
    try:
        listed_shares_map = await load_listed_shares(db_pool)
        logger.info("[3단계] listed_shares 로드: %d종목", len(listed_shares_map))
    except Exception as e:
        logger.warning("[3단계] listed_shares 로드 실패: %s", e)

    # 4. s1_history / s2_history 배치 조회
    all_ticker_list = [t for t, _, _ in all_tickers]
    since_14d = today - timedelta(days=14)
    s1_history = await get_stage1_history(db_pool, all_ticker_list, since_14d)
    s2_history = await get_stage2_history(db_pool, all_ticker_list, since_14d)
    logger.info("[3단계] s1_history: %d종목 이력, s2_history: %d종목 이력",
                len(s1_history), len(s2_history))

    # 5. classify_stage_v15() 병렬 실행
    # market은 종목코드 접미사(.KQ/.KS)로 판정해야 하는데 sector(s)로 판정하고
    # 있었음 — sector는 항상 빈 문자열이라 모든 종목이 "KOSPI"로 분류돼 KOSDAQ
    # 전용 임계값(_S1_THRESHOLD["KOSDAQ"]=0.07)이 한 번도 적용되지 않았음 (2026-07 발견).
    market_map = {t: ("KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t, _, _ in all_tickers}

    def _classify_one(ticker: str) -> tuple[str, Optional[int], bool]:
        price_df = price_map.get(ticker)
        flow_df  = flow_map.get(ticker, pd.DataFrame())
        if price_df is None:
            return ticker, None, False
        market = market_map.get(ticker, "KOSPI")
        stage = classify_stage_v15(
            ticker, price_df, flow_df, s1_history, s2_history, market, listed_shares_map
        )
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

                    price_df_for_score = price_map.get(ticker)
                    upsert_rows.append(build_row(
                        ticker, stage, peakout,
                        price_df_for_score if price_df_for_score is not None else pd.DataFrame(),
                        today,
                        flow_slice=flow_map.get(ticker, pd.DataFrame()),
                        listed_shares=listed_shares_map.get(ticker),
                    ))
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
