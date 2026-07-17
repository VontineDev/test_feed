"""jobs/stage_shared.py — stage_backfill.py와 stage_job.py(라이브)가 공유하는
순수 플러밍 헬퍼.

classify_stage_v15 자체(단일 소스)는 analysis.stage_classifier에 있고 건드리지
않는다. 여기 있는 것은 그 앞뒤의 데이터 정규화/조회/행 조립 — 두 잡에서
byte-identical하게 중복돼 있던 부분만 추출.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Callable, Optional

import asyncpg
import pandas as pd

from analysis.stage_classifier import compute_foreign_chg_pct, compute_flow_score

logger = logging.getLogger(__name__)


def normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """yfinance 응답 DataFrame 정규화: MultiIndex 컬럼 평탄화 + 컬럼 선택 + UTC tz 인덱스."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
    df.index = pd.to_datetime(df.index, utc=True)
    return df


def prefetch_ohlcv(
    tickers: list[str],
    fetch_fn: Callable[[str], Optional[pd.DataFrame]],
    workers: int,
    log_tag: str,
) -> dict[str, pd.DataFrame]:
    """티커 목록 OHLCV 병렬 1회 수집 (fetch-once). {ticker: df} 반환.

    실패(예외/None) 종목은 결과에서 제외하고 계속 진행 — 한 종목의 실패가
    전체 수집을 중단시키지 않는다. 종목별 수집 로직은 fetch_fn으로 주입
    (stage_backfill: 일봉 윈도우, screener_backfill: 주봉 3y).
    """
    result: dict[str, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_fn, t): t for t in tickers}
        done = 0
        for fut in as_completed(futs):
            done += 1
            if done % 200 == 0:
                logger.info("%s OHLCV 진행: %d/%d (성공 %d)", log_tag, done, len(tickers), len(result))
            ticker = futs[fut]
            try:
                df = fut.result()
                if df is not None:
                    result[ticker] = df
            except Exception as e:
                logger.debug("%s %s: %s", log_tag, ticker, e)
    logger.info("%s OHLCV 수집 완료: %d/%d종목", log_tag, len(result), len(tickers))
    return result


async def load_flow_range(
    pool: asyncpg.Pool, window_start: date, window_end: date
) -> dict[str, pd.DataFrame]:
    """daily_flow 를 윈도우 전체에 대해 1회 bulk 로드 → 종목별 DataFrame.

    columns: foreign_net, inst_net, foreign_streak, inst_streak, personal_net
    (DatetimeIndex). personal_net은 classify_stage_v15(Stage1/2 개인 수급 조건)용.
    """
    flow_map: dict[str, list[dict]] = {}
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT ticker, trade_date, foreign_net, inst_net,
                   foreign_streak, inst_streak, personal_net
            FROM   daily_flow
            WHERE  trade_date >= $1 AND trade_date <= $2
            ORDER  BY trade_date ASC
            """,
            window_start, window_end,
        )
    for row in rows:
        flow_map.setdefault(row["ticker"], []).append(dict(row))

    out: dict[str, pd.DataFrame] = {}
    for t, rlist in flow_map.items():
        df = pd.DataFrame(rlist).set_index("trade_date")
        df.index = pd.to_datetime(df.index)
        out[t] = df
    logger.info("[stage공유] daily_flow 로드: %d종목 (%s ~ %s)", len(out), window_start, window_end)
    return out


async def load_listed_shares(pool: asyncpg.Pool) -> dict[str, int]:
    """krx_listings 에서 yfinance_symbol → listed_shares 1회 bulk 로드.

    classify_stage_v15의 유통주식수 0.2% 수급 조건(Stage1/3)에 사용.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT yfinance_symbol, listed_shares FROM krx_listings WHERE listed_shares IS NOT NULL"
        )
    return {r["yfinance_symbol"]: r["listed_shares"] for r in rows}


def build_row(
    ticker: str,
    stage: int,
    peakout: bool,
    price_slice: pd.DataFrame,
    as_of_date: date,
    flow_slice: Optional[pd.DataFrame] = None,
    listed_shares: Optional[int] = None,
) -> dict:
    """classify 결과 → save_stage_classifications upsert 행 (live job과 동일 매핑)."""
    s1_high = s1_vol = s1_txamt = None
    if stage == 1 and not price_slice.empty:
        last = price_slice.iloc[-1]
        s1_high  = float(last.get("High") or last.get("Close") or 0) or None
        _vol     = int(last.get("Volume") or 0)
        _close   = float(last.get("Close") or 0)
        s1_vol   = _vol or None
        s1_txamt = int(_vol * _close) or None
    flow_for_score = flow_slice if flow_slice is not None else pd.DataFrame()
    return {
        "ticker":               ticker,
        "classified_date":      as_of_date,
        "stage":                stage,
        "s1_entry_date":        as_of_date if stage == 1 else None,
        "s1_high":              s1_high,
        "s1_volume":            s1_vol,
        "s1_txamt":             s1_txamt,
        "peakout_flag":         peakout,
        "foreign_chg_14d_pct":  compute_foreign_chg_pct(flow_for_score, listed_shares),
        "flow_score":           compute_flow_score(flow_for_score, price_slice),
    }
