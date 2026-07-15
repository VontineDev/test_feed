"""백테스트 OHLCV/지수 수집 (backtest_engine.py에서 이동, Phase C).

yfinance 기반 — dsn 설정 시 core.ohlcv_cache가 우선하고 이쪽은 폴백.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ── 데이터 수집 ──────────────────────────────────────────────────

def _fetch_single_ohlcv(
    ticker: str, fetch_start: date, fetch_end: date
) -> Optional[pd.DataFrame]:
    """단일 종목 일봉 OHLCV 수집 (yfinance)."""
    try:
        import yfinance as yf
        tkr = yf.Ticker(ticker)
        df  = tkr.history(
            start=fetch_start.isoformat(),
            end=fetch_end.isoformat(),
            interval="1d",
            auto_adjust=True,
        )
        if df.empty or df["Close"].notna().sum() < 30:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        df.index = pd.to_datetime(df.index, utc=True)
        return df
    except Exception as e:
        logger.debug("[백테스트] %s 수집 실패: %s", ticker, e)
        return None


def _fetch_index(
    symbol: str, fetch_start: date, fetch_end: date
) -> Optional[pd.DataFrame]:
    """지수 일봉 데이터 수집 (벤치마크 비교용)."""
    try:
        import yfinance as yf
        tkr = yf.Ticker(symbol)
        df  = tkr.history(
            start=fetch_start.isoformat(),
            end=fetch_end.isoformat(),
            interval="1d",
            auto_adjust=True,
        )
        if df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        df.index = pd.to_datetime(df.index, utc=True)
        return df
    except Exception as e:
        logger.warning("[백테스트] %s 지수 수집 실패: %s", symbol, e)
        return None


def _batch_fetch_ohlcv(
    tickers: list[str], fetch_start: date, fetch_end: date, workers: int
) -> dict[str, pd.DataFrame]:
    """티커 목록을 병렬로 OHLCV 수집."""
    result: dict[str, pd.DataFrame] = {}

    def _fetch(t: str) -> tuple[str, Optional[pd.DataFrame]]:
        return t, _fetch_single_ohlcv(t, fetch_start, fetch_end)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_fetch, t): t for t in tickers}
        for fut in as_completed(futures):
            t, df = fut.result()
            if df is not None:
                result[t] = df

    logger.info("[백테스트] OHLCV %d/%d 수집 완료", len(result), len(tickers))
    return result


