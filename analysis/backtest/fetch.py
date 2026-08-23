"""백테스트 OHLCV/지수 수집 (backtest_engine.py에서 이동, Phase C).

yfinance 기반 — dsn 설정 시 core.ohlcv_cache가 우선하고 이쪽은 폴백.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Optional, cast

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
        df = cast(pd.DataFrame, df[["Open", "High", "Low", "Close", "Volume"]]).copy()
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
        df = cast(pd.DataFrame, df[["Open", "High", "Low", "Close", "Volume"]]).copy()
        df.index = pd.to_datetime(df.index, utc=True)
        return df
    except Exception as e:
        logger.warning("[백테스트] %s 지수 수집 실패: %s", symbol, e)
        return None


# 이 비율 밑으로 성공률이 떨어지면 개별종목 결측이 아니라 일괄 장애(예: yfinance
# rate-limit)로 판단해 에러 로그를 남긴다. 2026-08-10 사고(2,763종목 중 4개만
# 성공했는데도 개별 실패가 logger.debug에만 남아 며칠 뒤에야 발견됨) 재발 방지.
_BATCH_FAIL_RATIO_ALERT = 0.90


def _batch_fetch_ohlcv(
    tickers: list[str], fetch_start: date, fetch_end: date, workers: int
) -> dict[str, pd.DataFrame]:
    """티커 목록을 병렬로 OHLCV 수집."""
    result: dict[str, pd.DataFrame] = {}
    failed: list[str] = []

    def _fetch(t: str) -> tuple[str, Optional[pd.DataFrame]]:
        return t, _fetch_single_ohlcv(t, fetch_start, fetch_end)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_fetch, t): t for t in tickers}
        for fut in as_completed(futures):
            t, df = fut.result()
            if df is not None:
                result[t] = df
            else:
                failed.append(t)

    logger.info("[백테스트] OHLCV %d/%d 수집 완료", len(result), len(tickers))

    if tickers:
        fail_ratio = len(failed) / len(tickers)
        if fail_ratio >= _BATCH_FAIL_RATIO_ALERT:
            logger.error(
                "[백테스트] OHLCV 수집 실패율 %.1f%% (%d/%d건 실패) — 개별종목 "
                "결측이 아니라 일괄 장애(rate-limit 등)로 의심됨. 결과가 조용히 "
                "오염될 수 있으니 백테스트 결과를 신뢰하기 전에 확인 필요. "
                "실패 샘플: %s",
                fail_ratio * 100, len(failed), len(tickers), failed[:10],
            )

    return result


