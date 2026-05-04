"""
ohlcv_cache.py — OHLCV 동기 캐시 (psycopg2 → daily_ohlcv)

백테스트 엔진(동기)에서 yfinance 반복 다운로드를 줄이기 위한 캐시 레이어.
daily_ohlcv 테이블은 db.py init_db()에서 자동 생성됨.

캐시 히트 조건:
  DB에 해당 심볼의 데이터가 fetch_start 이전부터 어제까지 있고 30행 이상인 경우.
  히트 → DB 직접 로드 (yfinance 생략)
  미스 → yfinance 수집 후 DB 저장 (다음 실행부터 히트)
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Callable, Optional

import pandas as pd
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


def _connect(dsn: str):
    return psycopg2.connect(dsn)


def _get_coverage(
    dsn: str, symbols: list[str], start: date, end: date
) -> dict[str, tuple[date, date, int]]:
    """심볼별 캐시 커버리지 단일 쿼리. {symbol: (min_date, max_date, count)}"""
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, MIN(date)::date, MAX(date)::date, COUNT(*)
                FROM daily_ohlcv
                WHERE symbol = ANY(%s) AND date BETWEEN %s AND %s
                GROUP BY symbol
                """,
                (symbols, start, end),
            )
            return {row[0]: (row[1], row[2], int(row[3])) for row in cur.fetchall()}
    finally:
        conn.close()


def _load_df(dsn: str, symbol: str, start: date, end: date) -> Optional[pd.DataFrame]:
    """DB에서 단일 종목 일봉 DataFrame 로드. 유효 행 30개 미만이면 None."""
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT date, open, high, low, close, volume
                FROM daily_ohlcv
                WHERE symbol = %s AND date BETWEEN %s AND %s
                ORDER BY date
                """,
                (symbol, start, end),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return None

    df = pd.DataFrame(rows, columns=["Date", "Open", "High", "Low", "Close", "Volume"])
    df["Date"] = pd.to_datetime(df["Date"], utc=True)
    df = df.set_index("Date")
    if df["Close"].notna().sum() < 30:
        return None
    return df


def _save_df(dsn: str, symbol: str, market: str, df: pd.DataFrame) -> None:
    """일봉 DataFrame을 daily_ohlcv에 upsert (충돌 시 OHLCV 갱신)."""
    rows = []
    for ts, row in df.iterrows():
        d = ts.date() if hasattr(ts, "date") else ts
        close = row.get("Close", float("nan"))
        if pd.isna(close):
            continue
        open_ = row.get("Open", float("nan"))
        high_ = row.get("High", float("nan"))
        low_  = row.get("Low",  float("nan"))
        vol_  = row.get("Volume", float("nan"))
        rows.append((
            symbol, market, d,
            None if pd.isna(open_) else float(open_),
            None if pd.isna(high_) else float(high_),
            None if pd.isna(low_)  else float(low_),
            float(close),
            None if pd.isna(vol_) else int(vol_),
            "yfinance",
        ))

    if not rows:
        return

    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO daily_ohlcv
                    (symbol, market, date, open, high, low, close, volume, source)
                VALUES %s
                ON CONFLICT (symbol, date) DO UPDATE SET
                    open       = EXCLUDED.open,
                    high       = EXCLUDED.high,
                    low        = EXCLUDED.low,
                    close      = EXCLUDED.close,
                    volume     = EXCLUDED.volume,
                    fetched_at = now()
                """,
                rows,
            )
        conn.commit()
        logger.debug("[cache] %s %d행 저장", symbol, len(rows))
    finally:
        conn.close()


def batch_fetch_cached(
    tickers: list[tuple[str, str]],
    fetch_start: date,
    fetch_end: date,
    workers: int,
    dsn: str,
    fetch_fn: Callable[[str, date, date], Optional[pd.DataFrame]],
) -> dict[str, pd.DataFrame]:
    """
    캐시 우선 OHLCV 배치 수집.

    1. DB 커버리지 단일 쿼리 → hits / misses 분류
    2. 히트 → DB 로드
    3. 미스 → yfinance 병렬 수집 + DB 저장
    """
    symbols    = [sym for sym, _ in tickers]
    market_of  = {sym: mkt for sym, mkt in tickers}

    # yfinance 데이터는 최대 어제까지만 존재
    yesterday     = date.today() - timedelta(days=1)
    effective_end = min(fetch_end, yesterday)

    # 1. 커버리지 조회
    try:
        coverage = _get_coverage(dsn, symbols, fetch_start, effective_end)
    except Exception as e:
        logger.warning("[cache] 커버리지 조회 실패 → yfinance 직접 수집: %s", e)
        coverage = {}

    hits: list[str] = []
    misses: list[str] = []
    for sym in symbols:
        cov = coverage.get(sym)
        if (
            cov is not None
            and cov[0] <= fetch_start
            and cov[1] >= effective_end
            and cov[2] >= 30
        ):
            hits.append(sym)
        else:
            misses.append(sym)

    logger.info(
        "[cache] 캐시 히트: %d  미스: %d  전체: %d",
        len(hits), len(misses), len(symbols),
    )

    result: dict[str, pd.DataFrame] = {}

    # 2. 캐시 히트 — DB 로드
    db_fail: list[str] = []
    for sym in hits:
        try:
            df = _load_df(dsn, sym, fetch_start, fetch_end)
            if df is not None:
                result[sym] = df
            else:
                db_fail.append(sym)
        except Exception as e:
            logger.debug("[cache] %s DB 로드 실패: %s", sym, e)
            db_fail.append(sym)

    if db_fail:
        logger.warning("[cache] DB 로드 실패 → yfinance fallback: %d개", len(db_fail))
        misses.extend(db_fail)

    # 3. 캐시 미스 — yfinance 병렬 수집 + DB 저장
    def _fetch_and_cache(sym: str) -> tuple[str, Optional[pd.DataFrame]]:
        df = fetch_fn(sym, fetch_start, fetch_end)
        if df is not None:
            try:
                _save_df(dsn, sym, market_of[sym], df)
            except Exception as e:
                logger.debug("[cache] %s DB 저장 실패 (무시): %s", sym, e)
        return sym, df

    if misses:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_fetch_and_cache, sym): sym for sym in misses}
            for fut in as_completed(futures):
                sym, df = fut.result()
                if df is not None:
                    result[sym] = df

    return result


def fetch_index_cached(
    symbol: str,
    market: str,
    fetch_start: date,
    fetch_end: date,
    dsn: str,
    fetch_fn: Callable[[str, date, date], Optional[pd.DataFrame]],
) -> Optional[pd.DataFrame]:
    """단일 지수 심볼 캐시 (^KS11 등). batch_fetch_cached 래퍼."""
    batch = batch_fetch_cached(
        [(symbol, market)],
        fetch_start, fetch_end,
        workers=1,
        dsn=dsn,
        fetch_fn=fetch_fn,
    )
    return batch.get(symbol)


# ── KRX OpenAPI 일별 전 종목 채우기 ──────────────────────────────

def fill_daily_from_krx(
    dsn: str,
    trade_date: date,
    appkey: Optional[str] = None,
) -> int:
    """KRX Open API로 하루치 전 종목 OHLCV를 가져와 daily_ohlcv에 저장.

    스케줄러에서 매일 장 마감 후 호출하면 다음 backtest부터 캐시 히트율 급상승.
    반환: 저장된 행 수 (기존 행 upsert 포함).
    """
    from krx_openapi import KRXOpenAPIClient
    import os

    key = appkey or os.environ.get("KRX_OPENAPI_KEY", "")
    if not key:
        logger.warning("[cache/krx] KRX_OPENAPI_KEY 미설정 — 채우기 건너뜀")
        return 0

    bas_dd = trade_date.strftime("%Y%m%d")
    client = KRXOpenAPIClient(key)

    # 주식 OHLCV
    rows = client.get_daily_ohlcv_all(bas_dd)
    # KOSPI 지수
    idx_row = client.get_kospi_index_ohlcv(bas_dd)
    if idx_row:
        rows.append(idx_row)

    if not rows:
        logger.info("[cache/krx] %s 데이터 없음 (휴장일?)", bas_dd)
        return 0

    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO daily_ohlcv
                    (symbol, market, date, open, high, low, close, volume, source)
                VALUES %s
                ON CONFLICT (symbol, date) DO UPDATE SET
                    open       = EXCLUDED.open,
                    high       = EXCLUDED.high,
                    low        = EXCLUDED.low,
                    close      = EXCLUDED.close,
                    volume     = EXCLUDED.volume,
                    fetched_at = now()
                """,
                [
                    (
                        r["symbol"], r["market"], r["date"],
                        r.get("open"), r.get("high"), r.get("low"),
                        r["close"], r.get("volume"), "krx-openapi",
                    )
                    for r in rows
                ],
            )
        conn.commit()
        logger.info("[cache/krx] %s %d종목 저장", bas_dd, len(rows))
        return len(rows)
    finally:
        conn.close()


# ── 수급 데이터 로드 ──────────────────────────────────────────

FlowKey   = tuple[str, date]                         # (yfinance_symbol, trade_date)
FlowValue = tuple[Optional[int], Optional[int]]      # (foreign_net, inst_net)


def load_flow_data(
    dsn: str,
    tickers: list[str],
    start: date,
    end: date,
) -> dict[FlowKey, FlowValue]:
    """daily_flow 테이블에서 수급 데이터 로드.

    반환: {(ticker, trade_date): (foreign_net, inst_net)}
    ticker = yfinance 심볼 (005930.KS / 035720.KQ).
    값이 없는 날짜는 dict에 포함되지 않으므로 호출자가 .get()으로 확인.
    """
    if not tickers:
        return {}

    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ticker, trade_date, foreign_net, inst_net
                FROM daily_flow
                WHERE ticker = ANY(%s)
                  AND trade_date BETWEEN %s AND %s
                """,
                (tickers, start, end),
            )
            return {(row[0], row[1]): (row[2], row[3]) for row in cur.fetchall()}
    finally:
        conn.close()
