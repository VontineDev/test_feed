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
from typing import Callable, Optional, cast

import pandas as pd
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


# core/db_sync.py로 통합 (2026-07 Phase B) — 기존 이름 유지
from core.db_sync import connect as _connect


def _get_coverage(
    dsn: str, symbols: list[str], start: date, end: date
) -> dict[str, tuple[date, date, int]]:
    """심볼별 캐시 커버리지 단일 쿼리. {symbol: (min_date, max_date, count)}

    `start`는 하한으로 쓰지 않는다 — `WHERE date BETWEEN start AND end`로
    자르면 MIN(date)가 정의상 절대 start보다 작아질 수 없어서, 실제로는
    start 이전까지 데이터가 있어도 "커버리지 없음"으로 오판하게 된다
    (2026-08-10 발견: daily_ohlcv를 2022년치까지 백필한 직후에도 캐시가
    계속 히트 0으로 잡히던 진짜 원인 — 종목코드 접두사 버그와는 별개).
    그래서 하한 없이 `date <= end`만 걸어 진짜 최초 데이터 날짜를 구하고,
    `_classify_coverage()`에서 그 값과 fetch_start를 비교한다.
    """
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, MIN(date)::date, MAX(date)::date, COUNT(*)
                FROM daily_ohlcv
                WHERE symbol = ANY(%s) AND date <= %s
                GROUP BY symbol
                """,
                (symbols, end),
            )
            return {row[0]: (row[1], row[2], int(row[3])) for row in cur.fetchall()}
    finally:
        conn.close()


def _load_df_bulk(
    dsn: str, symbols: list[str], start: date, end: date
) -> dict[str, pd.DataFrame]:
    """DB에서 여러 종목 일봉을 단일 쿼리로 로드 (커넥션 1개 재사용).

    2026-08-10 발견: 캐시 히트 로딩이 `_load_df()`를 종목마다 호출해 매번
    새 DB 커넥션을 열고 있었다 — Supabase 풀러 네트워크 왕복 때문에 종목당
    ~400ms, 1583종목이면 순차로 10분 넘게 걸림(daily_ohlcv를 제대로
    백필해 캐시가 실제로 히트하기 시작하자 이 병목이 처음 드러남). 단일
    쿼리 + Python 쪽 groupby로 교체 — 커넥션 1개, 쿼리 1번.
    """
    if not symbols:
        return {}
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, date, open, high, low, close, volume
                FROM daily_ohlcv
                WHERE symbol = ANY(%s) AND date BETWEEN %s AND %s
                ORDER BY symbol, date
                """,
                (symbols, start, end),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return {}

    full = pd.DataFrame(
        rows,
        columns=pd.Index(["Symbol", "Date", "Open", "High", "Low", "Close", "Volume"]),
    )
    full["Date"] = pd.to_datetime(full["Date"], utc=True)

    result: dict[str, pd.DataFrame] = {}
    for sym, g in full.groupby("Symbol", sort=False):
        df = g.drop(columns=["Symbol"]).set_index("Date")
        if df["Close"].notna().sum() < 30:
            continue
        result[str(sym)] = df
    return result


def _save_df(dsn: str, symbol: str, market: str, df: pd.DataFrame) -> None:
    """일봉 DataFrame을 daily_ohlcv에 upsert (충돌 시 OHLCV 갱신)."""
    rows = []
    for ts, row in df.iterrows():
        d = cast(pd.Timestamp, ts).date()
        close = cast(float, row.get("Close", float("nan")))
        if pd.isna(close):
            continue
        open_ = cast(float, row.get("Open", float("nan")))
        high_ = cast(float, row.get("High", float("nan")))
        low_  = cast(float, row.get("Low",  float("nan")))
        vol_  = cast(float, row.get("Volume", float("nan")))
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


def _classify_coverage(
    symbols: list[str],
    coverage: dict[str, tuple[date, date, int]],
    fetch_start: date,
    effective_end: date,
    coverage_slack: timedelta = timedelta(days=4),
) -> tuple[list[str], list[str]]:
    """커버리지 정보로 심볼을 히트/미스로 분류 (순수 함수 — DB/네트워크 없음, 테스트 대상).

    cov[1](DB의 마지막 날짜)이 effective_end보다 최대 coverage_slack일 이내로
    앞서 있어도 히트로 간주한다 — 주말/공휴일에는 거래가 없어 캘린더상
    "어제"와 "마지막 거래일"이 다를 수 있기 때문(2026-08-10 발견: 월요일마다
    데이터가 완전히 최신이어도 항상 캐시 히트 0으로 잡히던 원인 — daily_ohlcv
    KRX 백필 직후에도 재현돼 캐시 레이어 자체의 버그로 확인됨).
    """
    hits: list[str] = []
    misses: list[str] = []
    for sym in symbols:
        cov = coverage.get(sym)
        if (
            cov is not None
            and cov[0] <= fetch_start
            and cov[1] >= effective_end - coverage_slack
            and cov[2] >= 30
        ):
            hits.append(sym)
        else:
            misses.append(sym)
    return hits, misses


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

    hits, misses = _classify_coverage(symbols, coverage, fetch_start, effective_end)

    logger.info(
        "[cache] 캐시 히트: %d  미스: %d  전체: %d",
        len(hits), len(misses), len(symbols),
    )

    result: dict[str, pd.DataFrame] = {}

    # 2. 캐시 히트 — DB 로드 (단일 쿼리, 커넥션 재사용)
    if hits:
        try:
            result = _load_df_bulk(dsn, hits, fetch_start, fetch_end)
        except Exception as e:
            logger.warning("[cache] 벌크 DB 로드 실패 → yfinance fallback: %s", e)
            result = {}
        db_fail = [sym for sym in hits if sym not in result]
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
    from data.krx_openapi import KRXOpenAPIClient
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
                        r["symbol"],
                        # VARCHAR(4) 제약: KOSPI/KOSDAQ → KR, IDX 유지
                        "IDX" if r["market"] == "IDX" else "KR",
                        r["date"],
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

FlowKey    = tuple[str, date]                                              # (yfinance_symbol, trade_date)
FlowValue  = tuple[Optional[int], Optional[int], Optional[int]]            # (foreign_net, inst_net, personal_net)
StreakValue = tuple[Optional[int], Optional[int]]                           # (foreign_streak, inst_streak)


def load_flow_data(
    dsn: str,
    tickers: list[str],
    start: date,
    end: date,
) -> dict[FlowKey, FlowValue]:
    """daily_flow 테이블에서 수급 데이터 로드.

    반환: {(ticker, trade_date): (foreign_net, inst_net, personal_net)}
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
                SELECT ticker, trade_date, foreign_net, inst_net, personal_net
                FROM daily_flow
                WHERE ticker = ANY(%s)
                  AND trade_date BETWEEN %s AND %s
                """,
                (tickers, start, end),
            )
            return {(row[0], row[1]): (row[2], row[3], row[4]) for row in cur.fetchall()}
    finally:
        conn.close()


def load_flow_streaks(
    dsn: str,
    tickers: list[str],
    start: date,
    end: date,
) -> dict[FlowKey, StreakValue]:
    """daily_flow에서 수급 연속 streak 로드.

    반환: {(ticker, trade_date): (foreign_streak, inst_streak)}
    """
    if not tickers:
        return {}

    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ticker, trade_date, foreign_streak, inst_streak
                FROM daily_flow
                WHERE ticker = ANY(%s)
                  AND trade_date BETWEEN %s AND %s
                """,
                (tickers, start, end),
            )
            return {(row[0], row[1]): (row[2], row[3]) for row in cur.fetchall()}
    finally:
        conn.close()


def load_listed_shares(dsn: str) -> dict[str, int]:
    """krx_listings에서 상장주식수 로드.

    반환: {yfinance_symbol: listed_shares}
    상장주식수는 유통주식수의 근사치 (유상증자/분할 시에만 변동 → 주 1회 sync로 충분).
    """
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT yfinance_symbol, listed_shares
                FROM krx_listings
                WHERE listed_shares IS NOT NULL AND listed_shares > 0
                """
            )
            return {row[0]: int(row[1]) for row in cur.fetchall()}
    finally:
        conn.close()


# ── 시간외 데이터 로드 ─────────────────────────────────────────

AftermarketKey   = tuple[str, date]                          # (yfinance_symbol, trade_date)
AftermarketValue = tuple[Optional[int], Optional[int], Optional[float]]
# (after_volume, after_close, after_chg_pct)


def load_aftermarket_data(
    dsn: str,
    tickers: list[str],
    start: date,
    end: date,
) -> dict[AftermarketKey, AftermarketValue]:
    """aftermarket_snap 테이블에서 시간외 데이터 로드.

    반환: {(ticker, trade_date): (after_volume, after_close, after_chg_pct)}
    값이 없는 날짜(시간외 거래 없음 포함)는 dict에 포함되지 않음.
    """
    if not tickers:
        return {}
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ticker, trade_date, after_volume, after_close, after_chg_pct
                FROM aftermarket_snap
                WHERE ticker = ANY(%s)
                  AND trade_date BETWEEN %s AND %s
                """,
                (tickers, start, end),
            )
            return {
                (row[0], row[1]): (row[2], row[3], float(row[4]) if row[4] is not None else None)
                for row in cur.fetchall()
            }
    finally:
        conn.close()
