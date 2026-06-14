"""
stage_backfill.py — stage_classifications 과거 이력 백필

라이브 daily_stage_job은 매일 classified_date=today 만 적재하므로 과거 history가
없다(현재 ~5주치). 조합 백테스트(JOIN 경로)는 (ticker, ISO주) 단위 신호 시계열이
필요하므로, 라이브와 **동일한** analysis.stage_classifier.classify_stage 를 과거
주차마다 재실행해 stage_classifications 에 upsert 한다 (single source of truth).

설계 (plan-eng-review 2026-06-14, T1):
  - 주간 cadence: classified_date = 각 ISO주의 금요일 (한 (ticker,week) 1행).
    → chart_signals.week_of / compose ISO-week grain 과 정렬.
  - 시간순 처리: 주 W를 적재한 뒤 W+1의 get_stage1_history가 그 결과를 본다.
    Stage 2/3는 직전 S1 이력이 필요하므로 chronological 필수. 윈도우 시작
    ~2주는 prior 이력이 없어 Stage 2/3 과소탐지 — cold start, 의도된 동작.
  - fetch-once: 종목별 일봉 1회 수집 + daily_flow 1회 bulk 로드 → 주마다 메모리
    슬라이스 (per-(ticker,week) yfinance 재호출 N+1 회피).

데이터 흐름:
    get_all_tickers ──► 종목 목록 (KOSPI+KOSDAQ)
         │
         ▼ (병렬, fetch-once)
    yfinance 일봉  ──► price_map[ticker]  (fetch_start ~ end, ~2y+)
    daily_flow     ──► flow_map[ticker]   (1 bulk query)
         │
         ▼  for ISO주 W in [start..end] 시간순:
    get_stage1_history(친구 14d) ─► s1_history  (직전 백필분 포함)
    각 종목: price/flow 를 W금요일까지 슬라이스
             classify_stage(ticker, price_slice, flow_slice, s1_history, market)
             check_peakout(...) if stage==3
         │
         ▼
    save_stage_classifications(rows)  # ON CONFLICT (ticker, classified_date)

제약:
    - yfinance는 현재 시점 조정가(배당락 등)를 반환 → 당시 실제가와 미세 차이.
      screener_backfill.py 와 동일한 한계.
    - 주간 cadence는 라이브(일별)보다 s1_history가 성겨 Stage 2/3 탐지가 보수적.
      compose 백테스트가 ISO-week grain이라 의도된 선택. 패리티 회귀(T4)로 검증.

실행:
    python jobs/stage_backfill.py --start 2024-01-01 --end 2026-05-01
    python jobs/stage_backfill.py --start 2024-01-01 --end 2026-05-01 --max-tickers 200
    python jobs/stage_backfill.py --start 2024-01-01 --end 2026-05-01 --skip-existing
    python jobs/stage_backfill.py --start 2024-01-01 --end 2026-05-01 --workers 4 --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

# 프로젝트 루트를 sys.path에 추가
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

load_dotenv(os.path.join(ROOT, ".env"))

import asyncpg

from analysis.chart_screener import get_all_tickers
from analysis.stage_classifier import classify_stage, check_peakout
from core.db import save_stage_classifications, get_stage1_history

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# classify_stage가 안정적으로 동작하려면 최소 일봉 수 (MA60 + 여유).
_MIN_DAILY_BARS = 60
# 일봉 수집 시 윈도우 시작 이전 lookback (MA60/streak 워밍업).
_FETCH_LOOKBACK_DAYS = 200
# Stage 2/3 직전 S1 조회 윈도우 (라이브 14d와 동일).
_S1_HISTORY_DAYS = 14


# ── DB 연결 (screener_backfill.py 와 동일 패턴) ────────────────
async def get_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 5432)),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
        min_size=2, max_size=5,
    )


# ── ISO 주차 유틸 ─────────────────────────────────────────────
def iso_fridays(start: date, end: date) -> list[date]:
    """[start, end] 범위에 걸치는 각 ISO주의 금요일 목록 (오름차순).

    금요일을 주 대표 as-of 날짜로 사용 — (ticker, classified_date) PK가
    주마다 결정적이고, 재실행 시 upsert가 깔끔하게 갱신된다.
    """
    fridays: list[date] = []
    # start가 속한 주의 금요일부터 시작
    y, w, _ = start.isocalendar()
    cur = date.fromisocalendar(y, w, 5)
    if cur < start:
        cur += timedelta(days=7)
    while cur <= end:
        fridays.append(cur)
        cur += timedelta(days=7)
    return fridays


def week_label(d: date) -> str:
    """date → 'IYYY-Www' (find_missing_weeks / compose grain 과 동일)."""
    iso = d.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


# ── 데이터 수집 ───────────────────────────────────────────────
def fetch_daily_ohlcv(ticker: str, fetch_start: date, fetch_end: date) -> Optional[pd.DataFrame]:
    """종목 일봉 1회 수집 (fetch_start ~ fetch_end). UTC tz-aware 인덱스."""
    try:
        df = yf.Ticker(ticker).history(
            start=fetch_start.isoformat(),
            end=fetch_end.isoformat(),
            interval="1d",
            auto_adjust=True,
        )
        if df.empty or df["Close"].notna().sum() < _MIN_DAILY_BARS:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        df.index = pd.to_datetime(df.index, utc=True)
        return df
    except Exception as e:
        logger.debug("[stage백필] %s 일봉 수집 실패: %s", ticker, e)
        return None


def batch_fetch_ohlcv(
    tickers: list[str], fetch_start: date, fetch_end: date, workers: int
) -> dict[str, pd.DataFrame]:
    """티커 목록 일봉 병렬 수집 (fetch-once)."""
    result: dict[str, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch_daily_ohlcv, t, fetch_start, fetch_end): t for t in tickers}
        done = 0
        for fut in as_completed(futures):
            done += 1
            if done % 200 == 0:
                logger.info("[stage백필] OHLCV 진행: %d/%d (성공 %d)", done, len(tickers), len(result))
            t = futures[fut]
            df = fut.result()
            if df is not None:
                result[t] = df
    logger.info("[stage백필] OHLCV 수집 완료: %d/%d종목", len(result), len(tickers))
    return result


async def load_flow_range(
    pool: asyncpg.Pool, window_start: date, window_end: date
) -> dict[str, pd.DataFrame]:
    """daily_flow 를 윈도우 전체에 대해 1회 bulk 로드 → 종목별 DataFrame.

    columns: foreign_net, inst_net, foreign_streak, inst_streak (DatetimeIndex).
    """
    flow_map: dict[str, list[dict]] = {}
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT ticker, trade_date, foreign_net, inst_net,
                   foreign_streak, inst_streak
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
    logger.info("[stage백필] daily_flow 로드: %d종목 (%s ~ %s)", len(out), window_start, window_end)
    return out


# ── 슬라이스 헬퍼 ─────────────────────────────────────────────
def slice_until(df: pd.DataFrame, as_of: date) -> pd.DataFrame:
    """index 날짜가 as_of(포함) 이하인 행만 반환. tz 유무 모두 처리."""
    cutoff = pd.Timestamp(as_of) + pd.Timedelta(days=1)  # as_of 자정 다음날 미만 = as_of 포함
    idx = df.index
    if getattr(idx, "tz", None) is not None:
        cutoff = cutoff.tz_localize(idx.tz) if cutoff.tzinfo is None else cutoff.tz_convert(idx.tz)
    return df[idx < cutoff]


def build_row(ticker: str, stage: int, peakout: bool, price_slice: pd.DataFrame, friday: date) -> dict:
    """classify 결과 → save_stage_classifications upsert 행 (live job과 동일 매핑)."""
    s1_high = s1_vol = s1_txamt = None
    if stage == 1 and not price_slice.empty:
        last = price_slice.iloc[-1]
        s1_high  = float(last.get("High") or last.get("Close") or 0) or None
        _vol     = int(last.get("Volume") or 0)
        _close   = float(last.get("Close") or 0)
        s1_vol   = _vol or None
        s1_txamt = int(_vol * _close) or None
    return {
        "ticker":          ticker,
        "classified_date": friday,
        "stage":           stage,
        "s1_entry_date":   friday if stage == 1 else None,
        "s1_high":         s1_high,
        "s1_volume":       s1_vol,
        "s1_txamt":        s1_txamt,
        "peakout_flag":    peakout,
    }


# ── 단일 주차 분류 ────────────────────────────────────────────
def classify_week(
    tickers: list[tuple[str, str, str]],
    friday: date,
    price_map: dict[str, pd.DataFrame],
    flow_map: dict[str, pd.DataFrame],
    s1_history: dict[str, list[dict]],
    workers: int,
) -> list[dict]:
    """friday 시점으로 전 종목 분류 → upsert 행 목록 (stage 미탐지는 제외)."""
    market_map = {t: ("KOSDAQ" if s.endswith(".KQ") else "KOSPI") for t, _, s in tickers}

    def _one(ticker: str) -> Optional[dict]:
        price_df = price_map.get(ticker)
        if price_df is None:
            return None
        price_slice = slice_until(price_df, friday)
        if price_slice["Close"].notna().sum() < _MIN_DAILY_BARS:
            return None  # 해당 시점까지 데이터 부족 (상장 전 등)
        flow_full = flow_map.get(ticker)
        flow_slice = slice_until(flow_full, friday) if flow_full is not None else pd.DataFrame()
        market = market_map.get(ticker, "KOSPI")
        try:
            stage = classify_stage(ticker, price_slice, flow_slice, s1_history, market)
        except Exception as e:
            logger.debug("[stage백필] %s classify 오류: %s", ticker, e)
            return None
        if stage is None:
            return None
        peakout = False
        if stage == 3:
            try:
                peakout = check_peakout(ticker, flow_slice, price_slice)
            except Exception:
                peakout = False
        return build_row(ticker, stage, peakout, price_slice, friday)

    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_one, t): t for t, _, _ in tickers}
        for fut in as_completed(futures):
            try:
                r = fut.result()
                if r is not None:
                    rows.append(r)
            except Exception as e:
                logger.debug("[stage백필] %s: %s", futures[fut], e)
    return rows


async def existing_weeks(pool: asyncpg.Pool) -> set[str]:
    """stage_classifications에 이미 존재하는 ISO 주차 집합 ('IYYY-Www')."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT TO_CHAR(classified_date, 'IYYY-\"W\"IW') AS w "
            "FROM stage_classifications"
        )
    return {r["w"] for r in rows}


# ── 메인 ──────────────────────────────────────────────────────
async def main(args: argparse.Namespace) -> None:
    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)
    if start >= end:
        logger.error("--start 는 --end 보다 이전이어야 합니다")
        return

    fridays = iso_fridays(start, end)
    # 미래(데이터 없음) 주차 제외
    today = date.today()
    fridays = [f for f in fridays if f <= today]
    if not fridays:
        logger.info("백필 대상 주차 없음")
        return

    pool = await get_pool()

    if args.skip_existing:
        have = await existing_weeks(pool)
        before = len(fridays)
        fridays = [f for f in fridays if week_label(f) not in have]
        logger.info("[stage백필] --skip-existing: %d주 중 %d주 신규", before, len(fridays))
        if not fridays:
            logger.info("모든 주차 이미 존재 — 백필 불필요")
            await pool.close()
            return

    tickers = get_all_tickers(None)
    if not tickers:
        logger.error("종목 목록 조회 실패")
        await pool.close()
        return
    if args.max_tickers and args.max_tickers > 0:
        tickers = tickers[: args.max_tickers]
    ticker_syms = [t for t, _, _ in tickers]
    logger.info("[stage백필] 대상 %d종목 · %d주 (%s ~ %s)",
                len(tickers), len(fridays), week_label(fridays[0]), week_label(fridays[-1]))

    if args.dry_run:
        logger.info("[dry-run] 종목/주차만 산출하고 종료")
        await pool.close()
        return

    # fetch-once: 일봉 + daily_flow
    fetch_start = start - timedelta(days=_FETCH_LOOKBACK_DAYS)
    fetch_end   = end + timedelta(days=2)
    price_map = batch_fetch_ohlcv(ticker_syms, fetch_start, fetch_end, args.workers)
    if not price_map:
        logger.error("[stage백필] OHLCV 수집 0건 — 중단")
        await pool.close()
        return
    flow_map = await load_flow_range(pool, fetch_start, end)

    total_saved = 0
    for friday in fridays:  # 시간순 — s1_history가 직전 주 결과를 본다
        since = friday - timedelta(days=_S1_HISTORY_DAYS)
        s1_history = await get_stage1_history(pool, ticker_syms, since)
        rows = classify_week(tickers, friday, price_map, flow_map, s1_history, args.workers)
        if rows:
            saved = await save_stage_classifications(pool, rows)
            total_saved += saved
            n1 = sum(1 for r in rows if r["stage"] == 1)
            n2 = sum(1 for r in rows if r["stage"] == 2)
            n3 = sum(1 for r in rows if r["stage"] == 3)
            logger.info("[stage백필] %s → %d건 (S1:%d S2:%d S3:%d)",
                        week_label(friday), saved, n1, n2, n3)
        else:
            logger.info("[stage백필] %s → 탐지 0건", week_label(friday))

    logger.info("[stage백필] 완료 — 총 %d건 적재", total_saved)
    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="stage_classifications 과거 이력 백필")
    parser.add_argument("--start", required=True, metavar="YYYY-MM-DD",
                        help="백필 시작일 (해당 주부터)")
    parser.add_argument("--end", required=True, metavar="YYYY-MM-DD",
                        help="백필 종료일 (해당 주까지)")
    parser.add_argument("--workers", type=int,
                        default=int(os.environ.get("SCREENER_WORKERS", "4")),
                        help="병렬 워커 수 (기본: SCREENER_WORKERS 또는 4)")
    parser.add_argument("--max-tickers", type=int, default=0,
                        help="대상 종목 수 제한 (0 = 전종목). 테스트 시 200 권장")
    parser.add_argument("--skip-existing", action="store_true",
                        help="stage_classifications에 이미 있는 주차는 건너뜀")
    parser.add_argument("--dry-run", action="store_true",
                        help="대상 종목/주차만 산출하고 수집·저장은 생략")
    args = parser.parse_args()

    asyncio.run(main(args))
