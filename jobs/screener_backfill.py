"""
screener_backfill.py — 누락된 주차 스크리너 백필

chart_signals에 없지만 stage_classifications에 있는 주차를 자동 탐지해
과거 시점의 yfinance 주봉 데이터로 스크리너를 재실행하고 저장.

최적화: 티커당 yfinance 1회 호출 → 모든 주차에 재사용 (fetch-once 패턴).
  기존 방식: N_weeks × N_tickers 호출
  최적화 후: N_tickers 호출

실행:
    python jobs/screener_backfill.py              # 누락 주차 자동 탐지
    python jobs/screener_backfill.py --weeks W19 W24   # 특정 주차만
    python jobs/screener_backfill.py --workers 16  # 병렬 워커 수 지정
    python jobs/screener_backfill.py --dry-run    # 저장 없이 탐지만

제약:
    - yfinance는 현재 시점의 OHLCV를 반환하므로 당시 가격이 아닌
      현재 3년치 데이터에서 해당 주차 행을 잘라 사용함.
    - 당시 실제 스크리너 결과와 지표 계산 방식은 동일하나
      데이터 조정(배당락 등)으로 미세 차이가 있을 수 있음.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

# 프로젝트 루트를 sys.path에 추가 (jobs._common을 import하려면 선행 필요)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from jobs._common import bootstrap, get_pool

bootstrap(ROOT)

import asyncpg

from analysis.chart_screener import ScreenResult, calc_ichimoku, get_all_tickers, fetch_kind_sector_map
from core.db import save_chart_signals

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")


# ── 누락 주차 탐지 ─────────────────────────────────────────────
async def find_missing_weeks(pool: asyncpg.Pool) -> list[str]:
    """stage_classifications에 존재하지만 chart_signals에 없는 ISO 주차 목록."""
    async with pool.acquire() as conn:
        stage_weeks = {
            r["week_of"] for r in await conn.fetch(
                "SELECT DISTINCT TO_CHAR(classified_date, 'IYYY-\"W\"IW') AS week_of "
                "FROM stage_classifications"
            )
        }
        signal_weeks = {
            r["week_of"] for r in await conn.fetch(
                "SELECT DISTINCT week_of FROM chart_signals"
            )
        }
    missing = sorted(stage_weeks - signal_weeks)
    return missing


# ── 주차 → 월요일 날짜 ─────────────────────────────────────────
def week_of_to_monday(week_of: str) -> date:
    """'2026-W19' → date(2026, 5, 4) (해당 주 월요일)"""
    year, w = week_of.split("-W")
    return date.fromisocalendar(int(year), int(w), 1)


# ── 단일 종목 OHLCV 프리페치 ──────────────────────────────────
def _fetch_ohlcv(ticker: str) -> Optional[pd.DataFrame]:
    """yfinance에서 3년치 주봉 OHLCV 1회 다운로드. 실패 시 None."""
    try:
        tkr = yf.Ticker(ticker)
        df = tkr.history(period="3y", interval="1wk", auto_adjust=True)
        if df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        df.index = pd.to_datetime(df.index, utc=True)
        return df
    except Exception as e:
        logger.debug("[프리페치] %s 오류: %s", ticker, e)
        return None


def prefetch_all(
    tickers: list[tuple[str, str, str]],
    workers: int,
) -> dict[str, pd.DataFrame]:
    """모든 티커 OHLCV를 병렬로 1회씩 다운로드. {ticker: df} 반환."""
    logger.info("[프리페치] %d종목 주봉 OHLCV 다운로드 중 (workers=%d)...", len(tickers), workers)
    result: dict[str, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_fetch_ohlcv, t): t for t, _, _ in tickers}
        done = 0
        for fut in as_completed(futs):
            done += 1
            if done % 200 == 0:
                logger.info("[프리페치] %d/%d 완료 (데이터 있음: %d)", done, len(tickers), len(result))
            ticker = futs[fut]
            try:
                df = fut.result()
                if df is not None:
                    result[ticker] = df
            except Exception as e:
                logger.debug("[프리페치] %s: %s", ticker, e)
    logger.info("[프리페치] 완료 — %d/%d 종목 데이터 확보", len(result), len(tickers))
    return result


# ── 단일 종목×주차 스크리닝 (프리페치 데이터 사용) ──────────────
def screen_from_prefetched(
    ticker: str,
    name: str,
    sector: str,
    df_full: pd.DataFrame,
    target_monday: date,
    target_week_of: str,
) -> Optional[ScreenResult]:
    """
    프리페치된 전체 OHLCV에서 target_monday 기준으로 슬라이스 후 7-조건 스크리닝.
    yfinance 호출 없음 — df_full은 _fetch_ohlcv() 결과.
    """
    try:
        # 대상 주차까지만 슬라이스 (다음 주 월요일 미만)
        next_monday = pd.Timestamp(target_monday + timedelta(days=7), tz="UTC")
        df = df_full[df_full.index < next_monday].copy()

        if df.empty or df["Close"].notna().sum() < 60:
            return None

        df = calc_ichimoku(df)
        df["ma_20w"]  = df["Close"].rolling(20,  min_periods=20).mean()
        df["ma_60w"]  = df["Close"].rolling(60,  min_periods=60).mean()
        df["ma_120w"] = df["Close"].rolling(120, min_periods=100).mean()

        if len(df) < 2:
            return None

        cur  = df.iloc[-1]
        prev = df.iloc[-2]

        required = ["Close", "cloud_top", "ma_20w", "ma_60w"]
        if any(pd.isna(cur[col]) for col in required):
            return None
        if pd.isna(prev["Close"]) or pd.isna(prev["cloud_top"]):
            return None
        if pd.isna(prev["ma_20w"]) or pd.isna(prev["ma_60w"]):
            return None

        close         = float(cur["Close"])
        cloud_top     = float(cur["cloud_top"])
        ma_20w        = float(cur["ma_20w"])
        ma_60w        = float(cur["ma_60w"])
        prev_close    = float(prev["Close"])
        prev_cloud_top = float(prev["cloud_top"])
        prev_ma_20w   = float(prev["ma_20w"])
        prev_ma_60w   = float(prev["ma_60w"])

        ma_120w_raw   = cur.get("ma_120w", float("nan"))
        ma_120w_valid = not pd.isna(ma_120w_raw)
        ma_120w       = float(ma_120w_raw) if ma_120w_valid else None

        g_strict = os.environ.get("SCREENER_G_NAN_STRICT", "").lower() in ("1", "true", "yes")

        A = close > cloud_top
        B = prev_close <= prev_cloud_top
        C = close > ma_20w
        D = close > ma_60w
        E = ma_20w > prev_ma_20w
        F = ma_60w > prev_ma_60w
        G = (ma_120w_valid and close > ma_120w) if g_strict else (
            (not ma_120w_valid) or (close > ma_120w))

        if not (A and B and C and D and E and F and G):
            return None

        tenkan_cur  = cur.get("tenkan_sen", float("nan"))
        kijun_cur   = cur.get("kijun_sen",  float("nan"))
        tenkan_prev = prev.get("tenkan_sen", float("nan"))
        kijun_prev  = prev.get("kijun_sen",  float("nan"))
        tenkan_ok = not pd.isna(tenkan_cur) and not pd.isna(tenkan_prev)
        kijun_ok  = not pd.isna(kijun_cur)  and not pd.isna(kijun_prev)
        if tenkan_ok and kijun_ok:
            is_enhanced = (float(tenkan_cur) > float(kijun_cur) and
                           float(tenkan_cur) > float(tenkan_prev) and
                           float(kijun_cur)  > float(kijun_prev))
        else:
            is_enhanced = False

        high_raw = cur.get("High", float("nan"))
        high_w   = float(high_raw) if not pd.isna(high_raw) else None
        vol_raw  = cur.get("Volume", float("nan"))
        volume_w = int(vol_raw) if not pd.isna(vol_raw) and vol_raw is not None else None

        # screened_at: 해당 주 일요일 20:30 KST (= 11:30 UTC)
        sunday = target_monday + timedelta(days=6)
        screened_at = datetime(
            sunday.year, sunday.month, sunday.day,
            11, 30, 0, tzinfo=timezone.utc,
        ).isoformat()

        return ScreenResult(
            ticker=ticker,
            name=name,
            close=close,
            ma_20w=ma_20w,
            ma_60w=ma_60w,
            cloud_top=cloud_top,
            is_enhanced=is_enhanced,
            has_gapjum=(ma_20w > ma_60w),
            screened_at=screened_at,
            week_of=target_week_of,
            sector=sector,
            ma_120w=ma_120w,
            high_w=high_w,
            volume_w=volume_w,
        )
    except Exception as e:
        logger.debug("[스크린] %s/%s 오류: %s", ticker, target_week_of, e)
        return None


# ── 단일 주차 백필 (프리페치 데이터 사용) ─────────────────────
def backfill_week(
    tickers: list[tuple[str, str, str]],
    ohlcv_map: dict[str, pd.DataFrame],
    target_week_of: str,
    workers: int,
) -> list[ScreenResult]:
    target_monday = week_of_to_monday(target_week_of)
    ticker_count = sum(1 for t, _, _ in tickers if t in ohlcv_map)
    logger.info("[백필] %s (월요일: %s) — 데이터 있는 종목 %d개",
                target_week_of, target_monday, ticker_count)

    results: list[ScreenResult] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(
                screen_from_prefetched,
                t, n, s, ohlcv_map[t], target_monday, target_week_of
            ): t
            for t, n, s in tickers if t in ohlcv_map
        }
        for fut in as_completed(futures):
            try:
                r = fut.result()
                if r:
                    results.append(r)
            except Exception as e:
                logger.debug("[백필] %s: %s", futures[fut], e)

    logger.info("[백필] %s 완료 — 통과 종목 %d개", target_week_of, len(results))
    return results


# ── 메인 ──────────────────────────────────────────────────────
async def main(args: argparse.Namespace) -> None:
    pool = await get_pool()

    if args.weeks:
        missing = sorted(args.weeks)
    else:
        missing = await find_missing_weeks(pool)

    if not missing:
        logger.info("누락 주차 없음 — 백필 불필요")
        await pool.close()
        return

    logger.info("백필 대상 주차 %d개: %s ... %s", len(missing), missing[0], missing[-1])

    if args.dry_run:
        logger.info("[dry-run] 저장 생략")
        await pool.close()
        return

    sector_map = fetch_kind_sector_map()
    tickers = get_all_tickers(sector_map)
    if not tickers:
        logger.error("종목 목록 조회 실패")
        await pool.close()
        return

    # ── 핵심 최적화: 티커당 1회만 yfinance 호출 ──────────────────
    ohlcv_map = prefetch_all(tickers, args.workers)

    total_saved = 0
    for week_of in missing:
        results = backfill_week(tickers, ohlcv_map, week_of, args.workers)
        if results:
            saved = await save_chart_signals(pool, results)
            total_saved += saved
            logger.info("[백필] %s → %d건 저장", week_of, saved)
        else:
            logger.warning("[백필] %s — 통과 종목 없음", week_of)

    logger.info("백필 완료 — 총 %d건 저장", total_saved)
    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="스크리너 누락 주차 백필 (fetch-once 최적화)")
    parser.add_argument("--weeks", nargs="+", metavar="WEEK",
                        help="백필할 주차 (예: W19 W24). 없으면 자동 탐지.")
    parser.add_argument("--workers", type=int,
                        default=int(os.environ.get("SCREENER_WORKERS", "16")),
                        help="병렬 워커 수 (기본: SCREENER_WORKERS 환경변수 또는 16)")
    parser.add_argument("--dry-run", action="store_true",
                        help="누락 주차만 탐지하고 실제 저장은 하지 않음")
    args = parser.parse_args()

    # --weeks 는 "W19" 또는 "2026-W19" 둘 다 허용
    if args.weeks:
        normalized = []
        for w in args.weeks:
            if "-W" not in w:
                year = datetime.now().year
                normalized.append(f"{year}-{w}")
            else:
                normalized.append(w)
        args.weeks = normalized

    asyncio.run(main(args))
