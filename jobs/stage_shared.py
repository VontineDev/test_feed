"""jobs/stage_shared.py — stage_backfill.py와 stage_job.py(라이브)가 공유하는
순수 플러밍 헬퍼.

classify_stage_v15 자체(단일 소스)는 analysis.stage_classifier에 있고 건드리지
않는다. 여기 있는 것은 그 앞뒤의 데이터 정규화/조회/행 조립 — 두 잡에서
byte-identical하게 중복돼 있던 부분만 추출.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import asyncpg
import pandas as pd

from analysis.stage_classifier import compute_foreign_chg_pct, compute_flow_score


def normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """yfinance 응답 DataFrame 정규화: MultiIndex 컬럼 평탄화 + 컬럼 선택 + UTC tz 인덱스."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
    df.index = pd.to_datetime(df.index, utc=True)
    return df


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
