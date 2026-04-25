"""
chart_backtest.py  —  백테스트 로직
────────────────────────────────────────────────────────────
스크리너가 과거에 발동시킨 신호(Stage 1 조건 통과)를 재현하고,
각 신호 이후 1주/4주/13주 수익률 및 KOSPI 초과수익률을 계산한다.

주요 함수:
  _check_at_row()     — row_idx 이후 데이터 없이 조건 체크 (look-ahead 방지)
  compute_metrics()   — 신호 그룹별 집계 (전체/정배열/일반)
  build_summary()     — 월별 + 연간 요약 생성
  incremental_update() — 펜딩 신호 수익률 업데이트 + 저장
"""
from __future__ import annotations

import json
import statistics
from dataclasses import asdict, dataclass, replace as _dc_replace
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd

from chart_screener import calc_ichimoku

_KST = ZoneInfo("Asia/Seoul")

BACKTEST_DIR: Path = Path.home() / ".gstack" / "backtest" / "test_feed"
_SIGNALS_JSON = BACKTEST_DIR / "signals.json"
_SUMMARY_JSON = BACKTEST_DIR / "summary.json"


@dataclass
class BacktestSignal:
    ticker: str
    name: str
    signal_week: str
    signal_date: str
    close_at_signal: float
    has_gapjum: bool
    return_1w: Optional[float] = None
    return_4w: Optional[float] = None
    return_13w: Optional[float] = None
    excess_1w: Optional[float] = None
    excess_4w: Optional[float] = None


# ── 핵심 분석 함수 ──────────────────────────────────────────────

def _check_at_row(df: pd.DataFrame, row_idx: int) -> Optional[dict]:
    """row_idx 시점에서 look-ahead 없이 Stage 1 조건을 확인한다."""
    sliced = df.iloc[: row_idx + 1]
    if len(sliced) < 2:
        return None
    ichi = calc_ichimoku(sliced)
    last = ichi.iloc[-1]
    cloud_top = last.get("cloud_top", float("nan"))
    if pd.isna(cloud_top):
        return None
    close = float(last["Close"])
    if close > float(cloud_top):
        return {"close": close, "cloud_top": float(cloud_top)}
    return None


def _build_price_lookup(df: pd.DataFrame) -> dict[date, float]:
    """OHLCV DataFrame → {date: close} 딕셔너리."""
    result: dict[date, float] = {}
    for ts, row in df.iterrows():
        d = ts.date() if hasattr(ts, "date") else ts
        result[d] = float(row["Close"])
    return result


def _nearest_price(
    lookup: dict[date, float],
    target: date,
    max_days: int = 5,
) -> Optional[float]:
    """target부터 max_days 이내 가장 가까운 미래 가격 반환."""
    for offset in range(max_days + 1):
        candidate = target + timedelta(days=offset)
        if candidate in lookup:
            return lookup[candidate]
    return None


def _week_label(d: date) -> str:
    """ISO 주차 레이블 반환 (예: '2026-W16')."""
    iso = d.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _group_metrics(sigs: list[BacktestSignal]) -> dict:
    """신호 목록에서 집계 지표를 계산한다."""
    if not sigs:
        return {"signal_count": 0}

    result: dict = {"signal_count": len(sigs)}

    r1s = [s.return_1w for s in sigs if s.return_1w is not None]
    r4s = [s.return_4w for s in sigs if s.return_4w is not None]
    r13s = [s.return_13w for s in sigs if s.return_13w is not None]
    ex1s = [s.excess_1w for s in sigs if s.excess_1w is not None]
    ex4s = [s.excess_4w for s in sigs if s.excess_4w is not None]

    if r1s:
        result["win_rate_1w"] = sum(1 for r in r1s if r > 0) / len(r1s)
    if r4s:
        result["win_rate_4w"] = sum(1 for r in r4s if r > 0) / len(r4s)
        result["avg_return_4w"] = sum(r4s) / len(r4s)
        result["median_return_4w"] = statistics.median(r4s)
    if r13s:
        result["avg_return_13w"] = sum(r13s) / len(r13s)
    if ex1s:
        result["excess_return_1w"] = sum(ex1s) / len(ex1s)
    if ex4s:
        result["excess_return_4w"] = sum(ex4s) / len(ex4s)

    return result


def compute_metrics(sigs: list[BacktestSignal]) -> dict:
    """전체/정배열/일반 그룹별 지표 계산."""
    gap = [s for s in sigs if s.has_gapjum]
    reg = [s for s in sigs if not s.has_gapjum]
    return {
        "전체": _group_metrics(sigs),
        "정배열": _group_metrics(gap),
        "일반": _group_metrics(reg),
    }


def build_summary(
    sigs: list[BacktestSignal],
    effective_weeks: Optional[int] = None,
) -> dict:
    """월별 + 연간 요약 딕셔너리 생성."""
    if effective_weeks is None:
        effective_weeks = 0

    months: dict[str, list[BacktestSignal]] = {}
    for s in sigs:
        key = s.signal_date[:7]
        months.setdefault(key, []).append(s)

    return {
        "generated_at": datetime.now(_KST).isoformat(),
        "effective_weeks": effective_weeks,
        "annual": {
            "groups": compute_metrics(sigs),
            "label": f"전체 ({effective_weeks}주 분석)",
            "period": "",
        },
        "months": {k: {"groups": compute_metrics(v)} for k, v in months.items()},
    }


# ── 영속성 (JSON 파일) ──────────────────────────────────────────

def load_signals_json() -> list[BacktestSignal]:
    """저장된 신호 목록을 JSON에서 로드한다."""
    if not _SIGNALS_JSON.exists():
        return []
    with _SIGNALS_JSON.open(encoding="utf-8") as f:
        data = json.load(f)
    return [BacktestSignal(**d) for d in data]


def _save_signals_json(sigs: list[BacktestSignal]) -> None:
    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
    with _SIGNALS_JSON.open("w", encoding="utf-8") as f:
        json.dump([asdict(s) for s in sigs], f, ensure_ascii=False, indent=2)


def save_summary_json(summary: dict) -> None:
    """집계 요약을 JSON으로 저장한다."""
    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
    with _SUMMARY_JSON.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


# ── KOSPI 기준 수익률 ───────────────────────────────────────────

def fetch_kospi_weekly() -> pd.DataFrame:
    """코스피 지수(^KS11) 주봉 데이터 조회."""
    import yfinance as yf
    df = yf.download("^KS11", period="5y", interval="1wk", progress=False, auto_adjust=True)
    return df


# ── 증분 업데이트 ──────────────────────────────────────────────

def incremental_update(
    ohlcv_map: dict[str, pd.DataFrame],
    kospi_df: pd.DataFrame | None,
) -> None:
    """펜딩 신호의 수익률을 계산하고 signals.json을 업데이트한다."""
    sigs = load_signals_json()
    if not sigs:
        return

    kospi_lookup: dict[date, float] = (
        _build_price_lookup(kospi_df)
        if kospi_df is not None and not kospi_df.empty
        else {}
    )

    updated: list[BacktestSignal] = []
    for s in sigs:
        if (
            s.return_1w is not None
            and s.return_4w is not None
            and s.return_13w is not None
        ):
            updated.append(s)
            continue

        df = ohlcv_map.get(s.ticker)
        if df is None or df.empty:
            updated.append(s)
            continue

        stock_lookup = _build_price_lookup(df)
        sig_date = date.fromisoformat(s.signal_date)
        base_price = s.close_at_signal

        def _ret(weeks: int) -> Optional[float]:
            future = _nearest_price(stock_lookup, sig_date + timedelta(weeks=weeks))
            if future is None or base_price == 0:
                return None
            return (future - base_price) / base_price

        def _kospi_ret(weeks: int) -> Optional[float]:
            base_k = _nearest_price(kospi_lookup, sig_date)
            future_k = _nearest_price(kospi_lookup, sig_date + timedelta(weeks=weeks))
            if base_k is None or future_k is None or base_k == 0:
                return None
            return (future_k - base_k) / base_k

        r1 = s.return_1w if s.return_1w is not None else _ret(1)
        r4 = s.return_4w if s.return_4w is not None else _ret(4)
        r13 = s.return_13w if s.return_13w is not None else _ret(13)

        ex1: Optional[float] = None
        ex4: Optional[float] = None
        if r1 is not None:
            kr = _kospi_ret(1)
            ex1 = r1 - kr if kr is not None else None
        if r4 is not None:
            kr = _kospi_ret(4)
            ex4 = r4 - kr if kr is not None else None

        updated.append(
            _dc_replace(
                s,
                return_1w=r1,
                return_4w=r4,
                return_13w=r13,
                excess_1w=ex1,
                excess_4w=ex4,
            )
        )

    _save_signals_json(updated)
