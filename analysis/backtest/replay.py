"""백테스트 신호 재현(replay) 함수 (backtest_engine.py에서 이동, Phase C).

전략 세대별 walk-forward 재현 11종 + cross 필터. 세대(v1.0~v1.5)는
의도적으로 병합하지 않음 — 각 세대의 백테스트 재현성 보존
(tests/test_compose_parity.py 참조).
"""

from __future__ import annotations

import logging
from dataclasses import replace as _dc_replace
from datetime import date, datetime, timedelta
from typing import Optional, cast

import pandas as pd

from analysis.chart_screener import calc_ichimoku
from analysis.backtest.config import _S1_THRESHOLD
from analysis.backtest.models import BacktestConfig, SignalRecord
from analysis.backtest.helpers import _compute_rsi, _week_label
from core.ohlcv_cache import FlowKey, FlowValue

logger = logging.getLogger(__name__)

# ── 신호 재현 ─────────────────────────────────────────────────────

def _replay_ichimoku(
    ticker: str,
    name: str,
    daily_df: pd.DataFrame,
    market: str,
    config: BacktestConfig,
) -> list[SignalRecord]:
    """일봉 → 주봉 리샘플링 후 이치모쿠 7조건 walk-forward 재현.

    조건:
      A: close > cloud_top                (구름 상향 돌파)
      B: prev_close <= prev_cloud_top     (전 주 구름 내/하부)
      C: close > MA20w
      D: close > MA60w
      E: MA20w > prev_MA20w              (우상향)
      F: MA60w > prev_MA60w
      G: close > MA120w (데이터 부족 시 통과)
    """
    # KRX는 월~금 → 주봉 = 금요일 마감가 기준
    weekly = cast(pd.DataFrame, daily_df.resample("W-FRI", closed="right", label="right").agg({
        "Open":   "first",
        "High":   "max",
        "Low":    "min",
        "Close":  "last",
        "Volume": "sum",
    })).dropna(subset=["Close"])

    # Ichimoku 선행스팬B: 52주 lookback 최소 요건
    if len(weekly) < 62:
        return []

    weekly = calc_ichimoku(weekly)
    weekly["ma_20w"]  = weekly["Close"].rolling(20,  min_periods=20).mean()
    weekly["ma_60w"]  = weekly["Close"].rolling(60,  min_periods=60).mean()
    weekly["ma_120w"] = weekly["Close"].rolling(120, min_periods=100).mean()

    signals: list[SignalRecord] = []

    for i in range(1, len(weekly)):
        # Use the UTC date of the last daily row for this week so that signal_date
        # is consistent with the idx_map built from ohlcv_cache timestamps.
        # ohlcv_cache stores KST midnight as UTC (e.g. KST 2024-01-05 → UTC 2024-01-04T15:00Z).
        # weekly.index[i].date() returns the KST Friday, causing a 1-day mismatch.
        week_ts = weekly.index[i]
        _daily_before = daily_df.index[daily_df.index <= week_ts]
        if _daily_before.empty:
            continue
        row_date = cast(pd.Timestamp, _daily_before[-1]).date()

        if row_date < config.start or row_date > config.end:
            continue

        cur  = weekly.iloc[i]
        prev = weekly.iloc[i - 1]

        # 필수 컬럼 NaN 체크
        required_cur  = ["Close", "cloud_top", "ma_20w", "ma_60w"]
        required_prev = ["Close", "cloud_top"]
        if any(pd.isna(cur.get(c)) for c in required_cur):
            continue
        if any(pd.isna(prev.get(c)) for c in required_prev):
            continue

        close     = float(cur["Close"])
        cloud_top = float(cur["cloud_top"])
        ma20      = float(cur["ma_20w"])
        ma60      = float(cur["ma_60w"])
        prev_close = float(prev["Close"])
        prev_ct    = float(prev["cloud_top"])

        prev_ma20 = prev.get("ma_20w")
        prev_ma60 = prev.get("ma_60w")
        ma120     = cur.get("ma_120w")

        cond_A = close > cloud_top
        cond_B = prev_close <= prev_ct
        cond_C = close > ma20
        cond_D = close > ma60
        cond_E = prev_ma20 is not None and not pd.isna(prev_ma20) and ma20 > float(prev_ma20)
        cond_F = prev_ma60 is not None and not pd.isna(prev_ma60) and ma60 > float(prev_ma60)
        cond_G = ma120 is None or pd.isna(ma120) or close > float(ma120)

        if cond_A and cond_B and cond_C and cond_D and cond_E and cond_F and cond_G:
            signals.append(SignalRecord(
                ticker=ticker,
                name=name,
                signal_date=row_date,
                close_at_signal=close,
                mode="ichimoku",
                market=market,
            ))

    return signals


def _replay_stage(
    ticker: str,
    name: str,
    daily_df: pd.DataFrame,
    market: str,
    config: BacktestConfig,
    flow_lookup: Optional[dict[FlowKey, FlowValue]] = None,
) -> list[SignalRecord]:
    """일봉 Stage 1 가격 조건 walk-forward 재현 (5/5 조건).

    조건 1: 일일 상승률 ≥ 5%(KOSPI) / 7%(KOSDAQ)
    조건 2: 거래량 ≥ 2× 20일 평균
    조건 3: close > MA20 AND MA60
    조건 4: 52주 고점 대비 괴리율 ≤ 20%
    조건 5: 수급 — 외국인 또는 기관 순매수 > 0
             (flow_lookup 제공 시 적용. 해당 날짜 데이터 없으면 조건 생략)
    """
    threshold = _S1_THRESHOLD.get(market, 0.05)

    df     = daily_df.copy()
    closes = cast(pd.Series, df["Close"])
    vols   = df["Volume"]

    df["ma_20"] = closes.rolling(20, min_periods=20).mean()
    df["ma_60"] = closes.rolling(60, min_periods=60).mean()

    signals: list[SignalRecord] = []

    # i=0는 change_pct 계산 불가, i<21은 20일 거래량 평균 불가
    for i in range(21, len(df)):
        row_date = cast(pd.Timestamp, df.index[i]).date()
        if row_date < config.start or row_date > config.end:
            continue

        cur  = df.iloc[i]
        prev = df.iloc[i - 1]

        if pd.isna(cur["Close"]) or pd.isna(prev["Close"]) or float(prev["Close"]) <= 0:
            continue

        close_today = float(cur["Close"])
        close_prev  = float(prev["Close"])

        # 조건 1: 상승률
        change_pct = (close_today - close_prev) / close_prev
        if change_pct < threshold:
            continue

        # 조건 2: 거래량
        if pd.isna(cur["Volume"]):
            continue
        vol_today = float(cur["Volume"])
        avg_vol20 = float(vols.iloc[i - 20:i].mean())
        if avg_vol20 <= 0 or vol_today < 2.0 * avg_vol20:
            continue

        # 조건 3: MA20 / MA60
        if pd.isna(cur["ma_20"]) or pd.isna(cur["ma_60"]):
            continue
        if close_today <= float(cur["ma_20"]) or close_today <= float(cur["ma_60"]):
            continue

        # 조건 4: 52주 고점 괴리율
        closes_52 = closes.iloc[max(0, i - 251): i + 1].dropna()
        if closes_52.empty:
            continue
        week52_high = float(closes_52.max())
        if week52_high <= 0 or (week52_high - close_today) / week52_high > 0.20:
            continue

        # 조건 5: 수급 (외국인·기관 순매수)
        # flow_lookup이 제공됐고 해당 날짜 데이터가 있을 때만 필터링.
        # 데이터 없는 날짜는 통과 (과거 데이터 미수집 구간 보호).
        if flow_lookup is not None:
            flow = flow_lookup.get((ticker, row_date))
            if flow is not None:
                f_net, i_net, _p_net = flow
                if not (
                    (f_net is not None and f_net > 0)
                    or (i_net is not None and i_net > 0)
                ):
                    continue

        signals.append(SignalRecord(
            ticker=ticker,
            name=name,
            signal_date=row_date,
            close_at_signal=close_today,
            mode="stage",
            market=market,
        ))

    return signals


def _replay_stage_v11(
    ticker: str,
    name: str,
    daily_df: pd.DataFrame,
    market: str,
    config: BacktestConfig,
    flow_lookup: Optional[dict[FlowKey, FlowValue]] = None,
) -> list[SignalRecord]:
    """Stage 1 v1.1 walk-forward 재현.

    v1.0 조건 5개 모두 + 조건 6 신규: RSI(14) ≥ 50.
    """
    threshold = _S1_THRESHOLD.get(market, 0.05)

    df     = daily_df.copy()
    closes = cast(pd.Series, df["Close"])
    vols   = df["Volume"]

    df["ma_20"]  = closes.rolling(20, min_periods=20).mean()
    df["ma_60"]  = closes.rolling(60, min_periods=60).mean()
    df["rsi_14"] = _compute_rsi(closes)

    signals: list[SignalRecord] = []

    for i in range(21, len(df)):
        row_date = cast(pd.Timestamp, df.index[i]).date()
        if row_date < config.start or row_date > config.end:
            continue

        cur  = df.iloc[i]
        prev = df.iloc[i - 1]

        if pd.isna(cur["Close"]) or pd.isna(prev["Close"]) or float(prev["Close"]) <= 0:
            continue

        close_today = float(cur["Close"])
        close_prev  = float(prev["Close"])

        # 조건 1: 상승률
        change_pct = (close_today - close_prev) / close_prev
        if change_pct < threshold:
            continue

        # 조건 2: 거래량
        if pd.isna(cur["Volume"]):
            continue
        vol_today = float(cur["Volume"])
        avg_vol20 = float(vols.iloc[i - 20:i].mean())
        if avg_vol20 <= 0 or vol_today < 2.0 * avg_vol20:
            continue

        # 조건 3: MA20 / MA60
        if pd.isna(cur["ma_20"]) or pd.isna(cur["ma_60"]):
            continue
        if close_today <= float(cur["ma_20"]) or close_today <= float(cur["ma_60"]):
            continue

        # 조건 4: 52주 고점 괴리율
        closes_52 = closes.iloc[max(0, i - 251): i + 1].dropna()
        if closes_52.empty:
            continue
        week52_high = float(closes_52.max())
        if week52_high <= 0 or (week52_high - close_today) / week52_high > 0.20:
            continue

        # 조건 5: 수급
        if flow_lookup is not None:
            flow = flow_lookup.get((ticker, row_date))
            if flow is not None:
                f_net, i_net, _p_net = flow
                if not (
                    (f_net is not None and f_net > 0)
                    or (i_net is not None and i_net > 0)
                ):
                    continue

        # 조건 6 (v1.1): RSI(14) ≥ 50
        rsi = cur.get("rsi_14")
        if rsi is None or pd.isna(rsi) or float(rsi) < 50:
            continue

        signals.append(SignalRecord(
            ticker=ticker,
            name=name,
            signal_date=row_date,
            close_at_signal=close_today,
            mode="stage_v11",
            market=market,
        ))

    return signals


def _replay_stage2_v11(
    ticker: str,
    name: str,
    daily_df: pd.DataFrame,
    market: str,
    config: BacktestConfig,
) -> list[SignalRecord]:
    """Stage 2 v1.1 walk-forward 재현.

    Stage 1 v1.1 신호 기반 + C3 거래대금 범위 0.30~0.60 (v1.0: 0.25~0.65).
    """
    s1_cfg = _dc_replace(config, mode="stage_v11", start=config.start - timedelta(days=21))
    s1_signals = _replay_stage_v11(ticker, name, daily_df, market, s1_cfg)
    if not s1_signals:
        return []

    idx_map: dict[date, int] = {}
    for i, ts in enumerate(daily_df.index):
        d = ts.date() if isinstance(ts, datetime) else cast(date, ts)
        idx_map[d] = i

    df = daily_df.copy()
    df["ma_20"] = df["Close"].rolling(20, min_periods=20).mean()

    s2_signals: list[SignalRecord] = []
    seen_dates: set[date] = set()

    for s1 in s1_signals:
        s1_idx = idx_map.get(s1.signal_date)
        if s1_idx is None:
            continue

        s1_close = s1.close_at_signal
        if s1_close <= 0:
            continue

        s1_row = df.iloc[s1_idx]
        if pd.isna(s1_row["Volume"]) or pd.isna(s1_row["Close"]):
            continue
        txamt_s1 = float(s1_row["Volume"]) * float(s1_row["Close"])
        if txamt_s1 <= 0:
            continue

        cutoff = s1.signal_date + timedelta(days=14)

        for j in range(s1_idx + 1, len(df)):
            ts       = df.index[j]
            row_date = ts.date() if isinstance(ts, datetime) else cast(date, ts)
            if row_date > cutoff:
                break
            if row_date < config.start or row_date > config.end:
                continue
            if row_date in seen_dates:
                continue

            cur = df.iloc[j]
            if pd.isna(cur["Close"]) or pd.isna(cur["Volume"]):
                continue

            c_today     = float(cur["Close"])
            v_today     = float(cur["Volume"])
            txamt_today = v_today * c_today
            ma20        = cur["ma_20"]

            # C1: -5% ~ -20% 되돌림
            if not (0.80 <= c_today / s1_close <= 0.95):
                continue
            # C2: close ≥ MA20 × 0.95
            if pd.isna(ma20) or c_today < float(ma20) * 0.95:
                continue
            # C3 v1.1: 거래대금 비율 [0.30, 0.60]
            if not (0.30 <= txamt_today / txamt_s1 <= 0.60):
                continue

            seen_dates.add(row_date)
            s2_signals.append(SignalRecord(
                ticker=ticker,
                name=name,
                signal_date=row_date,
                close_at_signal=c_today,
                mode="stage2_v11",
                market=market,
            ))
            break

    return s2_signals


def _replay_stage_v12(
    ticker: str,
    name: str,
    daily_df: pd.DataFrame,
    market: str,
    config: "BacktestConfig",
    flow_lookup: Optional[dict] = None,
    streak_lookup: Optional[dict] = None,
    shares_lookup: Optional[dict] = None,
) -> list[SignalRecord]:
    """Stage 1 v1.2 walk-forward 재현.

    v1.1 조건 모두 + 조건 7: 수급 강화 + 조건 8: 개인 순매수 > 0 + 조건 9: 외인+기관 ≥ 상장주식수 0.2%.
    streak_lookup: {(ticker, date): (foreign_streak, inst_streak)}
    shares_lookup: {yfinance_symbol: listed_shares}
    """
    threshold = _S1_THRESHOLD.get(market, 0.05)

    df     = daily_df.copy()
    closes = cast(pd.Series, df["Close"])
    vols   = df["Volume"]

    df["ma_20"]     = closes.rolling(20, min_periods=20).mean()
    df["ma_60"]     = closes.rolling(60, min_periods=60).mean()
    df["ma_20_5d"]  = cast(pd.Series, closes.rolling(20, min_periods=20).mean()).shift(5)
    df["rsi_14"]    = _compute_rsi(closes)

    signals: list[SignalRecord] = []

    for i in range(21, len(df)):
        row_date = cast(pd.Timestamp, df.index[i]).date()
        if row_date < config.start or row_date > config.end:
            continue

        cur  = df.iloc[i]
        prev = df.iloc[i - 1]

        if pd.isna(cur["Close"]) or pd.isna(prev["Close"]) or float(prev["Close"]) <= 0:
            continue

        close_today = float(cur["Close"])
        close_prev  = float(prev["Close"])

        # 조건 1: 상승률
        if (close_today - close_prev) / close_prev < threshold:
            continue

        # 조건 2: 거래량
        if pd.isna(cur["Volume"]):
            continue
        vol_today = float(cur["Volume"])
        avg_vol20 = float(vols.iloc[i - 20:i].mean())
        if avg_vol20 <= 0 or vol_today < 2.0 * avg_vol20:
            continue

        # 조건 3: MA20 / MA60
        if pd.isna(cur["ma_20"]) or pd.isna(cur["ma_60"]):
            continue
        if close_today <= float(cur["ma_20"]) or close_today <= float(cur["ma_60"]):
            continue

        # 조건 4: 52주 고점 괴리율
        closes_52 = closes.iloc[max(0, i - 251): i + 1].dropna()
        if closes_52.empty:
            continue
        week52_high = float(closes_52.max())
        if week52_high <= 0 or (week52_high - close_today) / week52_high > 0.20:
            continue

        # 조건 6 (v1.1): RSI(14) >= 50
        rsi = cur.get("rsi_14")
        if rsi is None or pd.isna(rsi) or float(rsi) < 50:
            continue

        # 조건 8 (v1.2): 개인 순매수 > 0
        if flow_lookup is not None:
            flow = flow_lookup.get((ticker, row_date))
            if flow is not None:
                f_net, i_net, p_net = flow
                if p_net is not None and p_net <= 0:
                    continue

        # 조건 7 (v1.2): 수급 강화 — (외인>0 AND 기관>=0) OR 기관 streak >= 3
        if flow_lookup is not None or streak_lookup is not None:
            f_net = i_net = f_str = i_str = None
            if flow_lookup is not None:
                flow = flow_lookup.get((ticker, row_date))
                if flow is not None:
                    f_net, i_net, _p = flow
            if streak_lookup is not None:
                streak = streak_lookup.get((ticker, row_date))
                if streak is not None:
                    f_str, i_str = streak
            # 데이터가 있는 경우에만 필터 적용
            if f_net is not None or i_net is not None or f_str is not None:
                both_buy  = f_net is not None and f_net > 0 and i_net is not None and i_net >= 0
                inst_3day = i_str is not None and i_str >= 3
                if not (both_buy or inst_3day):
                    continue

        # 조건 9 (v1.2): 외인+기관 합산 순매수 ≥ 상장주식수 0.2% (당일, 데이터 없으면 통과)
        if flow_lookup is not None and shares_lookup is not None:
            shares = shares_lookup.get(ticker)
            if shares and shares > 0:
                flow9 = flow_lookup.get((ticker, row_date))
                if flow9 is not None:
                    fn9, in9, _ = flow9
                    if fn9 is not None and in9 is not None:
                        if (fn9 + in9) < shares * 0.002:
                            continue

        signals.append(SignalRecord(
            ticker=ticker,
            name=name,
            signal_date=row_date,
            close_at_signal=close_today,
            mode="stage_v12",
            market=market,
        ))

    return signals


def _replay_stage2_v12(
    ticker: str,
    name: str,
    daily_df: pd.DataFrame,
    market: str,
    config: "BacktestConfig",
) -> list[SignalRecord]:
    """Stage 2 v1.2 walk-forward 재현.

    Stage 1 v1.2 신호 기반 + C3 [0.30, 0.60] + C4 20일 평균 대비 100~150% + C5 고저폭 축소.
    """
    s1_cfg     = _dc_replace(config, mode="stage_v12", start=config.start - timedelta(days=21))
    s1_signals = _replay_stage_v12(ticker, name, daily_df, market, s1_cfg)
    if not s1_signals:
        return []

    idx_map: dict[date, int] = {}
    for i, ts in enumerate(daily_df.index):
        d = ts.date() if isinstance(ts, datetime) else cast(date, ts)
        idx_map[d] = i

    df      = daily_df.copy()
    closes  = df["Close"]
    df["ma_20"] = closes.rolling(20, min_periods=20).mean()

    # 고저폭 비율 시리즈 (20일 rolling 평균 기준)
    if "High" in df.columns and "Low" in df.columns:
        range_ratio = (df["High"] - df["Low"]) / df["Close"].replace(0, float("nan"))
        df["avg_range_ratio"] = range_ratio.shift(1).rolling(20, min_periods=10).mean()
        df["range_ratio"]     = range_ratio
    else:
        df["avg_range_ratio"] = float("nan")
        df["range_ratio"]     = float("nan")

    s2_signals: list[SignalRecord] = []
    seen_dates: set[date] = set()

    for s1 in s1_signals:
        s1_idx = idx_map.get(s1.signal_date)
        if s1_idx is None:
            continue

        s1_close = s1.close_at_signal
        if s1_close <= 0:
            continue

        s1_row = df.iloc[s1_idx]
        if pd.isna(s1_row["Volume"]) or pd.isna(s1_row["Close"]):
            continue
        txamt_s1 = float(s1_row["Volume"]) * float(s1_row["Close"])
        if txamt_s1 <= 0:
            continue

        cutoff = s1.signal_date + timedelta(days=14)

        for j in range(s1_idx + 1, len(df)):
            ts       = df.index[j]
            row_date = ts.date() if isinstance(ts, datetime) else cast(date, ts)
            if row_date > cutoff:
                break
            if row_date < config.start or row_date > config.end:
                continue
            if row_date in seen_dates:
                continue

            cur = df.iloc[j]
            if pd.isna(cur["Close"]) or pd.isna(cur["Volume"]):
                continue

            c_today     = float(cur["Close"])
            v_today     = float(cur["Volume"])
            txamt_today = v_today * c_today
            ma20        = cur["ma_20"]

            # C1: -5% ~ -20% 되돌림
            if not (0.80 <= c_today / s1_close <= 0.95):
                continue
            # C2: close >= MA20 * 0.95
            if pd.isna(ma20) or c_today < float(ma20) * 0.95:
                continue
            # C3 v1.2: 거래대금 비율 [0.30, 0.60]
            if not (0.30 <= txamt_today / txamt_s1 <= 0.60):
                continue
            # C4 v1.2: 거래대금이 20일 평균 대비 100~150%
            avg_txamt20 = float(closes.iloc[max(0, j - 20):j].mean()) * float(
                df["Volume"].iloc[max(0, j - 20):j].mean()
            ) if j >= 20 else 0.0
            if avg_txamt20 > 0 and not (1.0 <= txamt_today / avg_txamt20 <= 1.5):
                continue
            # C5 v1.2: 일일 고저폭이 20일 평균 대비 70% 이하
            rr_today = cur.get("range_ratio")
            rr_avg   = cur.get("avg_range_ratio")
            if (
                rr_today is not None and not pd.isna(rr_today)
                and rr_avg   is not None and not pd.isna(rr_avg)
                and float(rr_avg) > 0
                and float(rr_today) > float(rr_avg) * 0.70
            ):
                continue

            seen_dates.add(row_date)
            s2_signals.append(SignalRecord(
                ticker=ticker,
                name=name,
                signal_date=row_date,
                close_at_signal=c_today,
                mode="stage2_v12",
                market=market,
            ))
            break

    return s2_signals


def _replay_stage_v13(
    ticker: str,
    name: str,
    daily_df: pd.DataFrame,
    market: str,
    config: "BacktestConfig",
    flow_lookup: Optional[dict] = None,
    streak_lookup: Optional[dict] = None,
    shares_lookup: Optional[dict] = None,
) -> list[SignalRecord]:
    """Stage 1 v1.3 walk-forward 재현.

    v1.2에서 조건 8(개인 순매수 > 0) 제거. close > MA20은 조건 3에 이미 포함.
    """
    threshold = _S1_THRESHOLD.get(market, 0.05)

    df     = daily_df.copy()
    closes = cast(pd.Series, df["Close"])
    vols   = df["Volume"]

    df["ma_20"]  = closes.rolling(20, min_periods=20).mean()
    df["ma_60"]  = closes.rolling(60, min_periods=60).mean()
    df["rsi_14"] = _compute_rsi(closes)

    signals: list[SignalRecord] = []

    for i in range(21, len(df)):
        row_date = cast(pd.Timestamp, df.index[i]).date()
        if row_date < config.start or row_date > config.end:
            continue

        cur  = df.iloc[i]
        prev = df.iloc[i - 1]

        if pd.isna(cur["Close"]) or pd.isna(prev["Close"]) or float(prev["Close"]) <= 0:
            continue

        close_today = float(cur["Close"])
        close_prev  = float(prev["Close"])

        # 조건 1: 상승률
        if (close_today - close_prev) / close_prev < threshold:
            continue

        # 조건 2: 거래량
        if pd.isna(cur["Volume"]):
            continue
        vol_today = float(cur["Volume"])
        avg_vol20 = float(vols.iloc[i - 20:i].mean())
        if avg_vol20 <= 0 or vol_today < 2.0 * avg_vol20:
            continue

        # 조건 3: MA20 / MA60
        if pd.isna(cur["ma_20"]) or pd.isna(cur["ma_60"]):
            continue
        if close_today <= float(cur["ma_20"]) or close_today <= float(cur["ma_60"]):
            continue

        # 조건 4: 52주 고점 괴리율
        closes_52 = closes.iloc[max(0, i - 251): i + 1].dropna()
        if closes_52.empty:
            continue
        week52_high = float(closes_52.max())
        if week52_high <= 0 or (week52_high - close_today) / week52_high > 0.20:
            continue

        # 조건 6 (v1.1): RSI(14) >= 50
        rsi = cur.get("rsi_14")
        if rsi is None or pd.isna(rsi) or float(rsi) < 50:
            continue

        # 조건 7 (v1.2): 수급 강화 — (외인>0 AND 기관>=0) OR 기관 streak >= 3
        if flow_lookup is not None or streak_lookup is not None:
            f_net = i_net = f_str = i_str = None
            if flow_lookup is not None:
                flow = flow_lookup.get((ticker, row_date))
                if flow is not None:
                    f_net, i_net, _p = flow
            if streak_lookup is not None:
                streak = streak_lookup.get((ticker, row_date))
                if streak is not None:
                    f_str, i_str = streak
            if f_net is not None or i_net is not None or f_str is not None:
                both_buy  = f_net is not None and f_net > 0 and i_net is not None and i_net >= 0
                inst_3day = i_str is not None and i_str >= 3
                if not (both_buy or inst_3day):
                    continue

        # 조건 9 (v1.2): 외인+기관 합산 순매수 ≥ 상장주식수 0.2% (당일, 데이터 없으면 통과)
        if flow_lookup is not None and shares_lookup is not None:
            shares = shares_lookup.get(ticker)
            if shares and shares > 0:
                flow9 = flow_lookup.get((ticker, row_date))
                if flow9 is not None:
                    fn9, in9, _ = flow9
                    if fn9 is not None and in9 is not None:
                        if (fn9 + in9) < shares * 0.002:
                            continue

        signals.append(SignalRecord(
            ticker=ticker,
            name=name,
            signal_date=row_date,
            close_at_signal=close_today,
            mode="stage_v13",
            market=market,
        ))

    return signals


def _replay_stage_v14(
    ticker: str,
    name: str,
    daily_df: pd.DataFrame,
    market: str,
    config: "BacktestConfig",
    flow_lookup: Optional[dict] = None,
    streak_lookup: Optional[dict] = None,
    shares_lookup: Optional[dict] = None,
) -> list[SignalRecord]:
    """Stage 1 v1.4 walk-forward 재현.

    v1.3에서 거래량 비교 기준을 MA20 → MA30으로 변경.
    MA20 기준은 신호 직전 다른 급등이 있으면 평균값이 왜곡되는 문제가 있음.
    MA30은 더 안정적인 기저 거래량을 반영.
    """
    threshold = _S1_THRESHOLD.get(market, 0.05)

    df     = daily_df.copy()
    closes = cast(pd.Series, df["Close"])
    vols   = df["Volume"]

    df["ma_20"]     = closes.rolling(20, min_periods=20).mean()
    df["ma_60"]     = closes.rolling(60, min_periods=60).mean()
    df["rsi_14"]    = _compute_rsi(closes)
    df["avg_vol30"] = vols.rolling(30, min_periods=30).mean()  # MA30 사전 계산

    signals: list[SignalRecord] = []

    for i in range(21, len(df)):
        row_date = cast(pd.Timestamp, df.index[i]).date()
        if row_date < config.start or row_date > config.end:
            continue

        cur  = df.iloc[i]
        prev = df.iloc[i - 1]

        if pd.isna(cur["Close"]) or pd.isna(prev["Close"]) or float(prev["Close"]) <= 0:
            continue

        close_today = float(cur["Close"])
        close_prev  = float(prev["Close"])

        # 조건 1: 상승률
        if (close_today - close_prev) / close_prev < threshold:
            continue

        # 조건 2: 거래량 ≥ 2× MA30 (v1.4 핵심 변경)
        if pd.isna(cur["Volume"]) or pd.isna(cur["avg_vol30"]):
            continue
        vol_today  = float(cur["Volume"])
        avg_vol30  = float(cur["avg_vol30"])
        if avg_vol30 <= 0 or vol_today < 2.0 * avg_vol30:
            continue

        # 조건 3: MA20 / MA60
        if pd.isna(cur["ma_20"]) or pd.isna(cur["ma_60"]):
            continue
        if close_today <= float(cur["ma_20"]) or close_today <= float(cur["ma_60"]):
            continue

        # 조건 4: 52주 고점 괴리율
        closes_52 = closes.iloc[max(0, i - 251): i + 1].dropna()
        if closes_52.empty:
            continue
        week52_high = float(closes_52.max())
        if week52_high <= 0 or (week52_high - close_today) / week52_high > 0.20:
            continue

        # 조건 6: RSI(14) >= 50
        rsi = cur.get("rsi_14")
        if rsi is None or pd.isna(rsi) or float(rsi) < 50:
            continue

        # 조건 7: 수급 강화 — (외인>0 AND 기관>=0) OR 기관 streak >= 3
        if flow_lookup is not None or streak_lookup is not None:
            f_net = i_net = f_str = i_str = None
            if flow_lookup is not None:
                flow = flow_lookup.get((ticker, row_date))
                if flow is not None:
                    f_net, i_net, _p = flow
            if streak_lookup is not None:
                streak = streak_lookup.get((ticker, row_date))
                if streak is not None:
                    f_str, i_str = streak
            if f_net is not None or i_net is not None or f_str is not None:
                both_buy  = f_net is not None and f_net > 0 and i_net is not None and i_net >= 0
                inst_3day = i_str is not None and i_str >= 3
                if not (both_buy or inst_3day):
                    continue

        # 조건 9: 외인+기관 합산 순매수 ≥ 상장주식수 0.2%
        if flow_lookup is not None and shares_lookup is not None:
            shares = shares_lookup.get(ticker)
            if shares and shares > 0:
                flow9 = flow_lookup.get((ticker, row_date))
                if flow9 is not None:
                    fn9, in9, _ = flow9
                    if fn9 is not None and in9 is not None:
                        if (fn9 + in9) < shares * 0.002:
                            continue

        signals.append(SignalRecord(
            ticker=ticker,
            name=name,
            signal_date=row_date,
            close_at_signal=close_today,
            mode="stage_v14",
            market=market,
        ))

    return signals


def _replay_stage_v15(
    ticker: str,
    name: str,
    daily_df: pd.DataFrame,
    market: str,
    config: "BacktestConfig",
    flow_lookup: Optional[dict] = None,
    streak_lookup: Optional[dict] = None,
    shares_lookup: Optional[dict] = None,
) -> list[SignalRecord]:
    """Stage 1 v1.5 walk-forward 재현.

    v1.4 + 조건 10: 20일 박스권 이탈 — close > 직전 20일 High 최고값.
    단순 반등이 아닌 박스 상단 돌파 breakout만 통과.
    """
    threshold = _S1_THRESHOLD.get(market, 0.05)

    df     = daily_df.copy()
    closes = cast(pd.Series, df["Close"])
    vols   = df["Volume"]

    df["ma_20"]     = closes.rolling(20, min_periods=20).mean()
    df["ma_60"]     = closes.rolling(60, min_periods=60).mean()
    df["rsi_14"]    = _compute_rsi(closes)
    df["avg_vol30"] = vols.rolling(30, min_periods=30).mean()
    df["high_20d"]  = df["High"].shift(1).rolling(20, min_periods=20).max()  # 당일 제외 전 20일 High

    signals: list[SignalRecord] = []

    for i in range(21, len(df)):
        row_date = cast(pd.Timestamp, df.index[i]).date()
        if row_date < config.start or row_date > config.end:
            continue

        cur  = df.iloc[i]
        prev = df.iloc[i - 1]

        if pd.isna(cur["Close"]) or pd.isna(prev["Close"]) or float(prev["Close"]) <= 0:
            continue

        close_today = float(cur["Close"])
        close_prev  = float(prev["Close"])

        # 조건 1: 상승률
        if (close_today - close_prev) / close_prev < threshold:
            continue

        # 조건 2: 거래량 ≥ 2× MA30
        if pd.isna(cur["Volume"]) or pd.isna(cur["avg_vol30"]):
            continue
        vol_today = float(cur["Volume"])
        avg_vol30 = float(cur["avg_vol30"])
        if avg_vol30 <= 0 or vol_today < 2.0 * avg_vol30:
            continue

        # 조건 3: MA20 / MA60
        if pd.isna(cur["ma_20"]) or pd.isna(cur["ma_60"]):
            continue
        if close_today <= float(cur["ma_20"]) or close_today <= float(cur["ma_60"]):
            continue

        # 조건 4: 52주 고점 괴리율
        closes_52 = closes.iloc[max(0, i - 251): i + 1].dropna()
        if closes_52.empty:
            continue
        week52_high = float(closes_52.max())
        if week52_high <= 0 or (week52_high - close_today) / week52_high > 0.20:
            continue

        # 조건 6: RSI(14) >= 50
        rsi = cur.get("rsi_14")
        if rsi is None or pd.isna(rsi) or float(rsi) < 50:
            continue

        # 조건 7: 수급 강화 — (외인>0 AND 기관>=0) OR 기관 streak >= 3
        if flow_lookup is not None or streak_lookup is not None:
            f_net = i_net = f_str = i_str = None
            if flow_lookup is not None:
                flow = flow_lookup.get((ticker, row_date))
                if flow is not None:
                    f_net, i_net, _p = flow
            if streak_lookup is not None:
                streak = streak_lookup.get((ticker, row_date))
                if streak is not None:
                    f_str, i_str = streak
            if f_net is not None or i_net is not None or f_str is not None:
                both_buy  = f_net is not None and f_net > 0 and i_net is not None and i_net >= 0
                inst_3day = i_str is not None and i_str >= 3
                if not (both_buy or inst_3day):
                    continue

        # 조건 9: 외인+기관 합산 순매수 ≥ 상장주식수 0.2%
        if flow_lookup is not None and shares_lookup is not None:
            shares = shares_lookup.get(ticker)
            if shares and shares > 0:
                flow9 = flow_lookup.get((ticker, row_date))
                if flow9 is not None:
                    fn9, in9, _ = flow9
                    if fn9 is not None and in9 is not None:
                        if (fn9 + in9) < shares * 0.002:
                            continue

        # 조건 10 (v1.5): 20일 박스권 이탈 — close > 직전 20일 High 최고값
        high_20d = cur.get("high_20d")
        if high_20d is None or pd.isna(high_20d) or close_today <= float(high_20d):
            continue

        signals.append(SignalRecord(
            ticker=ticker,
            name=name,
            signal_date=row_date,
            close_at_signal=close_today,
            mode="stage_v15",
            market=market,
        ))

    return signals


def _replay_stage2_v13(
    ticker: str,
    name: str,
    daily_df: pd.DataFrame,
    market: str,
    config: "BacktestConfig",
    flow_lookup: Optional[dict] = None,
) -> list[SignalRecord]:
    """Stage 2 v1.3 walk-forward 재현.

    Stage 1 v1.3 신호 기반 + v1.2 조건 전체 + C6 개인 출회(personal_net ≤ 0).
    """
    s1_cfg     = _dc_replace(config, mode="stage_v13", start=config.start - timedelta(days=21))
    s1_signals = _replay_stage_v13(ticker, name, daily_df, market, s1_cfg)
    if not s1_signals:
        return []

    idx_map: dict[date, int] = {}
    for i, ts in enumerate(daily_df.index):
        d = ts.date() if isinstance(ts, datetime) else cast(date, ts)
        idx_map[d] = i

    df      = daily_df.copy()
    closes  = df["Close"]
    df["ma_20"] = closes.rolling(20, min_periods=20).mean()

    if "High" in df.columns and "Low" in df.columns:
        range_ratio = (df["High"] - df["Low"]) / df["Close"].replace(0, float("nan"))
        df["avg_range_ratio"] = range_ratio.shift(1).rolling(20, min_periods=10).mean()
        df["range_ratio"]     = range_ratio
    else:
        df["avg_range_ratio"] = float("nan")
        df["range_ratio"]     = float("nan")

    s2_signals: list[SignalRecord] = []
    seen_dates: set[date] = set()

    for s1 in s1_signals:
        s1_idx = idx_map.get(s1.signal_date)
        if s1_idx is None:
            continue

        s1_close = s1.close_at_signal
        if s1_close <= 0:
            continue

        s1_row = df.iloc[s1_idx]
        if pd.isna(s1_row["Volume"]) or pd.isna(s1_row["Close"]):
            continue
        txamt_s1 = float(s1_row["Volume"]) * float(s1_row["Close"])
        if txamt_s1 <= 0:
            continue

        cutoff = s1.signal_date + timedelta(days=14)

        for j in range(s1_idx + 1, len(df)):
            ts       = df.index[j]
            row_date = ts.date() if isinstance(ts, datetime) else cast(date, ts)
            if row_date > cutoff:
                break
            if row_date < config.start or row_date > config.end:
                continue
            if row_date in seen_dates:
                continue

            cur = df.iloc[j]
            if pd.isna(cur["Close"]) or pd.isna(cur["Volume"]):
                continue

            c_today     = float(cur["Close"])
            v_today     = float(cur["Volume"])
            txamt_today = v_today * c_today
            ma20        = cur["ma_20"]

            # C1: -5% ~ -20% 되돌림
            if not (0.80 <= c_today / s1_close <= 0.95):
                continue
            # C2: close >= MA20 * 0.95
            if pd.isna(ma20) or c_today < float(ma20) * 0.95:
                continue
            # C3: 거래대금 비율 [0.30, 0.60]
            if not (0.30 <= txamt_today / txamt_s1 <= 0.60):
                continue
            # C4: 거래대금이 20일 평균 대비 100~150%
            avg_txamt20 = float(closes.iloc[max(0, j - 20):j].mean()) * float(
                df["Volume"].iloc[max(0, j - 20):j].mean()
            ) if j >= 20 else 0.0
            if avg_txamt20 > 0 and not (1.0 <= txamt_today / avg_txamt20 <= 1.5):
                continue
            # C5: 일일 고저폭이 20일 평균 대비 70% 이하
            rr_today = cur.get("range_ratio")
            rr_avg   = cur.get("avg_range_ratio")
            if (
                rr_today is not None and not pd.isna(rr_today)
                and rr_avg   is not None and not pd.isna(rr_avg)
                and float(rr_avg) > 0
                and float(rr_today) > float(rr_avg) * 0.70
            ):
                continue
            # C6 (v1.3): 개인 출회 — personal_net ≤ 0 (데이터 없으면 통과)
            if flow_lookup is not None:
                flow_c6 = flow_lookup.get((ticker, row_date))
                if flow_c6 is not None:
                    _, _, p_net = flow_c6
                    if p_net is not None and p_net > 0:
                        continue

            seen_dates.add(row_date)
            s2_signals.append(SignalRecord(
                ticker=ticker,
                name=name,
                signal_date=row_date,
                close_at_signal=c_today,
                mode="stage2_v13",
                market=market,
            ))
            break

    return s2_signals


def _apply_cross_filter(signals: list[SignalRecord]) -> list[SignalRecord]:
    """Cross 모드: 동일 ISO 주에 ichimoku + stage 모두 발동한 티커의 stage 신호 반환."""
    from collections import defaultdict

    # 이치모쿠 신호가 있는 (ticker, week) 집합
    ichimoku_weeks: dict[str, set[str]] = defaultdict(set)
    for s in signals:
        if s.mode == "ichimoku":
            ichimoku_weeks[s.ticker].add(_week_label(s.signal_date))

    # 동일 주에 이치모쿠도 있는 stage 신호만 cross로 승격
    # (ticker, week) 당 가장 이른 신호 1건만 유지 — 같은 주에 Stage 1이 복수 발동해도 1건
    seen: set[tuple[str, str]] = set()
    cross: list[SignalRecord] = []
    for s in sorted(signals, key=lambda x: x.signal_date):
        if s.mode != "stage":
            continue
        week = _week_label(s.signal_date)
        if week not in ichimoku_weeks.get(s.ticker, set()):
            continue
        key = (s.ticker, week)
        if key in seen:
            continue
        seen.add(key)
        cross.append(SignalRecord(
            ticker=s.ticker,
            name=s.name,
            signal_date=s.signal_date,
            close_at_signal=s.close_at_signal,
            mode="cross",
            market=s.market,
        ))

    return cross


def _replay_stage2(
    ticker: str,
    name: str,
    daily_df: pd.DataFrame,
    market: str,
    config: BacktestConfig,
) -> list[SignalRecord]:
    """Stage 1 신호 후 14 캘린더일 이내 Stage 2 조건 walk-forward 재현.

    Stage 2 조건 (S1 신호일 다음날부터 14일 이내 매일 검사):
      C1: close가 S1 종가 대비 −5% ~ −20% 되돌림 (0.80 ≤ close/s1_close ≤ 0.95)
      C2: close ≥ MA20 × 0.95
      C3: 거래량 비율(vol_today / vol_s1) ∈ [0.30, 0.60]  — 조용한 눌림목
      C4: 기관 연속 매수 — 과거 데이터 없음, 건너뜀 (3/4 조건 재현)

    S1 1건당 S2 최대 1건(첫 번째 충족일). 같은 날짜에 복수의 S1이 S2를 가리키면
    가장 이른 S1 기준 신호만 유지.
    """
    # S1 재현: S2 윈도우 확보를 위해 start를 21일 앞으로 확장
    s1_cfg = _dc_replace(config, mode="stage", start=config.start - timedelta(days=21))
    s1_signals = _replay_stage(ticker, name, daily_df, market, s1_cfg)
    if not s1_signals:
        return []

    # 날짜 → 행 인덱스 매핑
    idx_map: dict[date, int] = {}
    for i, ts in enumerate(daily_df.index):
        d = ts.date() if isinstance(ts, datetime) else cast(date, ts)
        idx_map[d] = i

    df = daily_df.copy()
    df["ma_20"] = df["Close"].rolling(20, min_periods=20).mean()

    s2_signals: list[SignalRecord] = []
    seen_dates: set[date] = set()  # 동일 날짜 중복 신호 방지

    for s1 in s1_signals:
        s1_idx = idx_map.get(s1.signal_date)
        if s1_idx is None:
            continue

        s1_close = s1.close_at_signal
        if s1_close <= 0:
            continue

        s1_row = df.iloc[s1_idx]
        if pd.isna(s1_row["Volume"]) or pd.isna(s1_row["Close"]):
            continue
        txamt_s1 = float(s1_row["Volume"]) * float(s1_row["Close"])
        if txamt_s1 <= 0:
            continue

        cutoff = s1.signal_date + timedelta(days=14)

        for j in range(s1_idx + 1, len(df)):
            ts = df.index[j]
            row_date = ts.date() if isinstance(ts, datetime) else cast(date, ts)
            if row_date > cutoff:
                break
            if row_date < config.start or row_date > config.end:
                continue
            if row_date in seen_dates:
                continue

            cur = df.iloc[j]
            if pd.isna(cur["Close"]) or pd.isna(cur["Volume"]):
                continue

            c_today      = float(cur["Close"])
            v_today      = float(cur["Volume"])
            txamt_today  = v_today * c_today
            ma20         = cur["ma_20"]

            # C1: -5% ~ -20% 되돌림
            if not (0.80 <= c_today / s1_close <= 0.95):
                continue
            # C2: close ≥ MA20 × 0.95
            if pd.isna(ma20) or c_today < float(ma20) * 0.95:
                continue
            # C3: 거래대금 비율 [0.25, 0.65] (S2 가격 할인 -5%~-20% 반영한 조정 임계값)
            if not (0.25 <= txamt_today / txamt_s1 <= 0.65):
                continue
            # C4: 건너뜀

            seen_dates.add(row_date)
            s2_signals.append(SignalRecord(
                ticker=ticker,
                name=name,
                signal_date=row_date,
                close_at_signal=c_today,
                mode="stage2",
                market=market,
            ))
            break  # S1 1건당 S2 최대 1건

    return s2_signals


# _STOP_LOSS_PCT는 analysis/backtest/config.py로 이동 (상단 re-export 참조)


