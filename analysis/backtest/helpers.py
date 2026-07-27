"""백테스트 유틸/지표 헬퍼 (backtest_engine.py에서 이동, Phase C).

주 라벨·가격 조회·샤프/MDD/RSI·주봉 이치모쿠·그룹 메트릭 집계.
"""

from __future__ import annotations

import math
import statistics
from datetime import date, datetime, timedelta
from typing import Optional, cast

import pandas as pd

from analysis.backtest.models import GroupMetrics, SignalRecord

# ── 유틸 함수 ─────────────────────────────────────────────────────

def _week_label(d: date) -> str:
    """ISO 주차 레이블. 예: '2025-W03'"""
    iso = d.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _build_price_lookup(df: pd.DataFrame) -> dict[date, float]:
    """DataFrame → {날짜: 종가} dict."""
    result: dict[date, float] = {}
    for ts, row in df.iterrows():
        d = ts.date() if isinstance(ts, datetime) else cast(date, ts)
        close = cast(float, row["Close"])
        if not pd.isna(close):
            result[d] = float(close)
    return result


def _nearest_price(
    lookup: dict[date, float], target: date, max_days: int = 7
) -> Optional[float]:
    """target부터 최대 max_days 이내 가장 가까운 미래 거래일 종가 반환."""
    for offset in range(max_days + 1):
        p = lookup.get(target + timedelta(days=offset))
        if p is not None:
            return p
    return None


def _entry_on_or_after(
    lookup: dict[date, float], target: date, max_days: int = 7
) -> Optional[tuple[date, float]]:
    """target(포함)부터 최대 max_days 이내 첫 거래일의 (날짜, 종가) 반환.

    compose 진입가 산정용: 신호는 해당 주 금요일 기준이므로 그날(또는 다음
    거래일, 보통 월요일)에 진입. 반환 날짜는 OHLCV 인덱스에 존재하는 실제
    거래일이라 _compute_sell_signals_and_s2 의 idx_map 매칭이 보장된다."""
    for offset in range(max_days + 1):
        d = target + timedelta(days=offset)
        p = lookup.get(d)
        if p is not None:
            return d, p
    return None


def _compute_sharpe(
    returns: list[float], hold_days: int, rf_annual: float
) -> Optional[float]:
    """연환산 샤프비율. hold_days 보유 기준 신호 단위 수익률 사용."""
    if len(returns) < 3:
        return None
    periods_per_year = 252.0 / hold_days
    rf_per_period    = rf_annual / periods_per_year
    mean_r = statistics.mean(returns)
    std_r  = statistics.stdev(returns)
    if std_r == 0.0:
        return None
    return (mean_r - rf_per_period) / std_r * math.sqrt(periods_per_year)


def _compute_mdd(returns: list[float]) -> Optional[float]:
    """신호 순서대로 누적한 equity curve의 최대낙폭(MDD).

    equal-weight, 순차 포지션 가정. 수익률 목록은 날짜 순 정렬 후 전달.
    주의: 전달하는 수익률 시계열이 서로 겹치지 않는 "기간별" 수익률이어야
    이 가정이 성립한다 — 동시 보유 신호가 많은 전략(주간 top-N 등)에서는
    개별 신호 수익률을 그대로 넘기면 안 되고 _compute_portfolio_returns()로
    먼저 동시보유 포트폴리오 수익률 시계열로 변환해야 한다 (2026-07 발견:
    신호 수천 건을 전량 순차 올인으로 복리 계산해 MDD가 상시 -100%로 나오던 버그).
    """
    if not returns:
        return None
    equity = peak = 1.0
    max_dd = 0.0
    for r in returns:
        equity *= (1.0 + r)
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak
        if dd > max_dd:
            max_dd = dd
    return -max_dd


def _compute_portfolio_returns_from_intervals(
    intervals: list[tuple[date, date, float]],
    period_days: int = 7,
) -> list[float]:
    """(진입일, 청산일, 총수익률) 구간 목록을 동일비중 동시보유 포트폴리오의
    기간별 수익률 시계열로 변환한다.

    _compute_mdd는 "겹치지 않는 기간별 수익률"을 전제하는데, 신호 자체는
    보유기간이 겹치는 경우가 흔하다(주간 top-20 선정, 완화된 스크리너,
    분할청산으로 신호별 실제 보유일수가 제각각인 경우 등). 실제 일별
    가격 경로 없이 종료 수익률만으로 겹침을 반영하기 위해, 각 구간의
    수익률이 보유기간 동안 일정 비율로 복리 증가한다고 가정하고
    (일일률 = (1+r)**(1/hold_days)-1) 각 기간에 해당하는 몫만 추출해
    그 기간에 활성인 모든 포지션의 동일비중 평균을 취한다.
    활성 포지션이 0건인 기간은 현금 보유(수익률 0)로 취급한다.
    """
    valid = [(d0, d1, r) for d0, d1, r in intervals if d1 > d0]
    if not valid:
        return []
    start = min(d0 for d0, _, _ in valid)
    end   = max(d1 for _, d1, _ in valid)

    period_returns: list[float] = []
    cur = start
    while cur < end:
        active_period_rs = []
        for d0, d1, r in valid:
            if d0 <= cur < d1:
                hold_days = (d1 - d0).days
                daily_r = (1.0 + r) ** (1.0 / hold_days) - 1.0
                active_period_rs.append((1.0 + daily_r) ** period_days - 1.0)
        period_returns.append(
            sum(active_period_rs) / len(active_period_rs) if active_period_rs else 0.0
        )
        cur += timedelta(days=period_days)
    return period_returns


def _compute_portfolio_returns(
    signals: list[SignalRecord],
    hold_days: int = 28,
    period_days: int = 7,
) -> list[float]:
    """신호를 동시 보유 포지션으로 취급해 동일비중 리밸런싱 포트폴리오의
    기간별(기본 주간) 수익률 시계열을 만든다.

    고정 보유기간(hold_days)에 대해 return_Nd만으로 계산하는 얇은 래퍼 —
    실제 보유기간이 신호마다 다른 경우(분할청산 등)는
    _compute_portfolio_returns_from_intervals를 직접 사용할 것.
    """
    hold_field = {7: "return_7d", 28: "return_28d", 91: "return_91d"}.get(hold_days)
    if hold_field is None:
        raise ValueError(f"hold_days는 7|28|91 중 하나여야 합니다: {hold_days!r}")

    entries = [
        (s.signal_date, getattr(s, hold_field))
        for s in signals
        if getattr(s, hold_field) is not None
    ]
    if not entries:
        return []

    intervals = [(d, d + timedelta(days=hold_days), r) for d, r in entries]
    return _compute_portfolio_returns_from_intervals(intervals, period_days)


def _build_weekly_ichimoku(daily_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """일봉 → 주봉 리샘플 후 이치모쿠 지표(구름+전환선+기준선) 계산.

    반환 컬럼: cloud_top, cloud_bottom, tenkan(전환선), kijun(기준선).
    데이터 부족(< 62주)이면 None.
    """
    from ta.trend import IchimokuIndicator

    weekly = cast(pd.DataFrame, daily_df.resample("W-FRI", closed="right", label="right").agg({
        "Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum",
    })).dropna(subset=["Close"])
    if len(weekly) < 62:
        return None

    ind = IchimokuIndicator(high=cast(pd.Series, weekly["High"]), low=cast(pd.Series, weekly["Low"]), visual=False)
    weekly = weekly.copy()
    weekly["senkou_a"]    = ind.ichimoku_a()
    weekly["senkou_b"]    = ind.ichimoku_b()
    weekly["cloud_top"]   = weekly[["senkou_a", "senkou_b"]].max(axis=1)
    weekly["cloud_bottom"]= weekly[["senkou_a", "senkou_b"]].min(axis=1)
    weekly["tenkan"]      = ind.ichimoku_conversion_line()  # 전환선
    weekly["kijun"]       = ind.ichimoku_base_line()        # 기준선
    return weekly


def _find_ichimoku_sell(
    signal_date: date,
    entry_price: float,
    weekly_df: pd.DataFrame,
    tx_cost_rt: float,
    stop_loss_pct: float,
) -> tuple[Optional[date], str, Optional[float], Optional[int]]:
    """주봉 이치모쿠 기반 매도 신호 탐지.

    우선순위 (먼저 발생한 조건 채택):
      1. 손절: 주봉 종가 ≤ 진입가 × (1 − stop_loss_pct)
      2. 구름 이탈: 주봉 종가 < cloud_bottom (구름 하향 이탈)
      3. 데드크로스: 전환선이 기준선 아래로 하향 돌파

    반환: (sell_date, sell_reason, sell_return, hold_days)
    """
    stop_price   = entry_price * (1 - stop_loss_pct)
    prev_tenkan: Optional[float] = None
    prev_kijun:  Optional[float] = None

    for i in range(len(weekly_df)):
        ts       = weekly_df.index[i]
        row_date = ts.date() if isinstance(ts, datetime) else cast(date, ts)
        row      = weekly_df.iloc[i]

        tenkan = float(row["tenkan"]) if not pd.isna(row["tenkan"]) else None
        kijun  = float(row["kijun"])  if not pd.isna(row["kijun"])  else None

        if row_date <= signal_date:
            prev_tenkan = tenkan
            prev_kijun  = kijun
            continue

        close = float(row["Close"]) if not pd.isna(row["Close"]) else None
        if close is None:
            prev_tenkan = tenkan
            prev_kijun  = kijun
            continue

        hd  = (row_date - signal_date).days

        def _ret(p: float) -> float:
            return (p / entry_price - 1.0) - tx_cost_rt

        # 1순위: 손절
        if close <= stop_price:
            return row_date, f"손절 -{stop_loss_pct * 100:.0f}%", _ret(close), hd

        # 2순위: 구름 하향 이탈
        cloud_bottom = row.get("cloud_bottom")
        if cloud_bottom is not None and not pd.isna(cloud_bottom):
            if close < float(cloud_bottom):
                return row_date, "구름 이탈", _ret(close), hd

        # 3순위: 전환선 < 기준선 데드크로스
        if (tenkan is not None and kijun is not None and
                prev_tenkan is not None and prev_kijun is not None):
            if tenkan < kijun and prev_tenkan >= prev_kijun:
                return row_date, "전환<기준 DC", _ret(close), hd

        prev_tenkan = tenkan
        prev_kijun  = kijun

    return None, "보유 중", None, None


def _compute_rsi(closes: pd.Series, period: int = 14) -> pd.Series:
    """RSI(period) — Wilder 지수이동평균 방식."""
    delta    = closes.diff()
    gain     = delta.clip(lower=0)
    loss     = (-delta).clip(lower=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs       = avg_gain / avg_loss.replace(0.0, float("nan"))
    return cast(pd.Series, 100.0 - 100.0 / (1.0 + rs))


def _compute_group_metrics(
    signals: list[SignalRecord], rf_annual: float, hold_weeks: Optional[int] = None
) -> GroupMetrics:
    """신호 목록에서 집계 지표 계산."""
    if not signals:
        return GroupMetrics()

    m = GroupMetrics(n=len(signals))

    r7s   = [s.return_7d  for s in signals if s.return_7d  is not None]
    r28s  = [s.return_28d for s in signals if s.return_28d is not None]
    r91s  = [s.return_91d for s in signals if s.return_91d is not None]
    ex28s = [s.excess_28d for s in signals if s.excess_28d is not None]
    ex91s = [s.excess_91d for s in signals if s.excess_91d is not None]

    if r7s:
        m.win_rate_7d = sum(1 for r in r7s if r > 0) / len(r7s)
    if r28s:
        m.win_rate_28d      = sum(1 for r in r28s if r > 0) / len(r28s)
        m.avg_return_28d    = statistics.mean(r28s)
        m.median_return_28d = statistics.median(r28s)
    if r91s:
        m.win_rate_91d   = sum(1 for r in r91s if r > 0) / len(r91s)
        m.avg_return_91d = statistics.mean(r91s)
    if ex28s:
        m.avg_excess_28d = statistics.mean(ex28s)
    if ex91s:
        m.avg_excess_91d = statistics.mean(ex91s)

    m.sharpe_7d  = _compute_sharpe(r7s,  hold_days=7,  rf_annual=rf_annual)
    m.sharpe_28d = _compute_sharpe(r28s, hold_days=28, rf_annual=rf_annual)
    m.sharpe_91d = _compute_sharpe(r91s, hold_days=91, rf_annual=rf_annual)
    # 동시보유 포지션을 사전에 반영한 포트폴리오 수익률 시계열 → MDD.
    # r28s를 그대로 넘기면 겹치는 신호를 순차 올인으로 잘못 복리 계산한다.
    m.mdd        = _compute_mdd(_compute_portfolio_returns(signals, hold_days=28, period_days=7))

    if hold_weeks is not None:
        hold_days = hold_weeks * 7
        m.hold_days_custom = hold_days
        rcs  = [s.return_custom for s in signals if s.return_custom is not None]
        excs = [s.excess_custom for s in signals if s.excess_custom is not None]
        if rcs:
            m.win_rate_custom      = sum(1 for r in rcs if r > 0) / len(rcs)
            m.avg_return_custom    = statistics.mean(rcs)
            m.median_return_custom = statistics.median(rcs)
        if excs:
            m.avg_excess_custom = statistics.mean(excs)
        m.sharpe_custom = _compute_sharpe(rcs, hold_days=hold_days, rf_annual=rf_annual)

    # 매도 신호 기반 집계 — blended_return(분할 청산 가중평균) 우선, 없으면 sell_return
    sell_rets: list[float] = []
    for s in signals:
        v = s.blended_return if s.blended_return is not None else s.sell_return
        if v is not None:
            sell_rets.append(v)
    if sell_rets:
        m.win_rate_sell      = sum(1 for r in sell_rets if r > 0) / len(sell_rets)
        m.avg_return_sell    = statistics.mean(sell_rets)
        m.median_return_sell = statistics.median(sell_rets)
    hold_days_list = [s.hold_days for s in signals if s.hold_days is not None]
    if hold_days_list:
        m.avg_hold_days = statistics.mean(hold_days_list)
    s1_sigs = [s for s in signals if s.mode in ("stage", "stage_v11", "stage_v12", "stage_v13", "stage_v14", "stage_v15")]
    if s1_sigs:
        m.s2_progression_rate = sum(1 for s in s1_sigs if s.s2_date is not None) / len(s1_sigs)
    s2_sigs = [s for s in signals if s.s2_date is not None]
    if s2_sigs:
        m.s3_progression_rate = sum(1 for s in s2_sigs if s.s3_date is not None) / len(s2_sigs)
    mdd_list = [s.mdd_91d for s in signals if s.mdd_91d is not None]
    if mdd_list:
        m.avg_mdd_91d = statistics.mean(mdd_list)

    return m




def _fill_returns(
    sig: SignalRecord,
    stock_lookup: dict[date, float],
    kospi_lookup: dict[date, float],
    tx_cost_rt: float,
    hold_weeks: Optional[int] = None,
) -> None:
    """신호에 수익률 및 초과수익률 채우기 (거래비용 차감 포함).

    hold_weeks가 지정되면 N주(N*7일) 보유 수익률을 return_custom/excess_custom에도 채운다.
    표준 기간(1/4/13w)이더라도 return_custom에 중복 저장하므로 리포트 로직이 단순해진다.
    """
    base = sig.close_at_signal
    if base == 0:
        return

    def _ret(days: int) -> Optional[float]:
        price = _nearest_price(stock_lookup, sig.signal_date + timedelta(days=days))
        return (price / base - 1.0) - tx_cost_rt if price is not None else None

    def _kospi_ret(days: int) -> Optional[float]:
        k0 = _nearest_price(kospi_lookup, sig.signal_date)
        k1 = _nearest_price(kospi_lookup, sig.signal_date + timedelta(days=days))
        if k0 is None or k1 is None or k0 == 0:
            return None
        return k1 / k0 - 1.0

    sig.return_7d  = _ret(7)
    sig.return_28d = _ret(28)
    sig.return_91d = _ret(91)

    k7  = _kospi_ret(7)
    k28 = _kospi_ret(28)
    k91 = _kospi_ret(91)

    sig.excess_7d  = sig.return_7d  - k7  if sig.return_7d  is not None and k7  is not None else None
    sig.excess_28d = sig.return_28d - k28 if sig.return_28d is not None and k28 is not None else None
    sig.excess_91d = sig.return_91d - k91 if sig.return_91d is not None and k91 is not None else None

    if hold_weeks is not None:
        hold_days = hold_weeks * 7
        sig.return_custom = _ret(hold_days)
        kc = _kospi_ret(hold_days)
        sig.excess_custom = (
            sig.return_custom - kc
            if sig.return_custom is not None and kc is not None
            else None
        )


