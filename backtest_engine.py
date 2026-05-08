"""
backtest_engine.py  —  통합 백테스트 엔진 (Sprint 3)
────────────────────────────────────────────────────────────
3개 모드:
  ichimoku — 주봉 이치모쿠 7조건 walk-forward 재현
  stage    — 일봉 Stage 1 가격 조건 재현 (5/5 조건, 수급은 daily_flow 있을 때)
  cross    — 이치모쿠 + Stage 1 동일 ISO 주 교차

지표:
  승률 (7d/28d/91d), 평균/중앙값 수익률, KOSPI 초과수익률,
  샤프비율 (연환산), MDD (equity curve)

비용 기본값 (KRX 2025):
  매수 수수료 0.014% + 매도 수수료 0.014% + 증권거래세 0.180% + 농특세 0.002%
  ≈ 왕복 0.210%

CLI 사용법:
  python run_backtest.py --mode ichimoku --start 2025-01-01 --end 2026-01-01
  python run_backtest.py --mode stage    --start 2025-01-01 --end 2026-01-01 --market KOSDAQ
  python run_backtest.py --mode cross    --start 2025-01-01 --end 2026-01-01 --max 100
"""

from __future__ import annotations

import logging
import math
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd

from chart_screener import calc_ichimoku

logger = logging.getLogger(__name__)
_KST = ZoneInfo("Asia/Seoul")

# ── 거래 비용 ─────────────────────────────────────────────────────
_TX_BUY  = 0.000140                              # 증권사 매수 수수료
_TX_SELL = 0.000140 + 0.001800 + 0.000020        # 수수료 + 증권거래세 + 농특세
TX_COST_DEFAULT: float = _TX_BUY + _TX_SELL      # ≈ 0.0021 (0.210%)

_S1_THRESHOLD = {"KOSPI": 0.05, "KOSDAQ": 0.07}  # Stage 1 일봉 상승률 기준

MODE_KOR: dict[str, str] = {
    "ichimoku": "이치모쿠(주봉)",
    "stage":    "3단계(일봉)",
    "cross":    "이치모쿠×3단계",
}


# ── 데이터 클래스 ─────────────────────────────────────────────────

@dataclass
class BacktestConfig:
    mode: str                     # "ichimoku" | "stage" | "cross"
    start: date
    end: date
    market: str = "ALL"           # "KOSPI" | "KOSDAQ" | "ALL"
    tx_cost_rt: float = TX_COST_DEFAULT
    max_tickers: int = 200        # 0 = 전종목 (수십 분 소요)
    rf_rate_annual: float = 0.030  # 무위험수익률 (한국국채 3% 기준)
    workers: int = 8
    dsn: Optional[str] = None     # PostgreSQL DSN. 설정 시 daily_ohlcv 캐시 사용

    def __post_init__(self) -> None:
        if self.mode not in ("ichimoku", "stage", "cross"):
            raise ValueError(f"mode는 ichimoku|stage|cross 중 하나여야 합니다: {self.mode!r}")
        if self.start >= self.end:
            raise ValueError("start는 end보다 이전이어야 합니다")
        if self.market not in ("KOSPI", "KOSDAQ", "ALL"):
            raise ValueError(f"market은 KOSPI|KOSDAQ|ALL 중 하나여야 합니다: {self.market!r}")


@dataclass
class SignalRecord:
    ticker: str
    name: str
    signal_date: date
    close_at_signal: float
    mode: str                     # "ichimoku" | "stage" | "cross"
    market: str                   # "KOSPI" | "KOSDAQ"
    return_7d:  Optional[float] = None
    return_28d: Optional[float] = None
    return_91d: Optional[float] = None
    excess_7d:  Optional[float] = None
    excess_28d: Optional[float] = None
    excess_91d: Optional[float] = None


@dataclass
class GroupMetrics:
    n: int = 0
    win_rate_7d:        Optional[float] = None
    win_rate_28d:       Optional[float] = None
    win_rate_91d:       Optional[float] = None
    avg_return_28d:     Optional[float] = None
    median_return_28d:  Optional[float] = None
    avg_return_91d:     Optional[float] = None
    avg_excess_28d:     Optional[float] = None
    avg_excess_91d:     Optional[float] = None
    sharpe_7d:          Optional[float] = None  # 연환산 샤프비율 (7d 보유)
    sharpe_28d:         Optional[float] = None  # 연환산 샤프비율 (28d 보유)
    sharpe_91d:         Optional[float] = None  # 연환산 샤프비율 (91d 보유)
    mdd:                Optional[float] = None  # 최대낙폭 (equity curve)


@dataclass
class BacktestResult:
    config: BacktestConfig
    signals: list[SignalRecord]
    overall: GroupMetrics
    computed_at: str
    note: str = ""

    def to_telegram_report(self) -> str:
        """텔레그램 전송용 요약 리포트 (4096자 이내)."""
        cfg = self.config
        m   = self.overall

        def pct(v: Optional[float], dp: int = 1) -> str:
            return f"{v * 100:+.{dp}f}%" if v is not None else "N/A"

        def val(v: Optional[float], dp: int = 2) -> str:
            return f"{v:.{dp}f}" if v is not None else "N/A"

        mode_kor = MODE_KOR.get(cfg.mode, cfg.mode)

        lines = [
            f"📊 백테스트 — {mode_kor}",
            f"📅 {cfg.start} ~ {cfg.end}  {cfg.market}",
            f"🔢 신호 {m.n}건  비용 {cfg.tx_cost_rt * 100:.3f}% RT",
            "",
            "수익률 (7d / 28d / 91d)",
            f"  승률: {pct(m.win_rate_7d)} / {pct(m.win_rate_28d)} / {pct(m.win_rate_91d)}",
            f"  평균 28d: {pct(m.avg_return_28d)}  중앙값: {pct(m.median_return_28d)}",
            f"  평균 91d: {pct(m.avg_return_91d)}",
            f"  KOSPI 초과 28d: {pct(m.avg_excess_28d)}",
            "",
            "위험 지표",
            f"  샤프비율 7d: {val(m.sharpe_7d)}  28d: {val(m.sharpe_28d)}  91d: {val(m.sharpe_91d)}",
            f"  MDD: {pct(m.mdd)}",
        ]
        if self.note:
            lines += ["", f"⚠ {self.note}"]
        return "\n".join(lines)

    def to_text_report(self) -> str:
        """CLI 출력용 상세 텍스트 리포트."""
        cfg = self.config
        m   = self.overall

        def pct(v: Optional[float], dp: int = 2) -> str:
            return f"{v * 100:+.{dp}f}%" if v is not None else "N/A"

        def val(v: Optional[float], dp: int = 3) -> str:
            return f"{v:.{dp}f}" if v is not None else "N/A"

        mode_kor = MODE_KOR.get(cfg.mode, cfg.mode)

        lines = [
            "=" * 62,
            f"  백테스트 리포트 — {mode_kor}",
            f"  기간: {cfg.start} ~ {cfg.end}  |  시장: {cfg.market}",
            f"  거래비용(RT): {cfg.tx_cost_rt * 100:.3f}%  |  무위험률: {cfg.rf_rate_annual * 100:.1f}%",
            "=" * 62,
            f"  총 신호 수: {m.n}건",
            "",
            "[ 수익률 ]",
            f"  승률  7d : {pct(m.win_rate_7d, 1)}",
            f"  승률 28d : {pct(m.win_rate_28d, 1)}"
            f"   평균: {pct(m.avg_return_28d)}   중앙값: {pct(m.median_return_28d)}",
            f"  승률 91d : {pct(m.win_rate_91d, 1)}"
            f"   평균: {pct(m.avg_return_91d)}",
            "",
            "[ KOSPI 초과수익률 ]",
            f"  28d: {pct(m.avg_excess_28d)}   91d: {pct(m.avg_excess_91d)}",
            "",
            "[ 위험 지표 ]",
            f"  샤프비율  7d 연환산: {val(m.sharpe_7d)}",
            f"  샤프비율 28d 연환산: {val(m.sharpe_28d)}",
            f"  샤프비율 91d 연환산: {val(m.sharpe_91d)}",
            f"  MDD:               {pct(m.mdd)}",
            "",
            f"  산출일시: {self.computed_at}",
        ]
        if self.note:
            lines += ["", f"주의: {self.note}"]
        lines.append("=" * 62)
        return "\n".join(lines)


# ── 유틸 함수 ─────────────────────────────────────────────────────

def _week_label(d: date) -> str:
    """ISO 주차 레이블. 예: '2025-W03'"""
    iso = d.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _build_price_lookup(df: pd.DataFrame) -> dict[date, float]:
    """DataFrame → {날짜: 종가} dict."""
    result: dict[date, float] = {}
    for ts, row in df.iterrows():
        d = ts.date() if hasattr(ts, "date") else ts
        if not pd.isna(row["Close"]):
            result[d] = float(row["Close"])
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


def _compute_group_metrics(
    signals: list[SignalRecord], rf_annual: float
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
    m.mdd        = _compute_mdd(r28s)

    return m


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
    weekly = daily_df.resample("W-FRI", closed="right", label="right").agg({
        "Open":   "first",
        "High":   "max",
        "Low":    "min",
        "Close":  "last",
        "Volume": "sum",
    }).dropna(subset=["Close"])

    # Ichimoku 선행스팬B: 52주 lookback 최소 요건
    if len(weekly) < 62:
        return []

    weekly = calc_ichimoku(weekly)
    weekly["ma_20w"]  = weekly["Close"].rolling(20,  min_periods=20).mean()
    weekly["ma_60w"]  = weekly["Close"].rolling(60,  min_periods=60).mean()
    weekly["ma_120w"] = weekly["Close"].rolling(120, min_periods=100).mean()

    signals: list[SignalRecord] = []

    for i in range(1, len(weekly)):
        row_date = weekly.index[i].date()
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
    flow_lookup: Optional[dict[tuple[str, date], tuple[Optional[int], Optional[int]]]] = None,
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
    closes = df["Close"]
    vols   = df["Volume"]

    df["ma_20"] = closes.rolling(20, min_periods=20).mean()
    df["ma_60"] = closes.rolling(60, min_periods=60).mean()

    signals: list[SignalRecord] = []

    # i=0는 change_pct 계산 불가, i<21은 20일 거래량 평균 불가
    for i in range(21, len(df)):
        row_date = df.index[i].date()
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
                f_net, i_net = flow
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


def _fill_returns(
    sig: SignalRecord,
    stock_lookup: dict[date, float],
    kospi_lookup: dict[date, float],
    tx_cost_rt: float,
) -> None:
    """신호에 수익률 및 초과수익률 채우기 (거래비용 차감 포함)."""
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


# ── 메인 실행 ─────────────────────────────────────────────────────

def run_backtest(config: BacktestConfig) -> BacktestResult:
    """백테스트 메인 함수. CLI 및 Telegram 봇에서 동기 호출."""
    from chart_screener import get_all_tickers

    logger.info(
        "[백테스트] 모드=%s 기간=%s~%s 시장=%s 최대티커=%s",
        config.mode, config.start, config.end, config.market, config.max_tickers or "전종목",
    )

    # 1. 티커 목록 (FinanceDataReader)
    all_tickers = get_all_tickers()
    if config.market == "KOSPI":
        tickers = [(t, n, s) for t, n, s in all_tickers if t.endswith(".KS")]
    elif config.market == "KOSDAQ":
        tickers = [(t, n, s) for t, n, s in all_tickers if t.endswith(".KQ")]
    else:
        tickers = all_tickers

    if config.max_tickers > 0:
        tickers = tickers[:config.max_tickers]

    logger.info("[백테스트] 대상 티커 %d개", len(tickers))

    # 2. 데이터 수집 범위
    #   전방: MA120w(주봉 120주=840일) + 여유 → 2년(760일) lookback
    #   후방: 최대 보유 91일 + 여유 14일
    fetch_start = config.start - timedelta(days=760)
    fetch_end   = config.end   + timedelta(days=105)

    # 3. OHLCV 병렬 수집
    if config.dsn:
        from ohlcv_cache import batch_fetch_cached, fetch_index_cached
        ticker_pairs = [
            (t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI")
            for t, _, _ in tickers
        ]
        ohlcv_map = batch_fetch_cached(
            ticker_pairs, fetch_start, fetch_end,
            config.workers, config.dsn, _fetch_single_ohlcv,
        )
        kospi_df = fetch_index_cached(
            "^KS11", "IDX", fetch_start, fetch_end,
            config.dsn, _fetch_index,
        )
    else:
        ticker_syms = [t for t, _, _ in tickers]
        ohlcv_map   = _batch_fetch_ohlcv(ticker_syms, fetch_start, fetch_end, config.workers)
        kospi_df    = _fetch_index("^KS11", fetch_start, fetch_end)

    # 4. KOSPI 벤치마크 조회
    kospi_lookup = _build_price_lookup(kospi_df) if kospi_df is not None else {}

    # 5. 수급 데이터 사전 로드 (stage/cross 모드 + DSN 있을 때)
    flow_lookup: Optional[dict] = None
    if config.dsn and config.mode in ("stage", "cross"):
        try:
            from ohlcv_cache import load_flow_data
            ticker_syms = [t for t, _, _ in tickers]
            flow_lookup = load_flow_data(config.dsn, ticker_syms, config.start, config.end)
            logger.info("[백테스트] 수급 데이터 로드: %d건", len(flow_lookup))
        except Exception as e:
            logger.warning("[백테스트] 수급 데이터 로드 실패 (조건 5 생략): %s", e)

    # 6. 신호 재현
    all_signals: list[SignalRecord] = []

    for ticker, name, _ in tickers:
        df = ohlcv_map.get(ticker)
        if df is None or df.empty:
            continue
        mkt = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"

        if config.mode in ("ichimoku", "cross"):
            all_signals.extend(_replay_ichimoku(ticker, name, df, mkt, config))
        if config.mode in ("stage", "cross"):
            all_signals.extend(_replay_stage(ticker, name, df, mkt, config, flow_lookup))

    # 7. Cross 필터
    if config.mode == "cross":
        all_signals = _apply_cross_filter(all_signals)

    # 8. 수익률 계산
    stock_lookup_cache: dict[str, dict[date, float]] = {}
    for sig in all_signals:
        df = ohlcv_map.get(sig.ticker)
        if df is None:
            continue
        if sig.ticker not in stock_lookup_cache:
            stock_lookup_cache[sig.ticker] = _build_price_lookup(df)
        _fill_returns(sig, stock_lookup_cache[sig.ticker], kospi_lookup, config.tx_cost_rt)

    # 9. 날짜 순 정렬 → MDD equity curve가 시간 순서대로 누적
    all_signals.sort(key=lambda s: s.signal_date)

    # 10. 집계 지표
    overall = _compute_group_metrics(all_signals, config.rf_rate_annual)

    note = ""
    if config.mode in ("stage", "cross"):
        if flow_lookup is not None:
            note = f"Stage 1: 수급 조건(외국인·기관 순매수) 적용 — {len(flow_lookup)}건 기준"
        else:
            note = "Stage 1: 수급 조건(외국인·기관 순매수) 제외 — daily_flow 데이터 없음"

    logger.info(
        "[백테스트] 완료 — 신호:%d 승률28d:%s",
        overall.n,
        f"{overall.win_rate_28d * 100:.1f}%" if overall.win_rate_28d is not None else "N/A",
    )

    return BacktestResult(
        config=config,
        signals=all_signals,
        overall=overall,
        computed_at=datetime.now(_KST).isoformat(),
        note=note,
    )
