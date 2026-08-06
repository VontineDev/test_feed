"""quant_signals.py — TechnicalQuant.md 기반 기술적 조건 백테스트.

2026-08-06: 사용자가 제공한 퀀트 전략 문서(종목선택 + 매매타이밍)를 검증하기
위해 도입. 종목선택(펀더멘털: PBR/PER/ROE/부채비율/매출증가율)은 DB에 전체
시장 규모 데이터가 없어(dart_xbrl 5,143행뿐, 전종목 아님) 제외 — 사용자 결정
(2026-08-06 대화)으로 기술적 조건만 먼저 검증. 유니버스 필터는 거래대금/
시가총액(둘 다 daily_ohlcv/krx_listings로 계산 가능)만 지원.

라이브 모의투자가 쓰는 analysis/backtest/exit_models.py는 건드리지 않는다 —
이 모듈은 순수 리서치용 자기완결 청산 로직을 따로 둔다(실거래 안정성과 분리).

조건은 전부 "상태"가 아니라 "전이"(어제 False → 오늘 True)로 판정한다.
예: MA20 돌파(A)를 상태로 재면 상승추세 내내 매일 신호가 발생해 사실상 같은
추세를 수십 번 중복 계산하게 된다 — 실제 트레이딩에서 "진입 시점"의 의미와도
맞지 않아 전이 판정으로 통일.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Callable, Optional, cast

import pandas as pd

from analysis.backtest.config import TX_COST_DEFAULT
from analysis.backtest.helpers import _compute_rsi
from analysis.backtest.models import SignalRecord
from core.ohlcv_cache import FlowKey, StreakValue

# ── 지표 계산 ────────────────────────────────────────────────────

def _compute_macd(
    closes: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9
) -> tuple[pd.Series, pd.Series]:
    """MACD선(fast EMA - slow EMA)과 시그널선(MACD의 signal EMA) 반환."""
    ema_fast = closes.ewm(span=fast, min_periods=fast, adjust=False).mean()
    ema_slow = closes.ewm(span=slow, min_periods=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, min_periods=signal, adjust=False).mean()
    return macd_line, signal_line


def compute_indicators(daily_df: pd.DataFrame) -> pd.DataFrame:
    """OHLCV 일봉에 문서의 매매타이밍 조건에 필요한 지표 컬럼을 붙여 반환.

    추가 컬럼: ma5/ma20/ma60/ma120, rsi14, macd/macd_signal,
    high20_prev(전일까지 20일 최고가 — 오늘 돌파 여부 판정용), vol_prev(전일 거래량).
    """
    df = daily_df.copy()
    closes = cast(pd.Series, df["Close"])
    df["ma5"]   = closes.rolling(5,   min_periods=5).mean()
    df["ma20"]  = closes.rolling(20,  min_periods=20).mean()
    df["ma60"]  = closes.rolling(60,  min_periods=60).mean()
    df["ma120"] = closes.rolling(120, min_periods=100).mean()
    df["rsi14"] = _compute_rsi(closes)
    macd, macd_sig = _compute_macd(closes)
    df["macd"] = macd
    df["macd_signal"] = macd_sig
    # 전일까지의 20일 최고가 — 오늘 종가가 이를 넘으면 "신고가 돌파"
    df["high20_prev"] = df["High"].shift(1).rolling(20, min_periods=20).max()
    df["vol_prev"] = df["Volume"].shift(1)
    return df


# ── 진입 조건 (전이 판정: 어제 False → 오늘 True) ─────────────────
# 각 함수는 (cur, prev) row 페어를 받아 오늘이 "막 조건을 충족한 첫날"인지 판정.

def _cond_ma20_breakout(cur, prev) -> bool:
    """A. 이평선 돌파: 주가>MA20 AND MA5>MA20 (상태 전이)."""
    def _state(row) -> bool:
        if pd.isna(row.get("ma20")) or pd.isna(row.get("ma5")):
            return False
        return float(row["Close"]) > float(row["ma20"]) and float(row["ma5"]) > float(row["ma20"])
    return _state(cur) and not _state(prev)


def _cond_ma_alignment(cur, prev) -> bool:
    """B. 정배열 진입: MA5>MA20>MA60>MA120 (상태 전이)."""
    def _state(row) -> bool:
        vals = [row.get("ma5"), row.get("ma20"), row.get("ma60"), row.get("ma120")]
        if any(pd.isna(v) for v in vals):
            return False
        v5, v20, v60, v120 = (float(v) for v in vals)
        return v5 > v20 > v60 > v120
    return _state(cur) and not _state(prev)


def _cond_rsi_macd_rebound(cur, prev) -> bool:
    """C. 보조지표 반등: RSI(14) 30 상향돌파 OR MACD 골든크로스 (둘 다 전이 이벤트)."""
    rsi_cross = False
    if not pd.isna(cur.get("rsi14")) and not pd.isna(prev.get("rsi14")):
        rsi_cross = float(prev["rsi14"]) < 30 and float(cur["rsi14"]) >= 30
    macd_cross = False
    if (not pd.isna(cur.get("macd")) and not pd.isna(cur.get("macd_signal"))
            and not pd.isna(prev.get("macd")) and not pd.isna(prev.get("macd_signal"))):
        macd_cross = (float(prev["macd"]) <= float(prev["macd_signal"])
                      and float(cur["macd"]) > float(cur["macd_signal"]))
    return rsi_cross or macd_cross


def _cond_new_high20(cur, prev) -> bool:
    """D. 신고가 돌파: 종가 >= 전일까지 20일 최고가 (오늘 처음 돌파한 날만)."""
    if pd.isna(cur.get("high20_prev")):
        return False
    today_breaks = float(cur["Close"]) >= float(cur["high20_prev"])
    if not today_breaks:
        return False
    if pd.isna(prev.get("high20_prev")):
        return True
    prev_would_break = float(prev["Close"]) >= float(prev["high20_prev"])
    return not prev_would_break


def _cond_scenario1_entry(cur, prev) -> bool:
    """시나리오1 매수: MA20 돌파 + 당일 거래량 >= 전일 거래량의 200%."""
    if not _cond_ma20_breakout(cur, prev):
        return False
    if pd.isna(cur.get("vol_prev")) or float(cur["vol_prev"]) <= 0:
        return False
    return float(cur["Volume"]) >= 2.0 * float(cur["vol_prev"])


def _cond_scenario2_entry(cur, prev) -> bool:
    """시나리오2 매수: RSI(14) 30 하향 후 재상승 돌파 시 종가 매수 (= RSI 반등과 동일 정의)."""
    if pd.isna(cur.get("rsi14")) or pd.isna(prev.get("rsi14")):
        return False
    return float(prev["rsi14"]) < 30 and float(cur["rsi14"]) >= 30


# 수급 조건(E)은 flow_lookup이 필요해 별도 처리 — _replay_quant에서 직접 판정.

ENTRY_CONDITIONS: dict[str, Callable] = {
    "A_ma20_breakout":   _cond_ma20_breakout,
    "B_ma_alignment":    _cond_ma_alignment,
    "C_rsi_macd_rebound": _cond_rsi_macd_rebound,
    "D_new_high20":      _cond_new_high20,
    "SCENARIO1":         _cond_scenario1_entry,
    "SCENARIO2":         _cond_scenario2_entry,
}


# ── 청산 로직 (자기완결 — exit_models.py와 무관, 라이브 트레이딩 미영향) ──

def _scan_exit(
    df: pd.DataFrame,
    entry_idx: int,
    entry_price: float,
    signal_date: date,
    hard_stop_pct: float,
    target_pct: Optional[float],
    use_ma20_exit: bool,
    use_rsi70_exit: bool,
    tx_cost_rt: float,
) -> tuple[Optional[date], str, Optional[float], Optional[int]]:
    """진입 다음날부터 청산 조건을 스캔. (sell_date, sell_reason, sell_return, hold_days) 반환.

    우선순위: 손절 → 목표가/RSI70 익절 → MA20 이탈 → 기간 종료.
    """
    stop_price = entry_price * (1.0 - hard_stop_pct)
    target_price = entry_price * (1.0 + target_pct) if target_pct else None

    for j in range(entry_idx + 1, len(df)):
        ts = df.index[j]
        row_date = ts.date() if isinstance(ts, datetime) else cast(date, ts)
        cur = df.iloc[j]
        if pd.isna(cur["Close"]):
            continue
        close = float(cur["Close"])
        is_last = (j == len(df) - 1)

        if close <= stop_price:
            ret = (close / entry_price - 1.0) - tx_cost_rt
            return row_date, f"손절 -{hard_stop_pct*100:.0f}%", ret, (row_date - signal_date).days

        if target_price is not None and close >= target_price:
            ret = (close / entry_price - 1.0) - tx_cost_rt
            target_pct_disp = target_price / entry_price - 1.0
            return row_date, f"목표가 +{target_pct_disp*100:.0f}%", ret, (row_date - signal_date).days

        if use_rsi70_exit and not pd.isna(cur.get("rsi14")) and float(cur["rsi14"]) > 70:
            ret = (close / entry_price - 1.0) - tx_cost_rt
            return row_date, "RSI70 익절", ret, (row_date - signal_date).days

        if use_ma20_exit and not pd.isna(cur.get("ma20")) and close < float(cur["ma20"]):
            ret = (close / entry_price - 1.0) - tx_cost_rt
            return row_date, "MA20 이탈", ret, (row_date - signal_date).days

        if is_last:
            ret = (close / entry_price - 1.0) - tx_cost_rt
            return row_date, "보유 중 (기간 종료)", ret, (row_date - signal_date).days

    return None, "보유 중", None, None


def replay_quant(
    ticker: str,
    name: str,
    daily_df: pd.DataFrame,
    market: str,
    start: date,
    end: date,
    entry_key: str,
    hard_stop_pct: float = 0.05,
    target_pct: Optional[float] = 0.15,
    use_ma20_exit: bool = True,
    use_rsi70_exit: bool = False,
    flow_lookup: Optional[dict[FlowKey, StreakValue]] = None,
    flow_streak_min: int = 3,
    tx_cost_rt: float = TX_COST_DEFAULT,
) -> list[SignalRecord]:
    """TechnicalQuant.md 매매타이밍 조건 walk-forward 재현.

    entry_key: ENTRY_CONDITIONS의 키, 또는 "E_flow_streak"(수급 조건 단독 —
    flow_lookup 필요). 청산은 exit_models.py와 별개의 자기완결 로직(_scan_exit).
    BacktestConfig을 쓰지 않는 이유: 라이브 모의투자가 공유하는 models.py의
    mode 화이트리스트를 이 리서치 전용 모드 때문에 건드리지 않기 위함.
    """
    if entry_key != "E_flow_streak" and entry_key not in ENTRY_CONDITIONS:
        raise ValueError(f"알 수 없는 entry_key: {entry_key!r}")
    if entry_key == "E_flow_streak" and flow_lookup is None:
        raise ValueError("E_flow_streak는 flow_lookup이 필요합니다")

    df = compute_indicators(daily_df)
    cond_fn = ENTRY_CONDITIONS.get(entry_key)

    signals: list[SignalRecord] = []
    min_start = 121  # ma120 워밍업

    for i in range(min_start, len(df)):
        ts = df.index[i]
        row_date = ts.date() if isinstance(ts, datetime) else cast(date, ts)
        if row_date < start or row_date > end:
            continue

        cur, prev = df.iloc[i], df.iloc[i - 1]
        if pd.isna(cur["Close"]):
            continue

        if entry_key == "E_flow_streak":
            assert flow_lookup is not None
            prev_ts = df.index[i - 1]
            prev_date = prev_ts.date() if isinstance(prev_ts, datetime) else cast(date, prev_ts)
            streak = flow_lookup.get((ticker, row_date))
            prev_streak = flow_lookup.get((ticker, prev_date))
            if streak is None:
                continue
            f_str, i_str = streak
            triggered = ((f_str is not None and f_str >= flow_streak_min)
                         or (i_str is not None and i_str >= flow_streak_min))
            if prev_streak is not None:
                pf, pi = prev_streak
                was_triggered = ((pf is not None and pf >= flow_streak_min)
                                 or (pi is not None and pi >= flow_streak_min))
                if was_triggered:
                    triggered = False  # 전이 판정 — 이미 조건 충족 중이면 스킵
            if not triggered:
                continue
        else:
            assert cond_fn is not None
            if not cond_fn(cur, prev):
                continue

        entry_price = float(cur["Close"])
        if entry_price <= 0:
            continue

        sell_date, sell_reason, sell_return, hold_days = _scan_exit(
            df, i, entry_price, row_date,
            hard_stop_pct, target_pct, use_ma20_exit, use_rsi70_exit,
            tx_cost_rt,
        )

        sig = SignalRecord(
            ticker=ticker, name=name, signal_date=row_date,
            close_at_signal=entry_price, mode="quant", market=market,
        )
        sig.sell_date = sell_date
        sig.sell_reason = sell_reason
        sig.sell_return = sell_return
        sig.blended_return = sell_return
        sig.hold_days = hold_days
        signals.append(sig)

    return signals
