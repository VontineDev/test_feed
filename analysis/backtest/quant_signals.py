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

2026-08-26: BNF_TradingModel.md(코테카와 타카시 역추세 모델) 재현 추가
(compute_bnf_indicators/_cond_bnf_entry/_scan_exit_bnf/replay_bnf) — F/G/H와
동일하게 ENTRY_CONDITIONS 딕셔너리로 표현 안 되는 lookback/파라미터 의존
모델이라 전용 함수로 분리. scripts/run_bnf_backtest.py 참고.
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


def _compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """ATR(period) — Wilder 방식(True Range의 지수이동평균, _compute_rsi와 동일한
    ewm(com=period-1) 컨벤션). True Range = max(H-L, |H-PrevClose|, |L-PrevClose|)."""
    high, low, close = cast(pd.Series, df["High"]), cast(pd.Series, df["Low"]), cast(pd.Series, df["Close"])
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return cast(pd.Series, tr.ewm(com=period - 1, min_periods=period).mean())


def compute_indicators(daily_df: pd.DataFrame) -> pd.DataFrame:
    """OHLCV 일봉에 문서의 매매타이밍 조건에 필요한 지표 컬럼을 붙여 반환.

    추가 컬럼: ma5/ma20/ma60/ma120, rsi14, macd/macd_signal,
    high20_prev(전일까지 20일 최고가 — 오늘 돌파 여부 판정용), vol_prev(전일 거래량),
    bb_mid/bb_lower(볼린저밴드 20일 중심선/하단 -2σ), atr14(Wilder ATR),
    low10_prev(전일까지 10일 최저가 — Donchian 청산 채널), high_prev/low_prev
    (전일 고가/저가 — 변동성 돌파 매수가 계산용).

    2026-08-11: F(볼린저+RSI+거래량)/G(변동성 돌파)/H(Donchian+ATR 청산) 3개
    신규 방법론 추가하며 함께 확장.
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

    bb_std20 = closes.rolling(20, min_periods=20).std()
    df["bb_mid"] = df["ma20"]
    df["bb_lower"] = df["ma20"] - 2.0 * bb_std20

    df["atr14"] = _compute_atr(df, period=14)
    # 전일까지의 10일 최저가 — Donchian 청산 채널(H). high20_prev와 동일하게
    # "오늘"은 제외하고 계산해야 당일 청산 판정 시 미래참조가 안 생긴다.
    df["low10_prev"] = df["Low"].shift(1).rolling(10, min_periods=10).min()
    df["high_prev"] = df["High"].shift(1)
    df["low_prev"] = df["Low"].shift(1)
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


def _cond_scenario2_entry(cur, prev, rsi_oversold: float = 30.0) -> bool:
    """시나리오2 매수: RSI(14)가 rsi_oversold 미만으로 하향 후 재상승 돌파 시 종가 매수
    (문서 기본값 30 — = RSI 반등과 동일 정의). rsi_oversold는 진입/청산 파라미터
    최적화(scripts/run_quant_entry_exit_sweep.py)에서 스윕 대상."""
    if pd.isna(cur.get("rsi14")) or pd.isna(prev.get("rsi14")):
        return False
    return float(prev["rsi14"]) < rsi_oversold and float(cur["rsi14"]) >= rsi_oversold


def _cond_bb_rsi_volume(cur, prev) -> bool:
    """F. 볼린저밴드+RSI+거래량 3중확인(평균회귀): 종가가 하단밴드(MA20-2σ) 이하
    AND RSI(14)<30 AND 당일 거래량이 전일 대비 150% 이상(스파이크) — 상태 전이.
    RSI 단독(SCENARIO2)보다 확인 조건을 늘려 노이즈를 줄이는 게 목적."""
    def _state(row) -> bool:
        vals = [row.get("bb_lower"), row.get("rsi14"), row.get("vol_prev")]
        if any(pd.isna(v) for v in vals):
            return False
        if float(row["vol_prev"]) <= 0:
            return False
        below_band = float(row["Close"]) <= float(row["bb_lower"])
        oversold = float(row["rsi14"]) < 30.0
        vol_spike = float(row["Volume"]) >= 1.5 * float(row["vol_prev"])
        return below_band and oversold and vol_spike
    return _state(cur) and not _state(prev)


def _cond_volatility_breakout(cur, prev, k: float = 0.5) -> bool:
    """G. Larry Williams 변동성 돌파: 당일 종가가 (당일 시가 + k×전일 레인지
    (전일고가-전일저가))를 상회하면 매수. 저변동성 뒤 변동성 확장을 노리는
    로직이라 MA20돌파류와 달리 "하루짜리 이벤트"— 다른 조건들과 달리 상태
    전이가 아니라 그날의 조건 자체를 그대로 판정(전날 상태 비교 불필요)."""
    vals = [cur.get("Open"), prev.get("High"), prev.get("Low")]
    if any(pd.isna(v) for v in vals):
        return False
    buy_price = float(cur["Open"]) + k * (float(prev["High"]) - float(prev["Low"]))
    return float(cur["Close"]) >= buy_price


# 수급 조건(E)은 flow_lookup이 필요해 별도 처리 — _replay_quant에서 직접 판정.

ENTRY_CONDITIONS: dict[str, Callable] = {
    "A_ma20_breakout":   _cond_ma20_breakout,
    "B_ma_alignment":    _cond_ma_alignment,
    "C_rsi_macd_rebound": _cond_rsi_macd_rebound,
    "D_new_high20":      _cond_new_high20,
    "F_bb_rsi_volume":   _cond_bb_rsi_volume,
    "G_volatility_breakout": _cond_volatility_breakout,
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
    rsi_overbought: float = 70.0,
) -> tuple[Optional[date], str, Optional[float], Optional[int]]:
    """진입 다음날부터 청산 조건을 스캔. (sell_date, sell_reason, sell_return, hold_days) 반환.

    우선순위: 손절 → 목표가/RSI 과열 익절 → MA20 이탈 → 기간 종료.
    rsi_overbought(문서 기본값 70)는 진입/청산 파라미터 최적화 스윕 대상.
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

        if use_rsi70_exit and not pd.isna(cur.get("rsi14")) and float(cur["rsi14"]) > rsi_overbought:
            ret = (close / entry_price - 1.0) - tx_cost_rt
            return row_date, f"RSI{rsi_overbought:.0f} 익절", ret, (row_date - signal_date).days

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
    rsi_oversold: float = 30.0,
    rsi_overbought: float = 70.0,
) -> list[SignalRecord]:
    """TechnicalQuant.md 매매타이밍 조건 walk-forward 재현.

    entry_key: ENTRY_CONDITIONS의 키, 또는 "E_flow_streak"(수급 조건 단독 —
    flow_lookup 필요). 청산은 exit_models.py와 별개의 자기완결 로직(_scan_exit).
    BacktestConfig을 쓰지 않는 이유: 라이브 모의투자가 공유하는 models.py의
    mode 화이트리스트를 이 리서치 전용 모드 때문에 건드리지 않기 위함.

    rsi_oversold/rsi_overbought: SCENARIO2 전용(진입 RSI 반등 임계값 / 청산
    RSI 과열 임계값, 문서 기본값 30/70) — 진입/청산 파라미터 최적화 스윕
    (scripts/run_quant_entry_exit_sweep.py)에서만 기본값과 다르게 넘긴다.
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
        elif entry_key == "SCENARIO2":
            if not _cond_scenario2_entry(cur, prev, rsi_oversold):
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
            tx_cost_rt, rsi_overbought,
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


# ── F. 볼린저밴드+RSI+거래량 전용 청산 (중심선 복귀/RSI50/진입캔들 저점 손절) ──
# _scan_exit(하드% 손절/목표/MA20/RSI70)와는 다른 자기완결 청산 규칙이라 별도 스캐너로 분리.

def _scan_exit_bb(
    df: pd.DataFrame,
    entry_idx: int,
    entry_price: float,
    signal_date: date,
    tx_cost_rt: float,
    rsi_exit: float = 50.0,
) -> tuple[Optional[date], str, Optional[float], Optional[int]]:
    """진입 다음날부터 스캔. 우선순위: 손절(진입캔들 저점) → 목표(중심선 복귀)
    → RSI 과매도 탈출(RSI>50) → 기간 종료."""
    entry_low = float(df.iloc[entry_idx]["Low"])

    for j in range(entry_idx + 1, len(df)):
        ts = df.index[j]
        row_date = ts.date() if isinstance(ts, datetime) else cast(date, ts)
        cur = df.iloc[j]
        if pd.isna(cur["Close"]):
            continue
        close = float(cur["Close"])
        is_last = (j == len(df) - 1)

        def _ret(p: float) -> float:
            return (p / entry_price - 1.0) - tx_cost_rt

        if close <= entry_low:
            return row_date, "손절(진입캔들저점)", _ret(close), (row_date - signal_date).days

        if not pd.isna(cur.get("bb_mid")) and close >= float(cur["bb_mid"]):
            return row_date, "목표(중심선복귀)", _ret(close), (row_date - signal_date).days

        if not pd.isna(cur.get("rsi14")) and float(cur["rsi14"]) > rsi_exit:
            return row_date, f"RSI{rsi_exit:.0f} 탈출", _ret(close), (row_date - signal_date).days

        if is_last:
            return row_date, "보유 중 (기간 종료)", _ret(close), (row_date - signal_date).days

    return None, "보유 중", None, None


def replay_quant_bb_exit(
    ticker: str,
    name: str,
    daily_df: pd.DataFrame,
    market: str,
    start: date,
    end: date,
    tx_cost_rt: float = TX_COST_DEFAULT,
    rsi_exit: float = 50.0,
) -> list[SignalRecord]:
    """F(볼린저+RSI+거래량 3중확인) 진입 + 전용 청산(_scan_exit_bb) 재현.

    replay_quant의 표준 청산(hard_stop_pct/target_pct 고정%)과 달리 진입캔들
    저점 손절·중심선 복귀 익절이라는 서로 다른 로직이라 별도 함수로 분리했다
    — replay_quant 시그니처를 건드리지 않기 위함(기존 A-E/SCENARIO1-2 호출부 무변경)."""
    df = compute_indicators(daily_df)
    signals: list[SignalRecord] = []
    min_start = 121

    for i in range(min_start, len(df)):
        ts = df.index[i]
        row_date = ts.date() if isinstance(ts, datetime) else cast(date, ts)
        if row_date < start or row_date > end:
            continue
        cur, prev = df.iloc[i], df.iloc[i - 1]
        if pd.isna(cur["Close"]):
            continue
        if not _cond_bb_rsi_volume(cur, prev):
            continue

        entry_price = float(cur["Close"])
        if entry_price <= 0:
            continue

        sell_date, sell_reason, sell_return, hold_days = _scan_exit_bb(
            df, i, entry_price, row_date, tx_cost_rt, rsi_exit,
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


# ── BNF(코테카와 타카시) 역추세 모델 — BNF_TradingModel.md 재현 ─────────
# 이격도(EMA25 기준) 급락 + RSI 과매도 + MACD 히스토그램 전환의 4단계 매수
# 트리거, 손절/추세별 트레일링/모멘텀 소진의 3원칙 매도. F/G/H와 마찬가지로
# ENTRY_CONDITIONS 딕셔너리(cur/prev 페어만 받음)로는 표현이 안 되는 모델이라
# (이격도 임계값이 자산유형/시장추세별 파라미터고, RSI/이격도 판정에 lookback
# 윈도우가 필요) 전용 함수로 분리.

def compute_bnf_indicators(daily_df: pd.DataFrame) -> pd.DataFrame:
    """BNF 모델 전용 지표. compute_indicators()가 이미 만드는 rsi14/macd/
    macd_signal/ma60을 재사용하고, EMA25·이격도·MACD 히스토그램만 추가로
    얹는다. compute_indicators 자체에 넣지 않은 이유: EMA25/이격도는 이
    모델(BNF_TradingModel.md) 전용 개념이라 A-H 범용 지표와 섞으면 그쪽
    함수의 의미가 흐려짐."""
    df = compute_indicators(daily_df)
    closes = cast(pd.Series, df["Close"])
    df["ema25"] = closes.ewm(span=25, min_periods=25, adjust=False).mean()
    df["discrepancy"] = (closes - df["ema25"]) / df["ema25"]
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    return df


def _cond_bnf_entry(
    df: pd.DataFrame,
    i: int,
    disc_threshold: float,
    rsi_oversold: float = 30.0,
    lookback: int = 10,
) -> bool:
    """BNF 4단계 매수 트리거.

    [1단계] 단기 급락 스크리닝은 [2단계](이격도)로 판정을 대체한다 — 이격도가
    disc_threshold(음수, 예 -0.25)만큼 벌어지려면 그 자체로 단기 급락이
    선행해야 하므로 별도 조건이 불필요.
    [2단계] 최근 lookback일 내에 이격도가 disc_threshold 이하로 벌어진 적이
    있는지.
    [3단계] 같은 lookback 윈도우 내에 RSI(14)가 rsi_oversold 이하로 내려간
    적이 있는지.
    [4단계·최종 트리거] 오늘 MACD 히스토그램이 0선 아래(-)에서 위(+)로 전환
    (전이 판정 — 자연히 크로스 당일 하루만 발동).

    [2]/[3]을 "오늘" 시점이 아니라 lookback 윈도우로 판정하는 이유: 문서가
    묘사하는 바닥 다지기 과정에서 이격도/RSI는 먼저 회복되고 MACD 히스토그램
    전환은 며칠 뒤에 나타나는 경우가 흔하다 — 모든 조건을 같은 날 강제하면
    실제 매매 타점을 대부분 놓친다.
    """
    cur, prev = df.iloc[i], df.iloc[i - 1]
    if pd.isna(cur.get("macd_hist")) or pd.isna(prev.get("macd_hist")):
        return False
    macd_cross = float(prev["macd_hist"]) < 0 and float(cur["macd_hist"]) >= 0
    if not macd_cross:
        return False

    win_start = max(0, i - lookback + 1)
    window = df.iloc[win_start:i + 1]
    disc_hit = bool((window["discrepancy"] <= disc_threshold).any())
    rsi_hit = bool((window["rsi14"] <= rsi_oversold).any())
    return disc_hit and rsi_hit


def _scan_exit_bnf(
    df: pd.DataFrame,
    entry_idx: int,
    entry_price: float,
    signal_date: date,
    hard_stop_pct: float,
    trail_pct_uptrend: float,
    trail_pct_downtrend: float,
    tx_cost_rt: float,
) -> tuple[Optional[date], str, Optional[float], Optional[int]]:
    """BNF 청산 3원칙 재현. 우선순위: 손절(고정%) → 추세별 트레일링 스탑 →
    모멘텀 소진 청산 → 기간 종료.

    "전체 시장 추세"는 문서 원문 개념이지만 이 저장소에는 KOSPI/KOSDAQ 지수
    일봉 파이프라인이 없어(core/db_market.py는 개별 종목만 저장) 종목 자신의
    MA60 상태(종가>MA60 AND MA60 5일 전보다 상승)로 근사한다 — 다른 자기완결
    청산 로직(_scan_exit_bb 등)과 동일한 단순화 방침.
    상승추세로 판정되면 trail_pct_uptrend(넓게), 아니면 trail_pct_downtrend
    (좁게)를 고점 대비 트레일링 스탑으로 적용해 "상승장은 여유, 하락장은
    빠르게"를 재현한다.

    모멘텀 소진 조건(거래량 2일 연속 감소 + 캔들 몸통 축소 + MACD 히스토그램
    둔화)은 진입 직후(며칠 이내)엔 트리거 정의상 히스토그램이 막 플러스로
    돌아선 시점이라 오탐 가능성이 커 entry_idx+3일 이후부터만 판정한다.
    """
    stop_price = entry_price * (1.0 - hard_stop_pct)
    peak_close = entry_price

    for j in range(entry_idx + 1, len(df)):
        ts = df.index[j]
        row_date = ts.date() if isinstance(ts, datetime) else cast(date, ts)
        cur = df.iloc[j]
        if pd.isna(cur["Close"]):
            continue
        close = float(cur["Close"])
        is_last = (j == len(df) - 1)
        peak_close = max(peak_close, close)

        def _ret(p: float) -> float:
            return (p / entry_price - 1.0) - tx_cost_rt

        if close <= stop_price:
            return row_date, f"손절 -{hard_stop_pct*100:.0f}%", _ret(close), (row_date - signal_date).days

        ma60_now = cur.get("ma60")
        uptrend = False
        if not pd.isna(ma60_now) and close > float(ma60_now) and j >= 5:
            ma60_prev5 = df.iloc[j - 5].get("ma60")
            if not pd.isna(ma60_prev5):
                uptrend = float(ma60_now) > float(ma60_prev5)
        trail_pct = trail_pct_uptrend if uptrend else trail_pct_downtrend
        if close <= peak_close * (1.0 - trail_pct):
            label = "상승추세 트레일링" if uptrend else "하락추세 트레일링"
            return row_date, f"{label} -{trail_pct*100:.0f}%", _ret(close), (row_date - signal_date).days

        if j >= entry_idx + 3:
            vol_declining = (
                float(df.iloc[j]["Volume"]) < float(df.iloc[j - 1]["Volume"])
                < float(df.iloc[j - 2]["Volume"])
            )
            body_today = abs(float(cur["Close"]) - float(cur["Open"]))
            body_prev = abs(float(df.iloc[j - 1]["Close"]) - float(df.iloc[j - 1]["Open"]))
            body_shrinking = body_today < body_prev
            hist_today, hist_prev = cur.get("macd_hist"), df.iloc[j - 1].get("macd_hist")
            momentum_fading = (
                not pd.isna(hist_today) and not pd.isna(hist_prev)
                and float(hist_today) < float(hist_prev)
            )
            if vol_declining and body_shrinking and momentum_fading:
                return row_date, "모멘텀 소진 청산", _ret(close), (row_date - signal_date).days

        if is_last:
            return row_date, "보유 중 (기간 종료)", _ret(close), (row_date - signal_date).days

    return None, "보유 중", None, None


def replay_bnf(
    ticker: str,
    name: str,
    daily_df: pd.DataFrame,
    market: str,
    start: date,
    end: date,
    disc_threshold: float = -0.25,
    rsi_oversold: float = 30.0,
    lookback: int = 10,
    hard_stop_pct: float = 0.08,
    trail_pct_uptrend: float = 0.15,
    trail_pct_downtrend: float = 0.07,
    tx_cost_rt: float = TX_COST_DEFAULT,
) -> list[SignalRecord]:
    """BNF_TradingModel.md 매매 규칙 재현 — 이격도(EMA25) 급락 + RSI 과매도 +
    MACD 히스토그램 전환의 역추세(평균회귀) 매수, 손절/추세별 트레일링/모멘텀
    소진의 3원칙 매도.

    disc_threshold 기본값 -0.25(-25%)는 문서의 "대형 우량주/상승장" 구간
    (-20%~-25%) 중 보수적인 쪽. "중소형주/하락장"(-30%~-35%)을 재현하려면
    호출부에서 disc_threshold=-0.325 등으로 넘긴다
    (scripts/run_bnf_backtest.py --disc-threshold).
    """
    df = compute_bnf_indicators(daily_df)
    signals: list[SignalRecord] = []
    min_start = 121  # ma120 워밍업 — compute_indicators 계열 관례와 통일

    for i in range(min_start, len(df)):
        ts = df.index[i]
        row_date = ts.date() if isinstance(ts, datetime) else cast(date, ts)
        if row_date < start or row_date > end:
            continue
        cur = df.iloc[i]
        if pd.isna(cur["Close"]):
            continue
        if not _cond_bnf_entry(df, i, disc_threshold, rsi_oversold, lookback):
            continue

        entry_price = float(cur["Close"])
        if entry_price <= 0:
            continue

        sell_date, sell_reason, sell_return, hold_days = _scan_exit_bnf(
            df, i, entry_price, row_date,
            hard_stop_pct, trail_pct_uptrend, trail_pct_downtrend, tx_cost_rt,
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


# ── H. Donchian 20일 신고가 진입 + ATR 기반 청산(2×ATR 손절/10일저가 이탈) ──
# 진입은 D_new_high20을 그대로 재사용 — 손절·사이징 로직만 바꿔서 D의 성과가
# 개선되는지 분리 측정하는 게 목적("D는 이미 있지만 ATR 손절 없이 단순 진입만
# 측정됐다"는 관찰에서 착안).

def _scan_exit_donchian_atr(
    df: pd.DataFrame,
    entry_idx: int,
    entry_price: float,
    signal_date: date,
    atr_stop_mult: float,
    tx_cost_rt: float,
) -> tuple[Optional[date], str, Optional[float], Optional[int]]:
    """우선순위: 손절(진입가 - atr_stop_mult×ATR14) → Donchian 10일저가 이탈 → 기간 종료.
    ATR을 못 구하면(워밍업 부족) 손절 없이 채널 이탈/기간종료만 판정."""
    entry_atr = df.iloc[entry_idx].get("atr14")
    stop_price = (
        entry_price - atr_stop_mult * float(entry_atr)
        if entry_atr is not None and not pd.isna(entry_atr) else None
    )

    for j in range(entry_idx + 1, len(df)):
        ts = df.index[j]
        row_date = ts.date() if isinstance(ts, datetime) else cast(date, ts)
        cur = df.iloc[j]
        if pd.isna(cur["Close"]):
            continue
        close = float(cur["Close"])
        is_last = (j == len(df) - 1)

        def _ret(p: float) -> float:
            return (p / entry_price - 1.0) - tx_cost_rt

        if stop_price is not None and close <= stop_price:
            return row_date, f"손절(ATR×{atr_stop_mult:.0f})", _ret(close), (row_date - signal_date).days

        if not pd.isna(cur.get("low10_prev")) and close < float(cur["low10_prev"]):
            return row_date, "Donchian10 이탈", _ret(close), (row_date - signal_date).days

        if is_last:
            return row_date, "보유 중 (기간 종료)", _ret(close), (row_date - signal_date).days

    return None, "보유 중", None, None


def replay_quant_donchian_atr(
    ticker: str,
    name: str,
    daily_df: pd.DataFrame,
    market: str,
    start: date,
    end: date,
    atr_stop_mult: float = 2.0,
    tx_cost_rt: float = TX_COST_DEFAULT,
) -> list[SignalRecord]:
    """D_new_high20 진입 + ATR/Donchian10 청산(_scan_exit_donchian_atr) 재현."""
    df = compute_indicators(daily_df)
    signals: list[SignalRecord] = []
    min_start = 121

    for i in range(min_start, len(df)):
        ts = df.index[i]
        row_date = ts.date() if isinstance(ts, datetime) else cast(date, ts)
        if row_date < start or row_date > end:
            continue
        cur, prev = df.iloc[i], df.iloc[i - 1]
        if pd.isna(cur["Close"]):
            continue
        if not _cond_new_high20(cur, prev):
            continue

        entry_price = float(cur["Close"])
        if entry_price <= 0:
            continue

        sell_date, sell_reason, sell_return, hold_days = _scan_exit_donchian_atr(
            df, i, entry_price, row_date, atr_stop_mult, tx_cost_rt,
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
