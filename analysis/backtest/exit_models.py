"""백테스트 청산 모델 (backtest_engine.py에서 이동, Phase C).

default(분할청산) / model_a(ATR+Breakeven) / model_b(3단계 분할) 3종과
매도신호 디스패처 _compute_sell_signals_and_s2, ATR 계산.
세 모델의 공통 루프 통합은 이동과 분리된 후속 커밋에서만 진행.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import pandas as pd

from analysis.backtest.config import _STOP_LOSS_PCT
from analysis.backtest.models import BacktestConfig, SignalRecord
from analysis.backtest.helpers import (
    _build_weekly_ichimoku,
    _compute_rsi,
    _find_ichimoku_sell,
)

def _compute_exit_logic(
    sig: "SignalRecord",
    df: "pd.DataFrame",
    entry_idx: int,
    entry_price: float,
    cfg: "BacktestConfig",
    stage3_peakout_dates: "frozenset[date]",
) -> None:
    """분할 청산 모델: 1차 TP + 트레일링 스탑 + Stage3 피크아웃.

    청산 우선순위 (매일 순서대로):
      1. Hard stop: Close <= entry × (1 - hard_stop_pct)  → 전량 즉시 청산
      2. 1차 TP:   Close >= entry × (1 + tp1_pct)         → tp1_ratio 분할 청산
      3. Stage3 피크아웃: row_date in stage3_peakout_dates → 잔여분 전량 청산
      4. 트레일링 스탑: Close <= watermark × (1 - trail_pct) → 잔여분 전량 청산
         watermark = max(row["High"]) since entry
      5. 기간 종료 (마지막 row): 잔여분 전량 청산 (final_exit_type="period_end")

    동일일 hard_stop + tp1 동시: hard_stop 우선 (tp1 발동 안 됨).
    동일일 tp1 + trail 동시: tp1 먼저 기록 후 trail 검사 → 잔여분 즉시 trail.

    결과는 sig 필드에 in-place로 기록.
    기존 호환성: sell_date / sell_reason / sell_return / hold_days 도 채워서
    기존 집계 코드가 blended_return 없이도 동작하도록 한다.
    """
    tx_half = cfg.tx_cost_rt / 2.0  # 분할 청산: 이벤트마다 편도 비용
    stop_price = entry_price * (1.0 - cfg.hard_stop_pct)
    tp1_price  = entry_price * (1.0 + cfg.tp1_pct) if cfg.tp1_pct > 0 else None

    tp1_triggered = False
    watermark     = entry_price  # High 기준 고점 추적

    def _ret(close: float, cost: float) -> float:
        return (close / entry_price - 1.0) - cost

    for j in range(entry_idx + 1, len(df)):
        ts       = df.index[j]
        row_date = ts.date() if hasattr(ts, "date") else ts
        cur      = df.iloc[j]

        if pd.isna(cur["Close"]):
            continue

        close = float(cur["Close"])
        high  = float(cur["High"]) if not pd.isna(cur["High"]) else close
        watermark = max(watermark, high)

        is_last = (j == len(df) - 1)

        # ── 1. Hard stop (최우선) ──────────────────────────────────
        if close <= stop_price:
            cost = tx_half if tp1_triggered else cfg.tx_cost_rt
            ret  = _ret(close, cost)
            sig.final_exit_date = row_date
            sig.final_exit_ret  = ret
            sig.final_exit_type = "hard_stop"
            sig.sell_date   = row_date
            sig.sell_reason = f"손절 -{cfg.hard_stop_pct * 100:.0f}%"
            sig.sell_return = ret
            sig.hold_days   = (row_date - sig.signal_date).days
            break

        # ── 2. 1차 TP ────────────────────────────────────────────
        if not tp1_triggered and tp1_price is not None and close >= tp1_price:
            tp1_triggered    = True
            sig.tp1_date     = row_date
            sig.tp1_ret      = _ret(close, tx_half)
            # 1차 TP를 sell_date에도 기록 (기존 집계 호환)
            sig.sell_date    = row_date
            sig.sell_reason  = f"1차TP +{cfg.tp1_pct * 100:.0f}%"
            sig.sell_return  = sig.tp1_ret
            sig.hold_days    = (row_date - sig.signal_date).days
            # 잔여분(1-tp1_ratio)에 대해 trail/stage3 계속 감시
            continue

        # ── 3. Stage3 피크아웃 (잔여분) ───────────────────────────
        if tp1_triggered and cfg.use_stage3_peak and row_date in stage3_peakout_dates:
            ret = _ret(close, tx_half)
            sig.final_exit_date = row_date
            sig.final_exit_ret  = ret
            sig.final_exit_type = "stage3"
            sig.sell_date    = row_date
            sig.sell_reason  = "Stage3 피크아웃"
            sig.sell_return  = ret
            sig.hold_days    = (row_date - sig.signal_date).days
            break

        # ── 4. 트레일링 스탑 (잔여분, tp1 이후에만) ──────────────
        if tp1_triggered and cfg.trail_pct > 0:
            trail_price = watermark * (1.0 - cfg.trail_pct)
            if close <= trail_price:
                ret = _ret(close, tx_half)
                sig.final_exit_date = row_date
                sig.final_exit_ret  = ret
                sig.final_exit_type = "trail"
                sig.sell_date    = row_date
                sig.sell_reason  = f"트레일 -{cfg.trail_pct * 100:.0f}%"
                sig.sell_return  = ret
                sig.hold_days    = (row_date - sig.signal_date).days
                break

        # ── 5. 기간 종료 강제 청산 ────────────────────────────────
        if is_last:
            cost = tx_half if tp1_triggered else cfg.tx_cost_rt
            ret  = _ret(close, cost)
            sig.final_exit_date = row_date
            sig.final_exit_ret  = ret
            sig.final_exit_type = "period_end"
            if sig.sell_date is None:
                sig.sell_date    = row_date
                sig.sell_reason  = "보유 중 (기간 종료)"
                sig.sell_return  = ret
                sig.hold_days    = (row_date - sig.signal_date).days
            break

    # ── blended_return 계산 ───────────────────────────────────────
    if sig.tp1_ret is not None and sig.final_exit_ret is not None:
        r = cfg.tp1_ratio
        sig.blended_return = r * sig.tp1_ret + (1.0 - r) * sig.final_exit_ret
    elif sig.final_exit_ret is not None:
        sig.blended_return = sig.final_exit_ret
    elif sig.tp1_ret is not None:
        # tp1 발동 후 잔여분 청산 없이 루프 종료 (이론상 발생 안 함)
        sig.blended_return = sig.tp1_ret


def _compute_atr(df: "pd.DataFrame", period: int = 14) -> "pd.Series":
    """True Range 기반 ATR 계산 (period일 단순이동평균)."""
    high  = df["High"].astype(float)
    low   = df["Low"].astype(float)
    close = df["Close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=period).mean()


def _compute_exit_logic_model_a(
    sig: "SignalRecord",
    df: "pd.DataFrame",
    entry_idx: int,
    entry_price: float,
    cfg: "BacktestConfig",
    stage3_peakout_dates: "frozenset[date]",
    atr_series: "pd.Series",
) -> None:
    """모델 A: ATR 기반 가변형 트레일링 스탑 + Breakeven Rule.

    1. Hard stop: Close ≤ entry - 2×ATR(entry일)
    2. 1차 TP(tp1_pct, 기본 25%): 50% 청산 + 잔여분 손절가를 entry(본전)로 상향
    3. Breakeven stop: tp1 이후 Close ≤ entry → 잔여분 청산
    4. Stage3 피크아웃: 잔여분 청산
    5. Chandelier Exit: Close ≤ tp1 이후 최고가 - 3×ATR(현재) → 잔여분 청산
    6. 기간 종료
    """
    raw_atr      = atr_series.iloc[entry_idx]
    atr_at_entry = float(raw_atr) if not pd.isna(raw_atr) else entry_price * 0.05
    hard_stop    = entry_price - 2.0 * atr_at_entry
    tp1_pct      = cfg.tp1_pct if cfg.tp1_pct > 0 else 0.25
    tp1_price    = entry_price * (1.0 + tp1_pct)
    tx_half      = cfg.tx_cost_rt / 2.0

    tp1_triggered  = False
    breakeven_stop = 0.0
    watermark      = entry_price

    def _ret(close: float, cost: float) -> float:
        return (close / entry_price - 1.0) - cost

    for j in range(entry_idx + 1, len(df)):
        ts       = df.index[j]
        row_date = ts.date() if hasattr(ts, "date") else ts
        cur      = df.iloc[j]
        if pd.isna(cur["Close"]):
            continue

        close   = float(cur["Close"])
        high    = float(cur["High"]) if not pd.isna(cur["High"]) else close
        is_last = (j == len(df) - 1)

        raw_atr_cur = atr_series.iloc[j]
        atr_cur = float(raw_atr_cur) if not pd.isna(raw_atr_cur) else atr_at_entry

        if tp1_triggered:
            watermark = max(watermark, high)

        # 1. Hard stop (1차 TP 전)
        if not tp1_triggered and close <= hard_stop:
            ret = _ret(close, cfg.tx_cost_rt)
            sig.final_exit_date = row_date
            sig.final_exit_ret  = ret
            sig.final_exit_type = "hard_stop"
            sig.sell_date   = row_date
            sig.sell_reason = "ATR손절 (2×ATR)"
            sig.sell_return = ret
            sig.hold_days   = (row_date - sig.signal_date).days
            break

        # Breakeven stop (1차 TP 이후 Close ≤ entry)
        if tp1_triggered and close <= breakeven_stop:
            ret = _ret(close, tx_half)
            sig.final_exit_date = row_date
            sig.final_exit_ret  = ret
            sig.final_exit_type = "breakeven"
            sig.sell_date   = row_date
            sig.sell_reason = "본전 스탑"
            sig.sell_return = ret
            sig.hold_days   = (row_date - sig.signal_date).days
            break

        # 2. 1차 TP
        if not tp1_triggered and close >= tp1_price:
            tp1_triggered  = True
            breakeven_stop = entry_price
            watermark      = close
            sig.tp1_date   = row_date
            sig.tp1_ret    = _ret(close, tx_half)
            sig.sell_date  = row_date
            sig.sell_reason = f"1차TP +{tp1_pct*100:.0f}%"
            sig.sell_return = sig.tp1_ret
            sig.hold_days   = (row_date - sig.signal_date).days
            continue

        # 3. Stage3 피크아웃 (잔여분)
        if tp1_triggered and cfg.use_stage3_peak and row_date in stage3_peakout_dates:
            ret = _ret(close, tx_half)
            sig.final_exit_date = row_date
            sig.final_exit_ret  = ret
            sig.final_exit_type = "stage3"
            sig.sell_date   = row_date
            sig.sell_reason = "Stage3 피크아웃"
            sig.sell_return = ret
            sig.hold_days   = (row_date - sig.signal_date).days
            break

        # 4. Chandelier Exit: watermark - 3×ATR (잔여분, tp1 이후)
        if tp1_triggered:
            chandelier = watermark - 3.0 * atr_cur
            if close <= chandelier:
                ret = _ret(close, tx_half)
                sig.final_exit_date = row_date
                sig.final_exit_ret  = ret
                sig.final_exit_type = "trail"
                sig.sell_date   = row_date
                sig.sell_reason = "Chandelier (3×ATR)"
                sig.sell_return = ret
                sig.hold_days   = (row_date - sig.signal_date).days
                break

        # 5. 기간 종료
        if is_last:
            cost = tx_half if tp1_triggered else cfg.tx_cost_rt
            ret  = _ret(close, cost)
            sig.final_exit_date = row_date
            sig.final_exit_ret  = ret
            sig.final_exit_type = "period_end"
            if sig.sell_date is None:
                sig.sell_date   = row_date
                sig.sell_reason = "보유 중 (기간 종료)"
                sig.sell_return = ret
                sig.hold_days   = (row_date - sig.signal_date).days
            break

    if sig.tp1_ret is not None and sig.final_exit_ret is not None:
        r = cfg.tp1_ratio
        sig.blended_return = r * sig.tp1_ret + (1.0 - r) * sig.final_exit_ret
    elif sig.final_exit_ret is not None:
        sig.blended_return = sig.final_exit_ret
    elif sig.tp1_ret is not None:
        sig.blended_return = sig.tp1_ret


def _compute_exit_logic_model_b(
    sig: "SignalRecord",
    df: "pd.DataFrame",
    entry_idx: int,
    entry_price: float,
    cfg: "BacktestConfig",
    stage3_peakout_dates: "frozenset[date]",
) -> None:
    """모델 B: 3단계 분할 청산 (MDD 방어형).

    1. Hard stop (전량): Close ≤ entry × 0.92
    2. 1차 TP (30%): Close ≥ entry × 1.15
    3. 2차 TP (40%): Close ≥ entry × 1.30
    4. 3차 Trailing (30%): Close ≤ watermark × 0.90
    5. 기간 종료
    blended = 0.30×tp1 + 0.40×tp2 + 0.30×trail (실제 발동된 단계까지)
    """
    _B_HARD  = 0.08
    _B_TP1   = 0.15
    _B_TP2   = 0.30
    _B_TRAIL = 0.10
    _R1, _R2, _R3 = 0.30, 0.40, 0.30  # 청산 비율

    hard_stop = entry_price * (1.0 - _B_HARD)
    tp1_price = entry_price * (1.0 + _B_TP1)
    tp2_price = entry_price * (1.0 + _B_TP2)
    tx_part   = cfg.tx_cost_rt / 3.0  # 3단계 분할 시 단계별 비용

    tp1_triggered = False
    tp2_triggered = False
    tp1_ret_val: Optional[float] = None
    tp2_ret_val: Optional[float] = None
    watermark = entry_price

    def _ret(close: float, cost: float) -> float:
        return (close / entry_price - 1.0) - cost

    for j in range(entry_idx + 1, len(df)):
        ts       = df.index[j]
        row_date = ts.date() if hasattr(ts, "date") else ts
        cur      = df.iloc[j]
        if pd.isna(cur["Close"]):
            continue

        close   = float(cur["Close"])
        high    = float(cur["High"]) if not pd.isna(cur["High"]) else close
        is_last = (j == len(df) - 1)

        watermark = max(watermark, high)

        # 1. Hard stop (전량, 1차 TP 전)
        if not tp1_triggered and close <= hard_stop:
            ret = _ret(close, cfg.tx_cost_rt)
            sig.final_exit_date = row_date
            sig.final_exit_ret  = ret
            sig.final_exit_type = "hard_stop"
            sig.sell_date   = row_date
            sig.sell_reason = f"손절 -{_B_HARD*100:.0f}%"
            sig.sell_return = ret
            sig.hold_days   = (row_date - sig.signal_date).days
            sig.blended_return = ret
            break

        # 2. 1차 TP (30%)
        if not tp1_triggered and close >= tp1_price:
            tp1_triggered = True
            tp1_ret_val   = _ret(close, tx_part)
            sig.tp1_date  = row_date
            sig.tp1_ret   = tp1_ret_val
            sig.sell_date  = row_date
            sig.sell_reason = f"1차TP +{_B_TP1*100:.0f}%"
            sig.sell_return = tp1_ret_val
            sig.hold_days   = (row_date - sig.signal_date).days
            continue

        # 3. 2차 TP (40%)
        if tp1_triggered and not tp2_triggered and close >= tp2_price:
            tp2_triggered = True
            tp2_ret_val   = _ret(close, tx_part)
            continue

        # 4. 3차 Trailing (30%, tp1 이후)
        if tp1_triggered:
            trail_price = watermark * (1.0 - _B_TRAIL)
            if close <= trail_price:
                trail_ret = _ret(close, tx_part)
                sig.final_exit_date = row_date
                sig.final_exit_ret  = trail_ret
                sig.final_exit_type = "trail"
                sig.sell_date   = row_date
                sig.sell_reason = f"3차Trail -{_B_TRAIL*100:.0f}%"
                sig.sell_return = trail_ret
                sig.hold_days   = (row_date - sig.signal_date).days
                break

        # 5. 기간 종료
        if is_last:
            cost = tx_part if tp1_triggered else cfg.tx_cost_rt
            final_ret = _ret(close, cost)
            sig.final_exit_date = row_date
            sig.final_exit_ret  = final_ret
            sig.final_exit_type = "period_end"
            if sig.sell_date is None:
                sig.sell_date   = row_date
                sig.sell_reason = "보유 중 (기간 종료)"
                sig.sell_return = final_ret
                sig.hold_days   = (row_date - sig.signal_date).days
            break

    # blended_return — 실제 발동된 단계 기준
    final = sig.final_exit_ret
    if tp1_ret_val is not None and tp2_ret_val is not None and final is not None:
        sig.blended_return = _R1 * tp1_ret_val + _R2 * tp2_ret_val + _R3 * final
    elif tp1_ret_val is not None and final is not None:
        sig.blended_return = _R1 * tp1_ret_val + (1.0 - _R1) * final
    elif final is not None:
        sig.blended_return = final
    elif tp1_ret_val is not None:
        sig.blended_return = tp1_ret_val


def _compute_sell_signals_and_s2(
    signals: list[SignalRecord],
    ohlcv_map: dict[str, "pd.DataFrame"],
    tx_cost_rt: float,
    stop_loss_pct: float = _STOP_LOSS_PCT,
    flow_lookup: Optional[dict] = None,
    cfg: Optional["BacktestConfig"] = None,
    stage3_peakout_map: Optional[dict[str, "frozenset[date]"]] = None,
    streak_lookup: Optional[dict] = None,
) -> None:
    """매도 신호(MA20 이탈 / 손절) 및 S1→S2 진행일 인-플레이스 계산.

    cfg.tp1_pct > 0 또는 cfg.trail_pct > 0 이면 _compute_exit_logic()으로 분기.
    그 외(cfg=None 또는 tp1_pct=trail_pct=0): 기존 MA20 이탈 / hard_stop 로직.
    S2 감지는 Stage 1(mode="stage") 신호에만 적용.
    S3 조건 5(외인+기관 동시 순매수): flow_lookup 제공 + 해당 날짜 데이터 있을 때만 적용.
    """
    from collections import defaultdict

    by_ticker: dict[str, list[SignalRecord]] = defaultdict(list)
    for sig in signals:
        by_ticker[sig.ticker].append(sig)

    for ticker, sigs in by_ticker.items():
        raw_df = ohlcv_map.get(ticker)
        if raw_df is None:
            continue

        df = raw_df.copy()
        df["ma_5"]      = df["Close"].rolling(5,  min_periods=5).mean()
        df["ma_20"]     = df["Close"].rolling(20, min_periods=20).mean()
        df["rsi_14"]    = _compute_rsi(df["Close"])
        df["avg_vol30"] = df["Volume"].rolling(30, min_periods=30).mean()
        df["high_10d"]  = df["High"].shift(1).rolling(10, min_periods=10).max()
        df["pct_chg"]   = df["Close"].pct_change(fill_method=None)
        # v1.1 S3: 52주 고점 돌파 조건용 (해당 모드 신호 있을 때만)
        if any(s.mode in ("stage_v11", "stage2_v11", "stage_v12", "stage2_v12", "stage_v13", "stage2_v13", "stage_v14", "stage_v15") for s in sigs):
            df["high_52w"] = df["High"].shift(1).rolling(252, min_periods=52).max()

        # 이치모쿠 주봉 사전 계산 (ichimoku 모드 신호가 있을 때만)
        has_ichi    = any(s.mode == "ichimoku" for s in sigs)
        weekly_ichi = _build_weekly_ichimoku(raw_df) if has_ichi else None

        idx_map: dict[date, int] = {}
        for i, ts in enumerate(df.index):
            d = ts.date() if hasattr(ts, "date") else ts
            idx_map[d] = i

        # 분할 청산 모드 판별
        _em = cfg.exit_model if cfg is not None else "default"
        use_exit_logic = (
            cfg is not None and (
                cfg.tp1_pct > 0 or cfg.trail_pct > 0 or _em in ("model_a", "model_b")
            )
        )
        ticker_peakout: frozenset[date] = frozenset()
        if use_exit_logic and cfg.use_stage3_peak and stage3_peakout_map:
            ticker_peakout = stage3_peakout_map.get(ticker, frozenset())

        # 모델 A용 ATR 시리즈 — ticker 단위로 한 번만 계산
        _atr_series: Optional["pd.Series"] = None
        if _em == "model_a":
            _atr_series = _compute_atr(df)

        for sig in sigs:
            entry_idx = idx_map.get(sig.signal_date)
            if entry_idx is None:
                continue

            entry_price = sig.close_at_signal
            if entry_price <= 0:
                continue

            # ── 분할 청산 모델 분기 ─────────────────────────────────
            if use_exit_logic:
                if _em == "model_a" and _atr_series is not None:
                    _compute_exit_logic_model_a(sig, df, entry_idx, entry_price, cfg, ticker_peakout, _atr_series)
                elif _em == "model_b":
                    _compute_exit_logic_model_b(sig, df, entry_idx, entry_price, cfg, ticker_peakout)
                else:
                    _compute_exit_logic(sig, df, entry_idx, entry_price, cfg, ticker_peakout)
                # S2/S3/MDD는 아래 기존 루프에서 계속 처리 (sell 판정만 교체)

            stop_price = entry_price * (1 - stop_loss_pct)

            s1_txamt: float = 0.0
            if sig.mode in ("stage", "stage_v11", "stage_v12", "stage_v13", "stage_v14", "stage_v15"):
                v = df.iloc[entry_idx]["Volume"]
                c = df.iloc[entry_idx]["Close"]
                if not pd.isna(v) and not pd.isna(c):
                    s1_txamt = float(v) * float(c)

            s2_cutoff      = sig.signal_date + timedelta(days=14)
            mdd_window_end = sig.signal_date + timedelta(days=91)
            s2_found       = False
            peak_for_mdd   = entry_price
            max_dd_frac    = 0.0

            for j in range(entry_idx + 1, len(df)):
                ts       = df.index[j]
                row_date = ts.date() if hasattr(ts, "date") else ts
                cur      = df.iloc[j]

                if pd.isna(cur["Close"]):
                    continue

                close    = float(cur["Close"])
                vol      = float(cur["Volume"])  if not pd.isna(cur["Volume"])  else 0.0
                ma5      = float(cur["ma_5"])    if not pd.isna(cur["ma_5"])    else None
                ma20     = float(cur["ma_20"])   if not pd.isna(cur["ma_20"])   else None
                rsi14    = float(cur["rsi_14"])  if not pd.isna(cur["rsi_14"])  else None
                avg30    = float(cur["avg_vol30"]) if not pd.isna(cur["avg_vol30"]) else None
                high10d  = float(cur["high_10d"]) if not pd.isna(cur["high_10d"]) else None
                pct_chg  = float(cur["pct_chg"]) if not pd.isna(cur["pct_chg"]) else None

                # MDD(91d) 추적
                if row_date <= mdd_window_end:
                    if close > peak_for_mdd:
                        peak_for_mdd = close
                    dd = (peak_for_mdd - close) / peak_for_mdd
                    if dd > max_dd_frac:
                        max_dd_frac = dd

                # S2 진행 감지 (Stage 1 신호 × 14일 이내)
                if sig.mode in ("stage", "stage_v11", "stage_v12", "stage_v13", "stage_v14", "stage_v15") and not s2_found and row_date <= s2_cutoff:
                    if ma20 is not None and s1_txamt > 0:
                        ratio       = close / entry_price
                        txamt_today = vol * close
                        txamt_ratio = txamt_today / s1_txamt
                        # v1.1/v1.2: 거래대금 범위 0.30~0.60, v1.0: 0.25~0.65
                        tx_lo, tx_hi = (0.30, 0.60) if sig.mode in ("stage_v11", "stage_v12", "stage_v13", "stage_v14", "stage_v15") else (0.25, 0.65)
                        if (0.80 <= ratio <= 0.95
                                and close >= ma20 * 0.95
                                and tx_lo <= txamt_ratio <= tx_hi):
                            sig.s2_date = row_date
                            s2_found    = True

                # S3 감지 (S2 이후, 조정 고점 돌파 + RSI≥70 + 거래량 + 외인·기관 동시 순매수)
                if (s2_found and sig.s3_date is None
                        and sig.s2_date is not None and row_date > sig.s2_date):
                    # C3: v1.0=10일 고가 돌파, v1.1=10일 고가 또는 52주 고가 돌파
                    high52w = None
                    if "high_52w" in df.columns:
                        _h52 = cur.get("high_52w")
                        if _h52 is not None and not pd.isna(_h52):
                            high52w = float(_h52)
                    c3_breakout = high10d is not None and close > high10d
                    if sig.mode in ("stage_v11", "stage_v12", "stage_v13", "stage_v14", "stage_v15") and not c3_breakout:
                        c3_breakout = high52w is not None and close > high52w
                    if (pct_chg  is not None and pct_chg  >= 0.05   # C1: +5%
                            and rsi14   is not None and rsi14   >= 70    # C2: RSI≥70
                            and c3_breakout                              # C3: 돌파
                            and avg30   is not None and avg30   >  0
                            and vol >= 1.5 * avg30):                     # C4: 1.5× vol30
                        # C5: 외인+기관 동시 순매수 (flow_lookup 있고 해당 날짜 데이터 있을 때만 적용)
                        # v1.2: streak >= 2 강화 (streak_lookup 있을 때만)
                        s3_flow_ok = True
                        if flow_lookup is not None:
                            flow = flow_lookup.get((ticker, row_date))
                            if flow is not None:
                                f_net, i_net, _p = flow
                                if sig.mode in ("stage_v12", "stage_v13") and streak_lookup is not None:
                                    streak = streak_lookup.get((ticker, row_date))
                                    if streak is not None:
                                        f_str, i_str = streak
                                        s3_flow_ok = (
                                            f_str is not None and f_str >= 2
                                            and i_str is not None and i_str >= 2
                                        )
                                else:
                                    s3_flow_ok = (
                                        f_net is not None and f_net > 0
                                        and i_net is not None and i_net > 0
                                    )
                        if s3_flow_ok:
                            sig.s3_date = row_date

                # 매도 신호 — ichimoku는 주봉 스캔, 분할 청산 모드는 이미 처리됨
                if not use_exit_logic and sig.mode != "ichimoku" and sig.sell_date is None:
                    if close <= stop_price:
                        sig.sell_date   = row_date
                        sig.sell_reason = f"손절 -{stop_loss_pct * 100:.0f}%"
                        sig.sell_return = (close / entry_price - 1.0) - tx_cost_rt
                        sig.hold_days   = (row_date - sig.signal_date).days
                    elif (cfg is not None and cfg.use_ma5_stop
                          and ma5 is not None and close < ma5):
                        sig.sell_date   = row_date
                        sig.sell_reason = "MA5 이탈"
                        sig.sell_return = (close / entry_price - 1.0) - tx_cost_rt
                        sig.hold_days   = (row_date - sig.signal_date).days
                    elif ma20 is not None and close < ma20:
                        sig.sell_date   = row_date
                        sig.sell_reason = "MA20 이탈"
                        sig.sell_return = (close / entry_price - 1.0) - tx_cost_rt
                        sig.hold_days   = (row_date - sig.signal_date).days

                # 조기 종료
                past_s2_window = s2_found or row_date > s2_cutoff
                s3_done        = not s2_found or sig.s3_date is not None
                mdd_done       = row_date > mdd_window_end
                if mdd_done and past_s2_window and s3_done:
                    if sig.mode == "ichimoku" or sig.sell_date is not None:
                        break

            sig.mdd_91d = -max_dd_frac  # 음수 표기 (0이면 낙폭 없음)

            # ── 이치모쿠 주봉 매도 신호 (구름 이탈 / 데드크로스 / 손절) ──
            if sig.mode == "ichimoku":
                if weekly_ichi is not None:
                    sd, sr, sret, hd = _find_ichimoku_sell(
                        sig.signal_date, sig.close_at_signal,
                        weekly_ichi, tx_cost_rt, stop_loss_pct,
                    )
                    sig.sell_date   = sd
                    sig.sell_reason = sr
                    sig.sell_return = sret
                    sig.hold_days   = hd
                else:
                    sig.sell_reason = "보유 중"
            elif sig.sell_date is None:
                sig.sell_reason = "보유 중"


