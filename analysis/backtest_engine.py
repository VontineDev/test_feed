"""
backtest_engine.py  —  통합 백테스트 엔진 (Sprint 3)
────────────────────────────────────────────────────────────
4개 모드:
  ichimoku — 주봉 이치모쿠 7조건 walk-forward 재현
  stage    — 일봉 Stage 1 가격 조건 재현 (5/5 조건, 수급은 daily_flow 있을 때)
  cross    — 이치모쿠 + Stage 1 동일 ISO 주 교차
  stage2   — Stage 1 신호 후 14일 이내 Stage 2 재진입 조건 재현

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
from dataclasses import dataclass, replace as _dc_replace
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd

from analysis.chart_screener import calc_ichimoku

logger = logging.getLogger(__name__)
_KST = ZoneInfo("Asia/Seoul")

# ── 상수·튜닝 파라미터: analysis/backtest/config.py로 이동 (Phase C) ──
# 기존 이름 전부 re-export — 테스트/paper_jobs/텔레그램봇이 이 경로로 import.
from analysis.backtest.config import (  # noqa: F401
    _TX_BUY,
    _TX_SELL,
    TX_COST_DEFAULT,
    _S1_THRESHOLD,
    _STOP_LOSS_PCT,
    MODE_KOR,
    OPTIMAL_EXIT_PARAMS,
    OPTIMAL_EXIT_PARAMS_KOSDAQ,
    OPTIMAL_EXIT_PARAMS_CROSS,
    OPTIMAL_EXIT_PARAMS_ICHIMOKU,
)


# ── 데이터 클래스: analysis/backtest/models.py로 이동 (Phase C) ──
from analysis.backtest.models import (  # noqa: F401
    BacktestConfig,
    SignalRecord,
    GroupMetrics,
    BacktestResult,
)


# ── 유틸 함수: analysis/backtest/helpers.py로 이동 (Phase C) ──
from analysis.backtest.helpers import (  # noqa: F401
    _week_label,
    _build_price_lookup,
    _nearest_price,
    _entry_on_or_after,
    _compute_sharpe,
    _compute_mdd,
    _build_weekly_ichimoku,
    _find_ichimoku_sell,
    _compute_rsi,
    _compute_group_metrics,
)

# ── 데이터 수집: analysis/backtest/fetch.py로 이동 (Phase C) ──
from analysis.backtest.fetch import (  # noqa: F401
    _fetch_single_ohlcv,
    _fetch_index,
    _batch_fetch_ohlcv,
)

# ── 신호 재현: analysis/backtest/replay.py로 이동 (Phase C) ──
from analysis.backtest.replay import (  # noqa: F401
    _replay_ichimoku,
    _replay_stage,
    _replay_stage_v11,
    _replay_stage2_v11,
    _replay_stage_v12,
    _replay_stage2_v12,
    _replay_stage_v13,
    _replay_stage_v14,
    _replay_stage_v15,
    _replay_stage2_v13,
    _apply_cross_filter,
    _replay_stage2,
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


# ── 메인 실행 ─────────────────────────────────────────────────────

def _run_compose(config: BacktestConfig) -> BacktestResult:
    """compose 모드 — strategy_compose 합성 신호를 SignalRecord로 변환 후
    기존 forward-return/청산/지표/HTML 머신을 재사용한다.

    replay(_replay_*)를 우회한다: 신호는 백필된 precompute 테이블 JOIN에서 온다
    (plan-eng-review JOIN+백필 아키텍처). 효율을 위해 신호가 있는 티커의
    OHLCV만 수집한다(전종목 X).

    데이터 흐름:
        strategy_compose.load_signal_frame → derive_flags → STRATEGIES[s].run()
            → [(ticker, ISO주)]
        ISO주 금요일 이후 첫 거래일에 진입(SignalRecord, mode="compose")
            → _fill_returns / _compute_sell_signals_and_s2 / _compute_group_metrics
    """
    from analysis import strategy_compose as sc
    from analysis.chart_screener import get_all_tickers, fetch_kind_sector_map

    spec = sc.STRATEGIES.get(config.strategy)
    if spec is None:
        raise ValueError(
            f"알 수 없는 전략: {config.strategy!r} (가능: {sorted(sc.STRATEGIES)})"
        )

    logger.info(
        "[compose] 전략=%s 소스=%s 기간=%s~%s 시장=%s",
        config.strategy, spec.sources, config.start, config.end, config.market,
    )

    def _empty(note: str) -> BacktestResult:
        return BacktestResult(
            config=config, signals=[], overall=GroupMetrics(),
            computed_at=datetime.now(_KST).isoformat(), note=note,
        )

    # 1. 합성 신호 (ticker, ISO주)
    frame = sc.load_signal_frame(config.dsn, config.start, config.end, spec.sources)
    if frame.empty:
        return _empty(f"compose {config.strategy}: 소스 데이터 0건")
    frame = sc.derive_flags(frame)
    sig_df = spec.run(frame)
    if sig_df.empty:
        return _empty(f"compose {config.strategy}: 합성 신호 0건")

    # 2. 시장·기간 필터 → (ticker, 금요일) 진입 후보
    sector_map  = fetch_kind_sector_map()
    all_tickers = get_all_tickers(sector_map=sector_map if sector_map else None)
    meta: dict[str, tuple[str, str]] = {t: (n, s) for t, n, s in all_tickers}

    entries: list[tuple[str, date]] = []
    for row in sig_df.itertuples(index=False):
        ticker = row.ticker
        friday = sc.week_to_friday(row.week)
        if not (config.start <= friday <= config.end):
            continue
        mkt = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"
        if config.market != "ALL" and mkt != config.market:
            continue
        entries.append((ticker, friday))
    if not entries:
        return _empty(f"compose {config.strategy}: 기간/시장 필터 후 신호 0건")

    needed = sorted({t for t, _ in entries})
    logger.info("[compose] 합성 신호 %d건 · 대상 티커 %d개", len(entries), len(needed))

    # 3. OHLCV (신호 티커만) + KOSPI 벤치마크
    fetch_start = config.start - timedelta(days=760)
    hold_buffer = (config.hold_weeks * 7 + 14) if config.hold_weeks else 0
    fetch_end   = config.end + timedelta(days=max(105, hold_buffer))

    from core.ohlcv_cache import batch_fetch_cached, fetch_index_cached
    ticker_pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t in needed]
    ohlcv_map = batch_fetch_cached(
        ticker_pairs, fetch_start, fetch_end, config.workers, config.dsn, _fetch_single_ohlcv,
    )
    kospi_df     = fetch_index_cached("^KS11", "IDX", fetch_start, fetch_end, config.dsn, _fetch_index)
    kospi_lookup = _build_price_lookup(kospi_df) if kospi_df is not None else {}

    # 4. 수급 (청산 로직 S3 조건 공용)
    flow_lookup: Optional[dict] = None
    try:
        from core.ohlcv_cache import load_flow_data
        flow_lookup = load_flow_data(config.dsn, needed, config.start, fetch_end)
    except Exception as e:
        logger.warning("[compose] 수급 데이터 로드 실패: %s", e)

    # 5. SignalRecord 생성 — 금요일 이후 첫 거래일에 진입
    price_lookup_cache: dict[str, dict[date, float]] = {}
    seen: set[tuple[str, date]] = set()
    signals: list[SignalRecord] = []
    for ticker, friday in entries:
        df = ohlcv_map.get(ticker)
        if df is None or df.empty:
            continue
        plook = price_lookup_cache.get(ticker)
        if plook is None:
            plook = _build_price_lookup(df)
            price_lookup_cache[ticker] = plook
        entry = _entry_on_or_after(plook, friday)
        if entry is None:
            continue
        edate, eclose = entry
        if eclose <= 0 or (ticker, edate) in seen:
            continue
        seen.add((ticker, edate))
        mkt  = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"
        name = meta.get(ticker, (ticker, ""))[0]
        signals.append(SignalRecord(
            ticker=ticker, name=name, signal_date=edate,
            close_at_signal=eclose, mode="compose", market=mkt,
        ))
    if not signals:
        return _empty(f"compose {config.strategy}: 진입가 산정 후 신호 0건")

    # 6. 청산 파라미터 — 미설정 시 CROSS 최적값 적용 (조합도 교차 성격)
    cfg = config
    if config.tp1_pct == 0 and config.trail_pct == 0:
        cfg = _dc_replace(config, **OPTIMAL_EXIT_PARAMS_CROSS)
        logger.info("[compose] 기본 분할청산 파라미터 적용 (CROSS 최적값)")

    # 7. 수익률
    for sig in signals:
        plook = price_lookup_cache.get(sig.ticker)
        if plook is not None:
            _fill_returns(sig, plook, kospi_lookup, cfg.tx_cost_rt, cfg.hold_weeks)

    # 8. 업종 주입
    for sig in signals:
        sig.sector = meta.get(sig.ticker, ("", ""))[1]

    # 9. 매도 신호·MDD (S2/S3는 compose 모드에 비적용 — stage 전용)
    _compute_sell_signals_and_s2(
        signals, ohlcv_map, cfg.tx_cost_rt,
        stop_loss_pct=cfg.hard_stop_pct, flow_lookup=flow_lookup, cfg=cfg,
        stage3_peakout_map=None,
    )

    # 10. 정렬 + 집계
    signals.sort(key=lambda s: s.signal_date)
    overall = _compute_group_metrics(signals, cfg.rf_rate_annual, cfg.hold_weeks)

    note = f"compose {config.strategy} — 신호 {len(signals)}건"
    if flow_lookup is None:
        note += " | 수급 미적용(DSN/데이터 없음)"
    logger.info("[compose] 완료 — 신호:%d 승률28d:%s", overall.n,
                f"{overall.win_rate_28d*100:.1f}%" if overall.win_rate_28d is not None else "N/A")
    return BacktestResult(
        config=config, signals=signals, overall=overall,
        computed_at=datetime.now(_KST).isoformat(), note=note,
    )


def run_backtest(config: BacktestConfig) -> BacktestResult:
    """백테스트 메인 함수. CLI 및 Telegram 봇에서 동기 호출."""
    if config.mode == "compose":
        return _run_compose(config)

    from analysis.chart_screener import get_all_tickers

    logger.info(
        "[백테스트] 모드=%s 기간=%s~%s 시장=%s 최대티커=%s",
        config.mode, config.start, config.end, config.market, config.max_tickers or "전종목",
    )

    # 1. 업종 매핑 + 티커 목록
    from analysis.chart_screener import fetch_kind_sector_map
    sector_map  = fetch_kind_sector_map()
    all_tickers = get_all_tickers(sector_map=sector_map if sector_map else None)

    # 외부 API 타임아웃 등으로 목록이 비면 DB daily_flow에서 직접 로드
    if not all_tickers and config.dsn:
        try:
            from core.db_sync import connect
            conn = connect(config.dsn)
            cur  = conn.cursor()
            cur.execute("SELECT DISTINCT ticker FROM daily_flow ORDER BY ticker")
            rows = cur.fetchall()
            conn.close()
            all_tickers = [(r[0], r[0].split(".")[0], "") for r in rows]
            logger.info("[백테스트] DB fallback 티커 %d개 로드", len(all_tickers))
        except Exception as _e:
            logger.warning("[백테스트] DB fallback 실패: %s", _e)

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
    #   후방: 최대 보유 91일 + 여유 14일 (hold_weeks 지정 시 그 기간으로 확장)
    fetch_start = config.start - timedelta(days=760)
    hold_buffer = (config.hold_weeks * 7 + 14) if config.hold_weeks else 0
    fetch_end   = config.end + timedelta(days=max(105, hold_buffer))

    # 3. OHLCV 병렬 수집
    if config.dsn:
        from core.ohlcv_cache import batch_fetch_cached, fetch_index_cached
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

    # 5. 수급 데이터 사전 로드 (DSN 있을 때 전 모드 공통 — S1 조건 5 + S3 조건 5 공용)
    #    S3 감지는 신호일 이후 최대 91일까지 스캔 → fetch_end까지 로드
    flow_lookup: Optional[dict] = None
    streak_lookup: Optional[dict] = None
    if config.dsn:
        try:
            from core.ohlcv_cache import load_flow_data, load_flow_streaks
            ticker_syms = [t for t, _, _ in tickers]
            flow_lookup = load_flow_data(config.dsn, ticker_syms, config.start, fetch_end)
            logger.info("[백테스트] 수급 데이터 로드: %d건", len(flow_lookup))
            if config.mode in ("stage_v12", "stage2_v12", "stage_v13", "stage2_v13", "stage_v14", "stage_v15"):
                streak_lookup = load_flow_streaks(config.dsn, ticker_syms, config.start, fetch_end)
                logger.info("[백테스트] streak 데이터 로드: %d건", len(streak_lookup))
        except Exception as e:
            logger.warning("[백테스트] 수급 데이터 로드 실패 (조건 5 생략): %s", e)

    shares_lookup: Optional[dict] = None
    if config.mode in ("stage_v12", "stage_v13", "stage_v14", "stage_v15") and config.dsn:
        try:
            from core.ohlcv_cache import load_listed_shares
            shares_lookup = load_listed_shares(config.dsn)
            logger.info("[백테스트] 상장주식수 로드: %d종목", len(shares_lookup))
        except Exception as e:
            logger.warning("[백테스트] 상장주식수 로드 실패 (조건 9 생략): %s", e)

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
        if config.mode == "stage2":
            all_signals.extend(_replay_stage2(ticker, name, df, mkt, config))
        if config.mode == "stage_v11":
            all_signals.extend(_replay_stage_v11(ticker, name, df, mkt, config, flow_lookup))
        if config.mode == "stage2_v11":
            all_signals.extend(_replay_stage2_v11(ticker, name, df, mkt, config))
        if config.mode == "stage_v12":
            all_signals.extend(_replay_stage_v12(ticker, name, df, mkt, config, flow_lookup, streak_lookup, shares_lookup))
        if config.mode == "stage2_v12":
            all_signals.extend(_replay_stage2_v12(ticker, name, df, mkt, config))
        if config.mode == "stage_v13":
            all_signals.extend(_replay_stage_v13(ticker, name, df, mkt, config, flow_lookup, streak_lookup, shares_lookup))
        if config.mode == "stage2_v13":
            all_signals.extend(_replay_stage2_v13(ticker, name, df, mkt, config, flow_lookup))
        if config.mode == "stage_v14":
            all_signals.extend(_replay_stage_v14(ticker, name, df, mkt, config, flow_lookup, streak_lookup, shares_lookup))
        if config.mode == "stage_v15":
            all_signals.extend(_replay_stage_v15(ticker, name, df, mkt, config, flow_lookup, streak_lookup, shares_lookup))

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
        _fill_returns(sig, stock_lookup_cache[sig.ticker], kospi_lookup, config.tx_cost_rt, config.hold_weeks)

    # 8.5. 업종 정보 주입
    ticker_sector: dict[str, str] = {t: s for t, _n, s in tickers}
    for sig in all_signals:
        sig.sector = ticker_sector.get(sig.ticker, "")

    # 8.6. 매도 신호·MDD·S2/S3 진행일 계산
    # Stage3 peakout map: 분할 청산 모드(tp1>0 or trail>0)에서만 DB 조회
    stage3_peakout_map: Optional[dict] = None
    if config.dsn and config.use_stage3_peak and (config.tp1_pct > 0 or config.trail_pct > 0):
        try:
            import asyncio as _asyncio
            from core.db import get_stage3_peakout_map as _get_peakout
            ticker_syms = [t for t, _, _ in tickers]
            stage3_peakout_map = _asyncio.run(
                _get_peakout(None, ticker_syms, config.start, fetch_end, dsn=config.dsn)
            )
        except Exception as _pe:
            logger.warning("[백테스트] Stage3 peakout 조회 실패 (use_stage3_peak 무시): %s", _pe)

    _compute_sell_signals_and_s2(
        all_signals, ohlcv_map, config.tx_cost_rt,
        stop_loss_pct=config.hard_stop_pct,
        flow_lookup=flow_lookup,
        cfg=config,
        stage3_peakout_map=stage3_peakout_map,
        streak_lookup=streak_lookup,
    )

    # 9. 날짜 순 정렬 → MDD equity curve가 시간 순서대로 누적
    all_signals.sort(key=lambda s: s.signal_date)

    # 10. 집계 지표
    overall = _compute_group_metrics(all_signals, config.rf_rate_annual, config.hold_weeks)

    note = ""
    if flow_lookup is not None:
        notes = []
        if config.mode in ("stage", "cross"):
            notes.append(f"S1 조건 5(외·기관 순매수 OR) 적용")
        notes.append(f"S3 조건 5(외·기관 동시 순매수 AND) 적용 — {len(flow_lookup)}건 기준")
        note = " | ".join(notes)
    else:
        if config.mode in ("stage", "cross"):
            note = "S1·S3 수급 조건 제외 — daily_flow 없음 (DSN 미설정)"
        else:
            note = "S3 수급 조건 제외 — daily_flow 없음 (DSN 미설정)"

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
