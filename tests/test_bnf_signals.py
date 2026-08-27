"""analysis/backtest/quant_signals.py의 BNF(코테카와 타카시) 모델 단위 테스트.

BNF_TradingModel.md 매매 규칙(이격도+RSI+MACD 히스토그램 전환 매수,
손절/추세별 트레일링/모멘텀 소진 매도)을 합성 OHLCV로 검증.
"""
from datetime import date, datetime

import pandas as pd
import pytest

from analysis.backtest.quant_signals import (
    _cond_bnf_entry,
    _scan_exit_bnf,
    compute_bnf_indicators,
    replay_bnf,
)


def _make_df(
    closes: list[float],
    volumes: list[int] | None = None,
    opens: list[float] | None = None,
) -> pd.DataFrame:
    n = len(closes)
    idx = pd.date_range("2025-01-01", periods=n, freq="D")
    vols = volumes or [1_000_000] * n
    ops = opens or closes
    highs = [max(o, c) * 1.01 for o, c in zip(ops, closes)]
    lows = [min(o, c) * 0.99 for o, c in zip(ops, closes)]
    return pd.DataFrame({
        "Open": ops, "High": highs, "Low": lows, "Close": closes, "Volume": vols,
    }, index=idx)


def _crash_then_rebound(n_flat: int = 121, drop_pct: float = 0.35) -> list[float]:
    """121일 평탄한 흐름 뒤 급락(이격도/RSI 과매도 유발) → 완만한 반등(MACD
    히스토그램이 음에서 양으로 전환하도록)."""
    base = [100.0] * n_flat
    bottom = 100.0 * (1 - drop_pct)
    crash = [100.0 - (100.0 - bottom) * (i + 1) / 10 for i in range(10)]
    rebound = [bottom * (1 + 0.01 * i) for i in range(1, 40)]
    return base + crash + rebound


class TestComputeBnfIndicators:
    def test_adds_expected_columns(self):
        df = _make_df(_crash_then_rebound())
        out = compute_bnf_indicators(df)
        for col in ["ema25", "discrepancy", "macd_hist", "rsi14", "ma60"]:
            assert col in out.columns

    def test_discrepancy_matches_manual_calc(self):
        df = _make_df(_crash_then_rebound())
        out = compute_bnf_indicators(df)
        i = len(out) - 1
        expected = (out["Close"].iloc[i] - out["ema25"].iloc[i]) / out["ema25"].iloc[i]
        assert out["discrepancy"].iloc[i] == pytest.approx(float(expected))


class TestCondBnfEntry:
    def test_fires_after_disc_and_rsi_oversold_then_macd_cross_up(self):
        closes = _crash_then_rebound()
        df = compute_bnf_indicators(_make_df(closes))
        fires = [
            i for i in range(121, len(df))
            if _cond_bnf_entry(df, i, disc_threshold=-0.25, rsi_oversold=30.0, lookback=15)
        ]
        assert len(fires) >= 1
        # 전이 판정(MACD 히스토그램 크로스)이라 연속 이틀 발동은 없어야 함
        assert all(fires[k] + 1 != fires[k + 1] for k in range(len(fires) - 1))

    def test_no_fire_without_deep_enough_discrepancy(self):
        # 완만한 하락(-10% 근처)만 있어 -25% 이격도 문턱을 못 넘는 케이스
        closes = [100.0] * 121 + [100.0 - i * 0.3 for i in range(10)] + [97.0 + i * 0.5 for i in range(20)]
        df = compute_bnf_indicators(_make_df(closes))
        fires = [
            i for i in range(121, len(df))
            if _cond_bnf_entry(df, i, disc_threshold=-0.25, rsi_oversold=30.0, lookback=15)
        ]
        assert fires == []

    def test_disc_threshold_is_configurable(self):
        # 최대 이격도 약 -14% 하락 — 완화된 임계값(-0.10)에선 발동, 엄격한 임계값(-0.40)에선 미발동
        closes = [100.0] * 121 + [100.0 - i * 2.0 for i in range(10)] + [80.0 + i * 1.0 for i in range(20)]
        df = compute_bnf_indicators(_make_df(closes))
        fires_loose = [
            i for i in range(121, len(df))
            if _cond_bnf_entry(df, i, disc_threshold=-0.10, rsi_oversold=30.0, lookback=15)
        ]
        fires_strict = [
            i for i in range(121, len(df))
            if _cond_bnf_entry(df, i, disc_threshold=-0.40, rsi_oversold=30.0, lookback=15)
        ]
        assert fires_loose
        assert fires_strict == []


class TestScanExitBnf:
    def test_hard_stop_triggers_first(self):
        closes = [100.0, 92.0, 85.0, 80.0]  # -20% 급락
        df = compute_bnf_indicators(_make_df(closes))
        _sell_date, reason, ret, _hold_days = _scan_exit_bnf(
            df, entry_idx=0, entry_price=100.0, signal_date=date(2025, 1, 1),
            hard_stop_pct=0.08, trail_pct_uptrend=0.15, trail_pct_downtrend=0.07,
            tx_cost_rt=0.0,
        )
        assert reason.startswith("손절")
        assert ret is not None and ret < -0.07

    def test_trailing_stop_triggers_after_peak_pullback(self):
        # 진입 후 크게 올랐다가(고점 형성) 트레일링 폭만큼 되밀림 — 손절선은 안 건드림
        closes = [100.0, 110.0, 130.0, 150.0, 130.0, 120.0]
        df = compute_bnf_indicators(_make_df(closes))
        _sell_date, reason, ret, _hold_days = _scan_exit_bnf(
            df, entry_idx=0, entry_price=100.0, signal_date=date(2025, 1, 1),
            hard_stop_pct=0.08, trail_pct_uptrend=0.15, trail_pct_downtrend=0.07,
            tx_cost_rt=0.0,
        )
        assert "트레일링" in reason
        assert ret is not None and ret > 0  # 손절 아래로 가지 않고 익절권에서 청산

    def test_period_end_when_no_condition_hit(self):
        closes = [100.0, 101.0, 100.5, 101.2]  # 횡보 — 아무 조건도 안 걸림
        df = compute_bnf_indicators(_make_df(closes))
        sell_date, reason, _ret, _hold_days = _scan_exit_bnf(
            df, entry_idx=0, entry_price=100.0, signal_date=date(2025, 1, 1),
            hard_stop_pct=0.30, trail_pct_uptrend=0.30, trail_pct_downtrend=0.30,
            tx_cost_rt=0.0,
        )
        assert "기간 종료" in reason
        last_ts = df.index[-1]
        expected_date = last_ts.date() if isinstance(last_ts, datetime) else last_ts
        assert sell_date == expected_date


class TestReplayBnf:
    _start, _end = date(2025, 1, 1), date(2025, 12, 31)

    def test_runs_end_to_end_and_returns_signal_records(self):
        df = _make_df(_crash_then_rebound())
        signals = replay_bnf(
            "005930.KS", "삼성전자", df, "KOSPI", self._start, self._end,
            disc_threshold=-0.25,
        )
        assert isinstance(signals, list)
        for s in signals:
            assert s.mode == "quant"
            assert s.ticker == "005930.KS"
            assert s.sell_reason is not None

    def test_no_signals_when_disc_threshold_unreachable(self):
        df = _make_df(_crash_then_rebound(drop_pct=0.10))  # 완만한 조정만
        signals = replay_bnf(
            "005930.KS", "삼성전자", df, "KOSPI", self._start, self._end,
            disc_threshold=-0.40,
        )
        assert signals == []
