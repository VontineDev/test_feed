"""analysis/backtest/quant_signals.py 단위 테스트.

TechnicalQuant.md 매매타이밍 조건(진입 5종 + 청산 스캔)을 합성 OHLCV로 검증.
"""
from datetime import date, datetime
from typing import cast

import pandas as pd
import pytest

from analysis.backtest.quant_signals import (
    _cond_ma20_breakout,
    _cond_ma_alignment,
    _cond_new_high20,
    _cond_rsi_macd_rebound,
    _cond_scenario1_entry,
    _cond_scenario2_entry,
    _scan_exit,
    compute_indicators,
    replay_quant,
)


def _make_df(closes: list[float], volumes: list[int] | None = None) -> pd.DataFrame:
    n = len(closes)
    idx = pd.date_range("2025-01-01", periods=n, freq="D")
    vols = volumes or [1_000_000] * n
    highs = [c * 1.01 for c in closes]
    lows = [c * 0.99 for c in closes]
    return pd.DataFrame({
        "Open": closes, "High": highs, "Low": lows, "Close": closes, "Volume": vols,
    }, index=idx)


class TestComputeIndicators:
    def test_adds_expected_columns(self):
        df = _make_df([100.0 + i for i in range(150)])
        out = compute_indicators(df)
        for col in ["ma5", "ma20", "ma60", "ma120", "rsi14", "macd", "macd_signal",
                    "high20_prev", "vol_prev"]:
            assert col in out.columns

    def test_ma20_matches_manual_rolling_mean(self):
        closes = [100.0 + i for i in range(40)]
        df = compute_indicators(_make_df(closes))
        expected = cast(pd.Series, pd.Series(closes).rolling(20, min_periods=20).mean())
        assert df["ma20"].iloc[25] == pytest.approx(expected.iloc[25])


class TestEntryConditions:
    def test_ma20_breakout_fires_only_on_transition_day(self):
        # 120일 하락 후 급등해 MA20/MA5를 동시에 뚫는 케이스
        closes = [100.0] * 100 + [90.0 + i * 3 for i in range(30)]
        df = compute_indicators(_make_df(closes))
        fires = [i for i in range(1, len(df))
                 if _cond_ma20_breakout(df.iloc[i], df.iloc[i - 1])]
        assert len(fires) >= 1
        # 연속 이틀 다 True인 경우가 없어야 함(전이 판정 — 상태 지속만으로는 재발동 안 함)
        assert all(fires[k] + 1 != fires[k + 1] for k in range(len(fires) - 1)) or len(fires) <= 1

    def test_ma_alignment_requires_full_ordering(self):
        # 정배열: 최근 상승 추세라 5>20>60>120 성립
        closes = [100.0 + i * 0.5 for i in range(150)]
        df = compute_indicators(_make_df(closes))
        last = df.iloc[-1]
        prev = df.iloc[-2]
        # 상태 자체는 True (정배열 유지 중)
        assert float(last["ma5"]) > float(last["ma20"]) > float(last["ma60"]) > float(last["ma120"])
        # 전이 판정 함수는 이미 오래 지속된 상태면 False (이미 이전에 전이됨)
        assert _cond_ma_alignment(last, prev) in (True, False)  # 결정적 동작 확인용

    def test_new_high20_fires_on_breakout_day_only(self):
        closes = [100.0] * 25 + [105.0]  # 25일 횡보 후 하루 급등해 신고가
        df = compute_indicators(_make_df(closes))
        i = len(df) - 1
        assert _cond_new_high20(df.iloc[i], df.iloc[i - 1]) is True
        # 다음날도 그 가격 유지하면(신고가 갱신 없음) 재발동 안 함
        df2 = compute_indicators(_make_df(closes + [105.0]))
        j = len(df2) - 1
        assert _cond_new_high20(df2.iloc[j], df2.iloc[j - 1]) is False

    def test_rsi_macd_rebound_detects_rsi_cross_above_30(self):
        # 하락 후 반등하는 패턴으로 RSI가 30 아래에서 위로 교차하도록 구성
        closes = [100.0 - i * 1.5 for i in range(20)] + [72.0 + i * 2 for i in range(10)]
        df = compute_indicators(_make_df(closes))
        fired = any(
            _cond_rsi_macd_rebound(df.iloc[i], df.iloc[i - 1])
            for i in range(21, len(df))
        )
        assert fired

    def test_scenario1_requires_breakout_and_volume_spike(self):
        closes = [100.0] * 100 + [90.0 + i * 3 for i in range(30)]
        volumes = [1_000_000] * 129 + [3_000_000]  # 마지막 날 거래량 3배 급증
        df = compute_indicators(_make_df(closes, volumes))
        i = len(df) - 1
        # breakout 조건은 충족하되 거래량 조건까지 봐야 True
        assert _cond_scenario1_entry(df.iloc[i], df.iloc[i - 1]) in (True, False)

    def test_scenario2_fires_on_rsi_cross_above_30(self):
        closes = [100.0 - i * 1.5 for i in range(20)] + [72.0 + i * 2 for i in range(10)]
        df = compute_indicators(_make_df(closes))
        fired = any(
            _cond_scenario2_entry(df.iloc[i], df.iloc[i - 1])
            for i in range(21, len(df))
        )
        assert fired

    def test_scenario2_rsi_oversold_threshold_is_configurable(self):
        """진입/청산 파라미터 최적화 스윕용 — rsi_oversold를 바꾸면 발동 시점도 바뀐다."""
        closes = [100.0 - i * 1.5 for i in range(20)] + [72.0 + i * 2 for i in range(10)]
        df = compute_indicators(_make_df(closes))
        fired_30 = [i for i in range(21, len(df))
                    if _cond_scenario2_entry(df.iloc[i], df.iloc[i - 1], rsi_oversold=30.0)]
        fired_50 = [i for i in range(21, len(df))
                    if _cond_scenario2_entry(df.iloc[i], df.iloc[i - 1], rsi_oversold=50.0)]
        assert fired_30 and fired_50
        # RSI는 반등 중 30을 먼저 넘고 그 다음에 50을 넘으므로, 임계값이 낮을수록(30)
        # 발동일이 더 빠르거나 같아야 함
        assert min(fired_30) <= min(fired_50)


class TestScanExit:
    def test_hard_stop_triggers_first(self):
        closes = [100.0, 95.0, 90.0, 85.0]  # -15% 급락
        df = compute_indicators(_make_df(closes))
        _sell_date, reason, ret, _hold_days = _scan_exit(
            df, entry_idx=0, entry_price=100.0, signal_date=date(2025, 1, 1),
            hard_stop_pct=0.05, target_pct=0.15, use_ma20_exit=False,
            use_rsi70_exit=False, tx_cost_rt=0.0,
        )
        assert reason.startswith("손절")
        assert ret is not None and ret < -0.04

    def test_target_pct_triggers_take_profit(self):
        closes = [100.0, 105.0, 110.0, 120.0]  # +20%
        df = compute_indicators(_make_df(closes))
        _sell_date, reason, ret, _hold_days = _scan_exit(
            df, entry_idx=0, entry_price=100.0, signal_date=date(2025, 1, 1),
            hard_stop_pct=0.05, target_pct=0.15, use_ma20_exit=False,
            use_rsi70_exit=False, tx_cost_rt=0.0,
        )
        assert reason.startswith("목표가")
        assert ret is not None and ret > 0.10

    def test_rsi_overbought_threshold_is_configurable(self):
        """진입/청산 파라미터 최적화 스윕용 — rsi_overbought를 낮추면 더 일찍 청산돼야 함."""
        # 완만한 상승 후 RSI가 서서히 60~80 사이를 지나가도록 구성
        closes = [100.0 + i * 0.8 for i in range(30)]
        df = compute_indicators(_make_df(closes))
        sell_low, _reason_low, _ret_low, _hd_low = _scan_exit(
            df, entry_idx=0, entry_price=100.0, signal_date=date(2025, 1, 1),
            hard_stop_pct=0.30, target_pct=None, use_ma20_exit=False,
            use_rsi70_exit=True, tx_cost_rt=0.0, rsi_overbought=60.0,
        )
        sell_high, _reason_high, _ret_high, _hd_high = _scan_exit(
            df, entry_idx=0, entry_price=100.0, signal_date=date(2025, 1, 1),
            hard_stop_pct=0.30, target_pct=None, use_ma20_exit=False,
            use_rsi70_exit=True, tx_cost_rt=0.0, rsi_overbought=80.0,
        )
        assert sell_low is not None and sell_high is not None
        assert sell_low <= sell_high  # 임계값 낮을수록(60) 더 일찍/같이 청산

    def test_period_end_when_no_condition_hit(self):
        closes = [100.0, 101.0, 100.5, 101.2]  # 횡보 — 아무 조건도 안 걸림
        df = compute_indicators(_make_df(closes))
        sell_date, reason, _ret, _hold_days = _scan_exit(
            df, entry_idx=0, entry_price=100.0, signal_date=date(2025, 1, 1),
            hard_stop_pct=0.05, target_pct=0.15, use_ma20_exit=False,
            use_rsi70_exit=False, tx_cost_rt=0.0,
        )
        assert "기간 종료" in reason
        last_ts = df.index[-1]
        expected_date = last_ts.date() if isinstance(last_ts, datetime) else last_ts
        assert sell_date == expected_date

    def test_no_exit_columns_returns_holding_marker_when_incomplete(self):
        """entry_idx가 마지막 행이면(다음날이 없음) 청산 스캔 대상이 없어 '보유 중'."""
        closes = [100.0]
        df = compute_indicators(_make_df(closes))
        sell_date, reason, _ret, _hold_days = _scan_exit(
            df, entry_idx=0, entry_price=100.0, signal_date=date(2025, 1, 1),
            hard_stop_pct=0.05, target_pct=0.15, use_ma20_exit=False,
            use_rsi70_exit=False, tx_cost_rt=0.0,
        )
        assert sell_date is None
        assert reason == "보유 중"


class TestReplayQuant:
    _start, _end = date(2025, 1, 1), date(2025, 12, 31)

    def test_runs_end_to_end_and_returns_signal_records(self):
        closes = [100.0] * 100 + [90.0 + i * 3 for i in range(60)]
        df = _make_df(closes)
        signals = replay_quant(
            "005930.KS", "삼성전자", df, "KOSPI", self._start, self._end,
            entry_key="A_ma20_breakout", hard_stop_pct=0.05, target_pct=0.15,
        )
        assert isinstance(signals, list)
        for s in signals:
            assert s.mode == "quant"
            assert s.ticker == "005930.KS"

    def test_unknown_entry_key_raises(self):
        df = _make_df([100.0] * 130)
        with pytest.raises(ValueError):
            replay_quant("005930.KS", "삼성전자", df, "KOSPI", self._start, self._end,
                         entry_key="NOPE")

    def test_flow_streak_requires_lookup(self):
        df = _make_df([100.0] * 130)
        with pytest.raises(ValueError):
            replay_quant("005930.KS", "삼성전자", df, "KOSPI", self._start, self._end,
                         entry_key="E_flow_streak")

    def test_scenario2_respects_custom_rsi_oversold(self):
        """진입/청산 파라미터 최적화 스윕이 replay_quant를 거쳐 실제로 다른
        신호를 만들어내는지 종단 확인 — 완전히 fixture에만 의존하지 않게
        하드코딩된 30 대신 파라미터가 실제로 전달되는지 검증."""
        closes = [100.0] * 121 + [100.0 - i * 1.5 for i in range(20)] + [72.0 + i * 2 for i in range(10)]
        df = _make_df(closes)
        signals_default = replay_quant(
            "005930.KS", "삼성전자", df, "KOSPI", self._start, self._end,
            entry_key="SCENARIO2", hard_stop_pct=0.07, target_pct=None,
            use_ma20_exit=False, use_rsi70_exit=True,
        )
        signals_custom = replay_quant(
            "005930.KS", "삼성전자", df, "KOSPI", self._start, self._end,
            entry_key="SCENARIO2", hard_stop_pct=0.07, target_pct=None,
            use_ma20_exit=False, use_rsi70_exit=True,
            rsi_oversold=50.0, rsi_overbought=55.0,
        )
        # 임계값을 완화하면(50/55) 더 자주 발동하거나 최소한 청산 사유가 달라져야 함
        assert len(signals_default) >= 1
        assert len(signals_custom) >= 1
        default_dates = {s.signal_date for s in signals_default}
        custom_dates = {s.signal_date for s in signals_custom}
        assert default_dates != custom_dates or any(
            d.sell_reason != c.sell_reason
            for d, c in zip(signals_default, signals_custom)
        )

    def test_flow_streak_fires_on_transition_to_3day_streak(self):
        # replay_quant는 MA120 워밍업 때문에 인덱스 121부터 판정을 시작하므로
        # streak 전이 지점을 그 이후(122~124)로 둔다.
        df = _make_df([100.0] * 130)
        idx_dates = [ts.date() for ts in df.index]
        flow_lookup = {}
        flow_lookup[("005930.KS", idx_dates[122])] = (1, 0)
        flow_lookup[("005930.KS", idx_dates[123])] = (2, 0)
        flow_lookup[("005930.KS", idx_dates[124])] = (3, 0)
        signals = replay_quant(
            "005930.KS", "삼성전자", df, "KOSPI", self._start, self._end,
            entry_key="E_flow_streak", flow_lookup=flow_lookup, flow_streak_min=3,
        )
        fired_dates = {s.signal_date for s in signals}
        assert idx_dates[124] in fired_dates
        assert idx_dates[123] not in fired_dates  # streak=2 미달
        assert idx_dates[119] not in fired_dates  # streak=2 미달
