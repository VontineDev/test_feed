"""
test_exit_model.py — _compute_exit_logic() 단위 테스트

분할 청산 모델 (tp1 + trailing + stage3 + period_end) 검증.
실제 DB/yfinance 없이 mock DataFrame으로만 동작.
"""
from __future__ import annotations

import copy
from datetime import date, timedelta
from typing import Any

import pandas as pd
import pytest

# 프로젝트 루트에서 임포트
from analysis.backtest.models import BacktestConfig, SignalRecord
from analysis.backtest.exit_models import _compute_exit_logic, _compute_sell_signals_and_s2


# ── 헬퍼 ──────────────────────────────────────────────────────────────

def _make_cfg(**kwargs) -> BacktestConfig:
    """테스트용 BacktestConfig 생성."""
    defaults: dict[str, Any] = dict(
        mode="stage",
        start=date(2024, 1, 1),
        end=date(2025, 1, 1),
        tp1_pct=0.15,
        tp1_ratio=0.5,
        trail_pct=0.10,
        hard_stop_pct=0.08,
        use_stage3_peak=False,
    )
    defaults.update(kwargs)
    return BacktestConfig(**defaults)


def _make_sig(entry_price: float = 10_000.0) -> SignalRecord:
    return SignalRecord(
        ticker="000000.KS",
        name="테스트",
        signal_date=date(2024, 1, 2),
        close_at_signal=entry_price,
        mode="stage",
        market="KOSPI",
    )


def _make_df(prices: list[tuple[float, float, float]]) -> pd.DataFrame:
    """(Close, High, Low) 리스트로 일봉 DataFrame 생성.
    signal_date = 2024-01-02, 이후 날짜부터 데이터.
    """
    base = date(2024, 1, 3)
    rows = []
    for i, (close, high, low) in enumerate(prices):
        rows.append({
            "Date":   base + timedelta(days=i),
            "Close":  close,
            "High":   high,
            "Low":    low,
            "Open":   close,
            "Volume": 1_000_000,
        })
    df = pd.DataFrame(rows)
    df.set_index("Date", inplace=True)
    return df


def _run(sig: SignalRecord, df: pd.DataFrame, cfg: BacktestConfig,
         peakout_dates: frozenset = frozenset()) -> SignalRecord:
    sig_cp = copy.deepcopy(sig)
    entry_idx = 0  # df[0]이 signal_date 다음날
    _compute_exit_logic(sig_cp, df, entry_idx - 1, sig_cp.close_at_signal,
                        cfg, peakout_dates)
    return sig_cp


# ── 테스트 ────────────────────────────────────────────────────────────

class TestHardStop:
    def test_hard_stop_fires_before_tp1(self):
        """Hard stop이 TP1보다 우선한다."""
        entry = 10_000.0
        sig   = _make_sig(entry)
        # Day 1: -10% (stop at -8% = 9200)
        df    = _make_df([(9_000, 9_100, 8_900)])
        cfg   = _make_cfg(tp1_pct=0.15, hard_stop_pct=0.08)
        result = _run(sig, df, cfg)

        assert result.final_exit_type == "hard_stop"
        assert result.tp1_date is None
        assert result.blended_return is not None
        assert result.blended_return < 0

    def test_hard_stop_sets_sell_date(self):
        """Hard stop 발동 시 sell_date도 채워진다 (기존 집계 호환)."""
        entry = 10_000.0
        sig   = _make_sig(entry)
        df    = _make_df([(9_000, 9_100, 8_900)])
        cfg   = _make_cfg(tp1_pct=0.15, hard_stop_pct=0.08)
        result = _run(sig, df, cfg)

        assert result.sell_date is not None
        assert "손절" in result.sell_reason


class TestTp1:
    def test_tp1_triggers_partial_exit(self):
        """1차 TP 도달 시 tp1_date가 기록되고 잔여분은 계속 추적."""
        entry = 10_000.0
        sig   = _make_sig(entry)
        # Day 1: +15% (TP1), Day 2: 평단, Day 3 (마지막): period_end
        df    = _make_df([
            (11_500, 11_600, 11_400),   # TP1 발동
            (11_400, 11_500, 11_300),
            (11_300, 11_400, 11_200),   # period_end
        ])
        cfg   = _make_cfg(tp1_pct=0.15, tp1_ratio=0.5, trail_pct=0.0)
        result = _run(sig, df, cfg)

        assert result.tp1_date == date(2024, 1, 3)
        assert result.tp1_ret is not None and result.tp1_ret > 0
        assert result.final_exit_type == "period_end"
        # blended = 0.5 * tp1_ret + 0.5 * final_exit_ret
        assert result.blended_return is not None

    def test_tp1_blended_formula(self):
        """blended_return = ratio*tp1_ret + (1-ratio)*final_ret."""
        entry = 10_000.0
        sig   = _make_sig(entry)
        df    = _make_df([
            (11_500, 11_600, 11_400),  # tp1 (+15%)
            (11_000, 11_100, 10_900),  # period_end
        ])
        cfg   = _make_cfg(tp1_pct=0.15, tp1_ratio=0.5, trail_pct=0.0,
                          hard_stop_pct=0.08)
        result = _run(sig, df, cfg)

        assert result.tp1_ret is not None
        assert result.final_exit_ret is not None
        assert result.blended_return is not None
        expected = (
            0.5 * result.tp1_ret
            + 0.5 * result.final_exit_ret
        )
        assert abs(result.blended_return - expected) < 1e-9


class TestTrailingStop:
    def test_trail_fires_after_tp1(self):
        """TP1 이후 watermark 대비 trail_pct 하락 시 trail 청산."""
        entry = 10_000.0
        sig   = _make_sig(entry)
        # Day1: +15% (TP1), High=12000, Day2: 고점 유지, Day3: -10% from 12000 → trail
        df    = _make_df([
            (11_500, 12_000, 11_400),  # tp1 + watermark=12000
            (11_800, 12_100, 11_700),  # watermark=12100
            (10_880, 11_000, 10_800),  # 12100×0.90=10890 — trail 발동
        ])
        cfg   = _make_cfg(tp1_pct=0.15, tp1_ratio=0.5, trail_pct=0.10)
        result = _run(sig, df, cfg)

        assert result.final_exit_type == "trail"
        assert result.final_exit_date == date(2024, 1, 5)

    def test_trail_not_fire_before_tp1(self):
        """TP1 미발동 상태에서는 trail이 작동하지 않는다."""
        entry = 10_000.0
        sig   = _make_sig(entry)
        # TP1 미달 + 큰 하락
        df    = _make_df([
            (10_500, 10_600, 10_400),  # tp1 미달
            (9_000,  9_100,  8_900),   # hard_stop 발동
        ])
        cfg   = _make_cfg(tp1_pct=0.15, tp1_ratio=0.5, trail_pct=0.10, hard_stop_pct=0.08)
        result = _run(sig, df, cfg)

        assert result.final_exit_type == "hard_stop"


class TestStage3Peakout:
    def test_stage3_peakout_fires_after_tp1(self):
        """Stage3 peakout 날짜에 잔여분 청산."""
        entry  = 10_000.0
        sig    = _make_sig(entry)
        df     = _make_df([
            (11_500, 11_600, 11_400),  # tp1
            (11_300, 11_400, 11_200),  # stage3 peakout day
            (11_000, 11_100, 10_900),
        ])
        peakout_dates = frozenset([date(2024, 1, 4)])  # Day2
        cfg = _make_cfg(tp1_pct=0.15, trail_pct=0.10, use_stage3_peak=True)
        result = _run(sig, df, cfg, peakout_dates)

        assert result.final_exit_type == "stage3"
        assert result.final_exit_date == date(2024, 1, 4)


class TestPeriodEnd:
    def test_period_end_closes_open_position(self):
        """기간 마지막 row에서 미청산 포지션 강제 청산."""
        sig = _make_sig(10_000.0)
        df  = _make_df([(10_500, 10_600, 10_400)])  # 1일, period_end
        cfg = _make_cfg(tp1_pct=0.20, trail_pct=0.10)  # TP1 미달
        result = _run(sig, df, cfg)

        assert result.final_exit_type == "period_end"
        assert result.sell_date is not None

    def test_blended_equals_final_when_no_tp1(self):
        """TP1 미발동 시 blended_return == final_exit_ret."""
        sig = _make_sig(10_000.0)
        df  = _make_df([(10_500, 10_600, 10_400)])
        cfg = _make_cfg(tp1_pct=0.20, trail_pct=0.10)
        result = _run(sig, df, cfg)

        assert result.blended_return == result.final_exit_ret


class TestFallback:
    def test_tp1_zero_falls_back_to_ma20(self):
        """tp1_pct=0 AND trail_pct=0 → MA20 이탈 폴백 (기존 동작)."""
        sig = _make_sig(10_000.0)
        # tp1_pct=0, trail_pct=0 → use_exit_logic=False → 기존 경로
        # _compute_sell_signals_and_s2 경로로 테스트
        df = pd.DataFrame({
            "Date":   pd.date_range("2024-01-03", periods=30),
            "Close":  [10_000.0] * 20 + [9_000.0] * 10,
            "High":   [10_100.0] * 20 + [9_100.0] * 10,
            "Low":    [9_900.0]  * 20 + [8_900.0] * 10,
            "Open":   [10_000.0] * 30,
            "Volume": [1_000_000] * 30,
        }).set_index("Date")

        cfg = BacktestConfig(
            mode="stage", start=date(2024, 1, 1), end=date(2025, 1, 1),
            tp1_pct=0.0, trail_pct=0.0, hard_stop_pct=0.08,
        )
        sig_copy = copy.deepcopy(sig)
        ohlcv_map = {"000000.KS": df}
        _compute_sell_signals_and_s2(
            [sig_copy], ohlcv_map, 0.002,
            stop_loss_pct=cfg.hard_stop_pct,
            cfg=cfg,
        )
        # 분할 청산 미사용 → blended_return은 None
        assert sig_copy.blended_return is None


class TestBacktestConfigValidation:
    def test_tp1_pct_negative_raises(self):
        with pytest.raises(ValueError, match="tp1_pct"):
            BacktestConfig(
                mode="stage", start=date(2024, 1, 1), end=date(2025, 1, 1),
                tp1_pct=-0.1,
            )

    def test_trail_pct_over_one_raises(self):
        with pytest.raises(ValueError, match="trail_pct"):
            BacktestConfig(
                mode="stage", start=date(2024, 1, 1), end=date(2025, 1, 1),
                trail_pct=1.5,
            )

    def test_hard_stop_zero_raises(self):
        with pytest.raises(ValueError, match="hard_stop_pct"):
            BacktestConfig(
                mode="stage", start=date(2024, 1, 1), end=date(2025, 1, 1),
                hard_stop_pct=0.0,
            )


class TestGetStage3PeakoutMap:
    """get_stage3_peakout_map() 단위 테스트 (DB mock)."""

    @pytest.mark.asyncio
    async def test_returns_dates_when_found(self):
        from unittest.mock import AsyncMock, MagicMock
        from core.db import get_stage3_peakout_map

        mock_row1 = {"ticker": "005930.KS", "classified_date": date(2024, 3, 5)}
        mock_row2 = {"ticker": "005930.KS", "classified_date": date(2024, 3, 12)}
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[mock_row1, mock_row2])
        pool = MagicMock()
        pool.acquire = MagicMock(return_value=MagicMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(return_value=None),
        ))

        result = await get_stage3_peakout_map(
            pool, ["005930.KS"], date(2024, 1, 1), date(2024, 12, 31)
        )
        assert "005930.KS" in result
        assert date(2024, 3, 5)  in result["005930.KS"]
        assert date(2024, 3, 12) in result["005930.KS"]

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_rows(self):
        from unittest.mock import AsyncMock, MagicMock
        from core.db import get_stage3_peakout_map

        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[])
        pool = MagicMock()
        pool.acquire = MagicMock(return_value=MagicMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(return_value=None),
        ))

        result = await get_stage3_peakout_map(
            pool, ["000000.KS"], date(2024, 1, 1), date(2024, 12, 31)
        )
        assert result == {}

    @pytest.mark.asyncio
    async def test_returns_empty_on_db_error(self):
        from unittest.mock import AsyncMock, MagicMock
        from core.db import get_stage3_peakout_map

        conn = AsyncMock()
        conn.fetch = AsyncMock(side_effect=Exception("column peakout_flag does not exist"))
        pool = MagicMock()
        pool.acquire = MagicMock(return_value=MagicMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(return_value=None),
        ))

        result = await get_stage3_peakout_map(
            pool, ["000000.KS"], date(2024, 1, 1), date(2024, 12, 31)
        )
        assert result == {}
