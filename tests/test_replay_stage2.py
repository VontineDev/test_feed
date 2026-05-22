"""
test_replay_stage2.py — Stage 2 재현 단위 테스트

Stage 2 조건:
  C1: close / s1_close ∈ [0.80, 0.95]  (−5% ~ −20% 되돌림)
  C2: close ≥ MA20 × 0.95
  C3: vol_today / vol_s1 ∈ [0.30, 0.60]
  C4: 기관 연속 매수 — 건너뜀

모든 테스트는 네트워크 없이 합성 DataFrame 사용.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import pandas as pd
import pytest

from analysis.backtest_engine import (
    BacktestConfig,
    SignalRecord,
    _replay_stage2,
)

# ── 합성 데이터 헬퍼 ──────────────────────────────────────────────

def _make_df(
    flat_days: int = 80,
    flat_price: float = 10_000.0,
    flat_vol: int = 100_000,
    spike_pct: float = 0.08,
    spike_vol_mult: float = 3.0,
    post: Optional[list[tuple[float, int]]] = None,
    start_date: date = date(2022, 1, 3),
) -> pd.DataFrame:
    """Flat baseline → spike (S1 trigger) → optional post-spike days.

    post: list of (price, volume) tuples for days after the spike.
    Defaults to one quiet pullback day at −10%, 40% of spike volume.
    """
    if post is None:
        pullback = flat_price * (1 + spike_pct) * 0.90
        post = [(pullback, int(flat_vol * spike_vol_mult * 0.40))]

    rows: list[tuple[float, float, float, float, int]] = []  # O H L C V
    current = start_date
    prices = [flat_price] * flat_days + [flat_price * (1 + spike_pct)] + [p for p, _ in post]
    vols   = [flat_vol] * flat_days + [int(flat_vol * spike_vol_mult)] + [v for _, v in post]

    for p, v in zip(prices, vols):
        while current.weekday() >= 5:
            current += timedelta(days=1)
        rows.append((p * 0.999, p * 1.001, p * 0.999, p, v))
        current += timedelta(days=1)

    idx = []
    current = start_date
    for _ in rows:
        while current.weekday() >= 5:
            current += timedelta(days=1)
        idx.append(pd.Timestamp(current, tz="UTC"))
        current += timedelta(days=1)

    return pd.DataFrame(
        {"Open": [r[0] for r in rows], "High": [r[1] for r in rows],
         "Low":  [r[2] for r in rows], "Close": [r[3] for r in rows],
         "Volume": [r[4] for r in rows]},
        index=idx,
    )


def _cfg(start: date, end: date, market: str = "KOSPI") -> BacktestConfig:
    return BacktestConfig("stage2", start, end, market=market)


# ── BacktestConfig.mode 검증 ─────────────────────────────────────

class TestStage2Config:
    def test_stage2_mode_valid(self):
        cfg = BacktestConfig("stage2", date(2025, 1, 1), date(2026, 1, 1))
        assert cfg.mode == "stage2"

    def test_stage2_mode_error_message(self):
        with pytest.raises(ValueError, match="stage2"):
            BacktestConfig("stageX", date(2025, 1, 1), date(2026, 1, 1))


# ── S2 신호 발동 기본 케이스 ─────────────────────────────────────

class TestReplayStage2Basic:
    def test_s2_fires_after_valid_s1(self):
        """정상 S1 + 다음날 C1/C2/C3 충족 → S2 신호 1건."""
        df = _make_df(flat_days=80, spike_pct=0.08, spike_vol_mult=3.0,
                      post=[(10_800 * 0.90, int(100_000 * 3.0 * 0.40))])
        spike_date = df.index[-2].date()  # 스파이크(S1) 날짜
        s2_date    = df.index[-1].date()  # 그 다음날(S2 후보)
        cfg = _cfg(spike_date, s2_date + timedelta(days=1))
        sigs = _replay_stage2("T.KS", "테스트", df, "KOSPI", cfg)
        assert len(sigs) == 1
        assert sigs[0].signal_date == s2_date
        assert sigs[0].mode == "stage2"
        assert sigs[0].ticker == "T.KS"

    def test_no_s1_no_s2(self):
        """S1 조건 불충족(+2% 상승) → S2 없음."""
        df = _make_df(flat_days=80, spike_pct=0.02)
        spike_date = df.index[-2].date()
        cfg = _cfg(spike_date, spike_date + timedelta(days=5))
        sigs = _replay_stage2("T.KS", "테스트", df, "KOSPI", cfg)
        assert sigs == []

    def test_signal_record_fields(self):
        """S2 SignalRecord 필드 검증."""
        df = _make_df(flat_days=80, spike_pct=0.08, spike_vol_mult=3.0,
                      post=[(10_800 * 0.90, int(100_000 * 3.0 * 0.40))])
        spike_date = df.index[-2].date()
        cfg = _cfg(spike_date, spike_date + timedelta(days=5))
        sigs = _replay_stage2("005930.KS", "삼성전자", df, "KOSPI", cfg)
        assert len(sigs) == 1
        s = sigs[0]
        assert s.ticker == "005930.KS"
        assert s.name == "삼성전자"
        assert s.market == "KOSPI"
        assert s.close_at_signal == pytest.approx(10_800 * 0.90, rel=0.01)


# ── Condition 1: 되돌림 범위 ─────────────────────────────────────

class TestStage2C1:
    """C1 격리 테스트. spike_pct=0.40을 사용해 되돌림 후에도 MA20 지지(C2)가 항상 통과하도록 설정.

    flat=10_000, spike=+40%=14_000이면 20% 풀백 후 close=11_200이고
    MA20≈10_150이므로 11_200 ≥ 10_150×0.95=9_642 → C2 안전하게 통과.
    """

    _FLAT: float = 10_000.0
    _SPIKE_PCT: float = 0.40
    _SPIKE_CLOSE: float = _FLAT * (1 + _SPIKE_PCT)  # 14_000

    def _run(self, pullback_ratio: float) -> list[SignalRecord]:
        pullback = self._SPIKE_CLOSE * pullback_ratio
        s2_vol   = int(100_000 * 3.0 * 0.40)
        df = _make_df(flat_days=80, flat_price=self._FLAT,
                      spike_pct=self._SPIKE_PCT, spike_vol_mult=3.0,
                      post=[(pullback, s2_vol)])
        spike_date = df.index[-2].date()
        cfg = _cfg(spike_date, spike_date + timedelta(days=5))
        return _replay_stage2("T.KS", "테스트", df, "KOSPI", cfg)

    def test_c1_exact_lower_boundary(self):
        """비율 0.80 (딱 -20%) → C1 통과 (C2도 OK — spike 충분히 큼)."""
        assert len(self._run(0.80)) == 1

    def test_c1_exact_upper_boundary(self):
        """비율 0.95 (딱 -5%) → 통과."""
        assert len(self._run(0.95)) == 1

    def test_c1_drop_too_small(self):
        """비율 0.97 (-3%, 되돌림 부족) → C1 실패 → S2 없음."""
        assert self._run(0.97) == []

    def test_c1_drop_too_large(self):
        """비율 0.79 (-21%, 과도한 하락) → C1 실패 → S2 없음."""
        assert self._run(0.79) == []

    def test_c1_no_drop(self):
        """비율 1.02 (+2%, 추가 상승) → C1 실패."""
        assert self._run(1.02) == []


# ── Condition 2: MA20 지지 ───────────────────────────────────────

class TestStage2C2:
    def test_c2_above_ma20_95(self):
        """close = MA20 × 0.96 → C2 통과."""
        # flat_price = 10_000 → MA20 ≈ 10_000
        # spike = +8% → 10_800, pullback to 10_800 × 0.90 = 9_720
        # 9_720 / 10_000 = 0.972 ≥ 0.95 → C2 통과
        df = _make_df(flat_days=80, spike_pct=0.08, spike_vol_mult=3.0,
                      post=[(10_800 * 0.90, int(100_000 * 3.0 * 0.40))])
        spike_date = df.index[-2].date()
        cfg = _cfg(spike_date, spike_date + timedelta(days=5))
        sigs = _replay_stage2("T.KS", "테스트", df, "KOSPI", cfg)
        assert len(sigs) == 1

    def test_c2_below_ma20_95(self):
        """close < MA20 × 0.95 → C2 실패 → S2 없음."""
        # flat_price = 10_000 → MA20 ≈ 10_000; 95% = 9_500
        # spike = 20_000, pullback to 20_000 × 0.90 = 18_000 (C1 OK)
        # But we need close < 9_500 with C1 also satisfied — use spike at +8%, then
        # drop to 9_200 → ratio = 9_200/10_800 = 0.852 (C1 OK) but 9_200 < 9_500 (C2 fail)
        df = _make_df(flat_days=80, spike_pct=0.08, spike_vol_mult=3.0,
                      post=[(9_200, int(100_000 * 3.0 * 0.40))])
        spike_date = df.index[-2].date()
        cfg = _cfg(spike_date, spike_date + timedelta(days=5))
        sigs = _replay_stage2("T.KS", "테스트", df, "KOSPI", cfg)
        assert sigs == []

    def test_c2_nan_ma20_skipped(self):
        """MA20 NaN(데이터 부족) → C2 실패 → S2 없음."""
        # Use only 15 flat days so MA20 (min_periods=20) is NaN for all rows
        df = _make_df(flat_days=15, spike_pct=0.08, spike_vol_mult=3.0,
                      post=[(10_800 * 0.90, int(100_000 * 3.0 * 0.40))])
        spike_date = df.index[-2].date()
        cfg = _cfg(spike_date, spike_date + timedelta(days=5))
        sigs = _replay_stage2("T.KS", "테스트", df, "KOSPI", cfg)
        assert sigs == []


# ── Condition 3: 거래량 비율 ─────────────────────────────────────

class TestStage2C3:
    """거래대금 비율 기준 C3 검증.
    spike_close=11664, s2_close=9720 → txamt_ratio = vol_ratio × (9720/11664) ≈ vol_ratio × 0.833
    Lower boundary: vol_ratio=0.30 → txamt_ratio≈0.25 (threshold 0.25)
    Upper boundary: vol_ratio=0.60 → txamt_ratio≈0.50 (threshold 0.65)
    Fail-low:       vol_ratio=0.20 → txamt_ratio≈0.167 (<0.25)
    Fail-high:      vol_ratio=0.80 → txamt_ratio≈0.667 (>0.65)
    """
    def _run_with_vol_ratio(self, vol_ratio: float) -> list[SignalRecord]:
        spike_vol = int(100_000 * 3.0)
        s2_vol    = int(spike_vol * vol_ratio)
        df = _make_df(flat_days=80, spike_pct=0.08, spike_vol_mult=3.0,
                      post=[(10_800 * 0.90, s2_vol)])
        spike_date = df.index[-2].date()
        cfg = _cfg(spike_date, spike_date + timedelta(days=5))
        return _replay_stage2("T.KS", "테스트", df, "KOSPI", cfg)

    def test_c3_lower_boundary(self):
        """비율 0.30 → 통과."""
        assert len(self._run_with_vol_ratio(0.30)) == 1

    def test_c3_upper_boundary(self):
        """비율 0.60 → 통과."""
        assert len(self._run_with_vol_ratio(0.60)) == 1

    def test_c3_vol_too_low(self):
        """비율 0.20 (너무 낮음) → C3 실패."""
        assert self._run_with_vol_ratio(0.20) == []

    def test_c3_vol_too_high(self):
        """비율 0.80 (너무 높음, 아직 강한 세력) → C3 실패."""
        assert self._run_with_vol_ratio(0.80) == []

    def test_c3_mid_range(self):
        """비율 0.45 (중간값) → 통과."""
        assert len(self._run_with_vol_ratio(0.45)) == 1


# ── 14일 윈도우 경계 ─────────────────────────────────────────────

class TestStage2Window:
    def _make_with_delay(self, delay_days: int) -> list[SignalRecord]:
        """S1 이후 delay_days 거래일(+주말 건너뜀) 뒤에 S2 조건 충족."""
        spike_close = 10_800.0
        post: list[tuple[float, int]] = []
        for d in range(delay_days):
            if d < delay_days - 1:
                # 조건 불충족 일수 채우기 (가격 변동 없음 → C1 ratio=1.0 실패)
                post.append((spike_close, 100_000))
            else:
                post.append((spike_close * 0.90, int(100_000 * 3.0 * 0.40)))

        df = _make_df(flat_days=80, spike_pct=0.08, spike_vol_mult=3.0, post=post)
        spike_date = df.index[-(delay_days + 1)].date()
        s2_date    = df.index[-1].date()
        cfg = _cfg(spike_date, s2_date + timedelta(days=1))
        return _replay_stage2("T.KS", "테스트", df, "KOSPI", cfg)

    def test_fires_on_day_1(self):
        """S1 다음날(1거래일 뒤) → 윈도우 내 → 통과."""
        sigs = self._make_with_delay(1)
        assert len(sigs) == 1

    def test_first_valid_day_wins(self):
        """여러 날이 조건을 충족해도 첫 번째 날만 신호 발동."""
        spike_close = 10_800.0
        valid_post: list[tuple[float, int]] = [
            (spike_close * 0.90, int(100_000 * 3.0 * 0.40)),  # 첫째 날 → S2
            (spike_close * 0.88, int(100_000 * 3.0 * 0.35)),  # 둘째 날도 조건 OK, 하지만 skip
        ]
        df = _make_df(flat_days=80, spike_pct=0.08, spike_vol_mult=3.0, post=valid_post)
        spike_date = df.index[-3].date()
        cfg = _cfg(spike_date, spike_date + timedelta(days=5))
        sigs = _replay_stage2("T.KS", "테스트", df, "KOSPI", cfg)
        assert len(sigs) == 1

    def test_outside_window_no_signal(self):
        """S1 이후 15 캘린더일 뒤(윈도우 초과) → S2 없음."""
        spike_close = 10_800.0
        # 거래일 기준 15일 후면 캘린더 기준으로 21일 정도 됨 — 캘린더 기준 15일 뒤를 구성
        # 스파이크 직후: 14일 동안 빠지지 않다가, 15일째(캘린더) 조건 충족
        # 방법: spike 후 post에 충분히 많은 날 추가 후 마지막 날이 cutoff 초과하도록 설정
        flat_fill = [(spike_close, 100_000)] * 10  # 캘린더 14일 초과 보장(거래일 10개 ≈ 14일)
        fill_then_valid = flat_fill + [(spike_close * 0.90, int(100_000 * 3.0 * 0.40))]
        df = _make_df(flat_days=80, spike_pct=0.08, spike_vol_mult=3.0, post=fill_then_valid)
        spike_date = df.index[-(len(fill_then_valid) + 1)].date()
        last_date  = df.index[-1].date()
        cfg = _cfg(spike_date, last_date + timedelta(days=1))
        sigs = _replay_stage2("T.KS", "테스트", df, "KOSPI", cfg)
        # 마지막 S2 후보가 캘린더 14일 초과인지 확인
        if sigs:
            # 신호가 있다면 윈도우 내에 있어야 함
            for s in sigs:
                assert (s.signal_date - spike_date).days <= 14
        # 핵심: 거래일 10개는 캘린더 14일 경계이므로, 어차피 14일 초과는 없어야 함
        # 테스트 목적: 14일 윈도우 cutoff 로직이 작동함을 검증
        # 마지막 날이 cutoff(14d) 이후인지 확인
        days_since_spike = (last_date - spike_date).days
        if days_since_spike > 14:
            assert sigs == []


# ── C4 조건 건너뜀 (암묵적 검증) ────────────────────────────────

class TestStage2C4Skipped:
    def test_c4_skipped_signal_fires_without_flow(self):
        """flow_lookup 없이도 S2 신호 발동 — C4는 검사하지 않음."""
        df = _make_df(flat_days=80, spike_pct=0.08, spike_vol_mult=3.0,
                      post=[(10_800 * 0.90, int(100_000 * 3.0 * 0.40))])
        spike_date = df.index[-2].date()
        cfg = _cfg(spike_date, spike_date + timedelta(days=5))
        sigs = _replay_stage2("T.KS", "테스트", df, "KOSPI", cfg)
        # flow_lookup=None → C4 건너뜀 → C1+C2+C3만으로 신호 발동
        assert len(sigs) == 1


# ── S1 선행 조건 ─────────────────────────────────────────────────

class TestStage2S1Prerequisite:
    def test_no_signal_without_s1(self):
        """S1 없는 데이터(상승률 미달) → S2도 없음."""
        # +2% 상승 → S1 조건(≥5%) 미달 → S2 없음
        df = _make_df(flat_days=80, spike_pct=0.02, spike_vol_mult=3.0,
                      post=[(10_200 * 0.90, int(100_000 * 3.0 * 0.40))])
        spike_date = df.index[-2].date()
        cfg = _cfg(spike_date, spike_date + timedelta(days=5))
        sigs = _replay_stage2("T.KS", "테스트", df, "KOSPI", cfg)
        assert sigs == []

    def test_s1_before_window_anchors_s2_in_window(self):
        """S1이 config.start 이전 → S2가 config.start 이후면 수집."""
        df = _make_df(flat_days=80, spike_pct=0.08, spike_vol_mult=3.0,
                      post=[(10_800 * 0.90, int(100_000 * 3.0 * 0.40))])
        spike_date = df.index[-2].date()
        s2_date    = df.index[-1].date()
        # config.start = S2날짜 (S1은 하루 전이라 config.start 이전)
        cfg = _cfg(s2_date, s2_date + timedelta(days=1))
        sigs = _replay_stage2("T.KS", "테스트", df, "KOSPI", cfg)
        assert len(sigs) == 1
        assert sigs[0].signal_date == s2_date

    def test_deduplication_two_s1_same_s2_date(self):
        """동일 날짜로 수렴하는 두 S1 → S2 신호 1건만."""
        # 두 개의 S1을 내려면 flat→spike→flat→spike2→pullback 구조 필요
        # 여기서는 기능이 작동하는지만 검증 (신호 수 ≤ 1)
        spike_close = 10_800.0
        post = [
            (spike_close, 100_000),          # 조건 불충족(ratio 1.0)
            (spike_close * 1.08, int(100_000 * 3.0)),  # 두 번째 스파이크(+8%)
            (spike_close * 1.08 * 0.90, int(100_000 * 3.0 * 0.40)),  # S2 후보
        ]
        df = _make_df(flat_days=80, spike_pct=0.08, spike_vol_mult=3.0, post=post)
        spike_date = df.index[-(len(post) + 1)].date()
        cfg = _cfg(spike_date, spike_date + timedelta(days=20))
        sigs = _replay_stage2("T.KS", "테스트", df, "KOSPI", cfg)
        # 같은 날짜에 중복 없어야 함
        dates = [s.signal_date for s in sigs]
        assert len(dates) == len(set(dates))
