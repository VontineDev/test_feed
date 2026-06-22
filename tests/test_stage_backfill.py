"""
test_stage_backfill.py — stage_backfill 순수 로직 단위 테스트

네트워크/DB 없이 합성 데이터만 사용. DB·yfinance 의존 경로
(classify_week / existing_weeks / main)는 통합 테스트 대상으로 제외.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from jobs.stage_backfill import (
    iso_fridays,
    week_label,
    slice_until,
    build_row,
)


class TestIsoFridays:
    def test_single_week(self):
        # 2026-W17 월요일=2026-04-20, 금요일=2026-04-24
        out = iso_fridays(date(2026, 4, 20), date(2026, 4, 26))
        assert out == [date(2026, 4, 24)]

    def test_start_midweek_uses_that_week_friday(self):
        # 수요일에서 시작해도 그 주 금요일 포함
        out = iso_fridays(date(2026, 4, 22), date(2026, 4, 24))
        assert out == [date(2026, 4, 24)]

    def test_start_after_friday_skips_to_next_week(self):
        # 토요일에서 시작 → 그 주 금요일은 이미 지남 → 다음 주부터
        out = iso_fridays(date(2026, 4, 25), date(2026, 5, 8))
        assert out[0] == date(2026, 5, 1)  # 다음 주 금요일
        assert date(2026, 4, 24) not in out

    def test_multi_week_ascending_and_weekly_spacing(self):
        out = iso_fridays(date(2026, 4, 13), date(2026, 5, 3))
        assert out == [date(2026, 4, 17), date(2026, 4, 24), date(2026, 5, 1)]
        # 모두 금요일
        assert all(d.isoweekday() == 5 for d in out)

    def test_end_exclusive_of_later_friday(self):
        # end가 금요일 직전이면 그 주 금요일은 제외
        out = iso_fridays(date(2026, 4, 13), date(2026, 4, 23))
        assert out == [date(2026, 4, 17)]

    def test_empty_when_no_friday_in_range(self):
        # 같은 주 토~일만 → 금요일 없음
        out = iso_fridays(date(2026, 4, 25), date(2026, 4, 26))
        assert out == []


class TestWeekLabel:
    def test_format_matches_iso(self):
        assert week_label(date(2026, 4, 24)) == "2026-W17"

    def test_zero_padded_week(self):
        assert week_label(date(2026, 1, 9)) == "2026-W02"

    def test_iso_year_boundary(self):
        # 2025-12-29는 ISO 2026-W01 (ISO 연도가 달력 연도와 다름)
        assert week_label(date(2025, 12, 29)) == "2026-W01"


def _daily_df(dates, tz=None):
    idx = pd.to_datetime(list(dates))
    if tz is not None:
        idx = idx.tz_localize(tz)
    return pd.DataFrame(
        {
            "Open": range(len(idx)),
            "High": range(len(idx)),
            "Low": range(len(idx)),
            "Close": range(len(idx)),
            "Volume": range(len(idx)),
        },
        index=idx,
    )


class TestSliceUntil:
    def test_inclusive_of_as_of_date_naive(self):
        df = _daily_df(["2026-04-22", "2026-04-23", "2026-04-24", "2026-04-27"])
        out = slice_until(df, date(2026, 4, 24))
        assert len(out) == 3
        assert out.index[-1].date() == date(2026, 4, 24)

    def test_inclusive_of_as_of_date_tz_aware(self):
        df = _daily_df(["2026-04-22", "2026-04-23", "2026-04-24", "2026-04-27"], tz="UTC")
        out = slice_until(df, date(2026, 4, 24))
        assert len(out) == 3
        assert out.index[-1].date() == date(2026, 4, 24)

    def test_excludes_future_rows(self):
        df = _daily_df(["2026-04-24", "2026-05-01"])
        out = slice_until(df, date(2026, 4, 24))
        assert len(out) == 1

    def test_empty_when_all_future(self):
        df = _daily_df(["2026-05-01", "2026-05-08"])
        out = slice_until(df, date(2026, 4, 24))
        assert out.empty


class TestBuildRow:
    def _price(self):
        df = pd.DataFrame(
            {"High": [100.0, 110.0], "Close": [95.0, 105.0], "Volume": [1000, 2000]},
            index=pd.to_datetime(["2026-04-23", "2026-04-24"]),
        )
        return df

    def test_stage1_fills_s1_fields_from_last_bar(self):
        row = build_row("005930.KS", 1, False, self._price(), date(2026, 4, 24))
        assert row["ticker"] == "005930.KS"
        assert row["classified_date"] == date(2026, 4, 24)
        assert row["stage"] == 1
        assert row["s1_entry_date"] == date(2026, 4, 24)
        assert row["s1_high"] == 110.0
        assert row["s1_volume"] == 2000
        assert row["s1_txamt"] == int(2000 * 105.0)  # Volume × Close
        assert row["peakout_flag"] is False

    def test_stage2_leaves_s1_fields_none(self):
        row = build_row("005930.KS", 2, False, self._price(), date(2026, 4, 24))
        assert row["stage"] == 2
        assert row["s1_entry_date"] is None
        assert row["s1_high"] is None
        assert row["s1_volume"] is None
        assert row["s1_txamt"] is None

    def test_stage3_peakout_flag_propagates(self):
        row = build_row("005930.KS", 3, True, self._price(), date(2026, 4, 24))
        assert row["stage"] == 3
        assert row["peakout_flag"] is True
        assert row["s1_entry_date"] is None

    def test_stage1_empty_price_slice_no_crash(self):
        empty = pd.DataFrame(columns=["High", "Close", "Volume"])
        row = build_row("005930.KS", 1, False, empty, date(2026, 4, 24))
        assert row["stage"] == 1
        assert row["s1_high"] is None
        assert row["s1_volume"] is None


class TestBuildRowFlowScoring:
    """build_row의 foreign_chg_14d_pct/flow_score 계산 (classify_stage_v15 wiring)."""

    def _price(self):
        return pd.DataFrame(
            {"High": [100.0, 110.0], "Close": [95.0, 105.0], "Volume": [1000, 2000]},
            index=pd.to_datetime(["2026-04-23", "2026-04-24"]),
        )

    def test_no_flow_slice_defaults_to_zero_score_and_none_pct(self):
        row = build_row("005930.KS", 1, False, self._price(), date(2026, 4, 24))
        assert row["flow_score"] == 0.0
        assert row["foreign_chg_14d_pct"] is None

    def test_flow_slice_computes_score_and_pct(self):
        idx = pd.date_range("2026-04-10", periods=14, freq="D")
        flow = pd.DataFrame(
            {
                "foreign_net": [1000] * 14,
                "foreign_streak": [3] * 14,
                "inst_streak": [3] * 14,
            },
            index=idx,
        )
        row = build_row(
            "005930.KS", 1, False, self._price(), date(2026, 4, 24),
            flow_slice=flow, listed_shares=1_000_000,
        )
        # f_streak>=3 AND i_streak>=3 → base score 1.0 (no txamt bonus check here)
        assert row["flow_score"] == 1.0
        assert row["foreign_chg_14d_pct"] == 14 * 1000 / 1_000_000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
