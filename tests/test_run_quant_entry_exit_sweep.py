"""scripts/run_quant_entry_exit_sweep.py의 순수 헬퍼(build_fundamental_thresholds) 단위 테스트."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from run_quant_entry_exit_sweep import build_fundamental_thresholds  # noqa: E402


class TestBuildFundamentalThresholds:
    def test_per_mode_uses_per_max_only(self):
        th = build_fundamental_thresholds("per", 18.0)
        assert th.per_min == 0.0
        assert th.per_max == 18.0
        assert th.pbr_min is None
        assert th.pbr_max is None
        assert th.roe_min is None
        assert th.debt_ratio_max is None
        assert th.revenue_growth_min is None

    def test_pbr_mode_ignores_per_max_and_uses_fixed_band(self):
        th = build_fundamental_thresholds("pbr", 999.0)  # per_max는 pbr 모드에서 무시되어야 함
        assert th.pbr_min == 0.2
        assert th.pbr_max == 1.0
        assert th.per_min is None
        assert th.per_max is None
        assert th.roe_min is None
        assert th.debt_ratio_max is None
        assert th.revenue_growth_min is None

    def test_unknown_mode_raises(self):
        with pytest.raises(ValueError):
            build_fundamental_thresholds("bogus", 18.0)
