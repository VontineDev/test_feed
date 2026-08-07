"""scripts/run_quant_scenario_variants.py의 순수 헬퍼(build_variants) 단위 테스트."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from run_quant_scenario_variants import build_variants  # noqa: E402


class TestBuildVariants:
    def test_returns_six_variants(self):
        variants = build_variants()
        assert len(variants) == 6

    def test_names_are_unique(self):
        variants = build_variants()
        names = [v["name"] for v in variants]
        assert len(names) == len(set(names))

    def test_entry_keys_reuse_existing_scenarios_only(self):
        # 새 진입/청산 조건을 만들지 않는다 — 기존 SCENARIO1/SCENARIO2 기술만 재사용.
        variants = build_variants()
        for v in variants:
            assert v["entry_key"] in {"SCENARIO1", "SCENARIO2"}

    def test_scenario2_based_variants_use_mktcap_universe(self):
        variants = build_variants()
        for v in variants:
            if v["entry_key"] == "SCENARIO2":
                assert v["universe_mode"] == "mktcap_top200"

    def test_scenario1_based_variants_use_txamt_universe(self):
        variants = build_variants()
        for v in variants:
            if v["entry_key"] == "SCENARIO1":
                assert v["universe_mode"] == "txamt_top20"

    def test_loosen_universe_variant_widens_pct_beyond_default(self):
        variants = build_variants()
        v = next(x for x in variants if x["name"] == "SCENARIO6_loosen_universe")
        assert v["universe_kwargs"]["pct"] > 0.20

    def test_each_variant_has_thresholds_and_note(self):
        from analysis.fundamentals import RatioThresholds

        variants = build_variants()
        for v in variants:
            assert isinstance(v["thresholds"], RatioThresholds)
            assert v["note"]
