"""scripts/run_quant_backtest.py 순수 헬퍼 단위 테스트 (_select_universe, _pct)."""
from datetime import date

import pandas as pd
import pytest

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from run_quant_backtest import _pct, _select_universe  # noqa: E402


def _df(closes, volumes, start="2025-01-01"):
    idx = pd.date_range(start, periods=len(closes), freq="D")
    return pd.DataFrame({
        "Open": closes, "High": closes, "Low": closes, "Close": closes, "Volume": volumes,
    }, index=idx)


class TestPct:
    def test_formats_positive(self):
        assert _pct(0.153) == "+15.3%"

    def test_formats_negative(self):
        assert _pct(-0.05) == "-5.0%"

    def test_none_returns_na(self):
        assert _pct(None) == "N/A"


class TestSelectUniverse:
    def test_txamt_top20_picks_highest_avg_turnover(self):
        ohlcv_map = {
            "A.KS": _df([100.0] * 10, [1_000_000] * 10),   # 거래대금 = 1e8/일
            "B.KS": _df([100.0] * 10, [10_000_000] * 10),  # 거래대금 = 1e9/일 (최고)
            "C.KS": _df([100.0] * 10, [100_000] * 10),     # 거래대금 = 1e7/일 (최저)
            "D.KS": _df([100.0] * 10, [500_000] * 10),
            "E.KS": _df([100.0] * 10, [200_000] * 10),
        }
        universe = _select_universe(
            ohlcv_map, {}, date(2025, 1, 1), date(2025, 1, 10), "txamt_top20"
        )
        # 5종목의 20% = 1종목 → 최고 거래대금 B만 포함
        assert universe == {"B.KS"}

    def test_mktcap_top200_uses_listed_shares(self):
        ohlcv_map = {
            "A.KS": _df([100.0] * 5, [1_000] * 5),
            "B.KS": _df([100.0] * 5, [1_000] * 5),
        }
        listed_shares = {"A.KS": 1_000_000, "B.KS": 10_000_000}  # B가 시총 10배
        universe = _select_universe(
            ohlcv_map, listed_shares, date(2025, 1, 1), date(2025, 1, 5), "mktcap_top200"
        )
        assert universe == {"A.KS", "B.KS"}  # 둘 다 200위 안에 듬

    def test_mktcap_skips_tickers_without_listed_shares(self):
        ohlcv_map = {"A.KS": _df([100.0] * 5, [1_000] * 5)}
        universe = _select_universe(
            ohlcv_map, {}, date(2025, 1, 1), date(2025, 1, 5), "mktcap_top200"
        )
        assert universe == set()

    def test_empty_window_excludes_ticker(self):
        ohlcv_map = {"A.KS": _df([100.0] * 5, [1_000] * 5, start="2020-01-01")}
        universe = _select_universe(
            ohlcv_map, {}, date(2025, 1, 1), date(2025, 1, 5), "txamt_top20"
        )
        assert universe == set()

    def test_unknown_mode_raises(self):
        ohlcv_map = {"A.KS": _df([100.0] * 5, [1_000] * 5)}
        with pytest.raises(ValueError):
            _select_universe(ohlcv_map, {}, date(2025, 1, 1), date(2025, 1, 5), "bogus")
