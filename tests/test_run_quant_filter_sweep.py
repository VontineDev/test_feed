"""scripts/run_quant_filter_sweep.py 순수 헬퍼 단위 테스트 (_rank_by_market_cap)."""
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from run_quant_filter_sweep import _rank_by_market_cap  # noqa: E402


def _df(closes: list[float]) -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=len(closes), freq="D")
    return pd.DataFrame({
        "Open": closes, "High": closes, "Low": closes, "Close": closes,
        "Volume": [1_000] * len(closes),
    }, index=idx)


class TestRankByMarketCap:
    def test_ranks_descending_by_market_cap(self):
        ohlcv_map = {
            "A.KS": _df([100.0] * 5),   # 시총 = 100 * 1000 = 100,000
            "B.KS": _df([100.0] * 5),   # 시총 = 100 * 10000 = 1,000,000 (최고)
            "C.KS": _df([100.0] * 5),   # 시총 = 100 * 100 = 10,000 (최저)
        }
        listed_shares = {"A.KS": 1_000, "B.KS": 10_000, "C.KS": 100}
        ranked = _rank_by_market_cap(ohlcv_map, listed_shares, date(2025, 1, 1), date(2025, 1, 5))
        tickers = [t for t, _ in ranked]
        assert tickers == ["B.KS", "A.KS", "C.KS"]

    def test_skips_tickers_without_listed_shares(self):
        ohlcv_map = {"A.KS": _df([100.0] * 5)}
        ranked = _rank_by_market_cap(ohlcv_map, {}, date(2025, 1, 1), date(2025, 1, 5))
        assert ranked == []

    def test_skips_empty_window(self):
        ohlcv_map = {"A.KS": _df([100.0] * 5)}  # 2025-01-01~01-05만 존재
        listed_shares = {"A.KS": 1_000}
        ranked = _rank_by_market_cap(
            ohlcv_map, listed_shares, date(2026, 1, 1), date(2026, 1, 5)  # 겹치는 구간 없음
        )
        assert ranked == []

    def test_empty_ohlcv_map_returns_empty_list(self):
        assert _rank_by_market_cap({}, {}, date(2025, 1, 1), date(2025, 1, 5)) == []
