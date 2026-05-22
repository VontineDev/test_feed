"""
test_screener_ohlcv_regression.py  —  Regression test for ISSUE-004
────────────────────────────────────────────────────────────
ISSUE-004: fetch_weekly_ohlcv() used yf.download() which has shared internal
           state (cache/session) in concurrent contexts. When SCREENER_WORKERS > 1,
           ThreadPoolExecutor threads mixed data between tickers — e.g., 대동(000490.KS)
           returned 태광산업's price (1,263,000 instead of ~10,190).

Fix: switched to yf.Ticker(ticker).history() which is instance-based and
     thread-safe. Each call creates a fresh Ticker object with no shared state.

Found by /investigate on 2026-04-18
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def _make_ohlcv_df(close: float, rows: int = 65) -> pd.DataFrame:
    """Return a minimal weekly OHLCV DataFrame with flat columns."""
    idx = pd.date_range("2023-01-02", periods=rows, freq="W-MON", tz="UTC")
    return pd.DataFrame(
        {
            "Open":   [close] * rows,
            "High":   [close] * rows,
            "Low":    [close] * rows,
            "Close":  [close] * rows,
            "Volume": [1_000_000] * rows,
        },
        index=idx,
    )


class TestFetchWeeklyOhlcvUsesTickerInstance:
    """Regression: ISSUE-004 — fetch_weekly_ohlcv must use yf.Ticker.history(), not yf.download()."""

    def test_uses_ticker_history_not_download(self):
        """Verify the implementation calls Ticker.history(), not yf.download()."""
        import analysis.chart_screener as chart_screener
        import inspect
        src = inspect.getsource(chart_screener.fetch_weekly_ohlcv)
        assert "yf.Ticker(" in src, "fetch_weekly_ohlcv must use yf.Ticker()"
        assert ".history(" in src, "fetch_weekly_ohlcv must call .history()"

    def test_concurrent_calls_return_per_ticker_data(self):
        """
        Regression: ISSUE-004 — concurrent fetch_weekly_ohlcv() calls must not
        mix data between tickers. Each ticker must get its own price.
        """
        from analysis.chart_screener import fetch_weekly_ohlcv

        ticker_prices = {
            "AAA.KS": 10_000.0,
            "BBB.KS": 500_000.0,
            "CCC.KS": 75_000.0,
            "DDD.KQ": 3_000.0,
        }

        def _fake_ticker_cls(ticker):
            mock = MagicMock()
            mock.history.return_value = _make_ohlcv_df(ticker_prices[ticker])
            return mock

        results = {}
        with patch("analysis.chart_screener.yf.Ticker", side_effect=_fake_ticker_cls):
            with ThreadPoolExecutor(max_workers=4) as ex:
                futures = {ex.submit(fetch_weekly_ohlcv, t): t for t in ticker_prices}
                for f in as_completed(futures):
                    ticker = futures[f]
                    df = f.result()
                    assert df is not None, f"{ticker} returned None"
                    results[ticker] = df["Close"].iloc[-1]

        for ticker, expected in ticker_prices.items():
            actual = results[ticker]
            assert actual == expected, (
                f"Data contamination: {ticker} expected {expected:,}, got {actual:,}"
            )

    def test_returns_flat_columns(self):
        """Verify flat column names after history() call."""
        from analysis.chart_screener import fetch_weekly_ohlcv

        mock_ticker = MagicMock()
        mock_ticker.history.return_value = _make_ohlcv_df(50_000.0)
        with patch("analysis.chart_screener.yf.Ticker", return_value=mock_ticker):
            df = fetch_weekly_ohlcv("TEST.KS")

        assert df is not None
        assert list(df.columns) == ["Open", "High", "Low", "Close", "Volume"]
        assert not isinstance(df.columns, pd.MultiIndex)

    def test_returns_none_when_insufficient_rows(self):
        """Fewer than 60 valid Close rows → None."""
        from analysis.chart_screener import fetch_weekly_ohlcv

        mock_ticker = MagicMock()
        mock_ticker.history.return_value = _make_ohlcv_df(50_000.0, rows=30)
        with patch("analysis.chart_screener.yf.Ticker", return_value=mock_ticker):
            result = fetch_weekly_ohlcv("SPARSE.KS")

        assert result is None

    def test_returns_none_on_empty_dataframe(self):
        """Empty DataFrame from history() → None."""
        from analysis.chart_screener import fetch_weekly_ohlcv

        mock_ticker = MagicMock()
        mock_ticker.history.return_value = pd.DataFrame()
        with patch("analysis.chart_screener.yf.Ticker", return_value=mock_ticker):
            result = fetch_weekly_ohlcv("EMPTY.KS")

        assert result is None
