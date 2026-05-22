"""
test_volume_integration.py  —  Regression tests for volume integration
───────────────────────────────────────────────────────────────────────
Covers:
  - _send_plain           (telegram_bot.py) — no MarkdownV2 parse_mode
  - fetch_data tz         (volume_pattern.py) — US market uses America/New_York

Regression: feat: integrate volume_pattern analysis into Telegram bot and scheduler
Found by /qa on 2026-04-08
Report: .gstack/qa-reports/qa-report-volume-integration-2026-04-08.md
"""

from __future__ import annotations

import os
import pandas as pd
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ── helpers ──────────────────────────────────────────────────────

def _make_5m_df(days: dict[str, list[float]]) -> pd.DataFrame:
    """Build a fake 5-minute OHLCV DataFrame in Asia/Seoul timezone.

    days = {"2026-04-07": [100.0, 101.0, 102.0], ...}
    Each value is used as both Open, High, Low, Close.
    """
    rows = []
    for date_str, closes in days.items():
        base = pd.Timestamp(date_str + " 09:00", tz="Asia/Seoul")
        for i, c in enumerate(closes):
            ts = base + pd.Timedelta(minutes=i * 5)
            rows.append({"Close": c, "Volume": 1_000})
    df = pd.DataFrame(rows)
    df.index = pd.DatetimeIndex([r["Close"] for r in rows])  # placeholder
    # Rebuild with proper DatetimeIndex
    idx = []
    for date_str, closes in days.items():
        base = pd.Timestamp(date_str + " 09:00", tz="Asia/Seoul")
        for i in range(len(closes)):
            idx.append(base + pd.Timedelta(minutes=i * 5))
    closes_flat = [c for closes in days.values() for c in closes]
    df = pd.DataFrame(
        {"Close": closes_flat, "Volume": [1_000] * len(closes_flat)},
        index=pd.DatetimeIndex(idx),
    )
    return df


# ── _send_plain ──────────────────────────────────────────────────

class TestSendPlain:
    @pytest.mark.asyncio
    async def test_no_parse_mode_in_payload(self):
        """_send_plain must NOT include parse_mode (box-drawing chars break MarkdownV2)."""
        os.environ.setdefault("TELEGRAM_TOKEN", "test_token")
        from telegram.telegram_bot import _send_plain

        http = AsyncMock()
        http.post = AsyncMock(return_value=MagicMock(json=lambda: {"ok": True}))
        await _send_plain(http, "99999", "report with █▓ chars")

        payload = http.post.call_args[1]["json"]
        assert "parse_mode" not in payload

    @pytest.mark.asyncio
    async def test_text_sent_verbatim(self):
        """_send_plain sends text exactly as-is."""
        os.environ.setdefault("TELEGRAM_TOKEN", "test_token")
        from telegram.telegram_bot import _send_plain

        http = AsyncMock()
        http.post = AsyncMock(return_value=MagicMock(json=lambda: {"ok": True}))
        await _send_plain(http, "99999", "hello world")

        payload = http.post.call_args[1]["json"]
        assert payload["text"] == "hello world"


# ── fetch_data timezone regression (ISSUE-001) ───────────────────

class TestFetchDataTimezone:
    """Regression: volume_pattern.fetch_data had inverted tz condition.
    US market data must be converted to Asia/Seoul so build_report can label
    the left column as Korean time and parenthetical as US Eastern time.
    See _format_hour docstring: '미국 주식이면 한국시간(미국시간) 형태'."""

    def _make_ny_df(self) -> pd.DataFrame:
        """5-minute DataFrame already in America/New_York timezone."""
        idx = pd.date_range(
            "2026-04-07 09:30", periods=3, freq="5min", tz="America/New_York"
        )
        return pd.DataFrame(
            {"Open": 150.0, "High": 151.0, "Low": 149.0, "Close": 150.5, "Volume": 1000},
            index=idx,
        )

    def test_us_market_gets_seoul_timezone(self):
        """fetch_data for US stocks must convert index to Asia/Seoul.
        build_report labels the left column as Korean time and the parenthetical
        as US Eastern time. If data is in Eastern time, _kr_to_us_hour receives
        an Eastern hour instead of a Korean hour, producing wrong labels like
        '09:30(20:30)' instead of the correct '22:30(09:30)'.

        Regression: ISSUE-TZ — /volume MU showed 09:30(20:30) instead of 22:30(09:30)
        Found by /investigate on 2026-04-14
        """
        from analysis.volume_pattern import fetch_data

        ny_df = self._make_ny_df()

        with patch("analysis.volume_pattern.yf.Ticker") as mock_ticker, \
             patch("analysis.volume_pattern._load_from_db", return_value=pd.DataFrame()), \
             patch("analysis.volume_pattern._is_db_fresh", return_value=False):
            mock_t = MagicMock()
            mock_t.info = {}
            mock_t.history.return_value = ny_df
            mock_ticker.return_value = mock_t

            df, _, _ = fetch_data("AAPL", "US")

        assert df.index.tzinfo is not None
        tz_name = str(df.index.tz)
        assert "Seoul" in tz_name, (
            f"Expected Asia/Seoul timezone for US stocks (for Korean-time display), got: {tz_name}"
        )

    def test_us_report_time_label_shows_korean_time_first(self):
        """build_report for a US stock must show Korean time on the left and
        US Eastern time in parentheses.
        NYSE opens at 9:30 AM ET = 22:30 KST (EDT). The report must show
        '22:30(09:30)', not '09:30(20:30)'.

        Regression: ISSUE-TZ — /volume MU showed 09:30(20:30) instead of 22:30(09:30)
        Found by /investigate on 2026-04-14
        """
        from analysis.volume_pattern import fetch_data, build_report

        # One trading day: only the 9:30 AM ET bar (high volume, NYSE open)
        # In KST: 9:30 AM ET (EDT=UTC-4) = 13:30 UTC = 22:30 KST
        ny_df = pd.DataFrame(
            {"Open": 150.0, "High": 151.0, "Low": 149.0, "Close": 150.5, "Volume": 5_000_000},
            index=pd.date_range("2026-04-07 09:30", periods=1, freq="5min", tz="America/New_York"),
        )

        with patch("analysis.volume_pattern.yf.Ticker") as mock_ticker, \
             patch("analysis.volume_pattern._load_from_db", return_value=pd.DataFrame()), \
             patch("analysis.volume_pattern._is_db_fresh", return_value=False):
            mock_t = MagicMock()
            mock_t.info = {}
            mock_t.history.return_value = ny_df
            mock_ticker.return_value = mock_t

            df, full_name, source = fetch_data("MU", "US")

        report = build_report(df, "MU", "MU", "Micron Technology", "US", source)

        # 9:30 AM ET = 22:30 KST — report must show "22:30(09:30)", not "09:30(20:30)"
        assert "22:30(09:30)" in report, (
            f"Expected '22:30(09:30)' in report (Korean time first, US time in parens).\n"
            f"Got report snippet:\n{report[:500]}"
        )

    def test_kr_market_gets_seoul_timezone(self):
        """fetch_data for KR stocks must convert index to Asia/Seoul."""
        from analysis.volume_pattern import fetch_data

        kr_df = pd.DataFrame(
            {"Open": 70000.0, "High": 71000.0, "Low": 69000.0, "Close": 70500.0, "Volume": 5000},
            index=pd.date_range("2026-04-07 09:00", periods=3, freq="5min", tz="Asia/Seoul"),
        )

        with patch("analysis.volume_pattern.yf.Ticker") as mock_ticker, \
             patch("analysis.volume_pattern._load_from_db", return_value=pd.DataFrame()), \
             patch("analysis.volume_pattern._is_db_fresh", return_value=False):
            mock_t = MagicMock()
            mock_t.info = {}
            mock_t.history.return_value = kr_df
            mock_ticker.return_value = mock_t

            df, _, _ = fetch_data("005930.KS", "KR")

        assert df.index.tzinfo is not None
        tz_name = str(df.index.tz)
        assert "Seoul" in tz_name, (
            f"Expected Asia/Seoul timezone for KR stocks, got: {tz_name}"
        )


# ── _load_from_db connection efficiency (ISSUE-POOL) ────────────

class TestLoadFromDbNoPool:
    """Regression: _load_from_db used db.create_pool(min_size=2) on every call.
    Each /volume invocation opened 2 Supabase connections, ran init_db (8+ DDL
    statements), then closed the pool — all for a single SELECT.

    Fix: use asyncpg.connect() directly (single connection, no pool overhead).
    Verified by: db.create_pool and db.init_db must NOT be called.

    Found by /investigate on 2026-04-14
    """

    def _make_mock_conn(self) -> AsyncMock:
        conn = AsyncMock()
        conn.fetch.return_value = []
        conn.close.return_value = None
        return conn

    def test_load_from_db_does_not_call_create_pool(self):
        """_load_from_db must not create a connection pool.
        Single asyncpg.connect() suffices for a read-only cache check."""
        from analysis.volume_pattern import _load_from_db

        mock_conn = self._make_mock_conn()

        with patch("asyncpg.connect", new_callable=AsyncMock) as mock_connect, \
             patch("core.db.get_dsn", return_value="postgresql://test"), \
             patch("core.db.create_pool") as mock_create_pool:
            mock_connect.return_value = mock_conn

            result = _load_from_db("AAPL")

        mock_create_pool.assert_not_called()
        assert isinstance(result, pd.DataFrame)

    def test_load_from_db_does_not_call_init_db(self):
        """_load_from_db must not run init_db (which executes 8+ DDL statements)."""
        from analysis.volume_pattern import _load_from_db

        mock_conn = self._make_mock_conn()

        with patch("asyncpg.connect", new_callable=AsyncMock) as mock_connect, \
             patch("core.db.get_dsn", return_value="postgresql://test"), \
             patch("core.db.init_db") as mock_init_db:
            mock_connect.return_value = mock_conn

            _load_from_db("005930.KS")

        mock_init_db.assert_not_called()
