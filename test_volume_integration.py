"""
test_volume_integration.py  —  Regression tests for volume integration
───────────────────────────────────────────────────────────────────────
Covers:
  - compute_daily_change  (batch_run.py) — edge cases
  - _send_plain           (telegram_bot.py) — no MarkdownV2 parse_mode
  - _handle_volume        (telegram_bot.py) — no args, empty data, truncation
  - run_batch             (batch_run.py)    — return structure

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


# ── compute_daily_change ─────────────────────────────────────────

class TestComputeDailyChange:
    def test_empty_df_returns_zero(self):
        from batch_run import compute_daily_change
        assert compute_daily_change(pd.DataFrame()) == 0.0

    def test_single_day_returns_zero(self):
        from batch_run import compute_daily_change
        df = _make_5m_df({"2026-04-08": [100.0, 101.0, 102.0]})
        assert compute_daily_change(df) == 0.0

    def test_two_days_positive_change(self):
        from batch_run import compute_daily_change
        # yesterday last close = 100, today last close = 110  → +10%
        df = _make_5m_df({
            "2026-04-07": [98.0, 99.0, 100.0],
            "2026-04-08": [102.0, 108.0, 110.0],
        })
        result = compute_daily_change(df)
        assert abs(result - 10.0) < 0.01

    def test_two_days_negative_change(self):
        from batch_run import compute_daily_change
        # yesterday last close = 100, today last close = 90  → -10%
        df = _make_5m_df({
            "2026-04-07": [100.0, 100.0, 100.0],
            "2026-04-08": [95.0, 92.0, 90.0],
        })
        result = compute_daily_change(df)
        assert abs(result - (-10.0)) < 0.01

    def test_zero_yesterday_close_returns_zero(self):
        from batch_run import compute_daily_change
        df = _make_5m_df({
            "2026-04-07": [0.0, 0.0, 0.0],
            "2026-04-08": [10.0, 11.0, 12.0],
        })
        assert compute_daily_change(df) == 0.0

    def test_five_days_uses_last_two(self):
        from batch_run import compute_daily_change
        # 5 days — should compare day5 vs day4 only
        days = {
            "2026-04-01": [50.0],
            "2026-04-02": [60.0],
            "2026-04-03": [70.0],
            "2026-04-07": [80.0],   # yesterday last = 80
            "2026-04-08": [88.0],   # today last = 88  → +10%
        }
        df = _make_5m_df(days)
        result = compute_daily_change(df)
        assert abs(result - 10.0) < 0.01


# ── _send_plain ──────────────────────────────────────────────────

class TestSendPlain:
    @pytest.mark.asyncio
    async def test_no_parse_mode_in_payload(self):
        """_send_plain must NOT include parse_mode (box-drawing chars break MarkdownV2)."""
        os.environ.setdefault("TELEGRAM_TOKEN", "test_token")
        from telegram_bot import _send_plain

        http = AsyncMock()
        http.post = AsyncMock(return_value=MagicMock(json=lambda: {"ok": True}))
        await _send_plain(http, "99999", "report with █▓ chars")

        payload = http.post.call_args[1]["json"]
        assert "parse_mode" not in payload

    @pytest.mark.asyncio
    async def test_text_sent_verbatim(self):
        """_send_plain sends text exactly as-is."""
        os.environ.setdefault("TELEGRAM_TOKEN", "test_token")
        from telegram_bot import _send_plain

        http = AsyncMock()
        http.post = AsyncMock(return_value=MagicMock(json=lambda: {"ok": True}))
        await _send_plain(http, "99999", "hello world")

        payload = http.post.call_args[1]["json"]
        assert payload["text"] == "hello world"


# ── _handle_volume ───────────────────────────────────────────────

class TestHandleVolume:
    @pytest.mark.asyncio
    async def test_no_args_sends_usage_message(self):
        """/volume with no arguments → usage hint, no data fetch."""
        os.environ.setdefault("TELEGRAM_TOKEN", "test_token")
        from telegram_bot import _handle_volume

        http = AsyncMock()
        http.post = AsyncMock(return_value=MagicMock(json=lambda: {"ok": True}))

        with patch("volume_pattern.fetch_data") as mock_fetch:
            await _handle_volume(http, "99999", [])
            mock_fetch.assert_not_called()

        payload = http.post.call_args[1]["json"]
        assert "사용법" in payload["text"]

    @pytest.mark.asyncio
    async def test_empty_data_sends_error_message(self):
        """/volume <ticker> when yfinance returns nothing → informative error."""
        os.environ.setdefault("TELEGRAM_TOKEN", "test_token")
        from telegram_bot import _handle_volume

        http = AsyncMock()
        http.post = AsyncMock(return_value=MagicMock(json=lambda: {"ok": True}))

        with patch("volume_pattern.fetch_data", return_value=(pd.DataFrame(), "", "yfinance")):
            await _handle_volume(http, "99999", ["XXXX_FAKE"])

        # Two calls: "조회 중..." + "가져올 수 없습니다"
        assert http.post.call_count == 2
        last_payload = http.post.call_args_list[-1][1]["json"]
        assert "가져올 수 없습니다" in last_payload["text"]

    @pytest.mark.asyncio
    async def test_long_report_truncated_to_4090(self):
        """/volume report longer than 4090 chars is truncated before sending."""
        os.environ.setdefault("TELEGRAM_TOKEN", "test_token")
        from telegram_bot import _handle_volume

        http = AsyncMock()
        http.post = AsyncMock(return_value=MagicMock(json=lambda: {"ok": True}))

        fake_df = _make_5m_df({"2026-04-07": [100.0], "2026-04-08": [105.0]})
        long_report = "X" * 5000  # exceeds 4090

        with patch("volume_pattern.fetch_data", return_value=(fake_df, "Fake Co", "yfinance")), \
             patch("volume_pattern.build_report", return_value=long_report):
            await _handle_volume(http, "99999", ["삼성전자"])

        last_payload = http.post.call_args_list[-1][1]["json"]
        assert len(last_payload["text"]) <= 4096
        assert last_payload["text"].endswith("...(생략)")

    @pytest.mark.asyncio
    async def test_valid_report_sent_without_parse_mode(self):
        """/volume with valid data → plain text (no MarkdownV2) sent."""
        os.environ.setdefault("TELEGRAM_TOKEN", "test_token")
        from telegram_bot import _handle_volume

        http = AsyncMock()
        http.post = AsyncMock(return_value=MagicMock(json=lambda: {"ok": True}))

        fake_df = _make_5m_df({"2026-04-07": [100.0], "2026-04-08": [105.0]})
        with patch("volume_pattern.fetch_data", return_value=(fake_df, "Samsung", "yfinance")), \
             patch("volume_pattern.build_report", return_value="report text with █ bars"):
            await _handle_volume(http, "99999", ["삼성전자"])

        last_payload = http.post.call_args_list[-1][1]["json"]
        assert "parse_mode" not in last_payload


# ── run_batch return structure ───────────────────────────────────

class TestRunBatch:
    def test_returns_expected_keys(self, tmp_path):
        """run_batch() always returns the documented keys regardless of data."""
        from batch_run import run_batch

        fake_df = _make_5m_df({"2026-04-07": [100.0], "2026-04-08": [105.0]})

        with patch("batch_run.batch_download") as mock_dl, \
             patch("batch_run.build_report", return_value="report"), \
             patch("batch_run.save_report"), \
             patch("batch_run.make_html_report", return_value="<html/>"), \
             patch("batch_run.STOCKS", [("삼성전자", "005930")]):
            mock_dl.return_value = {"005930.KS": fake_df}
            result = run_batch(output_dir=str(tmp_path))

        assert set(result.keys()) == {"html_path", "total", "success", "failed", "summaries"}
        assert result["total"] == 1
        assert result["success"] == 1
        assert result["failed"] == []

    def test_failed_stocks_counted_correctly(self, tmp_path):
        """Stocks with no data are counted in failed, not success."""
        from batch_run import run_batch

        with patch("batch_run.batch_download") as mock_dl, \
             patch("batch_run.make_html_report", return_value="<html/>"), \
             patch("batch_run.STOCKS", [("삼성전자", "005930"), ("가짜종목", "999999")]):
            mock_dl.return_value = {
                "005930.KS": _make_5m_df({"2026-04-07": [100.0], "2026-04-08": [105.0]}),
                "999999.KS": pd.DataFrame(),  # no data
            }
            with patch("batch_run.build_report", return_value="report"), \
                 patch("batch_run.save_report"):
                result = run_batch(output_dir=str(tmp_path))

        assert result["total"] == 2
        assert result["success"] == 1
        assert "가짜종목" in result["failed"]
