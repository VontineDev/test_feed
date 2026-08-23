"""core/ohlcv_cache.py::batch_fetch_cached — 대량 수집 실패 감지 회귀 테스트.

2026-08-10 사고의 실제 경로: scripts/run_cross_combo_backtest.py 등 dsn이 설정된
모든 백테스트 경로는 analysis/backtest/fetch.py::_batch_fetch_ohlcv가 아니라
이 batch_fetch_cached(캐시 미스만 yfinance로 수집)를 탄다. _batch_fetch_ohlcv에만
실패율 가드를 추가하면 dsn 미설정 폴백 경로만 보호되고 실제 사고 경로는 그대로
무방비였다(2026-08-22 adversarial review 발견) — 그래서 여기도 동일한 가드를 추가.
"""
from __future__ import annotations

import logging
import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.ohlcv_cache import batch_fetch_cached


def _fake_df() -> pd.DataFrame:
    return pd.DataFrame({"Open": [1], "High": [1], "Low": [1], "Close": [1], "Volume": [1]})


def _no_cache_hits(*_args, **_kwargs):
    """_get_coverage를 빈 dict로 만들어 전부 미스(=yfinance 수집 대상) 처리."""
    return {}


def test_mass_miss_failure_logs_error(caplog):
    """캐시 미스 대다수(90%+)가 yfinance 수집에 실패하면 logger.error로 알린다."""
    tickers = [(f"{i:06d}.KS", "KOSPI") for i in range(20)]
    symbols = [sym for sym, _ in tickers]

    def _fake_fetch(sym, _start, _end):
        return _fake_df() if sym in symbols[:2] else None

    with (
        patch("core.ohlcv_cache._get_coverage", side_effect=_no_cache_hits),
        patch("core.ohlcv_cache._save_df"),
        caplog.at_level(logging.ERROR, logger="core.ohlcv_cache"),
    ):
        result = batch_fetch_cached(
            tickers, date(2025, 1, 1), date(2025, 6, 1), workers=4,
            dsn="dummy", fetch_fn=_fake_fetch,
        )

    assert len(result) == 2
    assert any("실패율" in r.message for r in caplog.records)


def test_healthy_miss_batch_does_not_log_error(caplog):
    """대부분 성공하는 정상 배치는 에러를 남기지 않는다."""
    tickers = [(f"{i:06d}.KS", "KOSPI") for i in range(20)]
    symbols = [sym for sym, _ in tickers]

    def _fake_fetch(sym, _start, _end):
        return None if sym == symbols[0] else _fake_df()

    with (
        patch("core.ohlcv_cache._get_coverage", side_effect=_no_cache_hits),
        patch("core.ohlcv_cache._save_df"),
        caplog.at_level(logging.ERROR, logger="core.ohlcv_cache"),
    ):
        result = batch_fetch_cached(
            tickers, date(2025, 1, 1), date(2025, 6, 1), workers=4,
            dsn="dummy", fetch_fn=_fake_fetch,
        )

    assert len(result) == 19
    assert not any("실패율" in r.message for r in caplog.records)


def test_all_cache_hits_skips_miss_ratio_check_entirely():
    """캐시 히트로 전부 채워지면(misses=[]) 미스 실패율 계산 자체가 실행되지
    않는다 — 0으로 나누기 방지."""
    tickers = [(f"{i:06d}.KS", "KOSPI") for i in range(3)]
    symbols = [sym for sym, _ in tickers]

    def _full_coverage(*_args, **_kwargs):
        return {sym: True for sym in symbols}

    with (
        patch("core.ohlcv_cache._get_coverage", side_effect=_full_coverage),
        patch("core.ohlcv_cache._classify_coverage", return_value=(symbols, [])),
        patch("core.ohlcv_cache._load_df_bulk", return_value={sym: _fake_df() for sym in symbols}),
    ):
        result = batch_fetch_cached(
            tickers, date(2025, 1, 1), date(2025, 6, 1), workers=4,
            dsn="dummy", fetch_fn=lambda *a, **k: _fake_df(),
        )

    assert len(result) == 3
