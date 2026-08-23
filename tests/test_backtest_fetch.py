"""analysis/backtest/fetch.py — 대량 수집 실패 감지 회귀 테스트.

2026-08-10 사고 재발 방지: yfinance rate-limit 등으로 대다수 티커의 OHLCV 수집이
실패해도 개별 실패는 logger.debug에만 남아, 며칠 뒤 신호수 비교로만 발견됐다.
_batch_fetch_ohlcv()가 실패율이 임계치를 넘으면 logger.error로 즉시 알리는지 검증한다.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from datetime import date

from analysis.backtest.fetch import _batch_fetch_ohlcv


def _fake_df() -> pd.DataFrame:
    return pd.DataFrame({"Open": [1], "High": [1], "Low": [1], "Close": [1], "Volume": [1]})


def test_mass_failure_logs_error(caplog):
    """대다수(90%+) 실패 시 logger.error로 실패율을 알린다."""
    tickers = [f"{i:06d}.KS" for i in range(20)]

    def _fake_fetch(ticker, _start, _end):
        # 2개만 성공, 나머지는 전부 실패(None) — 2026-08-10 사고와 동일한 패턴
        return _fake_df() if ticker in tickers[:2] else None

    with (
        patch("analysis.backtest.fetch._fetch_single_ohlcv", side_effect=_fake_fetch),
        caplog.at_level(logging.ERROR, logger="analysis.backtest.fetch"),
    ):
        result = _batch_fetch_ohlcv(tickers, date(2025, 1, 1), date(2025, 6, 1), workers=4)

    assert len(result) == 2
    assert any("실패율" in r.message for r in caplog.records)


def test_healthy_batch_does_not_log_error(caplog):
    """대부분 성공하는 정상적인 배치는 에러를 남기지 않는다."""
    tickers = [f"{i:06d}.KS" for i in range(20)]

    def _fake_fetch(ticker, _start, _end):
        # 1개만 실패(상장폐지 등) — 정상적인 개별종목 결측 수준
        return None if ticker == tickers[0] else _fake_df()

    with (
        patch("analysis.backtest.fetch._fetch_single_ohlcv", side_effect=_fake_fetch),
        caplog.at_level(logging.ERROR, logger="analysis.backtest.fetch"),
    ):
        result = _batch_fetch_ohlcv(tickers, date(2025, 1, 1), date(2025, 6, 1), workers=4)

    assert len(result) == 19
    assert not any("실패율" in r.message for r in caplog.records)


def test_empty_ticker_list_does_not_crash():
    """빈 티커 리스트는 0으로 나누기 없이 그냥 빈 dict를 반환한다."""
    result = _batch_fetch_ohlcv([], date(2025, 1, 1), date(2025, 6, 1), workers=4)
    assert result == {}


def test_just_below_threshold_does_not_log_error(caplog):
    """90% 문턱 바로 아래(85%)에서는 에러가 발생하지 않는다 — 경계값 회귀 방지
    (2026-08-22 review 발견: 기존엔 정확히 10%/90% 두 지점만 검증돼 임계치
    비교 연산자(>= vs >)의 회귀를 못 잡았음)."""
    tickers = [f"{i:06d}.KS" for i in range(20)]

    def _fake_fetch(ticker, _start, _end):
        return None if ticker in tickers[:17] else _fake_df()  # 17/20 = 85%

    with (
        patch("analysis.backtest.fetch._fetch_single_ohlcv", side_effect=_fake_fetch),
        caplog.at_level(logging.ERROR, logger="analysis.backtest.fetch"),
    ):
        _batch_fetch_ohlcv(tickers, date(2025, 1, 1), date(2025, 6, 1), workers=4)

    assert not any("실패율" in r.message for r in caplog.records)
