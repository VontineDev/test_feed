"""
test_kiwoom_ticker_convert.py — kiwoom→yfinance 티커 변환 특성화 테스트

리팩토링 전: routers_macro._kiwoom_to_yfinance,
kiwoom_aftermarket_sync._raw_to_yf/_to_snap_ticker 3개 구현의 현재 동작을 고정.
core/tickers.py 통합 후에도 이 테스트가 그대로 통과해야 한다(동작 무변화 게이트).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_BACKEND = Path(__file__).parent.parent / "dashboard" / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from unittest.mock import MagicMock

from routers_macro import _kiwoom_to_yfinance
from data.kiwoom_aftermarket_sync import _raw_to_yf, _enrich_with_reg_value, AftermarketRecord
from datetime import date


# ── _kiwoom_to_yfinance (market 파라미터 있음) ──────────────────

@pytest.mark.parametrize("raw,market,expected", [
    ("005930.KS", "", "005930.KS"),       # 이미 yfinance 포맷 → 그대로
    ("005930_AL", "", "005930.KS"),       # KOSPI 접미사
    ("035720_AQ", "", "035720.KQ"),       # KOSDAQ 접미사
    ("005930", "KOSPI", "005930.KS"),     # 접미사 없음 + market 폴백
    ("035720", "KOSDAQ", "035720.KQ"),
    ("005930", "", None),                 # 접미사도 market도 없음 → None
    ("005930", "UNKNOWN", None),
])
def test_kiwoom_to_yfinance(raw, market, expected):
    assert _kiwoom_to_yfinance(raw, market) == expected


# ── _raw_to_yf (market 파라미터 없음, 매치 실패 시 None) ────────

@pytest.mark.parametrize("raw,expected", [
    ("000660.KS", "000660.KS"),
    ("000660_AL", "000660.KS"),
    ("035720_AQ", "035720.KQ"),
    ("000660", None),   # 접미사 없음, market 없음 → None (quirk)
])
def test_raw_to_yf(raw, expected):
    assert _raw_to_yf(raw) == expected


# ── _to_snap_ticker (매치 실패 시 raw 그대로 — None 아님) ───────
# 이 함수는 _enrich_with_reg_value 내부에 nested되어 있어 직접 import 불가 —
# client.fetch_top_volume()을 mock해 간접적으로 동작을 고정한다.

def _make_record(ticker: str) -> AftermarketRecord:
    return AftermarketRecord(
        trade_date=date(2026, 7, 20), ticker=ticker,
        reg_close=None, after_close=None, after_volume=None,
        after_value=None, after_chg_pct=None,
    )


@pytest.mark.parametrize("raw_ticker,record_ticker,should_match", [
    ("000660_AL", "000660.KS", True),    # _AL 접미사 변환 후 매치
    ("035720_AQ", "035720.KQ", True),    # _AQ 접미사 변환 후 매치
    ("000660.KS", "000660.KS", True),    # 이미 yfinance 포맷 → 그대로 매치
    ("000660", "000660", True),          # 접미사 없음 → raw 그대로 매치 (quirk, None 아님)
])
def test_to_snap_ticker_via_enrich(raw_ticker, record_ticker, should_match):
    client = MagicMock()
    client.fetch_top_volume.return_value = [{"ticker": raw_ticker, "amount": 12345}]
    records = [_make_record(record_ticker)]

    _enrich_with_reg_value(client, records)

    assert (records[0].reg_value == 12345) == should_match
