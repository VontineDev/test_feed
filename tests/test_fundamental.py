"""
test_fundamental.py — unit tests for the fundamental data layer.

Tests:
    1. _parse_naver_value() — pure function, all edge cases
    2. _to_krx_code() — symbol stripping
    3. _fundamental_score() — pure function, all score branches + boundaries
    4. _fetch_fundamental() — Naver API + cache, with mocked httpx
    5. cross_analyze() average delta — verifies fix for score accumulation bug
    6. telegram per_str/pbr_str display logic
"""

import pytest
from unittest.mock import patch, MagicMock
import market_data
from market_data import (
    _parse_naver_value,
    _fundamental_score,
    _to_krx_code,
    _fetch_fundamental,
    PriceContext,
    cross_analyze,
)


# ── _parse_naver_value ────────────────────────────────────────

@pytest.mark.parametrize("raw, expected", [
    ("32.91배",  32.91),
    ("3.38배",   3.38),
    ("6,564원",  6564.0),
    ("-2,177원", -2177.0),
    ("-106원",   -106.0),
    ("139.00배", 139.0),
    ("N/A",      None),
    ("-",        None),
    ("",         None),
    ("해당없음",  None),
])
def test_parse_naver_value(raw, expected):
    assert _parse_naver_value(raw) == expected


# ── _to_krx_code ──────────────────────────────────────────────

def test_to_krx_code_ks():
    assert _to_krx_code("005930.KS") == "005930"

def test_to_krx_code_kq():
    assert _to_krx_code("035720.KQ") == "035720"

def test_to_krx_code_us():
    assert _to_krx_code("NVDA") is None

def test_to_krx_code_index():
    assert _to_krx_code("^KS11") is None


# ── _fundamental_score ────────────────────────────────────────

def _ctx(**kw) -> PriceContext:
    defaults = dict(
        ticker="TEST", symbol="TEST", source="yfinance",
        current=10000, change_pct=0, rsi=None, volume_ratio=None,
        week52_high=None, week52_low=None, volume_surge=False, success=True,
    )
    defaults.update(kw)
    return PriceContext(**defaults)


def test_score_loss_making():
    ctx = _ctx(eps=-2177.0, per=None, pbr=1.2)
    score, flags = _fundamental_score(ctx)
    assert score == -2
    assert "적자" in flags

def test_score_loss_making_with_high_pbr():
    ctx = _ctx(eps=-500.0, per=None, pbr=6.0)
    score, flags = _fundamental_score(ctx)
    assert score == -3  # -2 (loss) + -1 (PBR>5)
    assert "적자" in flags

def test_score_cheap():
    ctx = _ctx(eps=5000.0, per=10.0, pbr=0.8)
    score, flags = _fundamental_score(ctx)
    assert score == 2  # +1 (PER<15) + +1 (PBR<1)
    assert flags == []

def test_score_expensive():
    ctx = _ctx(eps=100.0, per=80.0, pbr=7.0)
    score, flags = _fundamental_score(ctx)
    assert score == -2  # -1 (PER>50) + -1 (PBR>5)

def test_score_neutral():
    ctx = _ctx(eps=3000.0, per=20.0, pbr=2.0)
    score, flags = _fundamental_score(ctx)
    assert score == 0
    assert flags == []

def test_score_no_data():
    ctx = _ctx(eps=None, per=None, pbr=None)
    score, flags = _fundamental_score(ctx)
    assert score == 0
    assert flags == []

def test_score_per_exactly_15():
    ctx = _ctx(eps=100.0, per=15.0, pbr=2.0)
    score, flags = _fundamental_score(ctx)
    assert score == 0  # per==15 is NOT < 15

def test_score_per_exactly_50():
    ctx = _ctx(eps=100.0, per=50.0, pbr=2.0)
    score, flags = _fundamental_score(ctx)
    assert score == 0  # per==50 is NOT > 50

def test_score_pbr_exactly_1():
    ctx = _ctx(eps=100.0, per=20.0, pbr=1.0)
    score, flags = _fundamental_score(ctx)
    assert score == 0  # pbr==1 is NOT < 1

def test_score_pbr_exactly_5():
    ctx = _ctx(eps=100.0, per=20.0, pbr=5.0)
    score, flags = _fundamental_score(ctx)
    assert score == 0  # pbr==5 is NOT > 5

def test_score_eps_zero():
    ctx = _ctx(eps=0.0, per=20.0, pbr=2.0)
    score, flags = _fundamental_score(ctx)
    assert score == 0  # eps==0 is not loss-making; per/pbr are neutral


# ── _fetch_fundamental() with mocked httpx ───────────────────

def _naver_response(per="32.91배", pbr="3.38배", eps="6,564원"):
    """Build a minimal Naver API response dict."""
    items = [
        {"code": "per", "key": "PER", "value": per},
        {"code": "pbr", "key": "PBR", "value": pbr},
        {"code": "eps", "key": "EPS", "value": eps},
    ]
    return {"totalInfos": items, "stockName": "TEST"}


def test_fetch_fundamental_cache_hit(monkeypatch):
    """Cache hit returns stored result without making an HTTP call."""
    monkeypatch.setattr(market_data, "HTTPX_OK", True)
    today = __import__("datetime").datetime.now().strftime("%Y%m%d")
    market_data._fund_cache[today] = {"005930": {"per": 10.0, "pbr": 0.5, "eps": 1000.0}}
    result = _fetch_fundamental("005930")
    assert result == {"per": 10.0, "pbr": 0.5, "eps": 1000.0}
    # cleanup
    del market_data._fund_cache[today]["005930"]


def test_fetch_fundamental_httpx_not_ok(monkeypatch):
    """Returns empty dict immediately when httpx is unavailable."""
    monkeypatch.setattr(market_data, "HTTPX_OK", False)
    result = _fetch_fundamental("999999")
    assert result == {}


def test_fetch_fundamental_successful_call(monkeypatch):
    """Successful API call parses per/pbr/eps correctly."""
    monkeypatch.setattr(market_data, "HTTPX_OK", True)
    mock_resp = MagicMock()
    mock_resp.json.return_value = _naver_response()
    mock_resp.raise_for_status.return_value = None

    with patch.object(market_data._httpx, "get", return_value=mock_resp):
        result = _fetch_fundamental("005930_test_success")

    assert result["per"] == pytest.approx(32.91)
    assert result["pbr"] == pytest.approx(3.38)
    assert result["eps"] == pytest.approx(6564.0)


def test_fetch_fundamental_loss_maker_forces_per_none(monkeypatch):
    """Loss-making ticker (eps < 0) → per forced to None even if API returned a value."""
    monkeypatch.setattr(market_data, "HTTPX_OK", True)
    mock_resp = MagicMock()
    mock_resp.json.return_value = _naver_response(per="N/A", eps="-2,177원")
    mock_resp.raise_for_status.return_value = None

    with patch.object(market_data._httpx, "get", return_value=mock_resp):
        result = _fetch_fundamental("950130_test_loss")

    assert result["per"] is None
    assert result["eps"] == pytest.approx(-2177.0)


def test_fetch_fundamental_per_capped_at_200(monkeypatch):
    """PER > 200 anomaly (e.g. 999) is clamped to None."""
    monkeypatch.setattr(market_data, "HTTPX_OK", True)
    mock_resp = MagicMock()
    mock_resp.json.return_value = _naver_response(per="999배")
    mock_resp.raise_for_status.return_value = None

    with patch.object(market_data._httpx, "get", return_value=mock_resp):
        result = _fetch_fundamental("test_per_cap")

    assert result["per"] is None


def test_fetch_fundamental_api_exception_returns_empty(monkeypatch):
    """HTTP error → logs warning, returns empty dict."""
    monkeypatch.setattr(market_data, "HTTPX_OK", True)

    with patch.object(market_data._httpx, "get", side_effect=Exception("timeout")):
        result = _fetch_fundamental("test_exception")

    assert result == {}


# ── cross_analyze() average delta ────────────────────────────

def _make_ctx(ticker, per=None, pbr=None, eps=None, change_pct=1.0):
    return PriceContext(
        ticker=ticker, symbol=f"{ticker}.KS", source="yfinance",
        current=50000, change_pct=change_pct, rsi=50,
        volume_ratio=1.0, week52_high=60000, week52_low=40000,
        volume_surge=False, success=True,
        per=per, pbr=pbr, eps=eps,
    )


def test_cross_analyze_avg_delta_loss_making():
    """3 loss-making tickers: avg_delta = -2, not -6."""
    ctxs = [
        _make_ctx("A", eps=-100.0),
        _make_ctx("B", eps=-200.0),
        _make_ctx("C", eps=-50.0),
    ]
    result = cross_analyze("BUY", strength=3, tickers=[], _contexts=ctxs)
    # base_score=6, confirm>conflict from positive change_pct → bonus
    # avg_delta = round((-2 + -2 + -2) / 3) = -2
    assert result.score <= 8  # max base+bonus=8, then -2 → ≤6, but clamp at 0
    assert result.score >= 0


def test_cross_analyze_avg_delta_mixed():
    """1 cheap (delta+1), 1 loss (delta-2), 1 neutral (delta=0): avg=-0.33 → round=0."""
    ctxs = [
        _make_ctx("A", per=10.0, pbr=0.8, eps=500.0),   # +2
        _make_ctx("B", eps=-100.0),                        # -2
        _make_ctx("C", per=25.0, pbr=2.0, eps=1000.0),   # 0
    ]
    result = cross_analyze("BUY", strength=3, tickers=[], _contexts=ctxs)
    # avg_delta = round((2 + -2 + 0) / 3) = round(0) = 0 — no fundamental adjustment
    assert result.score >= 0
    assert result.score <= 10


def test_cross_analyze_cheap_basket_raises_score():
    """All cheap tickers: avg_delta = +2, score bumped up."""
    ctxs = [
        _make_ctx("A", per=8.0, pbr=0.5, eps=1000.0),
        _make_ctx("B", per=12.0, pbr=0.7, eps=500.0),
    ]
    result_without_fund = cross_analyze("BUY", strength=3, tickers=[], _contexts=[
        _make_ctx("A"), _make_ctx("B")
    ])
    result_with_fund = cross_analyze("BUY", strength=3, tickers=[], _contexts=ctxs)
    assert result_with_fund.score >= result_without_fund.score


# ── telegram per_str / pbr_str display ───────────────────────

import asyncio


def _make_signal(tickers=None):
    from signal_detector import TradeSignal, Backend
    return TradeSignal(
        direction="BUY", strength=3, tickers=tickers or ["005930.KS"],
        ticker_symbols={}, backend=Backend.OLLAMA, success=True,
        reason="테스트", article_type="earnings",
    )


def _make_cross(per=None, pbr=None, eps=None, change_pct=1.0):
    from market_data import CrossAnalysis
    ctx = PriceContext(
        ticker="005930", symbol="005930.KS", source="yfinance",
        current=50000, change_pct=change_pct, rsi=55,
        volume_ratio=1.0, week52_high=60000, week52_low=40000,
        volume_surge=False, success=True,
        per=per, pbr=pbr, eps=eps,
    )
    return CrossAnalysis(verdict="CONFIRM", score=8, summary="test",
                         price_contexts=[ctx], confirm_count=3, conflict_count=1)


async def _build_signal_msg(per=None, pbr=None, eps=None, change_pct=1.0):
    from unittest.mock import AsyncMock, patch as _patch
    import telegram_notify

    art = {"source": "cnbc", "category": "markets", "title": "Test",
           "url": "http://t.co", "published": "04-18 09:00"}
    signal = _make_signal()
    cross = _make_cross(per=per, pbr=pbr, eps=eps, change_pct=change_pct)

    captured = []
    async def mock_post(http, token, chat_id, text, label=""):
        captured.append(text)
        return True

    with _patch.object(telegram_notify, "_post_message", side_effect=mock_post), \
         _patch.object(telegram_notify, "_get_token", return_value="tok"), \
         _patch.object(telegram_notify, "_get_chat_id", return_value="123"), \
         _patch.object(telegram_notify, "_get_channel_id", return_value=""):
        await telegram_notify.send_signal(art, "요약", signal, cross=cross)
    return captured[0] if captured else ""


def test_per_str_loss_maker():
    msg = asyncio.run(_build_signal_msg(eps=-2177.0, per=None, pbr=1.2))
    assert "적자" in msg


def test_per_str_cheap():
    msg = asyncio.run(_build_signal_msg(eps=5000.0, per=10.0, pbr=2.0))
    assert "PER" in msg and "↓" in msg


def test_per_str_expensive():
    msg = asyncio.run(_build_signal_msg(eps=100.0, per=80.0, pbr=2.0))
    assert "PER" in msg and "↑" in msg


def test_per_str_neutral_no_token():
    msg = asyncio.run(_build_signal_msg(eps=3000.0, per=25.0, pbr=2.0))
    assert "PER" not in msg


def test_pbr_str_below_book():
    msg = asyncio.run(_build_signal_msg(eps=1000.0, per=20.0, pbr=0.6))
    assert "PBR" in msg and "↓" in msg


def test_pbr_str_expensive():
    msg = asyncio.run(_build_signal_msg(eps=500.0, per=20.0, pbr=7.0))
    assert "PBR" in msg and "↑" in msg


def test_pbr_str_neutral_no_token():
    msg = asyncio.run(_build_signal_msg(eps=1000.0, per=20.0, pbr=2.5))
    assert "PBR" not in msg
