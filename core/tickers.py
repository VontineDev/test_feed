"""core/tickers.py — 티커 포맷 변환 헬퍼.

Kiwoom REST API(ka10032 등)가 반환하는 'XXXXXX_AL'(KOSPI)/'XXXXXX_AQ'(KOSDAQ)
포맷 티커를 yfinance가 기대하는 'XXXXXX.KS'/'XXXXXX.KQ' 포맷으로 변환한다.
"""
from __future__ import annotations


def kiwoom_to_yfinance(raw: str, market: str = "") -> str | None:
    """Kiwoom 티커를 yfinance 포맷으로 변환.

    이미 '.' 포함(yfinance 포맷)이면 그대로 반환. '_AL'→'.KS', '_AQ'→'.KQ'.
    market('KOSPI'/'KOSDAQ')이 주어지면 접미사 없는 원시 코드에도 폴백
    적용. 매치되는 규칙이 없으면 None — 호출부가 필요에 따라 raw 그대로
    쓸지, 스킵할지 결정한다(quirk는 호출부 wrapper에 남김).
    """
    if "." in raw:
        return raw
    if raw.endswith("_AL"):
        return raw[:-3] + ".KS"
    if raw.endswith("_AQ"):
        return raw[:-3] + ".KQ"
    if market == "KOSPI":
        return raw + ".KS"
    if market == "KOSDAQ":
        return raw + ".KQ"
    return None
