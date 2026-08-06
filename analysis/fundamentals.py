"""fundamentals.py — dart_fundamentals 기반 재무비율 계산 및 스크리닝.

2026-08-06: TechnicalQuant.md 종목선택 조건(PBR/PER/ROE/부채비율/매출증가율)
전체시장 검증용. 시가총액은 krx_listings.listed_shares × 최근 종가로 계산
(daily_ohlcv 재사용, 별도 시세 API 불필요).

주의(lookahead): screen()이 쓰는 종가는 "현재" 기준 스냅샷이다. 과거 특정
시점 기준 정확한 PBR/PER로 백테스트하려면 그 시점의 종가를 넣어야 한다 —
지금은 최신 스크리닝(현재 시점 종목 선택) 용도로만 정확하다.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, cast

import pandas as pd


@dataclass
class RatioThresholds:
    """스크리닝 임계값. 필드를 None으로 두면 해당 조건은 검사하지 않는다
    (그 컬럼이 NaN이어도 걸러지지 않음) — 시나리오별로 문서가 명시한 조건만
    정확히 적용하기 위해 전부 Optional."""
    pbr_min: Optional[float] = 0.2
    pbr_max: Optional[float] = 1.0
    per_min: Optional[float] = 0.0
    per_max: Optional[float] = 12.0
    roe_min: Optional[float] = 0.08
    debt_ratio_max: Optional[float] = 1.5
    revenue_growth_min: Optional[float] = 0.0


# TechnicalQuant.md 1안/2안의 "종목 선택" 조건 그대로 — 위 기본값(문서 1절의
# 범용 필터)과는 다른, 각 시나리오 고유 숫자다. 1안은 PER/매출증가율 조건이
# 없고, 2안은 PBR/ROE/부채비율/매출증가율 조건이 없다 — 문서에 없는 조건을
# 임의로 추가하지 않기 위해 나머지는 전부 None(미적용)으로 둔다.
SCENARIO1_THRESHOLDS = RatioThresholds(
    pbr_min=None, pbr_max=0.8,          # "PBR 0.8 이하"
    per_min=None, per_max=None,          # 문서에 조건 없음
    roe_min=0.10,                        # "ROE 10% 이상"
    debt_ratio_max=1.0,                  # "부채비율 100% 이하"
    revenue_growth_min=None,             # 문서에 조건 없음
)

# 2안 "PER 15 이하"는 하한을 명시하지 않았지만, 문서 1절 전반의 전제("적자
# 기업은 제외")를 따라 PER>0(흑자 기업)으로 해석 — 음수 PER(적자)까지
# "15 이하"로 통과시키는 건 문서 취지에 반한다고 판단.
SCENARIO2_THRESHOLDS = RatioThresholds(
    pbr_min=None, pbr_max=None,          # 문서에 조건 없음
    per_min=0.0, per_max=15.0,           # "PER 15 이하" (+ 적자 제외 해석)
    roe_min=None,                        # 문서에 조건 없음
    debt_ratio_max=None,                 # 문서에 조건 없음
    revenue_growth_min=None,             # 문서에 조건 없음
)


def load_fundamentals_raw(dsn: str, bsns_year: Optional[str] = None) -> pd.DataFrame:
    """dart_fundamentals에서 (연도 지정 시 해당 연도, 아니면 종목별 최신) 원장 로드.

    반환 컬럼: stock_code, bsns_year, revenue, revenue_prev, net_income,
    equity, liabilities, assets.
    """
    import psycopg2

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            if bsns_year:
                cur.execute(
                    """
                    SELECT stock_code, bsns_year, revenue, revenue_prev,
                           net_income, equity, liabilities, assets
                    FROM dart_fundamentals
                    WHERE bsns_year = %s
                    """,
                    (bsns_year,),
                )
            else:
                cur.execute(
                    """
                    SELECT DISTINCT ON (stock_code)
                           stock_code, bsns_year, revenue, revenue_prev,
                           net_income, equity, liabilities, assets
                    FROM dart_fundamentals
                    ORDER BY stock_code, bsns_year DESC
                    """
                )
            rows = cur.fetchall()
            cols = ["stock_code", "bsns_year", "revenue", "revenue_prev",
                    "net_income", "equity", "liabilities", "assets"]
    finally:
        conn.close()
    return pd.DataFrame(rows, columns=pd.Index(cols))


def load_market_cap(dsn: str) -> pd.DataFrame:
    """krx_listings.listed_shares × daily_ohlcv 최신 종가로 시가총액 계산.

    반환 컬럼: ticker(yfinance 심볼, 예 '005930.KS'), stock_code, market_cap, close.
    """
    import psycopg2

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT k.yfinance_symbol, k.short_code, k.listed_shares, o.close
                FROM krx_listings k
                JOIN LATERAL (
                    SELECT close FROM daily_ohlcv
                    WHERE symbol = k.yfinance_symbol
                    ORDER BY date DESC LIMIT 1
                ) o ON true
                WHERE k.listed_shares IS NOT NULL AND k.listed_shares > 0
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    out = []
    for ticker, stock_code, shares, close in rows:
        if close is None or shares is None:
            continue
        out.append({
            "ticker": ticker, "stock_code": stock_code,
            "market_cap": float(close) * int(shares), "close": float(close),
        })
    return pd.DataFrame(out)


def _safe_div(numer: pd.Series, denom: pd.Series) -> pd.Series:
    return cast(pd.Series, numer / denom.where(denom != 0))


def compute_ratio_columns(merged: pd.DataFrame) -> pd.DataFrame:
    """market_cap + revenue/revenue_prev/net_income/equity/liabilities 컬럼을 가진
    프레임에 pbr/per/roe/debt_ratio/revenue_growth 컬럼을 계산해 붙인다 (순수 함수 —
    DB 의존 compute_ratios()에서 분리해 유닛테스트 가능하게 함).

    분모가 0/None이면 결과는 NaN(계산 불가로 취급, screen()에서 자동 제외됨).
    """
    df = merged.copy()
    revenue      = cast(pd.Series, df["revenue"])
    revenue_prev = cast(pd.Series, df["revenue_prev"])
    df["pbr"] = _safe_div(cast(pd.Series, df["market_cap"]), cast(pd.Series, df["equity"]))
    df["per"] = _safe_div(cast(pd.Series, df["market_cap"]), cast(pd.Series, df["net_income"]))
    df["roe"] = _safe_div(cast(pd.Series, df["net_income"]), cast(pd.Series, df["equity"]))
    df["debt_ratio"] = _safe_div(cast(pd.Series, df["liabilities"]), cast(pd.Series, df["equity"]))
    df["revenue_growth"] = _safe_div(revenue - revenue_prev, cast(pd.Series, revenue_prev.abs()))
    return cast(pd.DataFrame, df[["ticker", "stock_code", "market_cap", "pbr", "per", "roe",
                                  "debt_ratio", "revenue_growth"]])


def compute_ratios(dsn: str, bsns_year: Optional[str] = None) -> pd.DataFrame:
    """재무 원장 + 시가총액을 합쳐 PBR/PER/ROE/부채비율/매출증가율 계산.

    반환 컬럼: ticker, stock_code, market_cap, pbr, per, roe, debt_ratio,
    revenue_growth (계산 불가 항목은 NaN — 분모 0/None/음수인 경우 포함).
    """
    fund = load_fundamentals_raw(dsn, bsns_year)
    mcap = load_market_cap(dsn)
    if fund.empty or mcap.empty:
        return pd.DataFrame(columns=pd.Index([
            "ticker", "stock_code", "market_cap", "pbr", "per", "roe",
            "debt_ratio", "revenue_growth",
        ]))

    merged = mcap.merge(fund, on="stock_code", how="inner")
    return compute_ratio_columns(merged)


def screen(df: pd.DataFrame, th: Optional[RatioThresholds] = None) -> set[str]:
    """th에 설정된(None이 아닌) 조건만 통과하는 ticker 집합 반환.

    조건: pbr_min < PBR < pbr_max, per_min < PER < per_max, ROE >= roe_min,
    부채비율 <= debt_ratio_max, 매출증가율 >= revenue_growth_min. 필드가
    None이면 그 조건은 아예 검사하지 않는다(해당 컬럼이 NaN이어도 안 걸림).
    검사 대상 컬럼이 NaN인 행은 비교 결과 자동으로 False가 돼 제외된다.
    """
    th = th or RatioThresholds()
    if df.empty:
        return set()

    mask = pd.Series(True, index=df.index)
    if th.pbr_min is not None:
        mask &= df["pbr"] > th.pbr_min
    if th.pbr_max is not None:
        mask &= df["pbr"] < th.pbr_max
    if th.per_min is not None:
        mask &= df["per"] > th.per_min
    if th.per_max is not None:
        mask &= df["per"] < th.per_max
    if th.roe_min is not None:
        mask &= df["roe"] >= th.roe_min
    if th.debt_ratio_max is not None:
        mask &= df["debt_ratio"] <= th.debt_ratio_max
    if th.revenue_growth_min is not None:
        mask &= df["revenue_growth"] >= th.revenue_growth_min

    return set(df.loc[mask, "ticker"])
