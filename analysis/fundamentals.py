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
    equity, liabilities, assets, cogs, gross_profit, operating_cash_flow, capex
    (뒤 4개는 2026-08-11 QVM 퀄리티 팩터용 추가 — 커버리지가 5개 핵심 계정보다
    낮을 수 있음, DART 계정명 별칭 미포함분은 NULL).
    """
    import psycopg2

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            if bsns_year:
                cur.execute(
                    """
                    SELECT stock_code, bsns_year, revenue, revenue_prev,
                           net_income, equity, liabilities, assets,
                           cogs, gross_profit, operating_cash_flow, capex
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
                           net_income, equity, liabilities, assets,
                           cogs, gross_profit, operating_cash_flow, capex
                    FROM dart_fundamentals
                    ORDER BY stock_code, bsns_year DESC
                    """
                )
            rows = cur.fetchall()
            cols = ["stock_code", "bsns_year", "revenue", "revenue_prev",
                    "net_income", "equity", "liabilities", "assets",
                    "cogs", "gross_profit", "operating_cash_flow", "capex"]
    finally:
        conn.close()
    return pd.DataFrame(rows, columns=pd.Index(cols))


def load_momentum(dsn: str, lookback_3m: int = 63, lookback_6m: int = 126) -> pd.DataFrame:
    """daily_ohlcv에서 종목별 최신 종가 기준 3개월/6개월 모멘텀(수익률) 계산.

    거래일 기준 N일 전 종가 대비 최신 종가의 수익률 — screen()과 동일하게
    "현재 시점" 스냅샷이다(과거 특정 시점 기준 point-in-time 아님, fundamentals.py
    모듈 docstring의 lookahead 주의사항과 동일 전제).

    반환 컬럼: ticker(yfinance_symbol), mom_3m, mom_6m.
    """
    import psycopg2

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH ranked AS (
                    SELECT symbol, close,
                           LAG(close, %s) OVER (PARTITION BY symbol ORDER BY date) AS close_3m_ago,
                           LAG(close, %s) OVER (PARTITION BY symbol ORDER BY date) AS close_6m_ago,
                           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
                    FROM daily_ohlcv
                    WHERE market = 'KR'
                )
                SELECT symbol, close, close_3m_ago, close_6m_ago
                FROM ranked WHERE rn = 1
                """,
                (lookback_3m, lookback_6m),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    out = []
    for symbol, close, close_3m_ago, close_6m_ago in rows:
        if close is None:
            continue
        mom_3m = (float(close) / float(close_3m_ago) - 1.0) if close_3m_ago else None
        mom_6m = (float(close) / float(close_6m_ago) - 1.0) if close_6m_ago else None
        out.append({"ticker": symbol, "mom_3m": mom_3m, "mom_6m": mom_6m})
    return pd.DataFrame(out)


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

    2026-08-11: cogs/gross_profit/operating_cash_flow/capex 컬럼이 있으면(QVM 팩터용)
    gross_margin/fcf_to_debt/accrual_ratio도 함께 계산 — 없으면(구버전 프레임 호환)
    NaN으로 채운다.

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

    cols = ["ticker", "stock_code", "market_cap", "pbr", "per", "roe",
            "debt_ratio", "revenue_growth"]
    if "gross_profit" in df.columns:
        # 매출총이익률 — 높을수록 고품질(가격결정력 있는 사업)
        df["gross_margin"] = _safe_div(cast(pd.Series, df["gross_profit"]), revenue)
        # FCF/부채 — capex는 CF표에서 보통 음수(유출)라 부호를 되돌려 차감
        # (extract_fundamentals에서 이미 절댓값 합으로 저장했으므로 그대로 차감).
        fcf = cast(pd.Series, df["operating_cash_flow"]) - cast(pd.Series, df["capex"])
        df["fcf_to_debt"] = _safe_div(fcf, cast(pd.Series, df["liabilities"]))
        # 발생액비율(accrual ratio) — (순이익-영업현금흐름)/자산. 낮을수록(이익이
        # 실제 현금흐름과 가까울수록) 이익의 질이 높다고 해석(회계상 발생액 남용 적음).
        df["accrual_ratio"] = _safe_div(
            cast(pd.Series, df["net_income"]) - cast(pd.Series, df["operating_cash_flow"]),
            cast(pd.Series, df["assets"]),
        )
        cols += ["gross_margin", "fcf_to_debt", "accrual_ratio"]
    return cast(pd.DataFrame, df[cols])


def compute_ratios(dsn: str, bsns_year: Optional[str] = None) -> pd.DataFrame:
    """재무 원장 + 시가총액을 합쳐 PBR/PER/ROE/부채비율/매출증가율(+QVM 퀄리티
    팩터: gross_margin/fcf_to_debt/accrual_ratio) 계산.

    반환 컬럼: ticker, stock_code, market_cap, pbr, per, roe, debt_ratio,
    revenue_growth, gross_margin, fcf_to_debt, accrual_ratio (계산 불가 항목은
    NaN — 분모 0/None/음수인 경우 포함).
    """
    fund = load_fundamentals_raw(dsn, bsns_year)
    mcap = load_market_cap(dsn)
    if fund.empty or mcap.empty:
        return pd.DataFrame(columns=pd.Index([
            "ticker", "stock_code", "market_cap", "pbr", "per", "roe",
            "debt_ratio", "revenue_growth", "gross_margin", "fcf_to_debt", "accrual_ratio",
        ]))

    merged = mcap.merge(fund, on="stock_code", how="inner")
    return compute_ratio_columns(merged)


# ── QVM(퀄리티+밸류+모멘텀) 복합 팩터 랭킹 (2026-08-11, 방법론4) ──────────
# AND 필터가 아니라 팩터별 백분위 순위를 합산하는 방식 — 6단계 결론("필터를
# 더 결합한다고 좋아지는 게 아니라 어떤 단일 팩터냐가 중요")과 달리, 개별
# 팩터를 AND로 좁히지 않고 종합 점수로 순위만 매겨 상위 N%를 취하는 게 핵심
# 차이. quant-investing.com의 QVM 전략(품질 하위 제거→밸류 상위20%→모멘텀
# 상위50%) 리서치에서 착안했으나, 여기서는 순차 필터 대신 랭킹 합산으로 구현
# (표본이 이미 좁은 한국 시장에서 순차 AND 필터를 쓰면 6단계처럼 유니버스가
# 급격히 줄어들 위험이 커서).

def _percentile_rank(s: pd.Series, ascending: bool = True) -> pd.Series:
    """0~1 백분위 순위. ascending=True면 값이 클수록 순위(점수)도 높음.
    NaN은 순위 계산에서 제외되고 결과도 NaN으로 유지된다(=팩터 결측 종목은
    합산에서 0으로 취급되지 않고 별도 처리 — compute_qvm_score 참고)."""
    return cast(pd.Series, s.rank(pct=True, ascending=ascending))


QVM_FACTORS = ("quality", "value", "momentum")

# QVM 유니버스 배리에이션 정의(순수 데이터) — scripts/run_quant_qvm_backtest.py와
# analysis/backtest/model_registry.py 양쪽에서 참조하는 단일 출처. 원래
# run_quant_qvm_backtest.py에만 있었는데 model_registry.py가 손으로 옮겨
# 적으면서 두 값이 따로 놀 위험이 생겼음(2026-08-22 review 발견) — 여기로
# 옮겨 양쪽 다 이 상수를 가져다 쓰게 통일(2026-08-23).
# mktcap_restrict=True면 시총상위200과 교집합(2안 원안 유니버스 규모와 맞춤),
# False면 전체 시장 QVM 상위 N%.
QVM_UNIVERSE_VARIANTS: list[dict] = [
    {"name": "QVM_top10pct_mktcap200", "top_pct": 0.10, "mktcap_restrict": True,
     "note": "시총상위200 ∩ QVM종합점수 상위10%"},
    {"name": "QVM_top20pct_mktcap200", "top_pct": 0.20, "mktcap_restrict": True,
     "note": "시총상위200 ∩ QVM종합점수 상위20%"},
    {"name": "QVM_top30pct_mktcap200", "top_pct": 0.30, "mktcap_restrict": True,
     "note": "시총상위200 ∩ QVM종합점수 상위30%"},
    {"name": "QVM_top10pct_all", "top_pct": 0.10, "mktcap_restrict": False,
     "note": "전체시장 QVM종합점수 상위10%(시총 제한 없음)"},
    {"name": "QVM_top20pct_all", "top_pct": 0.20, "mktcap_restrict": False,
     "note": "전체시장 QVM종합점수 상위20%(시총 제한 없음)"},
]


def compute_qvm_score(
    ratios_df: pd.DataFrame,
    momentum_df: pd.DataFrame,
    factors: tuple[str, ...] = QVM_FACTORS,
) -> pd.DataFrame:
    """퀄리티(gross_margin/fcf_to_debt/-accrual_ratio 평균) + 밸류(-PER) +
    모멘텀(6개월 수익률) 3개 팩터를 각각 백분위 순위로 변환해 동일가중 합산.

    2026-08-12: factors로 부분집합을 지정하면 그 팩터들만 동일가중 합산
    (기본은 3개 전부 — QVM_FACTORS). 방법론4 팩터 분해 실험용 —
    6단계에서 "필터를 더 결합한다고 좋아지는 게 아니라 어떤 단일 팩터냐가
    중요"했던 것처럼, QVM 3팩터 중 실제로 엣지를 만드는 게 무엇인지(단일/
    2개조합/전체) 비교하기 위함.

    3개 팩터 그룹 중 하나라도(선택된 것 중) 전부 결측이면 그 종목은 제외
    (합산 왜곡 방지). 반환: ratios_df + mom_3m/mom_6m + quality_score/
    value_score/momentum_score(계산 안 한 팩터는 NaN)/qvm_score 컬럼,
    qvm_score 내림차순 정렬.
    """
    unknown = set(factors) - set(QVM_FACTORS)
    if unknown:
        raise ValueError(f"알 수 없는 팩터: {unknown} (가능: {QVM_FACTORS})")
    if not factors:
        raise ValueError("factors는 최소 1개 이상이어야 합니다")

    df = ratios_df.merge(momentum_df, on="ticker", how="inner")

    q_parts = pd.concat([
        _percentile_rank(cast(pd.Series, df["gross_margin"])),
        _percentile_rank(cast(pd.Series, df["fcf_to_debt"])),
        _percentile_rank(cast(pd.Series, df["accrual_ratio"]), ascending=False),
    ], axis=1)
    df["quality_score"] = q_parts.mean(axis=1, skipna=True)
    df["value_score"] = _percentile_rank(cast(pd.Series, df["per"]), ascending=False)
    df["momentum_score"] = _percentile_rank(cast(pd.Series, df["mom_6m"]))

    score_cols = [f"{f}_score" for f in factors]
    df = df.dropna(subset=score_cols)
    df["qvm_score"] = df[score_cols].mean(axis=1)
    return cast(pd.DataFrame, df.sort_values("qvm_score", ascending=False).reset_index(drop=True))


def screen_qvm_top_pct(qvm_df: pd.DataFrame, top_pct: float = 0.20) -> set[str]:
    """qvm_score 상위 top_pct(기본 20%) 종목의 ticker 집합 반환."""
    if qvm_df.empty:
        return set()
    cutoff = max(1, int(len(qvm_df) * top_pct))
    return set(qvm_df.head(cutoff)["ticker"])


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
