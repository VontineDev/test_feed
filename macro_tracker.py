#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
macro_tracker.py — 매크로 요인 추적 및 개별 주식 영향 평가 모델

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
분석 팩터 (모두 yfinance에서 일별 무료 수집):
  rate   : 미국 10년 국채금리 ^TNX    — 글로벌 유동성·밸류에이션 기준
  fx     : USD/KRW 환율 KRW=X         — 수출경쟁력 (상승 = 원화 약세)
  oil    : 브렌트유 선물 BZ=F          — 에너지비용·인플레이션
  vix    : VIX 공포지수 ^VIX          — 위험선호·글로벌 리스크온/오프
  dxy    : 달러인덱스 DX-Y.NYB        — 신흥국 자본 유출입
  export : 한국수출 프록시 EWY ETF     — 한국 수출 모멘텀 대리변수

모델:
  r_주식[t] = α + β_rate·Δrate[t] + β_fx·Δfx[t] + β_oil·Δoil[t]
                + β_vix·Δvix[t] + β_dxy·Δdxy[t] + β_export·Δexport[t] + ε

  팩터 변화 계산:
    rate  → 절대 변화 (%p) — 4.25 → 4.30 이면 +0.05%p
    기타  → 로그 수익률 (%) — ln(P_t / P_{t-1}) × 100

  베타(β) 해석:
    β_rate > 0   → 금리 상승 시 주가 상승   (금융·보험주)
    β_rate < 0   → 금리 상승 시 주가 하락   (성장·IT·바이오·부동산)
    β_fx   > 0   → 원화 약세 시 주가 상승   (수출주: 반도체·자동차·조선)
    β_fx   < 0   → 원화 약세 시 주가 하락   (수입의존·항공·내수주)
    β_oil  > 0   → 유가 상승 시 주가 상승   (정유·에너지·소재)
    β_oil  < 0   → 유가 상승 시 주가 하락   (항공·화학·운수·소비재)
    β_vix  < 0   → 공포 증가 시 주가 하락   (대부분 위험자산)
    β_dxy  < 0   → 달러 강세 시 주가 하락   (신흥국 자본유출 수혜)
    β_export > 0 → 한국 수출 호조 시 수혜   (수출·제조)

  매크로 영향 점수 (Macro Score, -100 ~ +100):
    최근 5일 팩터 변화 × 해당 종목 베타 합산 → tanh 정규화
    양수: 현재 매크로 환경이 해당 종목에 유리
    음수: 현재 매크로 환경이 해당 종목에 불리
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

사용법:
  python macro_tracker.py                           # 기본 KOSPI 대형주 전체 분석
  python macro_tracker.py --tickers 005930.KS       # 삼성전자만 상세 분석
  python macro_tracker.py --tickers 005930.KS 000660.KS --period 3y
  python macro_tracker.py --snapshot-only           # 매크로 현황 스냅샷만 출력
  python macro_tracker.py --json > macro_result.json
"""
from __future__ import annotations

import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import argparse
import json
import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf
from scipy import stats as _scipy_stats

logger = logging.getLogger(__name__)


# ── 팩터 정의 ──────────────────────────────────────────────────────

MACRO_TICKERS: dict[str, str] = {
    "rate":   "^TNX",
    "fx":     "KRW=X",
    "oil":    "BZ=F",
    "vix":    "^VIX",
    "dxy":    "DX-Y.NYB",
    "export": "EWY",
}

MACRO_NAMES: dict[str, str] = {
    "rate":   "미국10년금리(%p)",
    "fx":     "USD/KRW(%)",
    "oil":    "브렌트유(%)",
    "vix":    "VIX(%)",
    "dxy":    "달러인덱스(%)",
    "export": "수출프록시EWY(%)",
}

FACTOR_GUIDE: dict[str, tuple[str, str]] = {
    "rate":   ("금리↑ 수혜: 금융·보험", "금리↑ 피해: 성장·IT·바이오"),
    "fx":     ("원화약세 수혜: 반도체·자동차·조선", "원화약세 피해: 항공·수입·내수"),
    "oil":    ("유가↑ 수혜: 정유·에너지·소재", "유가↑ 피해: 항공·화학·운수"),
    "vix":    ("VIX↑ 수혜: 방어주·금(safe-haven)", "VIX↑ 피해: 성장·소형·신흥국"),
    "dxy":    ("달러↑ 수혜: 달러자산 보유", "달러↑ 피해: 신흥국 자본유출"),
    "export": ("수출↑ 수혜: 반도체·제조·수출", "수출↓ 피해: 수출 의존 산업"),
}

# 기본 분석 대상 (KOSPI/KOSDAQ 주요 종목)
DEFAULT_TICKERS: dict[str, str] = {
    "005930.KS": "삼성전자",
    "000660.KS": "SK하이닉스",
    "005380.KS": "현대차",
    "000270.KS": "기아",
    "005490.KS": "POSCO홀딩스",
    "373220.KS": "LG에너지솔루션",
    "006400.KS": "삼성SDI",
    "035420.KS": "NAVER",
    "035720.KS": "카카오",
    "105560.KS": "KB금융",
    "055550.KS": "신한지주",
    "096770.KS": "SK이노베이션",
    "010950.KS": "S-Oil",
    "011200.KS": "HMM(해운)",
    "003490.KS": "대한항공",
    "012330.KS": "현대모비스",
    "009150.KS": "삼성전기",
    "066570.KS": "LG전자",
}

FACTOR_COLS = list(MACRO_TICKERS.keys())


# ── 데이터 수집 ─────────────────────────────────────────────────────

def _download_multi(tickers: list[str], period: str) -> pd.DataFrame:
    """yfinance multi-ticker 다운로드 → 종가 DataFrame 반환."""
    raw = yf.download(
        tickers,
        period=period,
        auto_adjust=True,
        progress=False,
        threads=True,
    )
    if raw.empty:
        raise RuntimeError(f"yfinance 다운로드 실패: {tickers}")
    close = raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw
    if isinstance(close, pd.Series):
        close = close.to_frame(name=tickers[0])
    return close


def fetch_macro_data(period: str = "2y") -> pd.DataFrame:
    """
    매크로 팩터 일별 종가 수집.
    반환: DataFrame(columns=[rate, fx, oil, vix, dxy, export], index=Date)
    """
    logger.info("[매크로] %s 기간 데이터 다운로드...", period)
    tickers = list(MACRO_TICKERS.values())
    close = _download_multi(tickers, period)

    # 컬럼명: yfinance ticker → 팩터 키
    inv = {v: k for k, v in MACRO_TICKERS.items()}
    close = close.rename(columns=inv)
    close = close[[c for c in FACTOR_COLS if c in close.columns]]

    # 누락 보간 (주말·공휴일로 인한 결측)
    close = close.ffill().bfill()
    logger.info("[매크로] %d 거래일 로드 완료", len(close))
    return close


def fetch_stock_data(tickers: list[str], period: str = "2y") -> pd.DataFrame:
    """개별 종목 일별 종가 수집."""
    logger.info("[종목] %d개 종목 다운로드...", len(tickers))
    close = _download_multi(tickers, period)
    close = close.ffill()
    logger.info("[종목] %d행 × %d종목 로드", len(close), len(close.columns))
    return close


# ── 팩터 변화 계산 ──────────────────────────────────────────────────

def compute_factor_changes(macro_prices: pd.DataFrame) -> pd.DataFrame:
    """
    팩터별 일별 변화량:
      rate  → 절대 변화 (%p)
      기타  → 로그 수익률 × 100 (%)
    """
    out = {}
    for col in macro_prices.columns:
        p = macro_prices[col]
        if col == "rate":
            out[col] = p.diff()
        else:
            out[col] = np.log(p / p.shift(1)) * 100
    return pd.DataFrame(out, index=macro_prices.index).dropna()


def compute_stock_returns(stock_prices: pd.DataFrame) -> pd.DataFrame:
    """일별 로그 수익률 (%)."""
    return (np.log(stock_prices / stock_prices.shift(1)) * 100).dropna()


# ── OLS 회귀 ────────────────────────────────────────────────────────

def _ols(y: np.ndarray, X: np.ndarray) -> dict:
    """
    OLS: y = Xβ + ε  (X는 상수항 열 포함)
    반환: beta, t_stats, p_values, r_squared, adj_r_squared, residual_std
    """
    n, k = X.shape
    beta, _, _, _ = np.linalg.lstsq(X, y, rcond=None)

    y_hat = X @ beta
    res = y - y_hat
    sse = float(res @ res)
    sst = float(((y - y.mean()) ** 2).sum())

    r2 = 1.0 - sse / sst if sst > 1e-12 else 0.0
    adj_r2 = 1.0 - (1.0 - r2) * (n - 1) / max(n - k, 1)

    s2 = sse / max(n - k, 1)
    var_beta = s2 * np.diag(np.linalg.pinv(X.T @ X))
    se = np.sqrt(np.maximum(var_beta, 1e-14))

    t = beta / se
    p = np.array([2 * (1 - _scipy_stats.t.cdf(abs(ti), df=max(n - k, 1))) for ti in t])

    return {
        "beta":         beta,
        "t_stats":      t,
        "p_values":     p,
        "r2":           round(r2, 4),
        "adj_r2":       round(adj_r2, 4),
        "residual_std": round(float(np.sqrt(s2)), 4),
        "n":            n,
    }


# ── 개별 종목 분석 ──────────────────────────────────────────────────

def analyze_stock(
    ticker: str,
    stock_prices: pd.DataFrame,
    macro_prices: pd.DataFrame,
    name: str = "",
    min_obs: int = 60,
) -> Optional[dict]:
    """
    개별 종목 매크로 팩터 회귀 분석.

    반환 dict:
      ticker, name, n_obs, r_squared, adj_r_squared, residual_std,
      betas, t_stats, p_values, significant_factors,
      macro_score_5d, macro_score_20d, macro_score (정규화 -100~+100)
    """
    if ticker not in stock_prices.columns:
        logger.warning("  [skip] %s — 종목 데이터 없음", ticker)
        return None

    stock_ret = compute_stock_returns(stock_prices[[ticker]])[ticker]
    factor_chg = compute_factor_changes(macro_prices)

    common = stock_ret.index.intersection(factor_chg.index)
    if len(common) < min_obs:
        logger.warning("  [skip] %s — 관측 부족 (%d)", ticker, len(common))
        return None

    y = stock_ret.loc[common].values
    X_raw = factor_chg.loc[common, FACTOR_COLS].values

    # 결측 행 제거
    valid = ~(np.isnan(y) | np.isnan(X_raw).any(axis=1))
    y, X_raw = y[valid], X_raw[valid]
    if len(y) < min_obs:
        logger.warning("  [skip] %s — 유효 관측 부족", ticker)
        return None

    # 상수항 추가
    X = np.column_stack([np.ones(len(y)), X_raw])
    col_names = ["alpha"] + FACTOR_COLS

    reg = _ols(y, X)
    beta_dict = {col_names[i]: round(float(reg["beta"][i]), 6) for i in range(len(col_names))}
    t_dict    = {col_names[i]: round(float(reg["t_stats"][i]), 3) for i in range(len(col_names))}
    p_dict    = {col_names[i]: round(float(reg["p_values"][i]), 4) for i in range(len(col_names))}

    significant = [f for f in FACTOR_COLS if p_dict.get(f, 1.0) < 0.10]

    # ── 매크로 기여 수익률 계산 ──────────────────────────────────
    def _contribution(n_days: int) -> float:
        """최근 n일 팩터 변화의 합 × 베타 = 예상 기여 수익률(%)"""
        tail = factor_chg.iloc[-n_days:][FACTOR_COLS]
        cumchg = tail.sum()
        return round(sum(beta_dict.get(f, 0) * float(cumchg.get(f, 0)) for f in FACTOR_COLS), 3)

    score_5d  = _contribution(5)
    score_20d = _contribution(20)

    # 정규화: 최근 60일 일별 기여 분포의 표준편차로 스케일 → tanh(-100~+100)
    n_hist = min(60, len(factor_chg))
    hist_chg = factor_chg.iloc[-n_hist:][FACTOR_COLS].values
    daily_contrib = hist_chg @ np.array([beta_dict.get(f, 0) for f in FACTOR_COLS])
    contrib_std_5d = daily_contrib.std() * np.sqrt(5)
    if contrib_std_5d > 1e-6:
        macro_score = round(float(np.tanh(score_5d / contrib_std_5d)) * 100, 1)
    else:
        macro_score = 0.0

    return {
        "ticker":             ticker,
        "name":               name or ticker,
        "n_obs":              int(reg["n"]),
        "r_squared":          reg["r2"],
        "adj_r_squared":      reg["adj_r2"],
        "residual_std":       reg["residual_std"],
        "betas":              beta_dict,
        "t_stats":            t_dict,
        "p_values":           p_dict,
        "significant_factors": significant,
        "macro_score_5d":     score_5d,     # 최근 5일 매크로 기여 수익률 (%)
        "macro_score_20d":    score_20d,    # 최근 20일 매크로 기여 수익률 (%)
        "macro_score":        macro_score,  # 정규화 점수 (-100 ~ +100)
    }


# ── 매크로 스냅샷 ───────────────────────────────────────────────────

def get_macro_snapshot(macro_prices: pd.DataFrame) -> dict:
    """
    현재 매크로 환경 스냅샷:
      current, change_1d, change_5d, change_20d, change_60d, z_score_60d
    """
    snapshot: dict[str, dict] = {}
    for col in macro_prices.columns:
        p = macro_prices[col].dropna()
        if len(p) < 5:
            continue

        curr = float(p.iloc[-1])

        def _chg(n: int) -> float:
            idx = min(n, len(p) - 1)
            prev = float(p.iloc[-idx - 1])
            if col == "rate":
                return round(curr - prev, 4)
            return round((curr / prev - 1) * 100, 3) if prev else 0.0

        # 60일 z점수: 현재 가격 수준이 최근 60일 분포에서 얼마나 극단적인가
        window = p.iloc[-61:-1] if len(p) > 60 else p.iloc[:-1]
        mean_w = float(window.mean())
        std_w  = float(window.std())
        z60 = round((curr - mean_w) / std_w, 2) if std_w > 0 else 0.0

        snapshot[col] = {
            "name":       MACRO_NAMES.get(col, col),
            "current":    round(curr, 4),
            "change_1d":  _chg(1),
            "change_5d":  _chg(5),
            "change_20d": _chg(20),
            "change_60d": _chg(60),
            "z_score_60d": z60,
        }

    return snapshot


# ── 출력 ────────────────────────────────────────────────────────────

_W = 70


def _bar(val: float, width: int = 20) -> str:
    """간단한 텍스트 바 차트 (-100~+100 범위)."""
    mid = width // 2
    pos = int(round(val / 100 * mid))
    pos = max(-mid, min(mid, pos))
    bar = ["-"] * width
    bar[mid] = "|"
    if pos > 0:
        for i in range(mid + 1, mid + pos + 1):
            bar[i] = "█"
    elif pos < 0:
        for i in range(mid + pos, mid):
            bar[i] = "█"
    return "".join(bar)


def print_macro_snapshot(snapshot: dict) -> None:
    print("\n" + "═" * _W)
    print("  📊 매크로 환경 스냅샷  |  " + datetime.now().strftime("%Y-%m-%d %H:%M"))
    print("═" * _W)
    hdr = f"  {'팩터키':<8} {'팩터명':<16} {'현재':>9} {'1일Δ':>8} {'5일Δ':>8} {'20일Δ':>8} {'z점수':>6}"
    print(hdr)
    print("─" * _W)
    for key, s in snapshot.items():
        z = s["z_score_60d"]
        z_mark = "▲▲" if z > 2 else "▲" if z > 1 else "▼▼" if z < -2 else "▼" if z < -1 else " "
        print(
            f"  {key:<8} {s['name'][:14]:<16} {s['current']:>9.3f}"
            f" {s['change_1d']:>+8.3f} {s['change_5d']:>+8.3f}"
            f" {s['change_20d']:>+8.3f} {z:>+5.1f}{z_mark}"
        )
    print("─" * _W)
    print("  z점수 기준: 60일 평균 대비 현재 수준. |z|>2는 역사적 극단값.")
    print("═" * _W)

    # 팩터 해석 가이드
    print("\n  📌 팩터 방향 해석 가이드")
    for k, (up, dn) in FACTOR_GUIDE.items():
        s = snapshot.get(k, {})
        delta5 = s.get("change_5d", 0)
        arrow = "▲" if delta5 > 0 else "▼"
        active = up if delta5 > 0 else dn
        print(f"  {k:>6} {arrow}  → {active}")


def print_summary_table(results: list[dict]) -> None:
    """매크로 영향 점수 순위표."""
    ranked = sorted(results, key=lambda x: x["macro_score"], reverse=True)

    print("\n" + "═" * (_W + 10))
    print("  📈 매크로 영향 점수 순위  (양수=유리 / 음수=불리)")
    print("═" * (_W + 10))
    print(f"  {'순위':<4} {'종목명':<12} {'티커':<12} {'점수':>6}  "
          f"{'바 차트':^22}  {'5d기여':>7} {'20d기여':>8} {'R²':>5} {'유의팩터'}")
    print("─" * (_W + 10))

    for i, r in enumerate(ranked, 1):
        sc = r["macro_score"]
        em = "🟢" if sc >= 20 else "🔴" if sc <= -20 else "🟡"
        bar = _bar(sc, 20)
        sig = ",".join(r["significant_factors"][:3]) if r["significant_factors"] else "-"
        print(
            f"  {i:<4} {r['name'][:10]:<12} {r['ticker']:<12} {em}{sc:>+5.0f}"
            f"  [{bar}]"
            f"  {r['macro_score_5d']:>+6.2f}%"
            f"  {r['macro_score_20d']:>+7.2f}%"
            f"  {r['r_squared']:>5.3f}"
            f"  {sig}"
        )
    print("═" * (_W + 10))


def print_stock_detail(result: dict, snapshot: dict) -> None:
    """개별 종목 상세 리포트."""
    ticker = result["ticker"]
    name   = result["name"]
    betas  = result["betas"]
    t_dict = result["t_stats"]
    p_dict = result["p_values"]
    sc     = result["macro_score"]

    em = "🟢" if sc >= 20 else "🔴" if sc <= -20 else "🟡"

    print(f"\n{'─' * _W}")
    print(f"  [{ticker}] {name}")
    print(f"  관측수: {result['n_obs']}일  |  R² = {result['r_squared']:.3f}  "
          f"|  adj-R² = {result['adj_r_squared']:.3f}  "
          f"|  잔차σ = {result['residual_std']:.3f}%/일")
    print(f"  매크로 점수: {em} {sc:+.1f} / 100  "
          f"(5일 기여: {result['macro_score_5d']:+.2f}%  |  20일 기여: {result['macro_score_20d']:+.2f}%)")

    sig = result["significant_factors"]
    print(f"  유의 팩터(p<0.1): {', '.join(sig) if sig else '없음 (매크로 민감도 낮음)'}")
    print(f"{'─' * _W}")

    print(f"  {'팩터':<8} {'베타':>8}  {'t값':>6}  {'p값':>6}  {'sig':>4}  "
          f"{'5일Δ':>7}  {'기여(%)':>8}  {'방향':^30}")
    print(f"  {'─' * (_W - 4)}")

    for f in FACTOR_COLS:
        b  = betas.get(f, 0)
        t  = t_dict.get(f, 0)
        p  = p_dict.get(f, 1)
        sm = "***" if p < 0.01 else "** " if p < 0.05 else "*  " if p < 0.1 else "   "

        snap = snapshot.get(f, {})
        d5   = snap.get("change_5d", 0.0)
        contrib = round(b * d5, 3)

        up_msg, dn_msg = FACTOR_GUIDE.get(f, ("", ""))
        direction = (up_msg if b > 0 else dn_msg)[:28]

        print(
            f"  {f:<8} {b:>+8.4f}  {t:>+6.2f}  {p:>6.4f}  {sm}"
            f"  {d5:>+6.3f}  {contrib:>+8.3f}%  {direction}"
        )

    alpha = betas.get("alpha", 0.0)
    alpha_annual = round(alpha * 252, 2)
    print(f"\n  알파 (α): {alpha:+.5f}%/일  →  연간 환산 {alpha_annual:+.1f}%")
    print(f"  ※ 잔차σ {result['residual_std']:.3f}%/일 = 매크로로 설명 안 되는 종목 고유 변동성")


# ── 모듈 공개 인터페이스 ────────────────────────────────────────────

class MacroTracker:
    """
    매크로 팩터 트래커 — 임포트해서 사용하는 클래스 인터페이스.

    사용 예:
        tracker = MacroTracker(period="2y")
        tracker.fit(["005930.KS", "000660.KS"])
        snapshot = tracker.snapshot()
        scores = tracker.scores()
        df = tracker.to_dataframe()
    """

    def __init__(self, period: str = "2y", min_obs: int = 60):
        self.period   = period
        self.min_obs  = min_obs
        self._macro_prices: Optional[pd.DataFrame] = None
        self._stock_prices: Optional[pd.DataFrame] = None
        self._results: list[dict] = []
        self._snapshot: dict = {}

    def fit(
        self,
        tickers: Optional[dict[str, str]] = None,
        verbose: bool = False,
    ) -> "MacroTracker":
        """
        데이터 수집 + 팩터 회귀 실행.

        Args:
            tickers: {ticker: 종목명} dict. None이면 DEFAULT_TICKERS 사용.
            verbose: 진행 로그 출력.
        """
        if verbose:
            logging.getLogger().setLevel(logging.INFO)

        tmap = tickers or DEFAULT_TICKERS
        self._macro_prices = fetch_macro_data(self.period)
        self._stock_prices = fetch_stock_data(list(tmap.keys()), self.period)
        self._snapshot = get_macro_snapshot(self._macro_prices)

        self._results = []
        for ticker, name in tmap.items():
            r = analyze_stock(
                ticker=ticker,
                stock_prices=self._stock_prices,
                macro_prices=self._macro_prices,
                name=name,
                min_obs=self.min_obs,
            )
            if r:
                self._results.append(r)

        return self

    def snapshot(self) -> dict:
        """현재 매크로 환경 스냅샷 딕셔너리."""
        return self._snapshot

    def scores(self) -> pd.DataFrame:
        """
        매크로 영향 점수 DataFrame (내림차순 정렬).
        컬럼: ticker, name, macro_score, macro_score_5d, macro_score_20d, r_squared, significant_factors
        """
        if not self._results:
            return pd.DataFrame()
        rows = [
            {
                "ticker":             r["ticker"],
                "name":               r["name"],
                "macro_score":        r["macro_score"],
                "macro_score_5d":     r["macro_score_5d"],
                "macro_score_20d":    r["macro_score_20d"],
                "r_squared":          r["r_squared"],
                "significant_factors": ",".join(r["significant_factors"]),
            }
            for r in self._results
        ]
        df = pd.DataFrame(rows).sort_values("macro_score", ascending=False).reset_index(drop=True)
        df.index += 1
        return df

    def betas(self) -> pd.DataFrame:
        """
        종목별 팩터 베타 DataFrame.
        행: 종목, 열: 팩터 (rate, fx, oil, vix, dxy, export)
        """
        if not self._results:
            return pd.DataFrame()
        rows = []
        for r in self._results:
            row = {"ticker": r["ticker"], "name": r["name"]}
            row.update({f: r["betas"].get(f, 0) for f in FACTOR_COLS})
            row["r_squared"] = r["r_squared"]
            rows.append(row)
        return pd.DataFrame(rows).set_index("ticker")

    def to_dataframe(self) -> pd.DataFrame:
        """전체 분석 결과를 단일 DataFrame으로 반환."""
        if not self._results:
            return pd.DataFrame()
        rows = []
        for r in self._results:
            row = {
                "ticker":        r["ticker"],
                "name":          r["name"],
                "n_obs":         r["n_obs"],
                "r_squared":     r["r_squared"],
                "adj_r_squared": r["adj_r_squared"],
                "residual_std":  r["residual_std"],
                "macro_score":   r["macro_score"],
                "macro_score_5d": r["macro_score_5d"],
                "macro_score_20d": r["macro_score_20d"],
            }
            for f in FACTOR_COLS:
                row[f"beta_{f}"]  = r["betas"].get(f, 0)
                row[f"pval_{f}"]  = r["p_values"].get(f, 1)
            rows.append(row)
        return pd.DataFrame(rows).sort_values("macro_score", ascending=False).reset_index(drop=True)

    def print_report(self) -> None:
        """전체 리포트를 콘솔에 출력."""
        print_macro_snapshot(self._snapshot)
        print_summary_table(self._results)
        for r in sorted(self._results, key=lambda x: x["macro_score"], reverse=True):
            print_stock_detail(r, self._snapshot)

    def get_result(self, ticker: str) -> Optional[dict]:
        """단일 종목 결과 반환."""
        for r in self._results:
            if r["ticker"] == ticker:
                return r
        return None


# ── 시나리오 분석 ───────────────────────────────────────────────────

def scenario_analysis(
    results: list[dict],
    scenarios: dict[str, dict[str, float]],
) -> pd.DataFrame:
    """
    매크로 시나리오별 종목 영향 분석.

    Args:
        results: analyze_stock() 결과 리스트
        scenarios: {
            "금리 급등": {"rate": +0.5, "fx": +2.0, "vix": +20},
            "리스크오프": {"vix": +50, "dxy": +3, "oil": -10},
            ...
        }

    반환: DataFrame(종목 × 시나리오 → 예상 수익률 기여 %)
    """
    rows = []
    for r in results:
        betas = r["betas"]
        row = {"ticker": r["ticker"], "name": r["name"]}
        for scenario_name, factor_changes in scenarios.items():
            contrib = sum(
                betas.get(f, 0) * chg
                for f, chg in factor_changes.items()
            )
            row[scenario_name] = round(contrib, 3)
        rows.append(row)
    df = pd.DataFrame(rows).set_index(["ticker", "name"])
    return df


# 사전 정의 시나리오
PRESET_SCENARIOS: dict[str, dict[str, float]] = {
    "금리_급등_+50bp":  {"rate": +0.50, "vix": +10, "dxy": +2.0},
    "금리_급락_-50bp":  {"rate": -0.50, "vix": -5,  "dxy": -1.5},
    "원화_급락_+3%":   {"fx":   +3.0,  "dxy": +2.0},
    "원화_급등_-3%":   {"fx":   -3.0,  "dxy": -2.0},
    "유가_급등_+15%":  {"oil":  +15.0, "vix": +5},
    "유가_급락_-15%":  {"oil":  -15.0, "vix": +10},
    "글로벌_리스크오프": {"vix": +50.0, "dxy": +4.0, "oil": -10.0, "export": -8.0},
    "글로벌_리스크온":  {"vix": -20.0, "dxy": -2.0, "oil": +5.0,  "export": +5.0},
}


# ── CLI ────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="매크로 요인 추적 및 개별 주식 영향 평가 모델",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--tickers", nargs="+", metavar="TICKER",
        help="분석 종목 (예: 005930.KS 000660.KS). 미입력 시 기본 종목 분석",
    )
    parser.add_argument(
        "--names", nargs="+", metavar="NAME",
        help="--tickers 와 1:1 대응하는 종목명 (선택)",
    )
    parser.add_argument(
        "--period", default="2y", metavar="PERIOD",
        help="분석 기간 (예: 1y 2y 3y 5y, 기본: 2y)",
    )
    parser.add_argument(
        "--min-obs", type=int, default=60,
        help="회귀에 필요한 최소 관측 수 (기본: 60)",
    )
    parser.add_argument(
        "--snapshot-only", action="store_true",
        help="매크로 현황 스냅샷만 출력 (종목 분석 생략)",
    )
    parser.add_argument(
        "--scenario", action="store_true",
        help="사전 정의 매크로 시나리오별 영향 분석 추가 출력",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="JSON 형식으로 출력 (파일 저장 시 활용)",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="상세 로그 출력",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)

    # 분석 대상 결정
    if args.tickers:
        names = args.names or []
        tmap = {t: (names[i] if i < len(names) else t) for i, t in enumerate(args.tickers)}
    else:
        tmap = DEFAULT_TICKERS

    # ── 매크로 데이터 ──
    print("[1/3] 매크로 데이터 수집 중...", file=sys.stderr)
    macro_prices = fetch_macro_data(period=args.period)
    macro_snapshot = get_macro_snapshot(macro_prices)

    if args.snapshot_only:
        if args.json:
            print(json.dumps(macro_snapshot, ensure_ascii=False, indent=2))
        else:
            print_macro_snapshot(macro_snapshot)
        return

    # ── 종목 데이터 ──
    print(f"[2/3] {len(tmap)}개 종목 데이터 수집 중...", file=sys.stderr)
    stock_prices = fetch_stock_data(list(tmap.keys()), period=args.period)

    # ── 팩터 분석 ──
    print("[3/3] 팩터 회귀 분석 중...", file=sys.stderr)
    results = []
    for ticker, name in tmap.items():
        r = analyze_stock(
            ticker=ticker,
            stock_prices=stock_prices,
            macro_prices=macro_prices,
            name=name,
            min_obs=args.min_obs,
        )
        if r:
            results.append(r)

    if not results:
        print("❌ 분석 결과 없음 — 티커 오류 또는 데이터 부족", file=sys.stderr)
        sys.exit(1)

    # ── 출력 ──
    if args.json:
        output = {
            "generated_at":   datetime.now().isoformat(),
            "period":         args.period,
            "macro_snapshot": macro_snapshot,
            "stocks":         results,
        }
        if args.scenario:
            scen_df = scenario_analysis(results, PRESET_SCENARIOS)
            output["scenario_analysis"] = scen_df.to_dict(orient="index")
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return

    # 콘솔 출력
    print_macro_snapshot(macro_snapshot)
    print_summary_table(results)

    for r in sorted(results, key=lambda x: x["macro_score"], reverse=True):
        print_stock_detail(r, macro_snapshot)

    if args.scenario:
        scen_df = scenario_analysis(results, PRESET_SCENARIOS)
        print("\n" + "═" * 80)
        print("  🎯 시나리오별 매크로 기여 수익률 (%)  — 해당 변화 발생 시 예상 기여분")
        print("═" * 80)
        print(scen_df.to_string(float_format=lambda x: f"{x:+.2f}"))
        print("═" * 80)

    print(
        f"\n✅ 분석 완료 | 종목 {len(results)}/{len(tmap)}개 | 기간 {args.period} | "
        f"{datetime.now().strftime('%H:%M:%S')}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
