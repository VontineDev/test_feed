"""
compare_tx_amt.py — 거래대금 근사 오차 검증
────────────────────────────────────────────
Naver Finance /integration API에서 당일 실제 거래대금을 가져와
yfinance Volume × Close 근사값과 비교합니다.

Naver API 특이사항:
  • 히스토리컬 거래대금 API 없음 — integration 엔드포인트만 당일 제공
  • accumulatedTradingValue 단위: 백만원 (×1,000,000 KRW)
  • 위 단위는 empirical 검증으로 확인 (이 파일 작성 시점)

사용법:
    python compare_tx_amt.py                 # 기본 10종목
    python compare_tx_amt.py 005930 000660   # 특정 KRX 6자리 코드
"""

from __future__ import annotations

import re
import statistics
import sys
from datetime import datetime, timedelta
from typing import Optional

import httpx
import pandas as pd
import yfinance as yf

# ── 기본 샘플 (KOSPI/KOSDAQ 혼합) ────────────────────────────────
_DEFAULT_CODES = [
    "005930",  # 삼성전자  (KOSPI)
    "000660",  # SK하이닉스
    "035420",  # NAVER
    "051910",  # LG화학
    "005490",  # POSCO홀딩스
    "086520",  # 에코프로  (KOSDAQ)
    "247540",  # 에코프로비엠
    "196170",  # 알테오젠
    "066970",  # 엘앤에프
    "141080",  # 리가켐바이오
]

# 검증된 Naver accumulatedTradingValue 단위 (empirical, 2026-04-27)
_NAVER_TX_UNIT = 1_000_000   # 백만원


def _parse_num(s: str) -> Optional[float]:
    """한국어 금액 문자열 → 숫자.  '7,738,120만' → 7738120.0"""
    s2 = re.sub(r"[^0-9.\-]", "", s.replace(",", ""))
    return float(s2) if s2 else None


def fetch_naver_today(code: str) -> Optional[dict]:
    """Naver /integration → 당일 거래대금·거래량·종가.

    Returns {close, volume, trade_amount_won} or None on failure.

    Note: Naver integration은 당일(최근 장마감일) 데이터만 제공.
    trade_amount_won은 실제 거래대금(원 단위).
    """
    try:
        r = httpx.get(
            f"https://m.stock.naver.com/api/stock/{code}/integration",
            timeout=8,
        )
        r.raise_for_status()
        ti = {
            item["code"]: item["value"]
            for item in r.json().get("totalInfos", [])
            if "code" in item and "value" in item
        }
        close  = _parse_num(ti.get("lastClosePrice", ""))
        volume = _parse_num(ti.get("accumulatedTradingVolume", ""))
        tx_raw = _parse_num(ti.get("accumulatedTradingValue", ""))

        if close is None or volume is None or tx_raw is None:
            return None

        return {
            "close":            close,
            "volume":           volume,
            "trade_amount_won": tx_raw * _NAVER_TX_UNIT,
        }
    except Exception as e:
        print(f"    [Naver] {code} 실패: {e}")
        return None


def fetch_yf_recent(code: str) -> Optional[dict]:
    """yfinance → 가장 최근 거래일의 Close·Volume.

    Returns {close, volume, approx_tx_won} or None on failure.
    approx_tx_won = Volume × Close (근사 거래대금, 원 단위).
    """
    start = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")

    for suffix in (".KS", ".KQ"):
        sym = f"{code}{suffix}"
        try:
            df = yf.Ticker(sym).history(
                start=start, interval="1d", auto_adjust=True,
            )
            if df.empty or df["Close"].notna().sum() < 1:
                continue
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            last = df.dropna(subset=["Close", "Volume"]).iloc[-1]
            c = float(last["Close"])
            v = float(last["Volume"])
            if c <= 0 or v <= 0:
                continue
            return {
                "close":        c,
                "volume":       v,
                "approx_tx_won": c * v,
            }
        except Exception:
            continue

    return None


def compare_one(code: str) -> Optional[dict]:
    """Naver 실제 거래대금 vs 근사값 비교.

    근사값 A: Naver 자체 vol x close (동일 날짜, 순수 VWAP/Close 오차만 측정)
    근사값 B: yfinance vol x close (날짜 불일치 포함한 실사용 시나리오)
    """
    naver = fetch_naver_today(code)
    if naver is None:
        return None

    actual = naver["trade_amount_won"]
    if actual <= 0:
        return None

    # A: 동일 소스(Naver) vol x close
    approx_a    = naver["volume"] * naver["close"]
    err_a_pct   = (approx_a - actual) / actual * 100

    # B: yfinance vol x close (가장 최근 거래일, 날짜 차이 있을 수 있음)
    yf_d = fetch_yf_recent(code)
    approx_b  = yf_d["approx_tx_won"] if yf_d else None
    err_b_pct = (approx_b - actual) / actual * 100 if approx_b else None
    yf_close  = yf_d["close"] if yf_d else None

    return {
        "code":          code,
        "naver_close":   naver["close"],
        "actual_tx_won": actual,
        "approx_a_won":  approx_a,
        "err_a_pct":     err_a_pct,     # 순수 근사 오차 (VWAP vs Close)
        "yf_close":      yf_close,
        "approx_b_won":  approx_b,
        "err_b_pct":     err_b_pct,     # 실사용 오차 (날짜 불일치 포함)
    }


def _verdict(mean_abs_err: float) -> str:
    if mean_abs_err < 3.0:
        return "오차 낮음 - yfinance Volume x Close 근사 사용 가능 (Stage 2 backtest OK)"
    if mean_abs_err < 6.0:
        return "오차 보통 - Stage 2 30~60% 밴드 체크 시 +-6% 허용 오차 감안 필요"
    return "오차 높음 - 실제 거래대금 사용 권장"


def main() -> None:
    codes = [a for a in sys.argv[1:] if not a.startswith("--")] or _DEFAULT_CODES

    print(f"\n거래대금 근사 오차 검증 (당일)  |  {len(codes)}개 종목")
    print(f"Naver 단위: 백만원(x{_NAVER_TX_UNIT:,})")
    print()
    print("[A] 순수 근사 오차: Naver vol x Naver close vs Naver 실제 거래대금")
    print("    (동일 날짜, VWAP/Close 차이만 측정)")
    print("-" * 70)
    print(f"  {'코드':>8}  {'종가(원)':>10}  {'실제(억)':>10}  {'근사(억)':>10}  {'오차%':>7}")
    print("-" * 70)

    a_errors: list[float] = []

    for code in codes:
        r = compare_one(code)
        if r is None:
            print(f"  {code:>8}  데이터 수집 실패")
            continue
        actual_uk  = r["actual_tx_won"] / 1e8
        approx_uk  = r["approx_a_won"]  / 1e8
        print(
            f"  {r['code']:>8}"
            f"  {r['naver_close']:>10,.0f}"
            f"  {actual_uk:>10,.1f}"
            f"  {approx_uk:>10,.1f}"
            f"  {r['err_a_pct']:>+7.2f}%"
        )
        a_errors.append(abs(r["err_a_pct"]))

    if not a_errors:
        print("  수집 성공 종목 없음")
        return

    print("-" * 70)
    mean_a = statistics.mean(a_errors)
    max_a  = max(a_errors)
    print(f"  평균절대오차: {mean_a:.2f}%   최대: {max_a:.2f}%")
    print(f"  판정: {_verdict(mean_a)}")
    print()
    print("  [참고] 오차 원인: 거래대금 = Vol x VWAP,  근사 = Vol x Close")
    print("         상승일에 Close > VWAP 이므로 근사값이 소폭 과대추정")
    print()
    print("[B] 실사용 오차: yfinance vol x close vs Naver 실제 거래대금")
    print("    (날짜 불일치 있을 수 있음 - 참고용)")
    print("-" * 70)
    print(f"  {'코드':>8}  {'Naver종가':>10}  {'yf종가':>10}  {'실제(억)':>10}  {'근사(억)':>10}  {'오차%':>9}")
    print("-" * 70)

    b_errors: list[float] = []
    for code in codes:
        r = compare_one(code)
        if r is None or r["err_b_pct"] is None:
            continue
        actual_uk  = r["actual_tx_won"] / 1e8
        approx_uk  = r["approx_b_won"]  / 1e8
        note = " *날짜불일치" if r["yf_close"] != r["naver_close"] else ""
        print(
            f"  {r['code']:>8}"
            f"  {r['naver_close']:>10,.0f}"
            f"  {r['yf_close']:>10,.0f}"
            f"  {actual_uk:>10,.1f}"
            f"  {approx_uk:>10,.1f}"
            f"  {r['err_b_pct']:>+9.2f}%{note}"
        )
        b_errors.append(abs(r["err_b_pct"]))

    if b_errors:
        print("-" * 70)
        print(f"  평균절대오차: {statistics.mean(b_errors):.2f}%  (날짜 맞으면 [A]와 동일)")
    print()


if __name__ == "__main__":
    main()
