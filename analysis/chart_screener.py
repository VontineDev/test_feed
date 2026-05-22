"""
chart_screener.py  —  주봉 차트 스크리닝 모듈
────────────────────────────────────────────────────────────
주봉 기준 일목균형표 구름 돌파 + 이동평균선 조건 스크리닝.

조건:
    A = 이번 주 종가 > max(선행스팬A, 선행스팬B)  (구름 상향 돌파)
    B = 직전 주 종가 <= 직전 주 구름 상단          (이전 주 구름 내/하부)
    C = 이번 주 종가 > 20주선
    D = 이번 주 종가 > 60주선
    E = 20주선 > 직전 주 20주선                    (우상향)
    F = 60주선 > 직전 주 60주선                    (우상향)
    G = 이번 주 종가 > 120주선 (데이터 부족 시 통과)
    최종 = A and B and C and D and E and F and G

환경변수:
    SCREENER_WORKERS : 병렬 워커 수 (기본값 8)

실행:
    python chart_screener.py   # 단독 테스트
"""

from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional

import pandas as pd
import yfinance as yf
from ta.trend import IchimokuIndicator

logger = logging.getLogger(__name__)

# ── 설정 ──────────────────────────────────────────────────────
SCREENER_WORKERS = int(os.environ.get("SCREENER_WORKERS", "1"))
_KST = ZoneInfo("Asia/Seoul")


def current_week_of() -> str:
    """현재 KST 기준 ISO 주차 문자열. 예: '2026-W16'"""
    return datetime.now(_KST).strftime("%G-W%V")


# ── 데이터 클래스 ──────────────────────────────────────────────
@dataclass
class ScreenResult:
    ticker: str            # yfinance 심볼 (예: '005930.KS')
    name: str              # 종목명 (pykrx 제공)
    close: float
    ma_20w: float
    ma_60w: float
    cloud_top: float
    is_enhanced: bool      # v1: 항상 False
    has_gapjum: bool       # ma_20w > ma_60w (정배열 가점)
    screened_at: str       # ISO 8601 UTC 문자열
    week_of: str           # 예: '2026-W16'
    sector: str = ""                       # KIND 업종명 (조회 실패 시 빈 문자열)
    ma_120w: Optional[float] = None       # 120주선 (데이터 부족 시 None)
    close_history: list[float] = field(default_factory=list)  # 최근 12주 종가
    per: Optional[float] = None           # Naver: PER (손실 시 None)
    pbr: Optional[float] = None           # Naver: PBR
    eps: Optional[float] = None           # Naver: EPS (원)
    dividend_yield: Optional[float] = None  # Naver: 배당수익률 (%)
    foreign_rate: Optional[float] = None   # Naver: 외국인 보유 비율 (%)
    volume_ratio_4w: Optional[float] = None  # 이번 주 거래량 / 직전 3주 평균
    high_w: Optional[float] = None          # 이번 주 고가 (Stage 2 S1-high 계산용)
    volume_w: Optional[int] = None          # 이번 주 거래량 (Stage 2 spike-vol 기준)
    foreign_net_buy: Optional[float] = None  # 외국인 순매수 (최근 거래일, Naver)
    inst_net_buy: Optional[float] = None     # 기관 순매수 (최근 거래일, Naver)


# ── KIND 섹터 매핑 ────────────────────────────────────────────
def fetch_kind_sector_map() -> dict[str, str]:
    """
    KIND(한국거래소 기업공시시스템)에서 KOSPI + KOSDAQ 전 종목 업종 매핑 조회.
    반환: {종목코드(6자리): 업종명} — 조회 실패 시 빈 dict 반환.
    """
    import httpx
    from bs4 import BeautifulSoup

    result: dict[str, str] = {}
    for market_type in ("stockMkt", "kosdaqMkt"):
        try:
            r = httpx.get(
                "https://kind.krx.co.kr/corpgeneral/corpList.do",
                params={"method": "download", "searchType": "13", "marketType": market_type},
                headers={"User-Agent": "Mozilla/5.0"},
                follow_redirects=True,
                timeout=20,
            )
            text = r.content.decode("euc-kr", errors="ignore")
            soup = BeautifulSoup(text, "html.parser")
            rows = soup.find_all("tr")
            if not rows:
                logger.warning("[스크리너] KIND %s — HTML에 tr 없음", market_type)
                continue
            headers = [td.get_text(strip=True) for td in rows[0].find_all(["th", "td"])]
            for row in rows[1:]:
                cols = [td.get_text(strip=True) for td in row.find_all(["th", "td"])]
                if len(cols) >= len(headers):
                    d = dict(zip(headers, cols))
                    code = d.get("종목코드", "").strip()
                    sector = d.get("업종", "").strip()
                    if code:
                        result[code] = sector
        except Exception as e:
            logger.warning("[스크리너] KIND 섹터 데이터 조회 실패 (%s): %s", market_type, e)
    logger.info("[스크리너] 섹터 매핑 %d건 로드", len(result))
    return result


# ── 티커 목록 ──────────────────────────────────────────────────
def get_all_tickers(sector_map: dict[str, str] | None = None) -> list[tuple[str, str, str]]:
    """KOSPI + KOSDAQ 전 종목 티커 조회.

    1순위: KRX Open API (KRX_OPENAPI_KEY 환경변수 설정 시)
    2순위: FinanceDataReader (fallback)

    반환: [(yfinance_symbol, name, sector), ...]
    예: [('005930.KS', '삼성전자', '전자부품'), ('035720.KQ', '카카오', '소프트웨어'), ...]
    """
    from datetime import date as _date

    # 1순위: KRX Open API
    import os
    if os.environ.get("KRX_OPENAPI_KEY"):
        try:
            from data.krx_openapi import KRXOpenAPIClient
            bas_dd = (_date.today() - _date.resolution).strftime("%Y%m%d")
            client = KRXOpenAPIClient()
            tickers = client.get_all_tickers(bas_dd)
            # sector 정보는 KRX Open API에 없음 — sector_map으로 보완
            results = [
                (sym, name, (sector_map or {}).get(sym.split(".")[0], ""))
                for sym, name, _ in tickers
            ]
            if results:
                logger.info("[스크리너] KRX Open API 종목 %d개 로드", len(results))
                return results
        except Exception as e:
            logger.warning("[스크리너] KRX Open API 조회 실패, FDR 폴백: %s", e)

    # 2순위: FinanceDataReader
    try:
        import FinanceDataReader as fdr
    except ImportError:
        logger.error("[스크리너] finance-datareader 미설치 — pip install finance-datareader")
        return []

    results = []
    for market, suffix in [("KOSPI", ".KS"), ("KOSDAQ", ".KQ")]:
        try:
            df = fdr.StockListing(market)
            for _, row in df.iterrows():
                code = str(row.get("Code", "")).zfill(6).strip()
                name = str(row.get("Name", code)).strip()
                if code:
                    sector = (sector_map or {}).get(code, "")
                    results.append((f"{code}{suffix}", name, sector))
        except Exception as e:
            logger.warning("[스크리너] %s 티커 목록 조회 실패: %s", market, e)

    logger.info("[스크리너] 전체 종목 %d개 (FinanceDataReader)", len(results))
    return results


# ── OHLCV 수집 ─────────────────────────────────────────────────
def fetch_weekly_ohlcv(ticker: str) -> Optional[pd.DataFrame]:
    """
    yfinance에서 주봉 3년치 OHLCV 조회.
    유효 데이터 부족(< 60행) 또는 실패 시 None 반환.

    yf.Ticker.history() 사용: 인스턴스 기반이라 스레드 간 데이터 오염 없음.
    (yf.download()는 내부 캐시/세션을 공유해 멀티스레드 환경에서 데이터 혼선 발생)
    """
    try:
        tkr = yf.Ticker(ticker)
        df = tkr.history(period="3y", interval="1wk", auto_adjust=True)
        if df.empty:
            return None
        # history()는 항상 flat 컬럼 반환하지만 방어적으로 처리
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        df.index = pd.to_datetime(df.index, utc=True)
        # NaN 미포함 유효 행 기준 60행 최소 요건
        if df["Close"].notna().sum() < 60:
            logger.debug("[스크리너] %s 유효 데이터 부족", ticker)
            return None
        return df
    except Exception as e:
        logger.debug("[스크리너] %s OHLCV 조회 실패: %s", ticker, e)
        return None


# ── 일목균형표 계산 ────────────────────────────────────────────
def calc_ichimoku(df: pd.DataFrame) -> pd.DataFrame:
    """
    ta 패키지로 일목균형표 계산.
    visual=False → 선행스팬이 현재 봉 기준 정렬 (앞당김 없음).
    df에 senkou_a, senkou_b, cloud_top, cloud_bottom,
         tenkan_sen, kijun_sen 컬럼 추가.
    """
    ichi = IchimokuIndicator(
        high=df["High"],
        low=df["Low"],
        visual=False,
    )
    df = df.copy()
    df["senkou_a"]   = ichi.ichimoku_a()
    df["senkou_b"]   = ichi.ichimoku_b()
    df["tenkan_sen"] = ichi.ichimoku_conversion_line()   # 전환선 (9-period)
    df["kijun_sen"]  = ichi.ichimoku_base_line()         # 기준선 (26-period)
    df["cloud_top"]    = df[["senkou_a", "senkou_b"]].max(axis=1)
    df["cloud_bottom"] = df[["senkou_a", "senkou_b"]].min(axis=1)
    return df


# ── 단일 종목 스크리닝 ─────────────────────────────────────────
def screen_ticker(ticker: str, name: str, sector: str = "") -> Optional[ScreenResult]:
    """
    단일 종목에 7-조건 스크리닝 실행.
    조건 통과 시 ScreenResult 반환, 미통과/오류 시 None.
    """
    df = fetch_weekly_ohlcv(ticker)
    if df is None:
        return None

    # ── 주봉 신선도 체크 ────────────────────────────────────────
    # 마지막 봉이 7일 이내여야 함 (yfinance 미확정 봉 방어)
    last_date = df.index[-1].date()
    today_utc = datetime.now(timezone.utc).date()
    if (today_utc - last_date).days > 7:
        logger.debug("[스크리너] %s 마지막 봉 stale (%s)", ticker, last_date)
        return None

    df = calc_ichimoku(df)

    # ── 이동평균선 ──────────────────────────────────────────────
    df["ma_20w"]  = df["Close"].rolling(20,  min_periods=20).mean()
    df["ma_60w"]  = df["Close"].rolling(60,  min_periods=60).mean()
    df["ma_120w"] = df["Close"].rolling(120, min_periods=100).mean()

    if len(df) < 2:
        return None

    cur  = df.iloc[-1]
    prev = df.iloc[-2]

    # 필수 컬럼 NaN 체크
    required = ["Close", "cloud_top", "cloud_bottom", "ma_20w", "ma_60w"]
    if any(pd.isna(cur[col]) for col in required):
        return None
    if pd.isna(prev["Close"]) or pd.isna(prev["cloud_top"]):
        return None
    if pd.isna(prev["ma_20w"]) or pd.isna(prev["ma_60w"]):
        return None

    close          = float(cur["Close"])
    cloud_top      = float(cur["cloud_top"])
    prev_close     = float(prev["Close"])
    prev_cloud_top = float(prev["cloud_top"])
    ma_20w         = float(cur["ma_20w"])
    ma_60w         = float(cur["ma_60w"])
    prev_ma_20w    = float(prev["ma_20w"])
    prev_ma_60w    = float(prev["ma_60w"])

    # 조건 G: 120주선 — 기본 NaN-pass; SCREENER_G_NAN_STRICT=1 시 NaN→fail
    ma_120w_raw   = cur.get("ma_120w", float("nan"))
    ma_120w_valid = not pd.isna(ma_120w_raw)
    ma_120w       = float(ma_120w_raw) if ma_120w_valid else None

    g_strict = os.environ.get("SCREENER_G_NAN_STRICT", "").lower() in ("1", "true", "yes")

    # ── 7개 조건 ────────────────────────────────────────────────
    A = close > cloud_top             # 이번 주 구름 상향 돌파
    B = prev_close <= prev_cloud_top  # 직전 주 구름 내/하부
    C = close > ma_20w                # 20주선 위
    D = close > ma_60w                # 60주선 위
    E = ma_20w > prev_ma_20w          # 20주선 우상향
    F = ma_60w > prev_ma_60w          # 60주선 우상향
    G = (ma_120w_valid and close > ma_120w) if g_strict else (
        (not ma_120w_valid) or (close > ma_120w))  # 120주선 위

    if not (A and B and C and D and E and F and G):
        return None

    # ── Enhanced 조건 (is_enhanced 플래그) ───────────────────────
    # H: 전환선(tenkan_sen) > 기준선(kijun_sen)
    # I: 전환선·기준선 모두 우상향
    tenkan_cur  = cur.get("tenkan_sen", float("nan"))
    kijun_cur   = cur.get("kijun_sen",  float("nan"))
    tenkan_prev = prev.get("tenkan_sen", float("nan"))
    kijun_prev  = prev.get("kijun_sen",  float("nan"))

    tenkan_ok = not pd.isna(tenkan_cur) and not pd.isna(tenkan_prev)
    kijun_ok  = not pd.isna(kijun_cur)  and not pd.isna(kijun_prev)

    if tenkan_ok and kijun_ok:
        H = float(tenkan_cur) > float(kijun_cur)           # 전환선 > 기준선
        I = (float(tenkan_cur) > float(tenkan_prev) and    # 전환선 상승
             float(kijun_cur)  > float(kijun_prev))        # 기준선 상승
        is_enhanced = H and I
    else:
        is_enhanced = False

    close_history = df["Close"].dropna().tail(12).tolist()

    # 거래량 비율: 이번 주 / 직전 3주 평균
    vol_series = df["Volume"].dropna()
    if len(vol_series) >= 4:
        recent_vol = float(vol_series.iloc[-1])
        avg_prior = float(vol_series.iloc[-4:-1].mean())
        volume_ratio_4w = round(recent_vol / avg_prior, 2) if avg_prior > 0 else None
    else:
        volume_ratio_4w = None

    # high_w / volume_w — 이번 주 고가·거래량 (Stage 2 S1 기준값)
    high_raw = cur.get("High", float("nan"))
    high_w = float(high_raw) if not pd.isna(high_raw) else None
    vol_raw = cur.get("Volume", float("nan"))
    volume_w = int(vol_raw) if not pd.isna(vol_raw) and vol_raw is not None else None

    result = ScreenResult(
        ticker=ticker,
        name=name,
        close=close,
        ma_20w=ma_20w,
        ma_60w=ma_60w,
        cloud_top=cloud_top,
        is_enhanced=is_enhanced,
        has_gapjum=(ma_20w > ma_60w),
        screened_at=datetime.now(timezone.utc).isoformat(),
        week_of=current_week_of(),
        sector=sector,
        ma_120w=ma_120w,
        close_history=close_history,
        volume_ratio_4w=volume_ratio_4w,
        high_w=high_w,
        volume_w=volume_w,
    )

    # 펀더멘털 보강 — Naver Integration API (캐시됨, ~0ms 재조회)
    krx_code = ticker.split(".")[0]
    if krx_code.isdigit() and len(krx_code) == 6:
        try:
            from data.market_data import _fetch_fundamental
            fund = _fetch_fundamental(krx_code)
            result.per = fund.get("per")
            result.pbr = fund.get("pbr")
            result.eps = fund.get("eps")
            result.dividend_yield = fund.get("dividend_yield")
            result.foreign_rate = fund.get("foreign_rate")
        except Exception as e:
            logger.debug("[스크리너] 펀더멘털 조회 실패 (%s): %s", ticker, e)

    return result


# ── 전체 스크리닝 실행 ─────────────────────────────────────────
def run_weekly_screen() -> list[ScreenResult]:
    """
    KOSPI + KOSDAQ 전 종목 주봉 스크리닝.
    ThreadPoolExecutor로 병렬 실행 (yfinance는 동기 라이브러리).
    반환: 조건 통과 종목 list[ScreenResult]
    """
    sector_map = fetch_kind_sector_map()
    tickers = get_all_tickers(sector_map)
    if not tickers:
        logger.warning("[스크리너] 종목 목록 비어있음")
        return []

    logger.info("[스크리너] 스크리닝 시작 — %d종목 / workers=%d", len(tickers), SCREENER_WORKERS)
    results: list[ScreenResult] = []
    failed = 0
    processed = 0

    with ThreadPoolExecutor(max_workers=SCREENER_WORKERS) as pool:
        futures = {pool.submit(screen_ticker, t, n, s): (t, n, s) for t, n, s in tickers}
        for future in as_completed(futures):
            processed += 1
            try:
                result = future.result()
                if result is not None:
                    results.append(result)
            except Exception as e:
                ticker_info = futures[future]
                logger.debug("[스크리너] %s 오류: %s", ticker_info[0], e)
                failed += 1

            if processed % 100 == 0:
                logger.info(
                    "[스크리너] 진행 %d/%d (통과:%d)",
                    processed, len(tickers), len(results),
                )

    logger.info(
        "[스크리너] 완료 — 전체:%d 통과:%d 실패:%d",
        len(tickers), len(results), failed,
    )
    return results


# ── 단독 테스트 ───────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
    )
    results = run_weekly_screen()
    print(f"\n{'=' * 60}")
    print(f"스크리닝 결과: {len(results)}종목 통과")
    print("=" * 60)
    for r in sorted(results, key=lambda x: (not x.has_gapjum, -x.close)):
        star = " ★" if r.has_gapjum else ""
        print(
            f"  {r.name} ({r.ticker}){star}  "
            f"종가:{r.close:,.0f}  "
            f"20주:{r.ma_20w:,.0f}  "
            f"60주:{r.ma_60w:,.0f}  "
            f"구름상단:{r.cloud_top:,.0f}"
        )
