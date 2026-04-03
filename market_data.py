"""
market_data.py  —  시세 데이터 조회 모듈
────────────────────────────────────────────────────────────
뉴스 신호에서 추출된 종목/지수명을 실제 ticker로 매핑 후
yfinance로 시세 조회 (한국/미국/지수/원자재 모두 지원).

pykrx는 Python 3.14 미지원으로 yfinance로 통일.
한국 주식: 종목코드.KS (예: 005930.KS)
한국 지수: ^KS11 (KOSPI), ^KQ11 (KOSDAQ)

주요 기능:
    get_price_context()          — 종목명 리스트 → 시세 컨텍스트 반환
    cross_analyze()              — 뉴스 신호 + 시세 교차 분석 → 강화/약화/필터 판정
    fetch_and_store_daily_ohlcv()— 1년치 일봉 수집 → DB 저장
    export_daily_ohlcv()         — DB 데이터 → CSV/Excel/JSON 내보내기

CLI 사용법:
    python market_data.py --daily [심볼...]           일봉 수집 → DB
    python market_data.py --export [--format csv|xlsx|json] [심볼...]  내보내기
    python market_data.py --help                      도움말
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import yfinance as yf
    YFINANCE_OK = True
except ImportError:
    YFINANCE_OK = False
    logger.warning("yfinance 미설치 — pip install yfinance")

PYKRX_OK = False  # Python 3.14 미지원 — yfinance로 대체


# ── 티커 매핑 테이블 ─────────────────────────────────────────
YFINANCE_MAP: dict[str, str] = {
    # ── 한국 지수 ──────────────────────────────────────────
    "코스피": "^KS11", "kospi": "^KS11",
    "코스닥": "^KQ11", "kosdaq": "^KQ11",
    "krx100": "^KRX100", "krx 100": "^KRX100",

    # ── 한국 주요 종목 ─────────────────────────────────────
    "삼성전자": "005930.KS",
    "sk하이닉스": "000660.KS", "skhynix": "000660.KS", "sk 하이닉스": "000660.KS",
    "lg화학": "051910.KS", "lg 화학": "051910.KS",
    "lg에너지솔루션": "373220.KS", "lg에너지": "373220.KS",
    "현대차": "005380.KS", "현대자동차": "005380.KS",
    "기아": "000270.KS", "기아차": "000270.KS",
    "카카오": "035720.KS",
    "카카오뱅크": "323410.KS",
    "네이버": "035420.KS", "naver": "035420.KS",
    "셀트리온": "068270.KS",
    "삼성바이오로직스": "207940.KS", "삼성바이오": "207940.KS",
    "포스코": "005490.KS", "posco": "005490.KS", "포스코홀딩스": "005490.KS",
    "한국전력": "015760.KS", "kepco": "015760.KS",
    "아시아나": "020560.KS", "아시아나항공": "020560.KS",
    "대한항공": "003490.KS",
    "크래프톤": "259960.KS",
    "한화에어로스페이스": "012450.KS", "한화에어로": "012450.KS",
    "두산에너빌리티": "034020.KS",
    "삼성sdi": "006400.KS", "삼성 sdi": "006400.KS",
    "고려아연": "010130.KS",

    # ── 미국 지수 ──────────────────────────────────────────
    "s&p500": "^GSPC", "s&p 500": "^GSPC", "spx": "^GSPC",
    "나스닥": "^IXIC", "nasdaq": "^IXIC", "ixic": "^IXIC",
    "다우": "^DJI", "dow": "^DJI",
    "vix": "^VIX",

    # ── 미국 주요 종목 ─────────────────────────────────────
    "nvda": "NVDA", "nvidia": "NVDA",
    "aapl": "AAPL", "apple": "AAPL",
    "msft": "MSFT", "microsoft": "MSFT",
    "amzn": "AMZN", "amazon": "AMZN",
    "googl": "GOOGL", "google": "GOOGL", "alphabet": "GOOGL",
    "meta": "META", "facebook": "META",
    "tsla": "TSLA", "tesla": "TSLA",
    "intu": "INTU", "intuit": "INTU",
    "smci": "SMCI", "super micro": "SMCI", "supermicro": "SMCI",
    "xom": "XOM", "exxon": "XOM", "exxonmobil": "XOM",
    "cvx": "CVX", "chevron": "CVX",
    "arm": "ARM",
    "amd": "AMD",
    "intc": "INTC", "intel": "INTC",
    "ual": "UAL", "united airlines": "UAL",
    "dal": "DAL", "delta": "DAL", "delta air": "DAL",
    "fedex": "FDX", "fdx": "FDX",
    "openai": "MSFT",  # 비상장 — 노출도 높은 msft로 proxy
    "jpmorgan": "JPM", "jpm": "JPM",
    "gs": "GS", "goldman sachs": "GS", "goldman": "GS",

    # ── 원자재 ─────────────────────────────────────────────
    "유가": "CL=F", "oil": "CL=F", "wti": "CL=F",
    "금": "GC=F", "gold": "GC=F",
    "은": "SI=F", "silver": "SI=F",
    "구리": "HG=F", "copper": "HG=F",
}

# pykrx 테이블 — 미사용 (하위 호환 유지용 빈 dict)
PYKRX_MAP: dict[str, str] = {}
PYKRX_INDEX_MAP: dict[str, str] = {}


# ── 시세 데이터 클래스 ────────────────────────────────────────

@dataclass
class PriceContext:
    ticker: str          # 원본 종목명
    symbol: str          # 실제 조회 심볼
    source: str          # "yfinance" | "pykrx"
    current: float       # 현재가
    change_pct: float    # 등락률 (%)
    rsi: Optional[float] # RSI (14일, 가능한 경우)
    volume_ratio: Optional[float]  # 거래량 비율 (현재/평균)
    week52_high: Optional[float]   # 52주 최고가
    week52_low: Optional[float]    # 52주 최저가
    volume_surge: bool             # 거래량 급증 (현재 거래량 ≥ 20일 평균의 2배)
    success: bool

    @property
    def near_52w_high(self) -> bool:
        """현재가가 52주 최고가의 95% 이상"""
        return bool(self.week52_high and self.current >= self.week52_high * 0.95)

    @property
    def near_52w_low(self) -> bool:
        """현재가가 52주 최저가의 105% 이하"""
        return bool(self.week52_low and self.current <= self.week52_low * 1.05)

    @property
    def trend(self) -> str:
        if self.change_pct >= 2:   return "급등"
        if self.change_pct >= 0.5: return "상승"
        if self.change_pct <= -2:  return "급락"
        if self.change_pct <= -0.5: return "하락"
        return "보합"

    @property
    def rsi_status(self) -> str:
        if not self.rsi: return ""
        if self.rsi >= 70: return "과매수"
        if self.rsi <= 30: return "과매도"
        return "중립"


# ── RSI 계산 ─────────────────────────────────────────────────

def _calc_rsi(closes: list[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains  = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period-1) + gains[i]) / period
        avg_loss = (avg_loss * (period-1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)


# ── yfinance 조회 ─────────────────────────────────────────────

def _fetch_yfinance(symbol: str, ticker_name: str) -> PriceContext:
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="1y")  # 52주 고/저가 계산을 위해 1년치 조회
        if hist.empty:
            raise ValueError("데이터 없음")
        closes = hist["Close"].tolist()
        volumes = hist["Volume"].tolist()
        current = closes[-1]
        prev    = closes[-2] if len(closes) >= 2 else current
        change_pct = round((current - prev) / prev * 100, 2)
        rsi = _calc_rsi(closes)

        # 거래량 비율 및 급증 판정
        avg_vol = sum(volumes[-20:]) / len(volumes[-20:]) if len(volumes) >= 5 else None
        vol_ratio = round(volumes[-1] / avg_vol, 2) if avg_vol else None
        volume_surge = bool(vol_ratio and vol_ratio >= 2.0)

        # 52주 고/저가
        week52_high = round(max(closes), 2) if closes else None
        week52_low  = round(min(closes), 2) if closes else None

        return PriceContext(
            ticker=ticker_name, symbol=symbol, source="yfinance",
            current=round(current, 2), change_pct=change_pct,
            rsi=rsi, volume_ratio=vol_ratio,
            week52_high=week52_high, week52_low=week52_low,
            volume_surge=volume_surge, success=True,
        )
    except Exception as e:
        logger.debug("[시세] yfinance 실패 (%s): %s", symbol, e)
        return PriceContext(ticker=ticker_name, symbol=symbol, source="yfinance",
                            current=0, change_pct=0, rsi=None, volume_ratio=None,
                            week52_high=None, week52_low=None, volume_surge=False, success=False)


# ── pykrx 조회 ────────────────────────────────────────────────

def _fetch_pykrx_stock(code: str, ticker_name: str) -> PriceContext:
    try:
        today = datetime.now().strftime("%Y%m%d")
        df = krx.get_market_ohlcv_by_date("20250101", today, code)
        if df.empty:
            raise ValueError("데이터 없음")
        closes  = df["종가"].tolist()
        volumes = df["거래량"].tolist()
        current = closes[-1]
        prev    = closes[-2] if len(closes) >= 2 else current
        change_pct = round((current - prev) / prev * 100, 2)
        rsi = _calc_rsi(closes)
        vol_ratio = round(volumes[-1] / (sum(volumes[-20:]) / len(volumes[-20:])), 2) if len(volumes) >= 5 else None

        week52_high = round(max(closes), 0) if closes else None
        week52_low  = round(min(closes), 0) if closes else None
        volume_surge = bool(vol_ratio and vol_ratio >= 2.0)

        return PriceContext(
            ticker=ticker_name, symbol=code, source="pykrx",
            current=round(current, 0), change_pct=change_pct,
            rsi=rsi, volume_ratio=vol_ratio,
            week52_high=week52_high, week52_low=week52_low,
            volume_surge=volume_surge, success=True,
        )
    except Exception as e:
        logger.debug("[시세] pykrx 주식 실패 (%s): %s", code, e)
        return PriceContext(ticker=ticker_name, symbol=code, source="pykrx",
                            current=0, change_pct=0, rsi=None, volume_ratio=None,
                            week52_high=None, week52_low=None, volume_surge=False, success=False)


def _fetch_pykrx_index(code: str, ticker_name: str) -> PriceContext:
    try:
        today = datetime.now().strftime("%Y%m%d")
        df = krx.get_index_ohlcv_by_date("20250101", today, code)
        if df.empty:
            raise ValueError("데이터 없음")
        closes = df["종가"].tolist()
        current = closes[-1]
        prev    = closes[-2] if len(closes) >= 2 else current
        change_pct = round((current - prev) / prev * 100, 2)
        rsi = _calc_rsi(closes)

        week52_high = round(max(closes), 2) if closes else None
        week52_low  = round(min(closes), 2) if closes else None

        return PriceContext(
            ticker=ticker_name, symbol=code, source="pykrx",
            current=round(current, 2), change_pct=change_pct,
            rsi=rsi, volume_ratio=None,
            week52_high=week52_high, week52_low=week52_low,
            volume_surge=False, success=True,
        )
    except Exception as e:
        logger.debug("[시세] pykrx 지수 실패 (%s): %s", code, e)
        return PriceContext(ticker=ticker_name, symbol=code, source="pykrx",
                            current=0, change_pct=0, rsi=None, volume_ratio=None,
                            week52_high=None, week52_low=None, volume_surge=False, success=False)


# ── 종목명 → PriceContext 매핑 ────────────────────────────────

def get_price_context(
    tickers: list[str],
    symbols: dict[str, str] | None = None,
) -> list[PriceContext]:
    """
    뉴스 신호에서 추출된 종목명 리스트 → 시세 컨텍스트 리스트.

    symbols: LLM이 제공한 name→yfinance심볼 매핑 (있으면 최우선 사용).
             예) {"삼성전자": "005930.KS", "S&P500": "^GSPC"}
    매핑 실패 종목은 조용히 건너뜀.
    """
    symbols = symbols or {}
    results = []
    for raw in tickers:
        key       = raw.lower().strip()           # 예: "SK 하이닉스" → "sk 하이닉스"
        key_nsp   = key.replace(" ", "")          # 공백 제거본: "sk하이닉스"

        # 0. LLM이 제공한 심볼 최우선 (매핑 테이블 불필요)
        llm_symbol = symbols.get(raw, "").strip()
        if llm_symbol and YFINANCE_OK:
            ctx = _fetch_yfinance(llm_symbol, raw)
            if ctx.success:
                logger.debug("[매핑] LLM 심볼 사용: %s → %s", raw, llm_symbol)
                results.append(ctx)
                continue
            logger.debug("[매핑] LLM 심볼 조회 실패: %s (%s) — 기존 맵으로 fallback", raw, llm_symbol)

        # 1. pykrx 지수
        if key in PYKRX_INDEX_MAP and PYKRX_OK:
            ctx = _fetch_pykrx_index(PYKRX_INDEX_MAP[key], raw)
            if ctx.success:
                results.append(ctx)
                continue

        # 2. pykrx 개별 주식
        if key in PYKRX_MAP and PYKRX_OK:
            ctx = _fetch_pykrx_stock(PYKRX_MAP[key], raw)
            if ctx.success:
                results.append(ctx)
                continue

        # 3. yfinance — 원본 key 시도 후, 공백 제거본(key_nsp)으로 재시도
        symbol = YFINANCE_MAP.get(key) or YFINANCE_MAP.get(key_nsp)
        if symbol and YFINANCE_OK:
            ctx = _fetch_yfinance(symbol, raw)
            if ctx.success:
                results.append(ctx)
                continue
            logger.debug("[매핑] yfinance 조회 실패: %s (%s)", raw, symbol)

        # 4. yfinance 직접 시도 (대문자 티커로 추정)
        elif raw.isupper() and len(raw) <= 5 and YFINANCE_OK:
            ctx = _fetch_yfinance(raw, raw)
            if ctx.success:
                results.append(ctx)
                continue

        # 5. 매핑 실패 로깅
        if not symbol and not (raw.isupper() and len(raw) <= 5) and not llm_symbol:
            logger.debug("[매핑] 티커 매핑 실패: '%s' (key='%s')", raw, key)

    return results


# ── 교차 분석 ─────────────────────────────────────────────────

@dataclass
class CrossAnalysis:
    verdict: str          # "CONFIRM" | "CAUTION" | "FILTER" | "NEUTRAL"
    score: int            # 0~10 (신호 신뢰도)
    summary: str          # 한 줄 요약
    price_contexts: list[PriceContext]
    confirm_count: int = 0
    conflict_count: int = 0


def cross_analyze(
    direction: str,                          # "BUY" | "SELL" | "WATCH"
    strength: int,                           # 1~5
    tickers: list[str],
    ticker_symbols: dict[str, str] | None = None,  # LLM 제공 심볼
) -> CrossAnalysis:
    """
    뉴스 신호 + 시세 교차 분석.

    판정 기준:
        CONFIRM  — 뉴스와 시세 방향 일치 → 신호 강화
        CAUTION  — 뉴스와 시세 방향 반대 → 주의
        FILTER   — 강한 역방향 시세 → 노이즈 필터
        NEUTRAL  — 시세 데이터 없거나 보합
    """
    contexts = get_price_context(tickers, symbols=ticker_symbols)

    if not contexts:
        return CrossAnalysis(
            verdict="NEUTRAL", score=strength * 2,
            summary="시세 데이터 없음 — 뉴스 신호만 참고",
            price_contexts=[],
        )

    confirm_count = 0
    conflict_count = 0
    details = []

    logger.debug(
        "[교차분석] 시작 — 방향:%s 강도:%d 종목:%s",
        direction, strength, tickers,
    )

    for ctx in contexts:
        prev_confirm  = confirm_count
        prev_conflict = conflict_count

        if direction == "BUY":
            if ctx.change_pct >= 0.5:
                confirm_count += 1
            elif ctx.change_pct <= -2:
                conflict_count += 1
            if ctx.rsi and ctx.rsi <= 30:
                confirm_count += 1   # 과매도(RSI≤30) = 반등 기대
            if ctx.near_52w_low:
                confirm_count += 1   # 52주 최저가 근처 = 저점 매수 기대
            if ctx.near_52w_high:
                conflict_count += 1  # 52주 최고가 근처 = 추가 상승 제한적
        elif direction == "SELL":
            if ctx.change_pct <= -0.5:
                confirm_count += 1
            elif ctx.change_pct >= 2:
                conflict_count += 1
            if ctx.rsi and ctx.rsi >= 70:
                confirm_count += 1   # 과매수(RSI≥70) = 하락 기대
            if ctx.near_52w_high:
                confirm_count += 1   # 52주 최고가 근처 = 차익실현 압력
            if ctx.near_52w_low:
                conflict_count += 1  # 52주 최저가 근처 = 추가 하락 제한적
        elif direction == "WATCH":
            # WATCH는 방향 무관, 변동성 ±0.5% 이상일 때만 유의미 신호로 판정
            if abs(ctx.change_pct) >= 0.5:
                confirm_count += 1
            elif abs(ctx.change_pct) < 0.2:
                conflict_count += 1  # 완전 보합이면 오히려 신호 약화

        # 거래량 급증 — 방향 무관하게 신호 신뢰도 강화
        if ctx.volume_surge:
            confirm_count += 1
            logger.debug("[교차분석]   %-12s  거래량 급증(×%.1f) → confirm+1", ctx.ticker, ctx.volume_ratio or 0)

        # 종목별 판정 로그
        c_delta = confirm_count  - prev_confirm
        x_delta = conflict_count - prev_conflict
        tag = (
            f"+confirm×{c_delta}" if c_delta  > 0 else
            f"+conflict×{x_delta}" if x_delta > 0 else
            "neutral"
        )
        rsi_str = f" RSI {ctx.rsi}" if ctx.rsi else ""
        logger.debug(
            "[교차분석]   %-12s  %+.1f%%%s (%s)  → %s",
            ctx.ticker, ctx.change_pct, rsi_str, ctx.trend, tag,
        )

        details.append(
            f"{ctx.ticker} {ctx.change_pct:+.1f}%{rsi_str} ({ctx.trend})"
        )

    base_score = strength * 2  # 2~10 (strength 1~5)
    total      = confirm_count + conflict_count

    logger.debug(
        "[교차분석] 집계 — confirm:%d conflict:%d base_score:%d",
        confirm_count, conflict_count, base_score,
    )

    if confirm_count > conflict_count:
        # confirm 비율로 보정 — 종목 수에 비례한 과대평가 방지
        ratio   = confirm_count / total if total else 1.0
        bonus   = round(ratio * 2)           # 최대 +2점
        score   = min(10, base_score + bonus)
        verdict = "CONFIRM"
        summary = f"뉴스+시세 방향 일치 ({', '.join(details[:2])})"
        logger.debug(
            "[교차분석] → CONFIRM  ratio=%.2f bonus=%d score=%d",
            ratio, bonus, score,
        )

    elif conflict_count > confirm_count:
        # verdict 먼저 결정 후 score 조정
        # FILTER 조건: conflict 2개 이상 AND 비율 75% 이상 (단일 conflict는 CAUTION)
        conflict_ratio = conflict_count / total if total else 1.0
        if conflict_count >= 2 and conflict_ratio >= 0.75:
            verdict = "FILTER"
            score   = max(0, base_score - conflict_count * 2)
            logger.debug(
                "[교차분석] → FILTER  conflict_ratio=%.2f score=%d",
                conflict_ratio, score,
            )
        else:
            verdict = "CAUTION"
            score   = max(1, base_score - conflict_count)
            logger.debug(
                "[교차분석] → CAUTION  conflict_ratio=%.2f score=%d",
                conflict_ratio, score,
            )
        summary = f"시세 역방향 — 주의 ({', '.join(details[:2])})"

    else:
        score   = base_score
        verdict = "NEUTRAL"
        summary = f"시세 보합 ({', '.join(details[:2])})"
        logger.debug(
            "[교차분석] → NEUTRAL  confirm==conflict score=%d",
            score,
        )

    logger.info(
        "[교차분석] 최종 — %s %s 점수:%d/10 | confirm:%d conflict:%d | %s",
        {"CONFIRM": "✅", "CAUTION": "⚠️", "FILTER": "🚫", "NEUTRAL": "➖"}.get(verdict, ""),
        verdict, score, confirm_count, conflict_count, summary[:60],
    )

    return CrossAnalysis(
        verdict=verdict, score=score,
        summary=summary, price_contexts=contexts,
        confirm_count=confirm_count, conflict_count=conflict_count,
    )


# ── 1년치 일봉 OHLCV 수집 & DB 저장 ─────────────────────────

def _classify_market(symbol: str) -> str:
    """심볼로 마켓 분류."""
    if symbol.endswith((".KS", ".KQ")):
        return "KR"
    if symbol.startswith("^"):
        return "IDX"
    if symbol.endswith("=F"):
        return "CMD"
    return "US"


def _normalize_symbol(symbol: str) -> str:
    """한국 종목코드(6자리 숫자)에 .KS/.KQ 접미사 자동 부여."""
    import re
    if re.fullmatch(r"\d{6}", symbol):
        # .KS(코스피) 먼저 시도, 실패 시 .KQ(코스닥)
        for suffix in (".KS", ".KQ"):
            candidate = symbol + suffix
            try:
                t = yf.Ticker(candidate)
                hist = t.history(period="5d", interval="1d")
                if not hist.empty:
                    logger.info("[일봉] %s → %s 자동 매핑", symbol, candidate)
                    return candidate
            except Exception:
                continue
        logger.warning("[일봉] %s — .KS/.KQ 모두 실패, 원본 사용", symbol)
    return symbol


def _fetch_daily_ohlcv(symbol: str) -> list[dict]:
    """yfinance로 1년치 일봉 OHLCV를 가져와 dict 리스트로 반환."""
    if not YFINANCE_OK:
        logger.error("[일봉] yfinance 미설치")
        return []
    try:
        symbol = _normalize_symbol(symbol)
        t = yf.Ticker(symbol)
        hist = t.history(period="1y", interval="1d")
        if hist.empty:
            logger.warning("[일봉] 데이터 없음: %s", symbol)
            return []

        market = _classify_market(symbol)
        rows = []
        for idx, row in hist.iterrows():
            dt = idx.date() if hasattr(idx, "date") else idx
            rows.append({
                "symbol": symbol,
                "market": market,
                "date": dt,
                "open": float(row["Open"]) if row.get("Open") is not None else None,
                "high": float(row["High"]) if row.get("High") is not None else None,
                "low": float(row["Low"]) if row.get("Low") is not None else None,
                "close": float(row["Close"]),
                "volume": int(row["Volume"]) if row.get("Volume") is not None else None,
                "source": "yfinance",
            })
        logger.info("[일봉] %s — %d일치 조회 완료 (%s ~ %s)",
                     symbol, len(rows),
                     rows[0]["date"] if rows else "?",
                     rows[-1]["date"] if rows else "?")
        return rows
    except Exception as e:
        logger.error("[일봉] %s 조회 실패: %s", symbol, e)
        return []


async def _check_db_freshness(pool, symbol: str, max_age_days: int = 1) -> tuple[bool, str]:
    """
    DB에 저장된 일봉 데이터가 최신인지 확인한다.

    Returns:
        (is_fresh, last_date_str)
        - is_fresh: True면 수집 불필요 (최근 거래일 데이터 존재)
        - last_date_str: DB 최신 날짜 문자열 (없으면 "없음")
    """
    import db
    from datetime import date, timedelta

    rows = await db.fetch_daily_ohlcv(pool, symbol, limit=1)
    if not rows:
        return False, "없음"

    last_date = rows[0]["date"]
    last_str = str(last_date)

    # 오늘 기준 영업일 판단 (주말/공휴일 고려)
    today = date.today()
    weekday = today.weekday()  # 0=월 ~ 6=일

    # 최근 거래일 추정: 오늘이 월요일이면 금요일, 주말이면 금요일
    if weekday == 0:    # 월요일 → 금요일 데이터까지 OK
        latest_expected = today - timedelta(days=3)
    elif weekday == 6:  # 일요일 → 금요일
        latest_expected = today - timedelta(days=2)
    elif weekday == 5:  # 토요일 → 금요일
        latest_expected = today - timedelta(days=1)
    else:               # 화~금 → 전일 (장 마감 전이면 전전일도 OK)
        latest_expected = today - timedelta(days=max_age_days)

    is_fresh = last_date >= latest_expected
    return is_fresh, last_str


async def fetch_and_store_daily_ohlcv(
    pool,
    symbols: list[str] | None = None,
    force: bool = False,
) -> dict[str, int]:
    """
    1년치 일봉 OHLCV를 수집하여 DB에 저장.
    DB에 최신 데이터가 있으면 건너뛴다 (force=True면 강제 재수집).

    Args:
        pool: asyncpg 커넥션 풀
        symbols: 수집할 심볼 리스트. None이면 YFINANCE_MAP의 모든 고유 심볼.
        force: True면 DB 캐시 무시하고 강제 재수집

    Returns:
        {symbol: 저장건수} dict  (DB 캐시 사용 시 -1)
    """
    import db  # lazy import — 순환 참조 방지

    if symbols is None:
        symbols = sorted(set(YFINANCE_MAP.values()))

    results = {}
    skipped = 0
    fetched = 0
    total = len(symbols)

    for i, sym in enumerate(symbols, 1):
        # DB 캐시 확인
        if not force:
            is_fresh, last_date = await _check_db_freshness(pool, sym)
            if is_fresh:
                logger.info("[일봉] (%d/%d) %s — DB 최신 (%s) ✓ 건너뜀",
                            i, total, sym, last_date)
                results[sym] = -1   # -1 = 캐시 사용
                skipped += 1
                continue
            else:
                logger.info("[일봉] (%d/%d) %s — DB 마지막: %s → API 수집",
                            i, total, sym, last_date)
        else:
            logger.info("[일봉] (%d/%d) %s — 강제 재수집", i, total, sym)

        rows = _fetch_daily_ohlcv(sym)
        if rows:
            count = await db.save_daily_ohlcv(pool, rows)
            results[sym] = count
            fetched += 1
        else:
            results[sym] = 0

    api_saved = sum(v for v in results.values() if v > 0)
    logger.info(
        "[일봉] 완료 — %d종목 중 API수집 %d개(%d건 저장) / DB캐시 %d개 건너뜀",
        total, fetched, api_saved, skipped,
    )
    return results


# ── DB 데이터 내보내기 ──────────────────────────────────────────

async def export_daily_ohlcv(
    symbols: list[str] | None = None,
    fmt: str = "csv",
    out_dir: str = "exports",
) -> str:
    """
    DB에 저장된 일봉 OHLCV 데이터를 파일로 내보낸다.

    Args:
        symbols: 내보낼 심볼 리스트. None이면 DB에 저장된 모든 종목.
        fmt: 출력 포맷 — "csv" | "xlsx" | "json"
        out_dir: 출력 디렉터리 (기본 exports/)

    Returns:
        저장된 파일 경로
    """
    import os
    import pandas as pd
    import db

    pool = await db.create_pool()
    await db.init_db(pool)

    try:
        # 내보낼 심볼 결정
        if symbols:
            target_symbols = symbols
        else:
            sym_info = await db.get_daily_ohlcv_symbols(pool)
            target_symbols = [s["symbol"] for s in sym_info]

        if not target_symbols:
            print("  ⚠️  DB에 저장된 일봉 데이터가 없습니다.")
            return ""

        # 역방향 매핑: yfinance 심볼 → 한글 이름
        reverse_map: dict[str, str] = {}
        for name, sym in YFINANCE_MAP.items():
            if sym not in reverse_map:
                reverse_map[sym] = name

        # 전 종목 데이터 수집
        all_frames = []
        for sym in sorted(target_symbols):
            rows = await db.fetch_daily_ohlcv(pool, sym, limit=365)
            if not rows:
                continue
            df = pd.DataFrame(rows)
            df["name"] = reverse_map.get(sym, "")
            all_frames.append(df)

        if not all_frames:
            print("  ⚠️  내보낼 데이터가 없습니다.")
            return ""

        combined = pd.concat(all_frames, ignore_index=True)

        # 컬럼 정리 및 정렬
        col_order = ["symbol", "name", "market", "date", "open", "high", "low", "close", "volume", "source"]
        for c in col_order:
            if c not in combined.columns:
                combined[c] = None
        combined = combined[col_order].sort_values(["symbol", "date"]).reset_index(drop=True)

        # 출력 디렉터리 생성
        os.makedirs(out_dir, exist_ok=True)
        date_str = datetime.now().strftime("%Y%m%d_%H%M")

        # 포맷별 저장
        fmt = fmt.lower()
        if fmt == "xlsx":
            filepath = os.path.join(out_dir, f"daily_ohlcv_{date_str}.xlsx")
            # 종목별 시트 + 통합 시트
            with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
                combined.to_excel(writer, sheet_name="전체", index=False)
                for sym in sorted(combined["symbol"].unique()):
                    sheet_df = combined[combined["symbol"] == sym].copy()
                    name_label = reverse_map.get(sym, sym)
                    # 시트 이름 31자 제한 & 특수문자 제거
                    safe_sheet = name_label[:28].replace("/", "_").replace("\\", "_")
                    safe_sheet = safe_sheet.replace(":", "").replace("*", "").replace("?", "")
                    safe_sheet = safe_sheet.replace("[", "(").replace("]", ")")
                    sheet_df.to_excel(writer, sheet_name=safe_sheet, index=False)

        elif fmt == "json":
            filepath = os.path.join(out_dir, f"daily_ohlcv_{date_str}.json")
            # date를 문자열로 변환
            combined["date"] = combined["date"].astype(str)
            combined.to_json(filepath, orient="records", force_ascii=False, indent=2)

        else:  # csv (기본값)
            filepath = os.path.join(out_dir, f"daily_ohlcv_{date_str}.csv")
            combined.to_csv(filepath, index=False, encoding="utf-8-sig")

        n_symbols = combined["symbol"].nunique()
        n_rows = len(combined)
        print(f"\n  ✅ 내보내기 완료")
        print(f"     포맷:  {fmt.upper()}")
        print(f"     종목:  {n_symbols}개")
        print(f"     데이터: {n_rows:,}건")
        print(f"     파일:  {filepath}")
        return filepath

    finally:
        await pool.close()


# ── 단독 테스트 ───────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import asyncio
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # --- 사용법 안내 ---
    def _print_usage():
        print("""
사용법:
    python market_data.py --daily [옵션] [심볼...]    일봉 OHLCV 수집 → DB 저장
    python market_data.py --export [옵션] [심볼...]   DB 데이터 → 파일 내보내기
    python market_data.py                             교차분석 테스트

--daily 옵션:
    --force                   DB 캐시 무시, 강제 재수집

--export 옵션:
    --format csv|xlsx|json    출력 포맷 (기본: csv)
    --out-dir <폴더>          출력 디렉터리 (기본: exports/)

예시:
    python market_data.py --daily                     전체 종목 수집 (DB에 있으면 건너뜀)
    python market_data.py --daily --force             전체 종목 강제 재수집
    python market_data.py --daily NVDA AAPL           지정 종목만 수집
    python market_data.py --daily --force NVDA        지정 종목 강제 재수집
    python market_data.py --export                    전체 CSV 내보내기
    python market_data.py --export --format xlsx      Excel 내보내기
    python market_data.py --export --format json      JSON 내보내기
    python market_data.py --export NVDA 005930.KS     지정 종목만 내보내기
    python market_data.py --export --format xlsx --out-dir ./data  폴더 지정
        """)

    if "--help" in sys.argv or "-h" in sys.argv:
        _print_usage()
        sys.exit(0)

    # --- 일봉 OHLCV 수집 모드 ---
    if "--daily" in sys.argv:
        idx = sys.argv.index("--daily")
        rest = sys.argv[idx+1:]
        force = "--force" in rest
        symbols = [s for s in rest if not s.startswith("--")] or None

        async def _run_daily():
            import db
            pool = await db.create_pool()
            await db.init_db(pool)
            result = await fetch_and_store_daily_ohlcv(pool, symbols or None, force=force)

            # 결과 분류
            cached  = {s: v for s, v in result.items() if v == -1}
            saved   = {s: v for s, v in result.items() if v > 0}
            no_data = {s: v for s, v in result.items() if v == 0}

            print(f"\n{'='*60}")
            print(f"  일봉 OHLCV 수집 결과 — {len(result)}종목")
            print(f"{'='*60}")

            if saved:
                print(f"\n  📥 API 수집 ({len(saved)}종목)")
                for sym, cnt in sorted(saved.items()):
                    print(f"     {sym:16s} : {cnt:>4d}건 저장")

            if cached:
                print(f"\n  ✅ DB 캐시 최신 ({len(cached)}종목) — 건너뜀")
                for sym in sorted(cached.keys()):
                    print(f"     {sym}")

            if no_data:
                print(f"\n  ⚠️  데이터 없음 ({len(no_data)}종목)")
                for sym in sorted(no_data.keys()):
                    print(f"     {sym}")

            print(f"\n{'─'*60}")
            print(f"  API 수집: {len(saved)}종목 ({sum(saved.values()):,}건)")
            print(f"  DB 캐시:  {len(cached)}종목 (건너뜀)")
            print(f"  실패:     {len(no_data)}종목")
            print(f"{'='*60}")
            await pool.close()

        asyncio.run(_run_daily())
        sys.exit(0)

    # --- 내보내기 모드 ---
    if "--export" in sys.argv:
        idx = sys.argv.index("--export")
        rest = sys.argv[idx+1:]

        # 옵션 파싱
        export_fmt = "csv"
        export_dir = "exports"
        export_symbols = []

        i = 0
        while i < len(rest):
            if rest[i] == "--format" and i + 1 < len(rest):
                export_fmt = rest[i + 1]
                i += 2
            elif rest[i] == "--out-dir" and i + 1 < len(rest):
                export_dir = rest[i + 1]
                i += 2
            elif rest[i].startswith("--"):
                i += 1  # 알 수 없는 옵션 건너뜀
            else:
                export_symbols.append(rest[i])
                i += 1

        if export_fmt not in ("csv", "xlsx", "json"):
            print(f"  ❌ 지원하지 않는 포맷: {export_fmt}")
            print(f"     사용 가능: csv, xlsx, json")
            sys.exit(1)

        print(f"\n{'='*60}")
        print(f"  일봉 OHLCV 내보내기 — {export_fmt.upper()}")
        print(f"{'='*60}")

        asyncio.run(export_daily_ohlcv(
            symbols=export_symbols or None,
            fmt=export_fmt,
            out_dir=export_dir,
        ))
        sys.exit(0)

    # --- 기존 교차분석 테스트 ---
    print("\n" + "="*60)
    print("시세 데이터 + 교차 분석 테스트")
    print("="*60)

    TEST_CASES = [
        {"direction": "SELL", "strength": 4, "tickers": ["삼성전자", "코스피", "NVDA"]},
        {"direction": "BUY",  "strength": 3, "tickers": ["S&P500", "나스닥", "유가"]},
        {"direction": "WATCH","strength": 3, "tickers": ["금", "코스닥"]},
    ]

    for tc in TEST_CASES:
        print(f"\n[테스트] {tc['direction']} 강도:{tc['strength']} 종목:{tc['tickers']}")
        result = cross_analyze(tc["direction"], tc["strength"], tc["tickers"])
        print(f"  판정:  {result.verdict}  점수: {result.score}/10")
        print(f"  요약:  {result.summary}")
        for ctx in result.price_contexts:
            print(f"  시세:  {ctx.ticker} {ctx.current:,} ({ctx.change_pct:+.1f}%) RSI:{ctx.rsi} [{ctx.source}]")
