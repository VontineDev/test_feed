"""
stage_classifier.py  —  3단계 일봉 분류기
────────────────────────────────────────────────────────────
KOSPI + KOSDAQ 전 종목(~2770개)을 대상으로 매일 16:30 KST에 실행.
Ichimoku 주봉 스크리너와 완전히 독립된 시스템.

classify_stage(ticker, price_df, flow_df, s1_history, market) -> int | None
    1  =  Stage 1 (랠리 초입)
    2  =  Stage 2 (중간 조정·재매집)
    3  =  Stage 3 (과열 재가속)
    None  =  어느 단계도 해당 없음

check_peakout(ticker, flow_df, price_df) -> bool
    Stage 3 피크아웃 신호 여부

asyncpg 스레드 안전: price_df, flow_df, s1_history 모두 ThreadPoolExecutor
진입 전에 배치 로드하여 전달. 분류기 내부에서 DB 조회 없음.
(learnings: asyncpg-threadpool-no-db)
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

from market_data import calc_rsi

logger = logging.getLogger(__name__)

# Stage 1 상승률 기준 (KOSPI / KOSDAQ)
_S1_CHANGE_THRESHOLD = {"KOSPI": 0.05, "KOSDAQ": 0.07}


def classify_stage(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    s1_history: dict[str, list[dict]],
    market: str = "KOSPI",
) -> Optional[int]:
    """
    3단계 분류. 우선순위: Stage 3 > Stage 2 > Stage 1.

    price_df: DatetimeIndex, columns=[Open, High, Low, Close, Volume], daily, 60일 이상.
    flow_df:  DatetimeIndex, columns=[foreign_net, inst_net, foreign_streak, inst_streak], 20일 이상.
    s1_history: {ticker: [{classified_date, s1_high, s1_volume}, ...]} — DB 배치 조회 결과.

    D9: Stage 2 lookback = 14일이므로 당일 Stage 1과 동시 발동 불가.
        (동일 job run 내 병렬 처리 시 당일 Stage 1은 DB에 미적재 상태 — 안전.)
    """
    stage3 = _check_stage3(ticker, price_df, flow_df, s1_history)
    if stage3:
        return 3

    stage2 = _check_stage2(ticker, price_df, flow_df, s1_history)
    if stage2:
        return 2

    stage1 = _check_stage1(ticker, price_df, flow_df, market)
    if stage1:
        return 1

    return None


def check_peakout(
    ticker: str,
    flow_df: pd.DataFrame,
    price_df: pd.DataFrame,
) -> bool:
    """
    Stage 3 피크아웃 신호.

    조건 1: 최근 2~3일 foreign_streak ≤ −2 AND inst_streak ≤ −2 (동시 순매도)
    조건 2: 윗꼬리 캔들 — (high−close) > 0.5×(high−low) — + 당일 volume spike
    """
    if len(flow_df) < 3:
        return False

    # 조건 1: 연속 순매도 streak
    tail = flow_df.tail(3)
    f_streak = tail["foreign_streak"].dropna()
    i_streak = tail["inst_streak"].dropna()
    if len(f_streak) >= 2 and len(i_streak) >= 2:
        if (f_streak.iloc[-1] <= -2) and (i_streak.iloc[-1] <= -2):
            return True

    # 조건 2: 윗꼬리 + 거래량 급증
    if len(price_df) >= 2:
        last = price_df.iloc[-1]
        high  = float(last.get("High", 0) or 0)
        close = float(last.get("Close", 0) or 0)
        low   = float(last.get("Low", 0) or 0)
        vol   = float(last.get("Volume", 0) or 0)
        candle_range = high - low
        if candle_range > 0 and (high - close) > 0.5 * candle_range:
            vol_series = price_df["Volume"].dropna()
            if len(vol_series) >= 20:
                avg_vol = float(vol_series.iloc[-20:-1].mean())
                if avg_vol > 0 and vol >= 1.5 * avg_vol:
                    return True

    return False


# ── 내부 헬퍼 ────────────────────────────────────────────────

def _check_stage1(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    market: str,
) -> bool:
    """Stage 1 (랠리 초입) 5개 조건 모두 통과 시 True."""
    closes = price_df["Close"].dropna()
    vols   = price_df["Volume"].dropna()

    # 최소 데이터 요건 (MA20 계산용)
    if len(closes) < 21:
        logger.debug("[stage1] %s 데이터 부족 (%d바)", ticker, len(closes))
        return False

    close_today = float(closes.iloc[-1])
    close_prev  = float(closes.iloc[-2]) if len(closes) >= 2 else close_today

    # 조건 1: 일일 상승률
    if close_prev <= 0:
        return False
    change_pct = (close_today - close_prev) / close_prev
    threshold = _S1_CHANGE_THRESHOLD.get(market, 0.05)
    if change_pct < threshold:
        return False

    # 조건 2: 거래량 ≥ 2× 20일 평균
    if len(vols) < 20:
        return False
    vol_today = float(vols.iloc[-1])
    avg_vol20 = float(vols.iloc[-21:-1].mean())
    if avg_vol20 <= 0:
        return False
    if vol_today < 2.0 * avg_vol20:
        return False

    # 조건 3: close > MA20 AND close > MA60 (with div-by-zero guard)
    if len(closes) < 60:
        return False
    ma20 = float(closes.iloc[-20:].mean())
    ma60 = float(closes.iloc[-60:].mean())
    if close_today <= ma20 or close_today <= ma60:
        return False

    # 조건 4: 52주 고점 대비 괴리율 ≤ 20%
    week52_high = float(closes.iloc[-252:].max()) if len(closes) >= 252 else float(closes.max())
    if week52_high <= 0:
        return False
    position = (week52_high - close_today) / week52_high
    if position > 0.20:
        return False

    # 조건 5: 외국인 OR 기관 순매수
    if flow_df.empty:
        return False
    last_flow = flow_df.iloc[-1]
    foreign_net = last_flow.get("foreign_net")
    inst_net    = last_flow.get("inst_net")
    if not (
        (foreign_net is not None and foreign_net > 0)
        or (inst_net is not None and inst_net > 0)
    ):
        return False

    return True


def _check_stage2(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    s1_history: dict[str, list[dict]],
) -> bool:
    """Stage 2 (중간 조정·재매집) 조건 — 직전 14일 이내 Stage 1 이력 필요."""
    history = s1_history.get(ticker, [])
    if not history:
        return False

    # 가장 최근 Stage 1 이력
    entry = history[0]
    s1_high   = entry.get("s1_high")
    s1_volume = entry.get("s1_volume")

    # s1_high / s1_volume NULL이면 가격·볼륨 조건 스킵 (안전 처리)
    closes = price_df["Close"].dropna()
    vols   = price_df["Volume"].dropna()

    if len(closes) < 21:
        return False

    close_today = float(closes.iloc[-1])

    # 조건 1: close가 Stage 1 high 대비 −5% ~ −20% 구간
    if s1_high is not None and s1_high > 0:
        discount = (float(s1_high) - close_today) / float(s1_high)
        if not (0.05 <= discount <= 0.20):
            return False

    # 조건 2: close ≥ MA20 × 0.95
    ma20 = float(closes.iloc[-20:].mean())
    if close_today < ma20 * 0.95:
        return False

    # 조건 3: 거래량이 Stage 1 스파이크의 30~60%
    if s1_volume is not None and s1_volume > 0 and len(vols) >= 1:
        vol_today = float(vols.iloc[-1])
        ratio = vol_today / float(s1_volume)
        if not (0.30 <= ratio <= 0.60):
            return False

    # 조건 4: inst_streak ≥ 0 (기관 순매수 유지 또는 중립)
    if not flow_df.empty:
        inst_streak = flow_df.iloc[-1].get("inst_streak")
        if inst_streak is not None and inst_streak < 0:
            return False

    return True


def _check_stage3(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    s1_history: dict[str, list[dict]],
) -> bool:
    """
    Stage 3 (과열 재가속) 조건.
    주의: Stage 3 lookback은 stage_classifications에서 stage=2를 조회해야 하지만,
    현재 classify_stage()는 s1_history(stage=1)만 받는다.
    Stage 3는 s1_history에서 stage 1 이력이 충분히 오래된 경우 + 모든 가격/수급 조건으로 대체한다.
    완전한 Stage 2→3 파이프라인은 s2_history를 별도로 받는 확장 버전에서 구현.
    현재: Stage 2 이력 없이 나머지 4개 조건(돌파, 거래량, 외인+기관, RSI)만으로 판정.
    """
    closes = price_df["Close"].dropna()
    vols   = price_df["Volume"].dropna()
    highs  = price_df["High"].dropna() if "High" in price_df.columns else pd.Series(dtype=float)

    if len(closes) < 31:
        return False

    close_today = float(closes.iloc[-1])
    close_prev  = float(closes.iloc[-2]) if len(closes) >= 2 else close_today

    # 조건 1: 조정 구간 고점 돌파 (최근 10일 고가 초과)
    if len(highs) >= 11:
        recent_10_high = float(highs.iloc[-11:-1].max())
        if close_today <= recent_10_high:
            return False

    # 조건 2: 일일 상승률 ≥ +5%
    if close_prev <= 0 or (close_today - close_prev) / close_prev < 0.05:
        return False

    # 조건 3: RSI(14) ≥ 70 (D8: 유지)
    rsi = calc_rsi(closes.tolist())
    if rsi is None or rsi < 70:
        return False

    # 조건 4: 거래량 ≥ 1.5× 30일 평균
    if len(vols) < 31:
        return False
    vol_today = float(vols.iloc[-1])
    avg_vol30 = float(vols.iloc[-31:-1].mean())
    if avg_vol30 <= 0 or vol_today < 1.5 * avg_vol30:
        return False

    # 조건 5: 외국인 AND 기관 동시 순매수
    if flow_df.empty:
        return False
    last_flow   = flow_df.iloc[-1]
    foreign_net = last_flow.get("foreign_net")
    inst_net    = last_flow.get("inst_net")
    if not (
        (foreign_net is not None and foreign_net > 0)
        and (inst_net is not None and inst_net > 0)
    ):
        return False

    return True
