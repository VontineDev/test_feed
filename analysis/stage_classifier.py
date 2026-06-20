"""
stage_classifier.py  —  3단계 일봉 분류기
────────────────────────────────────────────────────────────
KOSPI + KOSDAQ 전 종목(~2770개)을 대상으로 매일 16:30 KST에 실행.
Ichimoku 주봉 스크리너와 완전히 독립된 시스템.

classify_stage(ticker, price_df, flow_df, s1_history, market) -> int | None
    v1.0: 5+조건 분류기 (production)
    1  =  Stage 1 (랠리 초입)
    2  =  Stage 2 (중간 조정·재매집)
    3  =  Stage 3 (과열 재가속)
    None  =  어느 단계도 해당 없음

classify_stage_v11(ticker, price_df, flow_df, s1_history, market) -> int | None
    v1.1: howto-stage-classifier.md 기준 — v1.0 대비 차이:
      Stage 1: RSI(14) ≥ 50 조건 추가
      Stage 2: 거래대금 범위 0.30~0.60 (v1.0: 0.25~0.65)
      Stage 3: 조정 고점 또는 52주 고점 돌파 (v1.0: 10일 고점만)

classify_stage_v12(ticker, price_df, flow_df, s1_history, s2_history, market) -> int | None
    v1.2: v1.1 + 미구현 howto 항목 추가:
      Stage 1: 수급 조건 강화 — (외인>0 AND 기관≥0) OR 기관 3일 연속 순매수
      Stage 2: 거래대금 20일 평균 대비 100~150% 추가 + 일일 고저폭 축소
      Stage 3: Stage 2 이력 전제 + 외인·기관 streak ≥ 2 (연속 2일 이상 순매수)

classify_stage_v13(ticker, price_df, flow_df, s1_history, s2_history, market) -> int | None
    v1.3: v1.2 howto 잔여 갭 해소:
      Stage 1: 개인 순매수 > 0 조건 제거(howto 초과). close > MA20은 v1.0 base에 이미 포함.
      Stage 2: 개인 출회 신호 추가(personal_net ≤ 0)
      Stage 3: v1.2와 동일

classify_stage_v14(ticker, price_df, flow_df, s1_history, s2_history, market) -> int | None
    v1.4: v1.3 + 거래량 기준 강화:
      Stage 1: 거래대금 비교 기준 MA20 → MA30 (단기 급등 직전 왜곡 방지)

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

from data.market_data import calc_rsi

logger = logging.getLogger(__name__)

# Stage 1 상승률 기준 (KOSPI / KOSDAQ)
_S1_CHANGE_THRESHOLD = {"KOSPI": 0.05, "KOSDAQ": 0.07}


def _calc_txamt(price_df: pd.DataFrame) -> pd.Series:
    """거래대금 = Volume × Close (원 기준).

    시가총액이 작은 주식은 주식 수 기준 거래량이 많아 보여도
    거래대금이 작을 수 있어, 거래량 대신 거래대금으로 비교한다.
    (compare_tx_amt.py 검증: Volume×Close ≈ 실제 거래대금, MAE 1.38%)
    """
    return (price_df["Volume"] * price_df["Close"]).dropna()


def classify_stage_v11(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    s1_history: dict[str, list[dict]],
    market: str = "KOSPI",
) -> Optional[int]:
    """v1.1 분류기. 우선순위: Stage 3 > Stage 2 > Stage 1."""
    if _check_stage3_v11(ticker, price_df, flow_df, s1_history):
        return 3
    if _check_stage2_v11(ticker, price_df, flow_df, s1_history):
        return 2
    if _check_stage1_v11(ticker, price_df, flow_df, market):
        return 1
    return None


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
                avg_vol = float(vol_series.iloc[-21:-1].mean())
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
    closes  = price_df["Close"].dropna()
    txamts  = _calc_txamt(price_df)

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

    # 조건 2: 거래대금 ≥ 2× 20일 평균 (시가총액 편향 방지)
    if len(txamts) < 21:
        return False
    txamt_today = float(txamts.iloc[-1])
    avg_txamt20 = float(txamts.iloc[-21:-1].mean())
    if avg_txamt20 <= 0:
        return False
    if txamt_today < 2.0 * avg_txamt20:
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

    # s1_high / s1_txamt NULL이면 해당 조건 스킵 (안전 처리)
    closes  = price_df["Close"].dropna()
    txamts  = _calc_txamt(price_df)

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

    # 조건 3: 거래대금이 Stage 1 스파이크의 25~65% (눌림목 매집 구간)
    # 임계값 [0.25, 0.65]: Stage 2 가격 할인(-5%~-20%)만큼 거래대금이 낮아지는 것을 반영
    s1_txamt = entry.get("s1_txamt")
    # s1_txamt 없으면 s1_volume × s1_high로 추정 (구버전 DB 행 대응)
    if s1_txamt is None and s1_volume is not None and s1_high is not None:
        s1_txamt = int(float(s1_volume) * float(s1_high))
    if s1_txamt is not None and s1_txamt > 0 and len(txamts) >= 1:
        txamt_today = float(txamts.iloc[-1])
        ratio = txamt_today / float(s1_txamt)
        if not (0.25 <= ratio <= 0.65):
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
    closes  = price_df["Close"].dropna()
    txamts  = _calc_txamt(price_df)
    highs   = price_df["High"].dropna() if "High" in price_df.columns else pd.Series(dtype=float)

    if len(closes) < 31:
        return False
    if len(highs) < 11:
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

    # 조건 4: 거래대금 ≥ 1.5× 30일 평균 (시가총액 편향 방지)
    if len(txamts) < 31:
        return False
    txamt_today = float(txamts.iloc[-1])
    avg_txamt30 = float(txamts.iloc[-31:-1].mean())
    if avg_txamt30 <= 0 or txamt_today < 1.5 * avg_txamt30:
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


# ── v1.1 헬퍼 ────────────────────────────────────────────────

def _check_stage1_v11(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    market: str,
) -> bool:
    """Stage 1 v1.1: v1.0 조건 + RSI(14) ≥ 50."""
    if not _check_stage1(ticker, price_df, flow_df, market):
        return False
    closes = price_df["Close"].dropna()
    rsi = calc_rsi(closes.tolist())
    return rsi is not None and rsi >= 50


def _check_stage2_v11(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    s1_history: dict[str, list[dict]],
) -> bool:
    """Stage 2 v1.1: 거래대금 범위 0.30~0.60 (v1.0: 0.25~0.65)."""
    history = s1_history.get(ticker, [])
    if not history:
        return False

    entry     = history[0]
    s1_high   = entry.get("s1_high")
    s1_volume = entry.get("s1_volume")

    closes = price_df["Close"].dropna()
    txamts = _calc_txamt(price_df)

    if len(closes) < 21:
        return False

    close_today = float(closes.iloc[-1])

    if s1_high is not None and s1_high > 0:
        discount = (float(s1_high) - close_today) / float(s1_high)
        if not (0.05 <= discount <= 0.20):
            return False

    ma20 = float(closes.iloc[-20:].mean())
    if close_today < ma20 * 0.95:
        return False

    s1_txamt = entry.get("s1_txamt")
    if s1_txamt is None and s1_volume is not None and s1_high is not None:
        s1_txamt = int(float(s1_volume) * float(s1_high))
    if s1_txamt is not None and s1_txamt > 0 and len(txamts) >= 1:
        txamt_today = float(txamts.iloc[-1])
        ratio = txamt_today / float(s1_txamt)
        if not (0.30 <= ratio <= 0.60):  # v1.1: 0.30~0.60
            return False

    if not flow_df.empty:
        inst_streak = flow_df.iloc[-1].get("inst_streak")
        if inst_streak is not None and inst_streak < 0:
            return False

    return True


def _check_stage3_v11(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    s1_history: dict[str, list[dict]],
) -> bool:
    """Stage 3 v1.1: 조정 고점(10일) 또는 52주 고점 돌파 (v1.0: 10일만)."""
    closes = price_df["Close"].dropna()
    txamts = _calc_txamt(price_df)
    highs  = price_df["High"].dropna() if "High" in price_df.columns else pd.Series(dtype=float)

    if len(closes) < 31 or len(highs) < 11:
        return False

    close_today = float(closes.iloc[-1])
    close_prev  = float(closes.iloc[-2]) if len(closes) >= 2 else close_today

    # 조건 1: 조정 구간 고점(10일) 또는 52주 고점 돌파
    recent_10_high = float(highs.iloc[-11:-1].max()) if len(highs) >= 11 else None
    week52_high    = float(highs.iloc[-252:].max())  if len(highs) >= 52 else None
    breakout_ok = (
        (recent_10_high is not None and close_today > recent_10_high)
        or (week52_high  is not None and close_today > week52_high)
    )
    if not breakout_ok:
        return False

    # 조건 2: 일일 상승률 ≥ +5%
    if close_prev <= 0 or (close_today - close_prev) / close_prev < 0.05:
        return False

    # 조건 3: RSI(14) ≥ 70
    rsi = calc_rsi(closes.tolist())
    if rsi is None or rsi < 70:
        return False

    # 조건 4: 거래대금 ≥ 1.5× 30일 평균
    if len(txamts) < 31:
        return False
    txamt_today = float(txamts.iloc[-1])
    avg_txamt30 = float(txamts.iloc[-31:-1].mean())
    if avg_txamt30 <= 0 or txamt_today < 1.5 * avg_txamt30:
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


# ── v1.2 헬퍼 ────────────────────────────────────────────────

def classify_stage_v12(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    s1_history: dict[str, list[dict]],
    s2_history: Optional[dict[str, list[dict]]] = None,
    market: str = "KOSPI",
    listed_shares: Optional[dict[str, int]] = None,
) -> Optional[int]:
    """v1.2 분류기. 우선순위: Stage 3 > Stage 2 > Stage 1."""
    if _check_stage3_v12(ticker, price_df, flow_df, s1_history, s2_history, listed_shares):
        return 3
    if _check_stage2_v12(ticker, price_df, flow_df, s1_history, listed_shares):
        return 2
    if _check_stage1_v12(ticker, price_df, flow_df, market, listed_shares):
        return 1
    return None


def _check_stage1_v12(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    market: str,
    listed_shares: Optional[dict[str, int]] = None,
) -> bool:
    """Stage 1 v1.2: v1.1 + 개인 순매수 > 0 + 수급 강화 + 3거래일 외인+기관 ≥ 상장주식수 0.2%."""
    if not _check_stage1_v11(ticker, price_df, flow_df, market):
        return False

    if flow_df.empty:
        return False
    last_flow    = flow_df.iloc[-1]
    personal_net = last_flow.get("personal_net")
    # 개인 순매수 > 0 (데이터 없는 경우 통과)
    if personal_net is not None and personal_net <= 0:
        return False

    foreign_net = last_flow.get("foreign_net")
    inst_net    = last_flow.get("inst_net")
    inst_streak = last_flow.get("inst_streak")
    flow_ok = (
        (foreign_net is not None and foreign_net > 0 and inst_net is not None and inst_net >= 0)
        or (inst_streak is not None and inst_streak >= 3)
    )
    if not flow_ok:
        return False

    # 조건 9: 최근 3거래일 외인+기관 합산 순매수 ≥ 상장주식수 0.2% (데이터 없으면 통과)
    if listed_shares is not None:
        shares = listed_shares.get(ticker)
        if shares and shares > 0:
            recent = flow_df.tail(3)
            f_sum  = float(recent["foreign_net"].dropna().sum())
            i_sum  = float(recent["inst_net"].dropna().sum())
            if len(recent["foreign_net"].dropna()) >= 1:
                if (f_sum + i_sum) < shares * 0.002:
                    return False

    return True


def _check_stage2_v12(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    s1_history: dict[str, list[dict]],
    listed_shares: Optional[dict[str, int]] = None,
) -> bool:
    """Stage 2 v1.2: v1.1 + 거래대금 100~150% + 고저폭 축소 + 외국인 14일 누적 지분율 -1%p 차단."""
    if not _check_stage2_v11(ticker, price_df, flow_df, s1_history):
        return False

    closes = price_df["Close"].dropna()
    txamts = _calc_txamt(price_df)
    if len(closes) < 21 or len(txamts) < 21:
        return False

    close_today = float(closes.iloc[-1])
    txamt_today = float(txamts.iloc[-1])

    # 추가 조건 A: 거래대금이 20일 평균 대비 100~150% (과다 거래 없는 매집 구간)
    avg_txamt20 = float(txamts.iloc[-21:-1].mean())
    if avg_txamt20 > 0:
        ratio_to_avg = txamt_today / avg_txamt20
        if not (1.0 <= ratio_to_avg <= 1.5):
            return False

    # 추가 조건 B: 일일 고저폭이 20일 평균 대비 70% 이하 (눌림목 특성)
    if "High" in price_df.columns and "Low" in price_df.columns:
        highs = price_df["High"].dropna()
        lows  = price_df["Low"].dropna()
        if len(highs) >= 21 and len(lows) >= 21 and close_today > 0:
            today_range_ratio = (float(highs.iloc[-1]) - float(lows.iloc[-1])) / close_today
            hist_closes = closes.iloc[-21:-1]
            hist_range_ratio = float(
                ((highs.iloc[-21:-1] - lows.iloc[-21:-1]) / hist_closes).mean()
            )
            if hist_range_ratio > 0 and today_range_ratio > hist_range_ratio * 0.70:
                return False

    # 추가 조건 C: 외국인 14일 누적 순매수 ≥ 상장주식수 -1% (지분율 1%p 이상 이탈 차단)
    if listed_shares is not None and not flow_df.empty:
        shares = listed_shares.get(ticker)
        if shares and shares > 0:
            f_14d = float(flow_df.tail(14)["foreign_net"].dropna().sum())
            if f_14d < -0.01 * shares:
                return False

    return True


def _check_stage3_v12(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    s1_history: dict[str, list[dict]],
    s2_history: Optional[dict[str, list[dict]]] = None,
    listed_shares: Optional[dict[str, int]] = None,
) -> bool:
    """Stage 3 v1.2: v1.1 + Stage 2 이력 전제 + streak >= 2 + 3거래일 외인+기관 ≥ 상장주식수 0.2%."""
    # 전제: Stage 2 이력 (s2_history 제공 시에만 강제)
    if s2_history is not None:
        if not s2_history.get(ticker):
            return False

    if not _check_stage3_v11(ticker, price_df, flow_df, s1_history):
        return False

    # 수급 강화: 외인·기관 모두 2일 이상 연속 순매수
    if flow_df.empty:
        return False
    last_flow = flow_df.iloc[-1]
    f_streak  = last_flow.get("foreign_streak")
    i_streak  = last_flow.get("inst_streak")
    if not (
        (f_streak is not None and f_streak >= 2)
        and (i_streak is not None and i_streak >= 2)
    ):
        return False

    # 추가 조건: 최근 3거래일 외인+기관 합산 순매수 ≥ 상장주식수 0.2% (데이터 없으면 통과)
    if listed_shares is not None:
        shares = listed_shares.get(ticker)
        if shares and shares > 0:
            recent = flow_df.tail(3)
            f_sum  = float(recent["foreign_net"].dropna().sum())
            i_sum  = float(recent["inst_net"].dropna().sum())
            if len(recent["foreign_net"].dropna()) >= 1:
                if (f_sum + i_sum) < shares * 0.002:
                    return False

    return True


# ── v1.3 헬퍼 ────────────────────────────────────────────

def classify_stage_v13(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    s1_history: dict[str, list[dict]],
    s2_history: Optional[dict[str, list[dict]]] = None,
    market: str = "KOSPI",
    listed_shares: Optional[dict[str, int]] = None,
) -> Optional[int]:
    """v1.3 분류기. 우선순위: Stage 3 > Stage 2 > Stage 1."""
    if _check_stage3_v12(ticker, price_df, flow_df, s1_history, s2_history, listed_shares):
        return 3
    if _check_stage2_v13(ticker, price_df, flow_df, s1_history, listed_shares):
        return 2
    if _check_stage1_v13(ticker, price_df, flow_df, market, listed_shares):
        return 1
    return None


def _check_stage1_v13(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    market: str,
    listed_shares: Optional[dict[str, int]] = None,
) -> bool:
    """Stage 1 v1.3: v1.2에서 개인 순매수 조건 제거. close > MA20은 v1.0 base 조건으로 이미 포함."""
    if not _check_stage1_v11(ticker, price_df, flow_df, market):
        return False

    # 수급 강화: (외인>0 AND 기관≥0) OR 기관 3일 연속 순매수
    if flow_df.empty:
        return False
    last_flow   = flow_df.iloc[-1]
    foreign_net = last_flow.get("foreign_net")
    inst_net    = last_flow.get("inst_net")
    inst_streak = last_flow.get("inst_streak")
    flow_ok = (
        (foreign_net is not None and foreign_net > 0 and inst_net is not None and inst_net >= 0)
        or (inst_streak is not None and inst_streak >= 3)
    )
    if not flow_ok:
        return False

    # 3거래일 외인+기관 합산 ≥ 상장주식수 0.2% (데이터 없으면 통과)
    if listed_shares is not None:
        shares = listed_shares.get(ticker)
        if shares and shares > 0:
            recent = flow_df.tail(3)
            f_sum  = float(recent["foreign_net"].dropna().sum())
            i_sum  = float(recent["inst_net"].dropna().sum())
            if len(recent["foreign_net"].dropna()) >= 1:
                if (f_sum + i_sum) < shares * 0.002:
                    return False

    return True


def _check_stage2_v13(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    s1_history: dict[str, list[dict]],
    listed_shares: Optional[dict[str, int]] = None,
) -> bool:
    """Stage 2 v1.3: v1.2 + 개인 출회 신호(personal_net ≤0)."""
    if not _check_stage2_v12(ticker, price_df, flow_df, s1_history, listed_shares):
        return False

    # 개인 출회 신호: personal_net ≤0 (데이터 없으면 통과)
    if not flow_df.empty:
        personal_net = flow_df.iloc[-1].get("personal_net")
        if personal_net is not None and personal_net > 0:
            return False

    return True


def classify_stage_v14(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    s1_history: dict[str, list[dict]],
    s2_history: Optional[dict[str, list[dict]]] = None,
    market: str = "KOSPI",
    listed_shares: Optional[dict[str, int]] = None,
) -> Optional[int]:
    """v1.4 분류기. v1.3 + 거래량 기준 MA20 → MA30."""
    if _check_stage3_v12(ticker, price_df, flow_df, s1_history, s2_history, listed_shares):
        return 3
    if _check_stage2_v13(ticker, price_df, flow_df, s1_history, listed_shares):
        return 2
    if _check_stage1_v14(ticker, price_df, flow_df, market, listed_shares):
        return 1
    return None


def _check_stage1_v14(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    market: str,
    listed_shares: Optional[dict[str, int]] = None,
) -> bool:
    """Stage 1 v1.4: v1.3 + 거래대금 기준 MA20 → MA30.

    MA20 기준은 신호 직전 20일 내 다른 급등이 있으면 기준값이 높아져
    역설적으로 통과가 어려워지거나 기준이 왜곡된다.
    MA30은 더 안정적인 기저 거래량을 반영한다.
    """
    txamts = _calc_txamt(price_df)
    if len(txamts) < 31:
        return False
    txamt_today = float(txamts.iloc[-1])
    avg_txamt30 = float(txamts.iloc[-31:-1].mean())
    if avg_txamt30 <= 0 or txamt_today < 2.0 * avg_txamt30:
        return False

    return _check_stage1_v13(ticker, price_df, flow_df, market, listed_shares)


def compute_foreign_chg_pct(
    flow_df: pd.DataFrame,
    listed_shares: Optional[int],
) -> Optional[float]:
    """14일 누적 외국인 순매수 / 상장주식수 (지분율 변화 근사).

    양수 = 지분율 증가, 음수 = 감소.
    데이터 부족(< 5행) 또는 상장주식수 미제공 시 None 반환.
    """
    if flow_df is None or flow_df.empty:
        return None
    if not listed_shares or listed_shares <= 0:
        return None
    if "foreign_net" not in flow_df.columns:
        return None
    recent = flow_df["foreign_net"].dropna().tail(14)
    if len(recent) < 5:
        return None
    return float(recent.sum()) / listed_shares


def compute_flow_score(
    flow_df: pd.DataFrame,
    price_df: Optional[pd.DataFrame] = None,
) -> float:
    """수급 컨빅션 점수 (-1.0 ~ +1.0).

    기본 점수:
      +1.0: f_streak >= 3 AND i_streak >= 3
      +0.5: f_streak >= 3 OR  i_streak >= 3
      -0.5: f_streak <= -2 OR  i_streak <= -2
      -1.0: f_streak <= -2 AND i_streak <= -2
    보너스:
      +0.3: 오늘 거래대금 > 20일 평균 × 1.2 (price_df 있을 때)
    결과는 [-1.0, +1.0] 클램프.
    """
    if flow_df is None or flow_df.empty:
        return 0.0
    if "foreign_streak" not in flow_df.columns or "inst_streak" not in flow_df.columns:
        return 0.0

    last = flow_df.iloc[-1]
    f_streak = int(last.get("foreign_streak") or 0)
    i_streak = int(last.get("inst_streak") or 0)

    if f_streak >= 3 and i_streak >= 3:
        score = 1.0
    elif f_streak >= 3 or i_streak >= 3:
        score = 0.5
    elif f_streak <= -2 and i_streak <= -2:
        score = -1.0
    elif f_streak <= -2 or i_streak <= -2:
        score = -0.5
    else:
        score = 0.0

    if price_df is not None and not price_df.empty and len(price_df) >= 21:
        txamt = (price_df["Volume"] * price_df["Close"]).dropna()
        if len(txamt) >= 21:
            avg20 = float(txamt.iloc[-21:-1].mean())
            today_tx = float(txamt.iloc[-1])
            if avg20 > 0 and today_tx > avg20 * 1.2:
                score += 0.3

    return max(-1.0, min(1.0, score))


def classify_stage_v15(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    s1_history: dict[str, list[dict]],
    s2_history: Optional[dict[str, list[dict]]] = None,
    market: str = "KOSPI",
    listed_shares: Optional[dict[str, int]] = None,
) -> Optional[int]:
    """v1.5 분류기. v1.4 + 20일 박스권 이탈 조건."""
    if _check_stage3_v12(ticker, price_df, flow_df, s1_history, s2_history, listed_shares):
        return 3
    if _check_stage2_v13(ticker, price_df, flow_df, s1_history, listed_shares):
        return 2
    if _check_stage1_v15(ticker, price_df, flow_df, market, listed_shares):
        return 1
    return None


def _check_stage1_v15(
    ticker: str,
    price_df: pd.DataFrame,
    flow_df: pd.DataFrame,
    market: str,
    listed_shares: Optional[dict[str, int]] = None,
) -> bool:
    """Stage 1 v1.5: v1.4 + 20일 박스권 이탈 (close > 직전 20일 High 최고값).

    단순 반등이 아닌 박스 상단 돌파 breakout만 통과시킴.
    """
    if not _check_stage1_v14(ticker, price_df, flow_df, market, listed_shares):
        return False

    highs = price_df["High"].dropna() if "High" in price_df.columns else pd.Series(dtype=float)
    closes = price_df["Close"].dropna()
    if len(highs) < 21 or len(closes) < 1:
        return False

    close_today = float(closes.iloc[-1])
    high_20d = float(highs.iloc[-21:-1].max())  # 당일 제외 직전 20일 최고 High
    return close_today > high_20d
