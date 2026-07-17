"""
stage_classifier_legacy.py — 구버전(v1.0~v1.4) 분류기 진입점 보관
────────────────────────────────────────────────────────────
프로덕션은 analysis.stage_classifier.classify_stage_v15만 사용한다. 이 모듈은
버전 비교·회귀 테스트용으로만 남긴 구버전 디스패처와, 현행 체인이 쓰지 않는
v1.0 전용 헬퍼(_check_stage2/_check_stage3)를 모아둔 곳이다.

주의: 버전 체인은 누적 구조라 _check_stage1_v11 등 조건 헬퍼 대부분은 v15의
라이브 의존성이며 analysis.stage_classifier에 남아 있다. 이 모듈은 그것들을
import해 조합만 한다 (의존 방향: legacy → stage_classifier 단방향).

classify_stage(ticker, price_df, flow_df, s1_history, market) -> int | None
    v1.0: 5+조건 분류기
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
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

from data.market_data import calc_rsi
from analysis.stage_classifier import (
    _calc_txamt,
    _check_stage1,
    _check_stage1_v11,
    _check_stage1_v13,
    _check_stage1_v14,
    _check_stage2_v11,
    _check_stage2_v12,
    _check_stage2_v13,
    _check_stage3_v11,
    _check_stage3_v12,
)

logger = logging.getLogger(__name__)


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


# ── v1.0 전용 헬퍼 ───────────────────────────────────────────

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


# ── v1.2 ─────────────────────────────────────────────────────

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


# ── v1.3 / v1.4 ──────────────────────────────────────────────

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
