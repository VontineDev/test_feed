"""
strategy_compose.py — 종목분석 방법 조합 백테스트의 신호 합성 계층 (T2)

여러 단일 신호(이치모쿠·Stage·수급·거래량·내러티브)를 (ticker, ISO주) 통합
프레임에 모은 뒤, 3개 조합기로 합성 신호를 만든다. 합성 결과(ticker, week)는
T3의 backtest_engine compose 경로가 SignalRecord로 변환해 기존 청산·지표·HTML
머신에 투입한다.

설계 (plan-eng-review 2026-06-14, JOIN+백필 아키텍처):
  - precompute 테이블은 전부 (ticker, ISO주) grain. stage_classifications는
    T1(jobs/stage_backfill.py)으로 과거 백필. chart_signals.week_of는 이미
    'IYYY-Www'. daily_flow/daily_ohlcv는 주간 집계, youtube는 window_end가 주간.
  - 조합기(and_gate/composite_score/funnel)는 **순수 함수** — DB/네트워크 없이
    합성 프레임만 받는다(단위 테스트 가능). load_signal_frame만 DB 의존.

데이터 흐름:
    load_signal_frame(dsn, start, end, sources)         # 1소스 = 1 bulk query
        ├─ chart_signals          → ichimoku, is_enhanced, volume_w
        ├─ stage_classifications  → stage(주간 max), peakout
        ├─ daily_flow             → foreign_net_w, inst_net_w, foreign_streak(말)
        ├─ daily_ohlcv            → vol_w, vol_ma20w → vol_ratio
        └─ youtube_attention      → attention_score
              │  outer-merge on (ticker, week)
              ▼
    derive_flags(frame)  → ichimoku/stage2plus/flow_pos/vol_spike/narrative_hi (bool)
              │
              ▼  조합기 (순수)
    and_gate(frame, flags)                 # AND 교집합
    composite_score(frame, weights, top_n) # 주간 z-score 가중합 → top_n
    funnel(frame, screen_flag, trigger_flag, max_weeks)  # 순차 깔때기
              │
              ▼
    [(ticker, week), ...]  → T3: backtest_engine.run_backtest(mode="compose")

주의:
    - 수급(daily_flow) 커버리지는 2025-01-02 시작 → 그 이전 백테스트 불가.
    - week 문자열 'IYYY-Www'는 제로패딩이라 사전식 정렬이 곧 시간순 정렬.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Optional, cast

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# 통합 프레임의 키 컬럼
KEY_COLS = ["ticker", "week"]

# 기본 임계값
VOL_SPIKE_X   = 2.0     # 거래량 급증: 주간 거래량 / 20주 평균 ≥ 2배
TXAMT_SPIKE_X = 2.0     # 거래대금 급증: 주간 거래대금 / 20주 평균 ≥ 2배
NARRATIVE_Q   = 0.90    # 내러티브 상위분위 컷 (주간 attention_score 상위 10%)


# ── ISO 주차 유틸 ─────────────────────────────────────────────
def iso_week(d: date) -> str:
    """date → 'IYYY-Www' (chart_signals.week_of / stage_backfill.week_label 과 동일)."""
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"


def week_to_friday(week: str) -> date:
    """'2026-W17' → 그 주 금요일 date (주차 간 거리 계산용)."""
    y, w = week.split("-W")
    return date.fromisocalendar(int(y), int(w), 5)


def week_distance(w_from: str, w_to: str) -> int:
    """두 ISO 주차 사이 주 수 (w_to - w_from). 같은 주=0, 음수 가능."""
    return (week_to_friday(w_to) - week_to_friday(w_from)).days // 7


def _as_bool(s: pd.Series) -> pd.Series:
    """결측을 False로 채운 bool Series. dtype(object/float/bool) 무관, pandas
    다운캐스트/대입 경고 없이 numpy where로 변환."""
    arr = np.where(s.isna().to_numpy(), False, s.to_numpy())
    return pd.Series(arr, index=s.index).astype(bool)


def _num(x) -> pd.Series:
    """pd.to_numeric(x, errors='coerce')를 Series로 좁혀줌 (pandas-stubs가
    반환 타입을 scalar까지 포함한 거대 유니언으로 넓히는 것 방지)."""
    return cast(pd.Series, pd.to_numeric(x, errors="coerce"))


# ── 파생 불리언 플래그 ────────────────────────────────────────
def derive_flags(
    frame: pd.DataFrame,
    vol_spike_x: float = VOL_SPIKE_X,
    narrative_q: float = NARRATIVE_Q,
) -> pd.DataFrame:
    """원시 신호 컬럼에서 표준 불리언 플래그를 파생해 붙인 새 프레임 반환.

    추가 컬럼(있는 원시 컬럼에 한해):
      ichimoku    : chart_signals 통과 여부 (이미 bool이면 그대로)
      stage2plus  : stage >= 2
      flow_pos    : 외국인 또는 기관 주간 순매수 > 0 (엄격)
      flow_loose  : 외국인·기관 양쪽 모두 순매도가 아님 (완화 — 한쪽이 0/중립이면 통과)
      vol_spike   : vol_ratio >= vol_spike_x  (daily_ohlcv 기반, 데이터 없으면 NaN)
      vol_above_med: volume_w >= 그 주 이치모쿠 통과 종목 중앙값 (chart_signals 기반, 항상 계산 가능)
      narrative_hi: 주간 attention_score 상위 (1 - narrative_q) 분위
    """
    df = frame.copy()

    if "ichimoku" in df.columns:
        df["ichimoku"] = _as_bool(cast(pd.Series, df["ichimoku"]))

    if "stage" in df.columns:
        st = _num(df["stage"])
        df["stage2plus"] = st.fillna(0) >= 2
        df["stage1"]     = st.fillna(0) == 1          # cross 모드 패리티용 (Stage 1)
        df["stage_any"]  = st.notna() & (st.fillna(0) >= 1)  # 분류기가 무엇이든 플래그

    if "foreign_net_w" in df.columns or "inst_net_w" in df.columns:
        f = _num(df["foreign_net_w"] if "foreign_net_w" in df.columns else 0).fillna(0)
        i = _num(df["inst_net_w"] if "inst_net_w" in df.columns else 0).fillna(0)
        df["flow_pos"]   = (f > 0) | (i > 0)           # 하나라도 순매수
        df["flow_loose"] = ~((f < 0) & (i < 0))        # 양쪽 모두 순매도가 아님

    if "vol_ratio" in df.columns:
        df["vol_spike"] = _num(df["vol_ratio"]).fillna(0) >= vol_spike_x

    if "txamt_ratio" in df.columns:
        df["txamt_spike"] = _num(df["txamt_ratio"]).fillna(0) >= TXAMT_SPIKE_X
        txamt_weekly_med = df.groupby("week")["txamt_ratio"].transform(
            lambda x: _num(x).median()
        )
        df["txamt_above_med"] = _num(df["txamt_ratio"]).fillna(0) >= txamt_weekly_med

    if "volume_w" in df.columns:
        vw = _num(df["volume_w"])
        # 주별 이치모쿠 통과 종목 중 거래량 중앙값 이상 (cross-sectional, 항상 계산 가능)
        weekly_med = df.groupby("week")["volume_w"].transform(
            lambda x: _num(x).median()
        )
        df["vol_above_med"] = vw >= weekly_med

    # 거래대금 기반 플래그 (chart_signals.close × volume_w — daily_ohlcv 불필요)
    if "volume_w" in df.columns and "close_chart" in df.columns:
        vw2 = _num(df["volume_w"])
        cl  = _num(df["close_chart"])
        txamt_cs = vw2 * cl                    # 주봉 거래대금 (원)
        txamt_cs_med = txamt_cs.groupby(df["week"]).transform(
            lambda x: _num(x).median()
        )
        df["txamt_above_med_cs"] = txamt_cs.fillna(0) >= txamt_cs_med
        # 상위 30% (더 엄격)
        txamt_cs_q70 = txamt_cs.groupby(df["week"]).transform(
            lambda x: _num(x).quantile(0.70)
        )
        df["txamt_top30_cs"] = txamt_cs.fillna(0) >= txamt_cs_q70

    if "attention_score" in df.columns:
        # 주간 분위: 각 주 내에서 attention_score의 분위수 ≥ narrative_q
        s = _num(df["attention_score"])
        rank = s.groupby(df["week"]).rank(pct=True)
        df["narrative_hi"] = rank.fillna(0.0) >= narrative_q

    return df


# ── 조합기 1: 합의 게이트 (AND 교집합) ────────────────────────
def and_gate(frame: pd.DataFrame, flags: list[str]) -> pd.DataFrame:
    """모든 flags 불리언 컬럼이 참인 (ticker, week) 행 반환.

    _apply_cross_filter(이치모쿠 ∩ Stage)의 일반화. 결측은 False 취급.
    flags 컬럼이 없으면 ValueError (explicit > clever).
    """
    if not flags:
        raise ValueError("and_gate: flags가 비어 있습니다")
    missing = [f for f in flags if f not in frame.columns]
    if missing:
        raise ValueError(f"and_gate: 프레임에 없는 플래그 컬럼: {missing}")
    if frame.empty:
        return frame.loc[[], KEY_COLS].reset_index(drop=True)

    mask = pd.Series(True, index=frame.index)
    for f in flags:
        mask &= _as_bool(cast(pd.Series, frame[f]))
    out = frame.loc[mask, KEY_COLS].drop_duplicates().reset_index(drop=True)
    return out


# ── 조합기 2: 점수 합산 랭킹 (Composite score) ────────────────
def composite_score(
    frame: pd.DataFrame,
    weights: dict[str, float],
    top_n: int,
) -> pd.DataFrame:
    """주간 z-score 가중합으로 점수 → 각 주 top_n 종목 반환.

    각 컬럼을 **주(week) 내에서** 표준화(cross-sectional z-score)한 뒤 가중합.
    표본<2 또는 분산 0인 주는 해당 컬럼 z=0(중립). 결측도 0.
    동점은 ticker 오름차순으로 결정적 정렬.

    반환: columns=[ticker, week, score], 각 주 점수 내림차순 top_n.
    """
    if not weights:
        raise ValueError("composite_score: weights가 비어 있습니다")
    missing = [c for c in weights if c not in frame.columns]
    if missing:
        raise ValueError(f"composite_score: 프레임에 없는 점수 컬럼: {missing}")
    if top_n <= 0:
        raise ValueError(f"composite_score: top_n은 1 이상이어야 합니다: {top_n}")
    if frame.empty:
        return pd.DataFrame(columns=pd.Index(KEY_COLS + ["score"]))

    df = frame.copy()
    score = pd.Series(0.0, index=df.index)
    for col, w in weights.items():
        x = _num(df[col])
        g = x.groupby(df["week"])
        mean = g.transform("mean")
        std = g.transform("std")  # ddof=1; 표본<2 → NaN
        z = (x - mean) / std.replace(0.0, np.nan)
        z = z.fillna(0.0)
        score = score + float(w) * z
    df["score"] = score

    df = df.sort_values(["week", "score", "ticker"], ascending=[True, False, True])
    out = df.groupby("week", group_keys=False, sort=False).head(top_n)
    return cast(pd.DataFrame, out[KEY_COLS + ["score"]]).reset_index(drop=True)


# ── 조합기 3: 깔때기 (순차 깔터링) ────────────────────────────
def funnel(
    frame: pd.DataFrame,
    screen_flag: str,
    trigger_flag: str,
    max_weeks: int,
) -> pd.DataFrame:
    """종목별: screen_flag 통과 후 max_weeks 이내 첫 trigger_flag 주에 신호.

    예) 수급 스크린(flow_pos) 통과 → 이후 N주 내 이치모쿠 돌파(ichimoku) 진입.
    같은 주 동시(거리 0)도 트리거로 인정. 신호 발생 시 무장 해제, 이후 새
    screen으로 재무장. 윈도우 초과 시 무장 해제.

    반환: columns=[ticker, week] — 트리거 발생 주.
    """
    for col in (screen_flag, trigger_flag):
        if col not in frame.columns:
            raise ValueError(f"funnel: 프레임에 없는 컬럼: {col}")
    if max_weeks < 0:
        raise ValueError(f"funnel: max_weeks는 0 이상이어야 합니다: {max_weeks}")
    if frame.empty:
        return frame.loc[[], KEY_COLS].reset_index(drop=True)

    out_rows: list[tuple[str, str]] = []
    for ticker_raw, g in frame.groupby("ticker", sort=False):
        ticker = str(ticker_raw)
        g = g.sort_values("week")
        weeks = g["week"].tolist()
        screen = _as_bool(cast(pd.Series, g[screen_flag])).tolist()
        trig = _as_bool(cast(pd.Series, g[trigger_flag])).tolist()

        armed_week: Optional[str] = None
        for wk, sc, tg in zip(weeks, screen, trig):
            # 무장 중이고 윈도우 초과면 해제
            if armed_week is not None and week_distance(armed_week, wk) > max_weeks:
                armed_week = None
            # 같은 주에 screen이 다시 켜지면 무장(거리 0부터 윈도우 리셋)
            if sc:
                armed_week = wk
            # 무장 중 트리거 → 신호, 해제
            if armed_week is not None and tg and week_distance(armed_week, wk) <= max_weeks:
                out_rows.append((ticker, wk))
                armed_week = None

    return pd.DataFrame(out_rows, columns=pd.Index(KEY_COLS))


# ── 선언형 전략 로스터 (Tier-1) ───────────────────────────────
@dataclass
class StrategySpec:
    name: str
    kind: str                              # "and" | "score" | "funnel"
    sources: list[str]                     # load_signal_frame 에 넘길 소스
    flags: list[str] = field(default_factory=list)        # and / funnel
    weights: dict[str, float] = field(default_factory=dict)  # score
    top_n: int = 20                        # score
    screen_flag: str = ""                  # funnel
    trigger_flag: str = ""                 # funnel
    max_weeks: int = 4                     # funnel

    def run(self, frame: pd.DataFrame) -> pd.DataFrame:
        if self.kind == "and":
            return and_gate(frame, self.flags)
        if self.kind == "score":
            return composite_score(frame, self.weights, self.top_n)
        if self.kind == "funnel":
            return funnel(frame, self.screen_flag, self.trigger_flag, self.max_weeks)
        raise ValueError(f"알 수 없는 전략 kind: {self.kind!r}")


# ── 전략 로스터 ────────────────────────────────────────────────
# 각 전략은 (ticker, week) 신호를 생성 → backtest_engine compose 경로로 투입.
#
# 이름 규칙:
#   AND-*    AND 교집합 (조건 모두 충족 시 신호)
#   SCORE-*  z-score 가중합 → 주간 top-N (연속 점수 기반)
#   FUNNEL-* 순차 깔때기 (스크린 → 트리거 순서 요구)
#
# 거래대금 플래그 출처: chart_signals.close × volume_w (ichimoku 소스에서 파생)
#   — daily_ohlcv 불필요, 수급(daily_flow) 시작일(2025-01-02)까지 소급 가능.
STRATEGIES: dict[str, StrategySpec] = {
    # 이치모쿠 돌파 ∩ Stage2+ ∩ 수급 비매도 (기준선)
    "AND-1": StrategySpec(
        name="AND-1", kind="and",
        sources=["ichimoku", "stage", "flow"],
        flags=["ichimoku", "stage2plus", "flow_loose"],
    ),
    # AND-1 ∩ 거래량 주내 중앙값 이상 (chart_signals.volume_w 기반)
    "AND-2": StrategySpec(
        name="AND-2", kind="and",
        sources=["ichimoku", "stage", "flow"],
        flags=["ichimoku", "stage2plus", "flow_loose", "vol_above_med"],
    ),
    # AND-1 ∩ 거래대금 유니버스 중앙값 이상 (close×volume_w; 소형주 필터)
    "AND-3": StrategySpec(
        name="AND-3", kind="and",
        sources=["ichimoku", "stage", "flow"],
        flags=["ichimoku", "stage2plus", "flow_loose", "txamt_above_med_cs"],
    ),
    # AND-1 ∩ 거래대금 유니버스 상위 30% (AND-3보다 엄격)
    "AND-4": StrategySpec(
        name="AND-4", kind="and",
        sources=["ichimoku", "stage", "flow"],
        flags=["ichimoku", "stage2plus", "flow_loose", "txamt_top30_cs"],
    ),
    # AND-1 완화: stage_any(≥1) 포함 — 이른 진입, 신호 3.75x 증가 (8→30)
    "AND-5": StrategySpec(
        name="AND-5", kind="and",
        sources=["ichimoku", "stage", "flow"],
        flags=["ichimoku", "stage_any", "flow_loose"],
    ),
    # AND-3 완화: stage_any(≥1) + 거래대금 중앙값 이상 — 신호 4.4x 증가 (5→22)
    "AND-6": StrategySpec(
        name="AND-6", kind="and",
        sources=["ichimoku", "stage", "flow"],
        flags=["ichimoku", "stage_any", "flow_loose", "txamt_above_med_cs"],
    ),
    # Stage·거래대금·수급 z-score 가중합 → 주간 top-20 [권장, 2026-06-16 승격]
    # 구 SCORE-1(vol_ratio 기반) 대비: 승률28d +12%p, 수익28d +9%p, 샤프28d +0.6
    "SCORE-1": StrategySpec(
        name="SCORE-1", kind="score",
        sources=["ichimoku", "stage", "flow"],
        weights={"stage": 1.0, "txamt_above_med_cs": 1.0, "foreign_net_w": 1.0},
        top_n=20,
    ),
    # 수급 스크린 → 이후 4주 내 이치모쿠 돌파 진입
    "FUNNEL-1": StrategySpec(
        name="FUNNEL-1", kind="funnel",
        sources=["ichimoku", "flow"],
        screen_flag="flow_pos", trigger_flag="ichimoku", max_weeks=4,
    ),
}


# ── DB 로더 (sync, T3 compose 경로용) ─────────────────────────
def load_signal_frame(
    dsn: str,
    start: date,
    end: date,
    sources: list[str],
) -> pd.DataFrame:
    """precompute 테이블을 (ticker, ISO주)로 outer-merge한 통합 프레임 반환.

    sources: {"ichimoku","stage","flow","volume","narrative"} 부분집합.
    각 소스 1 bulk query (N+1 회피). 반환 프레임은 derive_flags 전 원시 컬럼.
    """
    from core.db_sync import connect

    frames: list[pd.DataFrame] = []
    conn = connect(dsn)
    try:
        if "ichimoku" in sources:
            frames.append(_load_ichimoku(conn, start, end))
        if "stage" in sources:
            frames.append(_load_stage(conn, start, end))
        if "flow" in sources:
            frames.append(_load_flow(conn, start, end))
        if "volume" in sources:
            frames.append(_load_volume(conn, start, end))
        if "narrative" in sources:
            frames.append(_load_narrative(conn, start, end))
    finally:
        conn.close()

    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        return pd.DataFrame(columns=pd.Index(KEY_COLS))

    merged = frames[0]
    for f in frames[1:]:
        merged = merged.merge(f, on=KEY_COLS, how="outer")
    return merged.reset_index(drop=True)


def _fetch_df(conn, sql: str, params: tuple, columns: list[str]) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=pd.Index(columns))


def _load_ichimoku(conn, start: date, end: date) -> pd.DataFrame:
    df = _fetch_df(
        conn,
        "SELECT ticker, week_of, is_enhanced, volume_w, close "
        "FROM chart_signals WHERE week_of BETWEEN %s AND %s",
        (iso_week(start), iso_week(end)),
        ["ticker", "week", "is_enhanced", "volume_w", "close_chart"],
    )
    if df.empty:
        return df
    df["ichimoku"] = True  # 행 존재 = 7조건 통과
    return df


def _load_stage(conn, start: date, end: date) -> pd.DataFrame:
    # 주간 대표값: 그 주 최대 stage (Stage 3 > 2 > 1), peakout OR
    df = _fetch_df(
        conn,
        "SELECT ticker, "
        "TO_CHAR(classified_date, 'IYYY-\"W\"IW') AS week, "
        "MAX(stage) AS stage, BOOL_OR(peakout_flag) AS peakout "
        "FROM stage_classifications "
        "WHERE classified_date BETWEEN %s AND %s "
        "GROUP BY ticker, TO_CHAR(classified_date, 'IYYY-\"W\"IW')",
        (start, end),
        ["ticker", "week", "stage", "peakout"],
    )
    return df


def _load_flow(conn, start: date, end: date) -> pd.DataFrame:
    # 주간 집계: 순매수 합, 주말 streak 대표(마지막 거래일)
    df = _fetch_df(
        conn,
        "SELECT ticker, "
        "TO_CHAR(trade_date, 'IYYY-\"W\"IW') AS week, "
        "SUM(foreign_net) AS foreign_net_w, SUM(inst_net) AS inst_net_w, "
        "(ARRAY_AGG(foreign_streak ORDER BY trade_date DESC))[1] AS foreign_streak "
        "FROM daily_flow WHERE trade_date BETWEEN %s AND %s "
        "GROUP BY ticker, TO_CHAR(trade_date, 'IYYY-\"W\"IW')",
        (start, end),
        ["ticker", "week", "foreign_net_w", "inst_net_w", "foreign_streak"],
    )
    return df


def _load_volume(conn, start: date, end: date) -> pd.DataFrame:
    """daily_ohlcv 주간 거래량/거래대금 + 20주 이동평균 → vol_ratio, txamt_ratio.

    20주 평균 워밍업 위해 start 이전 ~150일 포함해 로드 후 윈도우로 잘라 반환.
    txamt = volume × close (원화 거래대금).
    """
    from datetime import timedelta
    fetch_start = start - timedelta(days=200)
    raw = _fetch_df(
        conn,
        "SELECT symbol, date, volume, close FROM daily_ohlcv "
        "WHERE date BETWEEN %s AND %s",
        (fetch_start, end),
        ["ticker", "date", "volume", "close"],
    )
    if raw.empty:
        return pd.DataFrame(columns=pd.Index(["ticker", "week", "vol_ratio", "txamt_ratio"]))
    raw["date"] = pd.to_datetime(raw["date"])
    raw["volume"] = pd.to_numeric(raw["volume"], errors="coerce")
    raw["close"]  = pd.to_numeric(raw["close"],  errors="coerce")
    raw["txamt"]  = raw["volume"] * raw["close"]

    out = []
    for ticker, g in raw.groupby("ticker", sort=False):
        g_idx = g.set_index("date")
        wk = g_idx[["volume", "txamt"]].resample("W-FRI").sum()
        wk.columns = ["vol_w", "txamt_w"]

        wk["vol_ma20w"]   = wk["vol_w"].rolling(20, min_periods=20).mean()
        wk["txamt_ma20w"] = wk["txamt_w"].rolling(20, min_periods=20).mean()

        wk["vol_ratio"]   = wk["vol_w"]   / wk["vol_ma20w"].replace(0.0, np.nan)
        wk["txamt_ratio"] = wk["txamt_w"] / wk["txamt_ma20w"].replace(0.0, np.nan)

        wk = wk.dropna(subset=["vol_ratio"])
        if wk.empty:
            continue
        wk = wk.reset_index()
        wk["week"] = wk["date"].dt.date.map(iso_week)
        wk["ticker"] = ticker
        out.append(wk[["ticker", "week", "vol_ratio", "txamt_ratio"]])
    if not out:
        return pd.DataFrame(columns=pd.Index(["ticker", "week", "vol_ratio", "txamt_ratio"]))
    res = pd.concat(out, ignore_index=True)
    # start 이전 워밍업 주차 제거
    return cast(pd.DataFrame, res[res["week"] >= iso_week(start)])


def _load_narrative(conn, start: date, end: date) -> pd.DataFrame:
    df = _fetch_df(
        conn,
        "SELECT ticker, "
        "TO_CHAR(window_end, 'IYYY-\"W\"IW') AS week, "
        "MAX(attention_score) AS attention_score "
        "FROM youtube_attention_scores "
        "WHERE window_end BETWEEN %s AND %s "
        "GROUP BY ticker, TO_CHAR(window_end, 'IYYY-\"W\"IW')",
        (start, end),
        ["ticker", "week", "attention_score"],
    )
    return df
