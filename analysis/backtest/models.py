"""백테스트 데이터 모델 (backtest_engine.py에서 이동, Phase C).

BacktestConfig / SignalRecord / GroupMetrics / BacktestResult.
리포트 빌더 메서드는 후속 커밋에서 report.py로 추출 예정.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional

from analysis.backtest.config import TX_COST_DEFAULT

# ── 데이터 클래스 ─────────────────────────────────────────────────

@dataclass
class BacktestConfig:
    mode: str                     # "ichimoku" | "stage" | "cross"
    start: date
    end: date
    market: str = "ALL"           # "KOSPI" | "KOSDAQ" | "ALL"
    tx_cost_rt: float = TX_COST_DEFAULT
    max_tickers: int = 200        # 0 = 전종목 (수십 분 소요)
    rf_rate_annual: float = 0.030  # 무위험수익률 (한국국채 3% 기준)
    workers: int = 8
    dsn: Optional[str] = None     # PostgreSQL DSN. 설정 시 daily_ohlcv 캐시 사용
    strategy: Optional[str] = None    # mode="compose" 시 strategy_compose.STRATEGIES 키
    hold_weeks: Optional[int] = None  # None = 표준 1/4/13w; N = N주 보유 수익률 추가 계산
    # ── 분할 청산 파라미터 (sweep 최적화용) ──────────────────────────
    tp1_pct:         float = 0.0    # 1차 익절 타겟 (0이면 미사용 → MA20 폴백)
    tp1_ratio:       float = 0.5    # 1차 익절 시 청산 비율 (나머지 1-tp1_ratio 계속 보유)
    trail_pct:       float = 0.0    # 트레일링 스탑: 일봉 High 고점 대비 하락률 (0이면 미사용)
    hard_stop_pct:   float = 0.08            # 하드 스탑 (진입 Close 대비, 기존 _STOP_LOSS_PCT와 동일)
    use_stage3_peak: bool  = False   # Stage3 peakout_flag 트리거 사용 여부
    use_ma5_stop:    bool  = False   # MA5 이탈 손절 (Stage 1 빠른 손절, MA20 이전에 체크)
    exit_model:      str   = "default"  # "default" | "model_a" (ATR+Breakeven) | "model_b" (3단계분할)

    def __post_init__(self) -> None:
        _valid_modes = ("ichimoku", "stage", "cross", "stage2", "compose", "stage_v11", "stage2_v11", "stage_v12", "stage2_v12", "stage_v13", "stage2_v13", "stage_v14", "stage_v15")
        if self.mode not in _valid_modes:
            raise ValueError(f"mode는 {' | '.join(_valid_modes)} 중 하나여야 합니다: {self.mode!r}")
        if self.mode == "compose":
            if not self.strategy:
                raise ValueError("mode='compose'는 strategy 지정이 필요합니다 (예: 'AND-1')")
            if not self.dsn:
                raise ValueError("mode='compose'는 dsn(PostgreSQL)이 필요합니다")
        if self.start >= self.end:
            raise ValueError("start는 end보다 이전이어야 합니다")
        if self.market not in ("KOSPI", "KOSDAQ", "ALL"):
            raise ValueError(f"market은 KOSPI|KOSDAQ|ALL 중 하나여야 합니다: {self.market!r}")
        if self.hold_weeks is not None and self.hold_weeks < 1:
            raise ValueError(f"hold_weeks는 1 이상이어야 합니다: {self.hold_weeks!r}")
        if self.tp1_pct < 0 or self.tp1_pct > 1:
            raise ValueError(f"tp1_pct는 0~1 범위여야 합니다: {self.tp1_pct!r}")
        if self.trail_pct < 0 or self.trail_pct > 1:
            raise ValueError(f"trail_pct는 0~1 범위여야 합니다: {self.trail_pct!r}")
        if self.hard_stop_pct <= 0 or self.hard_stop_pct > 0.5:
            raise ValueError(f"hard_stop_pct는 0초과 0.5 이하여야 합니다: {self.hard_stop_pct!r}")
        if self.exit_model not in ("default", "model_a", "model_b"):
            raise ValueError(f"exit_model은 default|model_a|model_b 중 하나여야 합니다: {self.exit_model!r}")


@dataclass
class SignalRecord:
    ticker: str
    name: str
    signal_date: date
    close_at_signal: float
    mode: str                     # "ichimoku" | "stage" | "cross"
    market: str                   # "KOSPI" | "KOSDAQ"
    return_7d:  Optional[float] = None
    return_28d: Optional[float] = None
    return_91d: Optional[float] = None
    excess_7d:  Optional[float] = None
    excess_28d: Optional[float] = None
    excess_91d: Optional[float] = None
    return_custom: Optional[float] = None  # BacktestConfig.hold_weeks 지정 시 채워짐
    excess_custom: Optional[float] = None
    # 업종·MDD·매도 신호·단계 진행 (run_backtest에서 채워짐)
    sector:      str             = ""    # KIND 업종명 (빈 문자열 = 미확인)
    mdd_91d:     Optional[float] = None  # 진입일 기준 91일 MDD (≤ 0)
    s2_date:     Optional[date]  = None  # S1 신호 후 14일 이내 S2 조건 충족일
    s3_date:     Optional[date]  = None  # S2 이후 조정 고점 돌파 + RSI≥70 (과열 재가속)
    sell_date:   Optional[date]  = None  # MA20 이탈 또는 손절 발생일
    sell_reason: str             = ""    # "MA20 이탈" | "손절 -N%" | "보유 중"
    sell_return: Optional[float] = None  # 매도 시점 수익률 (거래비용 차감)
    hold_days:   Optional[int]   = None  # 신호일~매도일 달력일수
    # ── 분할 청산 추적 (tp1_pct > 0 일 때만 채워짐) ──────────────────
    tp1_date:        Optional[date]  = None  # 1차 익절 날짜
    tp1_ret:         Optional[float] = None  # 1차 익절 수익률 (tx cost 포함)
    final_exit_date: Optional[date]  = None  # 잔여분 청산 날짜
    final_exit_ret:  Optional[float] = None  # 잔여분 수익률
    final_exit_type: str             = ""    # "trail"|"stage3"|"hard_stop"|"period_end"|"ma20"
    blended_return:  Optional[float] = None  # 가중평균 최종 수익률
    # blended = tp1_ratio*tp1_ret + (1-tp1_ratio)*final_exit_ret  (tp1 발동 시)
    # blended = final_exit_ret  (tp1 미발동 — 전량 단일 청산)


@dataclass
class GroupMetrics:
    n: int = 0
    win_rate_7d:        Optional[float] = None
    win_rate_28d:       Optional[float] = None
    win_rate_91d:       Optional[float] = None
    avg_return_28d:     Optional[float] = None
    median_return_28d:  Optional[float] = None
    avg_return_91d:     Optional[float] = None
    avg_excess_28d:     Optional[float] = None
    avg_excess_91d:     Optional[float] = None
    sharpe_7d:          Optional[float] = None  # 연환산 샤프비율 (7d 보유)
    sharpe_28d:         Optional[float] = None  # 연환산 샤프비율 (28d 보유)
    sharpe_91d:         Optional[float] = None  # 연환산 샤프비율 (91d 보유)
    mdd:                Optional[float] = None  # 최대낙폭 (equity curve)
    # 사용자 지정 보유 기간 (hold_weeks 설정 시)
    hold_days_custom:     Optional[int]   = None
    win_rate_custom:      Optional[float] = None
    avg_return_custom:    Optional[float] = None
    median_return_custom: Optional[float] = None
    avg_excess_custom:    Optional[float] = None
    sharpe_custom:        Optional[float] = None
    # 매도 신호 기반 집계 (MA20 이탈 / 손절)
    win_rate_sell:        Optional[float] = None
    avg_return_sell:      Optional[float] = None
    median_return_sell:   Optional[float] = None
    avg_hold_days:        Optional[float] = None
    s2_progression_rate:  Optional[float] = None  # S1→S2 진행 비율
    s3_progression_rate:  Optional[float] = None  # S2→S3 진행 비율
    avg_mdd_91d:          Optional[float] = None  # 종목별 MDD(91d) 평균


@dataclass
class BacktestResult:
    config: BacktestConfig
    signals: list[SignalRecord]
    overall: GroupMetrics
    computed_at: str
    note: str = ""

    # 리포트 빌더 본문은 analysis/backtest/report.py로 추출 (Phase C) —
    # 기존 호출부(result.to_telegram_report() 등) 호환용 얇은 위임.
    def to_telegram_report(self) -> str:
        from analysis.backtest.report import to_telegram_report
        return to_telegram_report(self)

    def to_text_report(self) -> str:
        from analysis.backtest.report import to_text_report
        return to_text_report(self)

    def top_bottom_telegram_text(self, n: int = 5) -> str:
        from analysis.backtest.report import top_bottom_telegram_text
        return top_bottom_telegram_text(self, n)

    def to_html_report(self, output_path: str) -> str:
        from analysis.backtest.report import to_html_report
        return to_html_report(self, output_path)
