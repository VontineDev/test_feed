"""
run_cross_combo_backtest.py — TechnicalQuant SCENARIO2 청산조건 × FUNNEL-1/SCORE-1/
AND-1 매수신호를 교차 결합한 조합 백테스트.

2026-08-10: TechnicalQuant 2안(FUNNEL-1/SCORE-1과 동일방법론 정밀비교) 세션에서
"승률은 compose 전략이 우위, 건당 평균수익은 2안 청산 최적화가 근접"이 확인됨 —
그렇다면 "compose의 높은 승률 매수신호"에 "quant가 찾아낸 넓은 청산(RSI80/-12%)"을
붙이면 더 좋아지는지, 반대로 "quant의 저확률·고수익 매수신호"에 "compose의
분할청산(TP1+트레일링)"을 붙이면 좋아지는지 — 매수/청산을 서로 바꿔 끼워보기
위한 스크립트. 두 방향 모두 지원한다:

  A) compose 매수신호(FUNNEL-1/SCORE-1/AND-1) × quant 자기완결 청산
     (analysis/backtest/quant_signals.py:_scan_exit — RSI 익절/MA20이탈/목표가/손절)
  B) quant SCENARIO2 매수신호(PER≤18 또는 PBR단독 유니버스, RSI30 반등) ×
     compose 분할청산 모델(analysis/backtest/exit_models.py — hard_stop→TP1분할→
     트레일링→기간종료, CROSS/SCORE-1/FUNNEL-1 튜닝값 중 택1)

entry와 exit을 서로 다른 기존 검증된 함수에서 그대로 재사용한다 — 새 진입/청산
로직을 만들지 않고, 이미 각자 검증된 두 파이프라인의 절반씩만 섞는다(entry는
strategy_compose.py/quant_signals.py, exit도 exit_models.py/quant_signals.py
그대로). SignalRecord(ticker, signal_date, close_at_signal)만 있으면 어느 쪽
exit 함수에도 그대로 먹인다는 점(둘 다 signal_date로 OHLCV의 entry_idx를
역산)을 이용.

가능한 조합(전체 실행 시 15개, --combo로 개별 실행 가능):
  compose_entry: FUNNEL-1 | SCORE-1 | AND-1
  quant_exit:    quant_original(RSI70/-7%, 2안 문서원안) |
                 quant_optimized(RSI80/-12%, 5단계 최적화) |
                 quant_scenario1(+20%익절/-5%손절/MA20하향, 1안 문서원안)
  quant_entry:   SCENARIO2_PER18(PER≤18∩시총상위200) | SCENARIO2_PBR(PBR0.2~1.0단독)
  compose_exit:  cross(OPTIMAL_EXIT_PARAMS_CROSS) | score1(SCORE-1 전용) |
                 funnel1(FUNNEL-1 전용)

사용법:
    # 전체 15개 조합 실행 (결과 CSV + 콘솔 요약표)
    python scripts/run_cross_combo_backtest.py --start 2025-01-02 --end 2026-08-06

    # 특정 조합만
    python scripts/run_cross_combo_backtest.py --combo FUNNEL-1:quant_optimized
    python scripts/run_cross_combo_backtest.py --combo SCENARIO2_PBR:score1

train/val 분리는 하지 않는다(기존 quant/compose 스윕과 동일 이유 — 표본
100~5000건대가 섞여 있어 일괄 분리가 무의미, project_compose_strategies 메모리
AND-1 사례 참고). 신호 30건 미만 조합은 결과 표에 표시는 하되 신뢰 구간 밖으로
간주할 것.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import pandas as pd
from dotenv import load_dotenv

load_dotenv(os.path.join(Path(__file__).parent.parent, ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── 조합 정의 ────────────────────────────────────────────────────

COMPOSE_ENTRY_STRATEGIES = ["FUNNEL-1", "SCORE-1", "AND-1"]
QUANT_ENTRY_UNIVERSES = ["SCENARIO2_PER18", "SCENARIO2_PBR"]

# quant_signals._scan_exit()에 그대로 넘기는 파라미터 (자기완결 RSI/MA20/목표가 청산)
QUANT_EXIT_VARIANTS: dict[str, dict] = {
    "quant_original": dict(    # 2안 문서 원안: RSI70 익절 / -7% 손절
        hard_stop_pct=0.07, target_pct=None, use_ma20_exit=False,
        use_rsi70_exit=True, rsi_overbought=70.0,
    ),
    "quant_optimized": dict(   # 2안 최적화(5단계 그리드서치 1위): RSI80 익절 / -12% 손절
        hard_stop_pct=0.12, target_pct=None, use_ma20_exit=False,
        use_rsi70_exit=True, rsi_overbought=80.0,
    ),
    "quant_scenario1": dict(   # 1안 문서 원안: +20%익절 / -5%손절 / MA20 하향이탈
        hard_stop_pct=0.05, target_pct=0.20, use_ma20_exit=True,
        use_rsi70_exit=False,
    ),
}

# exit_models.py 분할청산 모델(BacktestConfig 필드)에 그대로 넘기는 파라미터
COMPOSE_EXIT_VARIANTS = ["cross", "score1", "funnel1"]


def _build_combo_matrix() -> list[tuple[str, str, str]]:
    """(entry_kind, entry_key, exit_key) 전체 교차 조합."""
    combos: list[tuple[str, str, str]] = []
    for strat in COMPOSE_ENTRY_STRATEGIES:
        for exit_key in QUANT_EXIT_VARIANTS:
            combos.append(("compose_entry", strat, exit_key))
    for uni in QUANT_ENTRY_UNIVERSES:
        for exit_key in COMPOSE_EXIT_VARIANTS:
            combos.append(("quant_entry", uni, exit_key))
    return combos


# ── A) compose 매수신호 생성 (exit 필드는 비워둔 채 entry만) ────────

def _generate_compose_entries(strategy: str, dsn: str, start: date, end: date, workers: int):
    """strategy_compose 신호 → 진입가 확정까지만 재사용, exit 필드는 초기화해 반환.

    analysis.backtest.engine.run_backtest(mode="compose")를 그대로 호출해
    entry(ticker/signal_date/close_at_signal) 산정 로직(합성 신호 → 금요일
    이후 첫 거래일 진입가)의 정확성을 그대로 물려받는다 — 여기서 다시 구현하지
    않는다. compose 자체 분할청산 결과(sell_*/blended_return 등)는 버리고,
    이후 quant 청산을 새로 적용한다.
    """
    from analysis.backtest.engine import run_backtest
    from analysis.backtest.models import BacktestConfig

    cfg = BacktestConfig(mode="compose", strategy=strategy, start=start, end=end,
                          dsn=dsn, workers=workers)
    result = run_backtest(cfg)
    signals = list(result.signals)
    for s in signals:
        s.sell_date = None
        s.sell_reason = ""
        s.sell_return = None
        s.hold_days = None
        s.tp1_date = None
        s.tp1_ret = None
        s.final_exit_date = None
        s.final_exit_ret = None
        s.final_exit_type = ""
        s.blended_return = None
    return signals


# ── quant 자기완결 청산(_scan_exit) 적용 ────────────────────────────

def _apply_quant_exit(signals, dsn: str, start: date, end: date, workers: int,
                       exit_params: dict) -> None:
    from analysis.backtest.config import TX_COST_DEFAULT
    from analysis.backtest.fetch import _fetch_single_ohlcv
    from analysis.backtest.quant_signals import _scan_exit, compute_indicators
    from core.ohlcv_cache import batch_fetch_cached

    tickers = sorted({s.ticker for s in signals})
    pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t in tickers]
    fetch_start = start - timedelta(days=250)   # ma120 워밍업
    fetch_end = end + timedelta(days=200)       # 보유기간 확보(기간종료 청산용)
    ohlcv_map = batch_fetch_cached(pairs, fetch_start, fetch_end, workers, dsn, _fetch_single_ohlcv)

    indic_cache: dict[str, pd.DataFrame] = {}
    idx_cache: dict[str, dict] = {}

    for sig in signals:
        raw = ohlcv_map.get(sig.ticker)
        if raw is None or raw.empty:
            continue
        df = indic_cache.get(sig.ticker)
        if df is None:
            df = compute_indicators(raw)
            indic_cache[sig.ticker] = df
            idx_map: dict = {}
            for i, ts in enumerate(df.index):
                d = ts.date() if hasattr(ts, "date") else ts
                idx_map[d] = i
            idx_cache[sig.ticker] = idx_map
        entry_idx = idx_cache[sig.ticker].get(sig.signal_date)
        if entry_idx is None:
            continue

        sell_date, sell_reason, sell_return, hold_days = _scan_exit(
            df, entry_idx, sig.close_at_signal, sig.signal_date,
            tx_cost_rt=TX_COST_DEFAULT, **exit_params,
        )
        sig.sell_date = sell_date
        sig.sell_reason = sell_reason
        sig.sell_return = sell_return
        sig.blended_return = sell_return
        sig.hold_days = hold_days


# ── B) quant SCENARIO2 매수신호 생성 (entry만, ohlcv_map도 함께 반환해 재사용) ──

def _generate_quant_entries(universe_key: str, dsn: str, start: date, end: date, workers: int):
    from analysis.backtest.fetch import _fetch_single_ohlcv
    from analysis.backtest.models import SignalRecord
    from analysis.backtest.quant_signals import replay_quant
    from analysis.chart_screener import fetch_kind_sector_map, get_all_tickers
    from analysis.fundamentals import compute_ratios, screen
    from core.ohlcv_cache import batch_fetch_cached, load_listed_shares
    from run_quant_entry_exit_sweep import build_fundamental_thresholds
    from run_quant_filter_sweep import _rank_by_market_cap

    filter_mode = "pbr" if universe_key == "SCENARIO2_PBR" else "per"
    per_max = 18.0
    mktcap_top_n = 200

    sector_map = fetch_kind_sector_map()
    tickers = get_all_tickers(sector_map=sector_map if sector_map else None)
    name_of = {t: n for t, n, _ in tickers}

    fetch_start = start - timedelta(days=400)
    pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t, _, _ in tickers]
    ohlcv_map = batch_fetch_cached(pairs, fetch_start, end, workers, dsn, _fetch_single_ohlcv)

    listed_shares = load_listed_shares(dsn)
    ranked = _rank_by_market_cap(ohlcv_map, listed_shares, start, end)
    mktcap_universe = {t for t, _ in ranked[:mktcap_top_n]}

    ratios_df = compute_ratios(dsn)
    thresholds = build_fundamental_thresholds(filter_mode, per_max)
    fund_universe = screen(ratios_df, thresholds)

    universe = mktcap_universe & fund_universe
    logger.info("[quant-entry] %s 유니버스: %d종목", universe_key, len(universe))

    signals = []
    for ticker in universe:
        df = ohlcv_map.get(ticker)
        if df is None or df.empty:
            continue
        market = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"
        try:
            # 청산은 이후 compose 분할청산으로 새로 계산하므로 여기서는 사실상
            # 비활성화(하드스탑 -99%)해 entry(signal_date/close_at_signal)만 취한다.
            raw_sigs = replay_quant(
                ticker, name_of.get(ticker) or ticker, df, market, start, end,
                entry_key="SCENARIO2", hard_stop_pct=0.99, target_pct=None,
                use_ma20_exit=False, use_rsi70_exit=False, rsi_oversold=30.0,
            )
        except Exception as e:
            logger.debug("  %s SCENARIO2 재현 실패(무시): %s", ticker, e)
            continue
        for s in raw_sigs:
            signals.append(SignalRecord(
                ticker=s.ticker, name=s.name, signal_date=s.signal_date,
                close_at_signal=s.close_at_signal, mode="cross", market=s.market,
            ))
    return signals, ohlcv_map


# ── compose 분할청산 모델(exit_models.py) 적용 ──────────────────────

def _apply_compose_exit(signals, ohlcv_map: dict, start: date, end: date,
                         exit_param_key: str) -> None:
    from analysis.backtest import config as cfgmod
    from analysis.backtest.config import TX_COST_DEFAULT
    from analysis.backtest.exit_models import _compute_sell_signals_and_s2
    from analysis.backtest.models import BacktestConfig

    param_map = {
        "cross":   cfgmod.OPTIMAL_EXIT_PARAMS_CROSS,
        "score1":  cfgmod.OPTIMAL_EXIT_PARAMS_SCORE1,
        "funnel1": cfgmod.OPTIMAL_EXIT_PARAMS_FUNNEL1,
    }
    params = param_map[exit_param_key]
    cfg = BacktestConfig(mode="cross", start=start, end=end, **params)
    _compute_sell_signals_and_s2(signals, ohlcv_map, TX_COST_DEFAULT, cfg=cfg)


# ── 조합 1건 실행 + 집계 ─────────────────────────────────────────

def run_combo(combo: tuple[str, str, str], dsn: str, start: date, end: date,
              workers: int) -> Optional[dict]:
    from analysis.backtest.helpers import _compute_group_metrics

    entry_kind, entry_key, exit_key = combo
    if entry_kind == "compose_entry":
        signals = _generate_compose_entries(entry_key, dsn, start, end, workers)
        if not signals:
            return None
        _apply_quant_exit(signals, dsn, start, end, workers, QUANT_EXIT_VARIANTS[exit_key])
        label = f"{entry_key}매수 × {exit_key}청산"
    else:
        signals, ohlcv_map = _generate_quant_entries(entry_key, dsn, start, end, workers)
        if not signals:
            return None
        _apply_compose_exit(signals, ohlcv_map, start, end, exit_key)
        label = f"{entry_key}매수 × {exit_key}분할청산"

    m = _compute_group_metrics(signals, rf_annual=0.03, hold_weeks=None)
    return {
        "combo": label,
        "entry": entry_key,
        "exit": exit_key,
        "n_signals": len(signals),
        "n_closed": sum(1 for s in signals
                        if s.blended_return is not None or s.sell_return is not None),
        "win_rate": m.win_rate_sell,
        "avg_return": m.avg_return_sell,
        "median_return": m.median_return_sell,
        "mdd": m.mdd,
    }


def _pct(v: Optional[float], dp: int = 1) -> str:
    return f"{v * 100:+.{dp}f}%" if v is not None else "N/A"


def main() -> None:
    parser = argparse.ArgumentParser(description="퀀트청산×compose매수 교차조합 백테스트")
    parser.add_argument("--start", default="2025-01-02", help="YYYY-MM-DD")
    parser.add_argument("--end", default="2026-08-06", help="YYYY-MM-DD")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--combo", default=None,
                         help="'진입키:청산키' 형식으로 특정 조합 1개만 실행 "
                              "(예: FUNNEL-1:quant_optimized, SCENARIO2_PBR:score1). "
                              "미지정 시 15개 조합 전체 실행")
    parser.add_argument("--output", default="results/cross_combo_backtest.csv")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        u, p = os.environ.get("DB_USER", ""), os.environ.get("DB_PASSWORD", "")
        h = os.environ.get("DB_HOST", "localhost")
        port = os.environ.get("DB_PORT", "5432")
        db = os.environ.get("DB_NAME", "news_db")
        if u and p:
            from urllib.parse import quote
            dsn = f"postgresql://{u}:{quote(p)}@{h}:{port}/{db}"
    if not dsn:
        sys.exit("DATABASE_URL (또는 DB_USER/DB_PASSWORD) 환경변수가 필요합니다")

    if args.combo:
        entry_key, exit_key = args.combo.split(":")
        entry_kind = "compose_entry" if entry_key in COMPOSE_ENTRY_STRATEGIES else "quant_entry"
        combos = [(entry_kind, entry_key, exit_key)]
    else:
        combos = _build_combo_matrix()

    rows = []
    t0 = time.time()
    for i, combo in enumerate(combos, 1):
        logger.info("[cross-combo] (%d/%d) %s", i, len(combos), combo)
        try:
            row = run_combo(combo, dsn, start, end, args.workers)
        except Exception as e:
            logger.warning("  실패(%s): %s", combo, e)
            row = None
        if row:
            rows.append(row)
            logger.info("  → 신호%d 승률%s 평균%s", row["n_signals"],
                        _pct(row["win_rate"]), _pct(row["avg_return"]))

    logger.info("[cross-combo] 전체 완료 — %.1f초, %d/%d 조합 성공",
                time.time() - t0, len(rows), len(combos))

    df = pd.DataFrame(rows)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("[cross-combo] 결과 저장: %s", out_path)

    print(f"\n{'='*100}")
    print(f"교차조합 백테스트 결과 ({start}~{end}) — 평균수익 내림차순")
    print(f"{'='*100}")
    if df.empty:
        print("실행된 조합이 없습니다.")
    else:
        show = df.copy()
        show["win_rate"] = show["win_rate"].map(_pct)
        show["avg_return"] = show["avg_return"].map(_pct)
        show["median_return"] = show["median_return"].map(_pct)
        show["mdd"] = show["mdd"].map(_pct)
        cols = ["combo", "n_signals", "win_rate", "avg_return", "median_return", "mdd"]
        print(show[cols].sort_values("avg_return", ascending=False).to_string(index=False))
    print(f"{'='*100}")
    print("(참고) 신호 30건 미만 조합은 표본 부족 — 결과 CSV에는 남기되 신뢰 구간 밖으로 볼 것.")


if __name__ == "__main__":
    main()
