"""
run_quant_qvm_backtest.py — 방법론4: QVM(퀄리티+밸류+모멘텀) 복합 팩터 백테스트

2026-08-11: TechnicalQuant.md 신규 방법론4. 기존 필터(PER 단독, PBR 단독 —
6단계 SCENARIO3~5)는 전부 "단일 팩터 AND" 방식이었는데, 이번엔 퀄리티
(매출총이익률/FCF÷부채/발생액비율)+밸류(PER)+모멘텀(6개월 수익률) 3개 팩터를
각각 백분위 순위로 바꿔 동일가중 합산한 종합점수(qvm_score) 상위 N%를
유니버스로 쓴다 — analysis/fundamentals.py의 compute_qvm_score 참고.

매매타이밍은 새로 만들지 않고 2안(SCENARIO2: RSI30 상향돌파 매수/RSI70 익절/
-7%손절) 원안을 그대로 재사용 — run_quant_scenario_variants.py와 동일한
패턴으로 "필터만 바꿔서 비교"한다.

필요 데이터: dart_fundamentals에 cogs/gross_profit/operating_cash_flow/capex
(scripts/dart_fundamentals_backfill.py 재백필로 채움, 2026-08-11 스키마 확장).

사용법:
    python scripts/run_quant_qvm_backtest.py --start 2025-01-02 --end 2026-08-06
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

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


def _run_scenario2_with_mdd(
    universe: set[str],
    ohlcv_map: dict,
    tickers: list[tuple[str, str, str]],
    start,
    end,
) -> dict:
    """entry_key=SCENARIO2 원안(RSI30진입/RSI70익절/-7%손절)을 직접 재현해
    원시 신호를 남긴 뒤 MDD까지 계산한다 (run_quant_backtest.run_condition은
    집계된 metrics dict만 반환해 MDD 계산에 필요한 signal_date/sell_date를
    버리므로 여기서는 replay_quant를 직접 호출).
    """
    from analysis.backtest.helpers import _compute_signal_interval_mdd
    from analysis.backtest.quant_signals import replay_quant

    name_of = {t: n for t, n, _ in tickers}
    all_signals = []
    for ticker in universe:
        df = ohlcv_map.get(ticker)
        if df is None or df.empty:
            continue
        market = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"
        try:
            sigs = replay_quant(
                ticker, name_of.get(ticker) or ticker, df, market, start, end,
                entry_key="SCENARIO2",
                hard_stop_pct=0.07, target_pct=None,
                use_ma20_exit=False, use_rsi70_exit=True,
            )
        except Exception as e:
            logger.debug("  %s 재현 실패(무시): %s", ticker, e)
            continue
        all_signals.extend(sigs)

    rets = [s.blended_return for s in all_signals if s.blended_return is not None]
    win = sum(1 for r in rets if r > 0)
    return {
        "n": len(all_signals),
        "n_closed": len(rets),
        "win_rate": (win / len(rets)) if rets else None,
        "avg_return": (sum(rets) / len(rets)) if rets else None,
        "mdd": _compute_signal_interval_mdd(all_signals),
    }


def build_variants() -> list[dict]:
    """QVM 유니버스 배리에이션 정의(순수 함수). mktcap_restrict=True면 시총상위200과
    교집합(2안 원안 유니버스 규모와 맞춤), False면 전체 시장 QVM 상위 N%.

    실제 정의는 analysis/fundamentals.py::QVM_UNIVERSE_VARIANTS — analysis/backtest/
    model_registry.py도 같은 상수를 가져다 쓴다(2026-08-23, 손으로 복제해 값이
    따로 놀던 문제 해소). 리스트를 그대로 반환하면 호출자가 실수로 원본을
    변형할 수 있어 얕은 복사본을 반환한다."""
    from analysis.fundamentals import QVM_UNIVERSE_VARIANTS
    return [dict(v) for v in QVM_UNIVERSE_VARIANTS]


def main() -> None:
    parser = argparse.ArgumentParser(description="방법론4: QVM 복합 팩터 백테스트")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--output", default="results/quant_qvm_backtest.csv")
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

    from analysis.backtest.fetch import _fetch_single_ohlcv
    from analysis.chart_screener import fetch_kind_sector_map, get_all_tickers
    from analysis.fundamentals import compute_qvm_score, compute_ratios, load_momentum, screen_qvm_top_pct
    from core.ohlcv_cache import batch_fetch_cached, load_listed_shares
    from run_quant_backtest import _pct, _select_universe

    logger.info("[qvm] 종목 목록 조회 중...")
    sector_map = fetch_kind_sector_map()
    tickers = get_all_tickers(sector_map=sector_map if sector_map else None)
    logger.info("[qvm] 대상 티커 %d개", len(tickers))

    fetch_start = start - timedelta(days=400)
    ticker_pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t, _, _ in tickers]

    t0 = time.time()
    logger.info("[qvm] OHLCV 로드 중 (캐시 우선)...")
    ohlcv_map = batch_fetch_cached(ticker_pairs, fetch_start, end, args.workers, dsn, _fetch_single_ohlcv)
    logger.info("[qvm] OHLCV 로드 완료 — %.1f초, %d/%d 티커", time.time() - t0, len(ohlcv_map), len(tickers))

    listed_shares = load_listed_shares(dsn)

    logger.info("[qvm] 펀더멘털 + 모멘텀 로드 중...")
    ratios_df = compute_ratios(dsn)
    momentum_df = load_momentum(dsn)
    qvm_df = compute_qvm_score(ratios_df, momentum_df)
    logger.info("[qvm] QVM 스코어 계산 완료 — %d종목(3개 팩터 전부 계산 가능한 종목만, "
                "펀더멘털 %d ∩ 모멘텀 %d에서 결측 제외)",
                len(qvm_df), len(ratios_df), len(momentum_df))
    if qvm_df.empty:
        sys.exit("QVM 스코어를 계산할 수 있는 종목이 0개입니다 — "
                 "dart_fundamentals의 cogs/gross_profit/operating_cash_flow/capex가 "
                 "채워져 있는지(scripts/dart_fundamentals_backfill.py 재백필) 확인하세요.")

    universe_mktcap200 = _select_universe(ohlcv_map, listed_shares, start, end, "mktcap_top200")

    rows = []
    for v in build_variants():
        qvm_universe = screen_qvm_top_pct(qvm_df, v["top_pct"])
        universe = (universe_mktcap200 & qvm_universe) if v["mktcap_restrict"] else qvm_universe
        logger.info("[qvm] %s 유니버스: QVM상위%d ∩ (시총200제한=%s) = %d종목",
                    v["name"], len(qvm_universe), v["mktcap_restrict"], len(universe))

        m = _run_scenario2_with_mdd(universe, ohlcv_map, tickers, start, end)
        logger.info("[qvm] %s 완료 — 신호 %d건, 승률 %s, 평균 %s, MDD %s",
                    v["name"], m["n"], _pct(m["win_rate"]), _pct(m["avg_return"]), _pct(m["mdd"]))
        rows.append({
            "시나리오": v["name"], "유니버스종목수": len(universe), **m, "설명": v["note"],
        })

    df_out = pd.DataFrame(rows)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("[qvm] 결과 저장: %s", out_path)

    print(f"\n{'='*100}")
    print(f"방법론4: QVM(퀄리티+밸류+모멘텀) 복합 팩터 백테스트 ({start}~{end})")
    print(f"{'='*100}")
    print(f"{'시나리오':28s} {'종목수':>6s} {'신호':>6s} {'승률':>8s} {'평균수익':>10s} {'MDD':>8s}")
    for r in rows:
        print(f"{r['시나리오']:28s} {r['유니버스종목수']:>6d} {r['n']:>6d} "
              f"{_pct(r['win_rate']):>8s} {_pct(r['avg_return']):>10s} {_pct(r['mdd']):>8s}")
    print(f"{'='*100}")
    print("비교 기준선(project_technicalquant_backtest 메모리, 전부 entry_key=SCENARIO2 원안 청산):")
    print("  2안 원안(PER≤15,시총상위200)      = 신호100, 승률43.0%, 평균+2.9%")
    print("  SCENARIO5(PBR단독,시총상위200)    = 신호121, 승률51.2%, 평균+5.4%")
    print("  FUNNEL-1(모의투자)                = 신호4643, 승률64.1%, 평균+9.7%, MDD-35.9%")
    print("  SCORE-1(모의투자)                 = 신호1658, 승률70.6%, 평균+9.2%, MDD-39.5%")


if __name__ == "__main__":
    main()
