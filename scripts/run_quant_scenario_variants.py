"""
run_quant_scenario_variants.py — TechnicalQuant.md 1안/2안의 매매타이밍(기술
진입/청산) 로직은 그대로 두고, 종목선택(펀더멘털 필터 · 유니버스 컷오프)
조합만 바꿔 SCENARIO3~8을 비교한다.

배경 (2026-08-07): 1안(밸류+추세추종)은 "거래대금 상위20% ∩ PBR≤0.8+
ROE≥10%+부채비율≤100%" 교집합이 3종목뿐이라 신호9건으로 판단 자체가
불가능했다(project_technicalquant_backtest 메모리 3단계) — 유니버스나
필터를 완화하면 살아나는지 확인이 필요. 2안(RSI과매도반등)은 이미 필터
스윕(PER 상한×시총 유니버스, 4단계)으로 최적값을 찾았으나, PER 단일조건
외에 문서 1절이 제시한 다른 팩터(ROE·PBR)도 단독/조합으로 시도해볼 가치가
있어 추가 검증한다.

새 진입/청산 조건 함수는 추가하지 않는다 — entry_key로 SCENARIO1/SCENARIO2의
기존 기술 로직(scripts/run_quant_backtest.py의 run_condition)을 그대로
재사용하고, 유니버스 선정(_select_universe)과 펀더멘털 스크리닝(screen)만
조합을 바꾼다.

시나리오 정의:

| 이름                    | 기술 기반      | 유니버스           | 필터                                   |
|-------------------------|----------------|--------------------|-----------------------------------------|
| SCENARIO3_roe_only       | 2안(RSI반등)   | 시총상위200        | ROE≥8%                                  |
| SCENARIO4_per_roe        | 2안(RSI반등)   | 시총상위200        | PER≤18(4단계 최적치) AND ROE≥8%         |
| SCENARIO5_pbr_only       | 2안(RSI반등)   | 시총상위200        | 0.2<PBR<1.0                             |
| SCENARIO6_loosen_universe| 1안(MA20돌파)  | 거래대금상위50%    | PBR≤0.8+ROE≥10%+부채비율≤100%(원안 유지)|
| SCENARIO7_loosen_filter  | 1안(MA20돌파)  | 거래대금상위20%(원안)| PBR≤1.2+ROE≥8%+부채비율≤150%(문서1절 일반조건)|
| SCENARIO8_loosen_both    | 1안(MA20돌파)  | 거래대금상위50%    | PBR≤1.2+ROE≥8%+부채비율≤150%            |

신호 30건 미만 조합은 소표본으로 노이즈 위험이 커 결론에서 신뢰하지 않는다
(2026-08-06 AND-1 스윕 사례, project_compose_strategies 메모리 참고) — 표는
전부 출력하되 이 기준으로 해석할 것.

사용법:
    python scripts/run_quant_scenario_variants.py --start 2025-01-02 --end 2026-08-06
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


def build_variants() -> list[dict]:
    """SCENARIO3~8 정의를 반환한다(순수 함수 — DB/네트워크 접근 없음, 유닛
    테스트 대상). 각 항목: name/entry_key/universe_mode/universe_kwargs/
    thresholds(RatioThresholds)/note."""
    from analysis.fundamentals import RatioThresholds

    return [
        {
            "name": "SCENARIO3_roe_only",
            "entry_key": "SCENARIO2",
            "universe_mode": "mktcap_top200",
            "universe_kwargs": {},
            "thresholds": RatioThresholds(
                pbr_min=None, pbr_max=None, per_min=None, per_max=None,
                roe_min=0.08, debt_ratio_max=None, revenue_growth_min=None,
            ),
            "note": "2안 기술(RSI30진입/RSI70청산/-7%손절) 유지, 필터를 PER→ROE≥8% 단독으로 교체",
        },
        {
            "name": "SCENARIO4_per_roe",
            "entry_key": "SCENARIO2",
            "universe_mode": "mktcap_top200",
            "universe_kwargs": {},
            "thresholds": RatioThresholds(
                pbr_min=None, pbr_max=None, per_min=0.0, per_max=18.0,
                roe_min=0.08, debt_ratio_max=None, revenue_growth_min=None,
            ),
            "note": "2안 기술 유지, 필터를 PER≤18(4단계 최적치)+ROE≥8% 결합으로 확장",
        },
        {
            "name": "SCENARIO5_pbr_only",
            "entry_key": "SCENARIO2",
            "universe_mode": "mktcap_top200",
            "universe_kwargs": {},
            "thresholds": RatioThresholds(
                pbr_min=0.2, pbr_max=1.0, per_min=None, per_max=None,
                roe_min=None, debt_ratio_max=None, revenue_growth_min=None,
            ),
            "note": "2안 기술 유지, 필터를 PER→PBR(0.2~1.0, 문서1절 밸류에이션 구간) 단독으로 교체",
        },
        {
            "name": "SCENARIO6_loosen_universe",
            "entry_key": "SCENARIO1",
            "universe_mode": "txamt_top20",
            "universe_kwargs": {"pct": 0.50},
            "thresholds": RatioThresholds(
                pbr_min=None, pbr_max=0.8, per_min=None, per_max=None,
                roe_min=0.10, debt_ratio_max=1.0, revenue_growth_min=None,
            ),
            "note": "1안 필터(PBR≤0.8+ROE≥10%+부채비율≤100%) 원안 유지, 유니버스만 거래대금 상위20%→50%로 완화",
        },
        {
            "name": "SCENARIO7_loosen_filter",
            "entry_key": "SCENARIO1",
            "universe_mode": "txamt_top20",
            "universe_kwargs": {"pct": 0.20},
            "thresholds": RatioThresholds(
                pbr_min=None, pbr_max=1.2, per_min=None, per_max=None,
                roe_min=0.08, debt_ratio_max=1.5, revenue_growth_min=None,
            ),
            "note": "1안 유니버스(거래대금 상위20%) 원안 유지, 필터만 문서 1절 일반조건(PBR≤1.2/ROE≥8%/부채비율≤150%)으로 완화",
        },
        {
            "name": "SCENARIO8_loosen_both",
            "entry_key": "SCENARIO1",
            "universe_mode": "txamt_top20",
            "universe_kwargs": {"pct": 0.50},
            "thresholds": RatioThresholds(
                pbr_min=None, pbr_max=1.2, per_min=None, per_max=None,
                roe_min=0.08, debt_ratio_max=1.5, revenue_growth_min=None,
            ),
            "note": "1안 유니버스+필터 둘 다 완화(거래대금 상위50%, PBR≤1.2/ROE≥8%/부채비율≤150%)",
        },
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="TechnicalQuant SCENARIO3~8 필터 배리에이션 비교")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--output", default="results/quant_scenario_variants.csv")
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
    from analysis.fundamentals import compute_ratios, screen
    from core.ohlcv_cache import batch_fetch_cached, load_listed_shares
    from run_quant_backtest import _pct, _select_universe, run_condition

    logger.info("[variants] 종목 목록 조회 중...")
    sector_map = fetch_kind_sector_map()
    tickers = get_all_tickers(sector_map=sector_map if sector_map else None)
    logger.info("[variants] 대상 티커 %d개", len(tickers))

    fetch_start = start - timedelta(days=400)
    ticker_pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t, _, _ in tickers]

    t0 = time.time()
    logger.info("[variants] OHLCV 로드 중 (캐시 우선)...")
    ohlcv_map = batch_fetch_cached(ticker_pairs, fetch_start, end, args.workers, dsn, _fetch_single_ohlcv)
    logger.info("[variants] OHLCV 로드 완료 — %.1f초, %d/%d 티커", time.time() - t0, len(ohlcv_map), len(tickers))

    listed_shares = load_listed_shares(dsn)

    logger.info("[variants] 펀더멘털 로드 중...")
    ratios_df = compute_ratios(dsn)

    rows = []
    for v in build_variants():
        base_universe = _select_universe(
            ohlcv_map, listed_shares, start, end, v["universe_mode"], **v["universe_kwargs"],
        )
        fund_universe = screen(ratios_df, v["thresholds"])
        universe = base_universe & fund_universe
        logger.info("[variants] %s 유니버스: 기본%d ∩ 필터%d = %d종목",
                    v["name"], len(base_universe), len(fund_universe), len(universe))

        m = run_condition(v["entry_key"], ohlcv_map, tickers, start, end, universe=universe)
        logger.info("[variants] %s 완료 — 신호 %d건, 승률 %s, 평균 %s",
                    v["name"], m["n"], _pct(m["win_rate"]), _pct(m["avg_return"]))
        rows.append({
            "시나리오": v["name"], "기술기반": v["entry_key"],
            "유니버스종목수": len(universe), **m, "설명": v["note"],
        })

    df_out = pd.DataFrame(rows)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("[variants] 결과 저장: %s", out_path)

    print(f"\n{'='*110}")
    print(f"TechnicalQuant SCENARIO3~8 필터 배리에이션 비교 ({start}~{end})")
    print(f"{'='*110}")
    print(f"{'시나리오':26s} {'기술':10s} {'종목수':>6s} {'신호':>6s} {'승률':>8s} {'평균수익':>10s}")
    for r in rows:
        print(f"{r['시나리오']:26s} {r['기술기반']:10s} {r['유니버스종목수']:>6d} {r['n']:>6d} "
              f"{_pct(r['win_rate']):>8s} {_pct(r['avg_return']):>10s}")
    print(f"{'='*110}")
    print("비교 기준선(project_technicalquant_backtest 메모리):")
    print("  2안 원안(PER≤15,시총상위200)          = 신호100, 승률43.0%, 평균+2.9%")
    print("  2안 4단계 최적필터(PER≤18,시총상위200) = 신호129, 승률46.5%, 평균+4.06%")
    print("  2안 5단계 최적청산(진입30/청산80/-12%) = 신호129, 승률50.4%, 평균+9.47%")
    print("  1안 원안(거래대금상위20%,PBR≤0.8+ROE≥10%+부채비율≤100%) = 신호9, 승률22.2%, 평균-1.3%")


if __name__ == "__main__":
    main()
