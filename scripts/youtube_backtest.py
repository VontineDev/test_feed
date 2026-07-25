"""
youtube_backtest.py — 블라인드 백테스트: attention_score vs forward return IC 측정

사용법:
  # 개별 언급 단위 IC (기본)
  python scripts/youtube_backtest.py --ret ret_5d

  # 주간 집계 → 1달 수익률 IC (권장)
  python scripts/youtube_backtest.py --mode weekly --min-samples 20

출력:
  - 전체 IC / t-stat / p-value
  - direction별 평균 forward return  (mention 모드)
  - 종목별 히트율 상위 10개          (mention 모드)
  - 주간 (ticker, window_end) 단위 IC (weekly 모드)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import cast

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

import psycopg2
import psycopg2.extras


def _connect():
    from core.db import get_dsn
    return psycopg2.connect(get_dsn())


_VALID_RET_COLS = {"ret_1d", "ret_5d", "ret_20d"}
_RET_COL_SQL = {c: f"fr.{c}" for c in _VALID_RET_COLS}


def run_backtest(ret_col: str = "ret_5d", min_samples: int = 100) -> dict:
    if ret_col not in _VALID_RET_COLS:
        raise ValueError(f"Invalid ret_col: {ret_col!r}. Must be one of {_VALID_RET_COLS}")
    from scipy import stats
    import statistics

    col_expr = _RET_COL_SQL[ret_col]
    conn = _connect()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 언급 레코드 + attention_score + forward return 조인
            cur.execute(f"""
                SELECT
                    r.ticker,
                    r.direction,
                    r.video_date,
                    a.attention_score,
                    {col_expr}        AS forward_ret
                FROM   youtube_mention_raw r
                JOIN   youtube_mention_forward_returns fr ON fr.mention_id = r.id
                LEFT JOIN youtube_attention_scores a
                    ON  a.ticker     = r.ticker
                    AND a.window_end = r.video_date
                WHERE  r.ticker IS NOT NULL
                  AND  {col_expr} IS NOT NULL
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        print("데이터 없음. --backfill 먼저 실행하세요.")
        return {}

    print(f"\n{'='*55}")
    print(f"  YouTube 내러티브 블라인드 백테스트 ({ret_col})")
    print(f"{'='*55}")
    print(f"  총 레코드: {len(rows)}건")

    if len(rows) < min_samples:
        print(f"  샘플 부족 ({len(rows)} < {min_samples}). 더 많은 데이터 수집 필요.")
        return {"n": len(rows), "ic": None}

    # ── 전체 IC ───────────────────────────────────────
    import math
    ic = pval = t_stat = None
    scores = []
    rets   = []
    for r in rows:
        if r["attention_score"] is not None and r["forward_ret"] is not None:
            scores.append(float(r["attention_score"]))
            rets.append(float(r["forward_ret"]))

    if len(scores) < 10:
        print("  attention_score 있는 레코드 부족")
    else:
        ic_raw, pval = stats.spearmanr(scores, rets)
        ic = cast(float, ic_raw)
        n = len(scores)
        if math.isnan(ic):
            print("  IC = NaN (점수가 모두 동일 — 분산 없음). 데이터 품질 확인 필요.")
            ic = pval = None
        else:
            t_stat = ic * math.sqrt(n - 2) / math.sqrt(max(1 - ic**2, 1e-9))
            print(f"\n[IC 분석] n={n}")
            print(f"  Spearman IC : {ic:+.4f}")
            print(f"  t-stat      : {t_stat:+.2f}")
            print(f"  p-value     : {pval:.4f}")
            _verdict(ic, abs(t_stat), n)

    # ── direction별 평균 return ───────────────────────
    from collections import defaultdict
    dir_rets: dict[str, list] = defaultdict(list)
    for r in rows:
        d = r["direction"] or "neutral"
        dir_rets[d].append(float(r["forward_ret"]))

    print(f"\n[Direction별 평균 {ret_col}]")
    for direction in ["buy", "neutral", "sell"]:
        vals = dir_rets.get(direction, [])
        if vals:
            mean = statistics.mean(vals)
            print(f"  {direction:8s}: n={len(vals):4d}  avg={mean:+.4f} ({mean*100:+.2f}%)")

    # ── 종목별 히트율 (상위 10) ───────────────────────
    ticker_hits: dict[str, list] = defaultdict(list)
    for r in rows:
        if r["ticker"] and r["direction"] == "buy":
            ticker_hits[r["ticker"]].append(float(r["forward_ret"]) > 0)

    ranked = sorted(
        [(tk, sum(v)/len(v), len(v)) for tk, v in ticker_hits.items() if len(v) >= 3],
        key=lambda x: -x[1],
    )[:10]

    if ranked:
        print("\n[buy 언급 히트율 상위 10 (n>=3)]")
        for tk, hitrate, n in ranked:
            print(f"  {tk:12s}: {hitrate:.0%} hit  (n={n})")

    print(f"\n{'='*55}\n")

    result = {"n": len(rows), "ic": ic, "t_stat": t_stat, "pval": pval}
    return result


def run_weekly_monthly_backtest(min_samples: int = 20) -> dict:
    """주간 attention_score → 다음 1달(ret_20d) 수익률 IC 측정.

    집계 단위: (ticker, window_end) — 5 영업일 롤링 창의 마지막 날.
    forward return: window_end 당일 ~ 20 영업일 후 실제 수익률.
    LATERAL JOIN으로 각 (ticker, window_end)에서 가장 최근 언급의 ret_20d를 사용.
    """
    from scipy import stats
    import math
    import statistics

    conn = _connect()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    a.ticker,
                    a.window_end,
                    a.attention_score,
                    fr.ret_20d AS forward_ret
                FROM youtube_attention_scores a
                JOIN LATERAL (
                    SELECT r.id
                    FROM   youtube_mention_raw r
                    WHERE  r.ticker     = a.ticker
                      AND  r.video_date <= a.window_end
                      AND  r.video_date >= a.window_end - INTERVAL '6 days'
                      AND  r.ticker IS NOT NULL
                    ORDER  BY r.video_date DESC
                    LIMIT  1
                ) lm ON true
                JOIN youtube_mention_forward_returns fr ON fr.mention_id = lm.id
                WHERE fr.ret_20d         IS NOT NULL
                  AND a.attention_score  IS NOT NULL
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    print(f"\n{'='*55}")
    print("  YouTube 내러티브 주간 신호 → 1달 수익률 백테스트")
    print(f"{'='*55}")
    print(f"  집계 단위: (ticker, window_end)  총 {len(rows)}건")

    if not rows:
        print("  데이터 없음. --backfill 먼저 실행하세요.")
        return {}

    if len(rows) < min_samples:
        print(f"  샘플 부족 ({len(rows)} < {min_samples}). 더 많은 데이터 수집 필요.")
        return {"n": len(rows), "ic": None}

    scores = [float(r["attention_score"]) for r in rows]
    rets   = [float(r["forward_ret"])    for r in rows]

    ic_raw, pval = stats.spearmanr(scores, rets)
    ic = cast(float, ic_raw)
    n = len(scores)

    if math.isnan(ic):
        print("  IC = NaN (점수 분산 없음). 데이터 품질 확인 필요.")
        return {"n": n, "ic": None}

    t_stat = ic * math.sqrt(n - 2) / math.sqrt(max(1 - ic**2, 1e-9))
    print(f"\n[IC 분석 — 주간 집계 기준] n={n}")
    print(f"  Spearman IC : {ic:+.4f}")
    print(f"  t-stat      : {t_stat:+.2f}")
    print(f"  p-value     : {pval:.4f}")
    _verdict(ic, abs(t_stat), n)

    # 상위/하위 attention_score 구간별 평균 ret_20d
    pairs = sorted(zip(scores, rets), key=lambda x: x[0])
    q = max(1, n // 3)
    low_ret  = statistics.mean(r for _, r in pairs[:q])
    high_ret = statistics.mean(r for _, r in pairs[-q:])
    print(f"\n[attention_score 구간별 평균 ret_20d]")
    print(f"  하위 3분위 (n={q}): {low_ret:+.4f} ({low_ret*100:+.2f}%)")
    print(f"  상위 3분위 (n={q}): {high_ret:+.4f} ({high_ret*100:+.2f}%)")

    print(f"\n{'='*55}\n")
    return {"n": n, "ic": ic, "t_stat": t_stat, "pval": pval}


def _verdict(ic: float, t_stat: float, n: int) -> None:
    print()
    if ic > 0.05 and t_stat > 1.65:
        print("  [합격] IC > 0.05 AND t > 1.65 — attention_score 편입 검토 가능")
    elif ic > 0.01:
        print("  [조건부] IC 약양. rolling window/가중치 조정 후 v2 재검증 권장")
    elif ic < 0:
        print("  [역지표 후보] IC 음수. 청산/경계 신호로 재활용 설계 검토")
    else:
        print("  [불합격] 신호 없음. 채널 교체 또는 전처리 개선 필요")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="mention",
                        choices=["mention", "weekly"],
                        help="mention: 개별 언급 단위 IC / weekly: 주간 집계→1달 수익률 IC")
    parser.add_argument("--ret", default="ret_5d",
                        choices=["ret_1d", "ret_5d", "ret_20d"],
                        help="mention 모드 전용 forward return 컬럼")
    parser.add_argument("--min-samples", type=int, default=100)
    args = parser.parse_args()

    try:
        from scipy import stats
        import statistics
    except ImportError:
        raise SystemExit("pip install scipy 필요")

    if args.mode == "weekly":
        run_weekly_monthly_backtest(args.min_samples)
    else:
        run_backtest(args.ret, args.min_samples)
