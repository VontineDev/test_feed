"""
youtube_backtest.py — 블라인드 백테스트: attention_score vs forward return IC 측정

사용법:
  python scripts/youtube_backtest.py
  python scripts/youtube_backtest.py --ret ret_5d --min-samples 50

출력:
  - 전체 IC / t-stat / p-value
  - direction별 평균 forward return
  - 종목별 히트율 (상위 10개)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import psycopg2
import psycopg2.extras


def _connect():
    dsn = os.environ.get("DATABASE_URL", "")
    if not dsn:
        raise SystemExit("DATABASE_URL 환경변수 미설정")
    return psycopg2.connect(dsn)


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
        ic, pval = stats.spearmanr(scores, rets)
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
    parser.add_argument("--ret", default="ret_5d",
                        choices=["ret_1d", "ret_5d", "ret_20d"])
    parser.add_argument("--min-samples", type=int, default=100)
    args = parser.parse_args()

    try:
        from scipy import stats
        import statistics
    except ImportError:
        raise SystemExit("pip install scipy 필요")

    run_backtest(args.ret, args.min_samples)
