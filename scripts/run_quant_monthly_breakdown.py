"""
run_quant_monthly_breakdown.py — 교차조합 백테스트 신호를 월별로 집계.

2026-08-14: TechnicalQuant.md 4-4단계 최고 기록("모멘텀단독×funnel1분할청산",
103건/승률72.8%/평균+14.4%)의 신호별 상세(진입월/청산월/수익률)를 월별로
묶어 "달마다 수익률이 얼마였는지" 보여준다. run_cross_combo_backtest.py의
내부 함수(_generate_quant_entries/_apply_compose_exit)를 그대로 재사용 —
새 백테스트 로직을 만들지 않고 이미 검증된 파이프라인 결과만 재집계한다.

집계 기준: final_exit_date(잔여분 청산일) 월 — "그 달에 실현된 손익"
기준(펀드 월간 리포트와 동일한 관점). signal_date(진입월) 기준 분포도
참고용으로 함께 출력한다.

사용법:
    python scripts/run_quant_monthly_breakdown.py --entry MOMENTUM_TOP20 --exit funnel1
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")  # Windows cp949 콘솔 한글 깨짐 방지

import pandas as pd
from dotenv import load_dotenv

load_dotenv(os.path.join(Path(__file__).parent.parent, ".env"))

from run_cross_combo_backtest import _apply_compose_exit, _generate_quant_entries  # noqa: E402


def _dsn() -> str:
    dsn = os.environ.get("DATABASE_URL")
    if dsn:
        return dsn
    u, p = os.environ.get("DB_USER", ""), os.environ.get("DB_PASSWORD", "")
    h = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", "5432")
    db = os.environ.get("DB_NAME", "news_db")
    if u and p:
        from urllib.parse import quote
        return f"postgresql://{u}:{quote(p)}@{h}:{port}/{db}"
    sys.exit("DATABASE_URL (또는 DB_USER/DB_PASSWORD) 환경변수가 필요합니다")


def main() -> None:
    parser = argparse.ArgumentParser(description="교차조합 신호를 월별로 집계")
    parser.add_argument("--entry", default="MOMENTUM_TOP20",
                         choices=["MOMENTUM_TOP20", "QVM_TOP20"])
    parser.add_argument("--exit", default="funnel1", choices=["cross", "score1", "funnel1"])
    parser.add_argument("--start", default="2025-01-02")
    parser.add_argument("--end", default="2026-08-06")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--output", default=None,
                         help="신호별 상세 CSV 저장 경로 (기본: results/quant_monthly_<entry>_<exit>.csv)")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    dsn = _dsn()

    signals, ohlcv_map = _generate_quant_entries(args.entry, dsn, start, end, args.workers)
    if not signals:
        sys.exit(f"{args.entry} 유니버스에서 신호가 생성되지 않았습니다.")
    _apply_compose_exit(signals, ohlcv_map, start, end, args.exit)

    rows = []
    for s in signals:
        if s.blended_return is None:
            continue  # 아직 미청산(기간 내 미종결) — 월별 집계 제외
        exit_dt = s.final_exit_date or s.tp1_date
        rows.append({
            "ticker": s.ticker, "name": s.name,
            "signal_date": s.signal_date, "entry_month": s.signal_date.strftime("%Y-%m"),
            "exit_date": exit_dt, "exit_month": exit_dt.strftime("%Y-%m") if exit_dt else None,
            "blended_return": s.blended_return,
            "tp1_date": s.tp1_date, "final_exit_type": s.final_exit_type,
        })
    df = pd.DataFrame(rows)

    out_path = Path(args.output or f"results/quant_monthly_{args.entry}_{args.exit}.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"\n{'='*70}")
    print(f"{args.entry}매수 × {args.exit}분할청산 — 청산월별 집계 (n={len(df)})")
    print(f"{'='*70}")
    # 참고: 신호들은 서로 다른 종목을 동시에 보유(병렬)하는 구조라 "월별 복리
    # 누적수익률" 같은 단일자본 재투자 지표는 의미가 없다(순차 재투자를 가정하게
    # 돼 왜곡됨) — 신호 단위 평균/중앙값/승률만 월별로 집계한다.
    by_exit = df.groupby("exit_month")["blended_return"].agg(
        n="count", win_rate=lambda s: (s > 0).mean(), avg="mean", median="median",
    )
    show = by_exit.copy()
    show["win_rate"] = show["win_rate"].map(lambda v: f"{v*100:.1f}%")
    show["avg"] = show["avg"].map(lambda v: f"{v*100:+.1f}%")
    show["median"] = show["median"].map(lambda v: f"{v*100:+.1f}%")
    print(show.to_string())

    print(f"\n{'='*70}")
    print(f"{args.entry}매수 × {args.exit}분할청산 — 진입월별 신호수 분포 (참고용)")
    print(f"{'='*70}")
    by_entry = df.groupby("entry_month")["blended_return"].agg(n="count", avg="mean")
    by_entry["avg"] = by_entry["avg"].map(lambda v: f"{v*100:+.1f}%")
    print(by_entry.to_string())

    overall_win = (df["blended_return"] > 0).mean()
    overall_avg = df["blended_return"].mean()
    print(f"\n전체: n={len(df)}  승률={overall_win*100:.1f}%  평균={overall_avg*100:+.1f}%")
    print(f"신호별 상세 저장: {out_path}")


if __name__ == "__main__":
    main()
