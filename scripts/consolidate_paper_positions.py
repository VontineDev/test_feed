"""
consolidate_paper_positions.py — 같은 티커의 중복 open 포지션을 모델별로 병합

배경 (2026-08-13): paper_eod_sampler_job이 슬롯 수만 확인하고 이미 보유 중인
티커인지는 확인하지 않아, 같은 모델이 같은 티커에 신호를 여러 번 받으면
포지션이 중복으로 열렸다 (241710.KQ 사례: kosdaq/cross 모델이 이틀 연속
신호를 받아 각각 2개씩, 총 4개 포지션 동시보유). 진입 단계의 근본 원인은
jobs/paper_jobs.py의 get_open_or_pending_tickers() 필터로 막았지만
(2026-08-13 커밋), 이미 벌어진 중복은 수동 정리가 필요해 이 스크립트로
남긴다.

같은 티커를 여러 모델이 동시보유하면 브로커 잔고가 모델별로 분리되지 않아
_reconcile_stale_positions()가 자동 보정을 포기하고 스킵한다
(jobs/paper_jobs.py:52) — 이 스크립트로 모델당 1개 포지션까지 정리해두면
그 다음부터는 자동 재조정이 다시 정상 동작한다.

정책:
  1. 대상 티커의 status='open' 포지션을 모델별로 그룹핑.
  2. 모델 내부에 행이 여러 개면 하나로 병합 대상.
  3. 전체 수량(모든 모델 합계)을 모델 수만큼 균등분배(divmod) — 나머지는
     원래 수량이 더 많았던 모델부터 1주씩 배정.
  4. 공통 진입가(entry_actual/entry_theory)는 전체 행을 수량가중평균.
  5. 모델 고유 파라미터(tp1_pct/tp1_ratio/trail_pct/hard_stop_pct)는 그
     모델 행에서 그대로 유지 — 병합해도 모델별 청산 규칙은 안 바뀐다.
  6. 모델 내에 TP1이 이미 발동된 행이 있으면 그 tp1_date/tp1_price를 새
     행에 유지 (제일 이른 발동 기준). TP1 판정 로직은 tp1_date가 있으면
     재트리거 없이 트레일링으로 넘어가므로, 진입가가 바뀌어도 기능적으로는
     문제없다.
  7. signal_date/watermark는 전체 행 중 이른 날짜/최고가를 유지 (보유일
     제한이 갑자기 리셋되거나 워터마크가 후퇴하지 않도록).
  8. 원본 행은 삭제하지 않고 status='closed', exit_type='consolidated_equal',
     exit_price/blended_return=NULL로 남긴다 — dashboard 승률/수익률 집계가
     전부 `blended_return IS NOT NULL` 조건이라 통계는 오염되지 않으면서
     감사(audit) 추적은 유지된다.

사용법:
  python scripts/consolidate_paper_positions.py --ticker 241710.KQ           # dry-run (기본, DB 변경 없음)
  python scripts/consolidate_paper_positions.py --ticker 241710.KQ --apply   # 실제 반영

주의: 병합 대상 모델이 1개뿐이면(중복 없음) 아무 것도 하지 않는다.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import core.db as _db  # noqa: E402


async def _load_open_rows(conn, ticker: str) -> list[dict]:
    rows = await conn.fetch(
        "SELECT * FROM paper_positions WHERE ticker=$1 AND status='open' ORDER BY model, signal_date",
        ticker,
    )
    return [dict(r) for r in rows]


def _group_by_model(rows: list[dict]) -> dict[str, list[dict]]:
    by_model: dict[str, list[dict]] = {}
    for r in rows:
        by_model.setdefault(r["model"], []).append(r)
    return by_model


def _split_equal(total_qty: int, by_model: dict[str, list[dict]]) -> dict[str, int]:
    """전체 수량을 모델 수만큼 균등분배, 나머지는 원래 수량이 컸던 모델부터 1주씩."""
    models = list(by_model)
    n = len(models)
    base, remainder = divmod(total_qty, n)
    order = sorted(models, key=lambda m: sum(r["qty"] for r in by_model[m]), reverse=True)
    split = {m: base for m in models}
    for m in order[:remainder]:
        split[m] += 1
    return split


async def consolidate(pool, ticker: str, apply: bool) -> None:
    async with pool.acquire() as conn:
        rows = await _load_open_rows(conn, ticker)
        if not rows:
            print(f"[{ticker}] open 포지션 없음 — 할 일 없음")
            return

        by_model = _group_by_model(rows)
        if len(rows) == len(by_model):
            print(f"[{ticker}] 이미 모델당 1개 포지션뿐 — 병합 불필요 "
                  f"({', '.join(f'{m}={len(r)}건' for m, r in by_model.items())})")
            return

        total_qty = sum(r["qty"] for r in rows)
        total_cost = sum(r["qty"] * r["entry_actual"] for r in rows)
        total_theory_cost = sum(r["qty"] * r["entry_theory"] for r in rows)
        common_entry_actual = total_cost / total_qty
        common_entry_theory = total_theory_cost / total_qty
        slippage = (common_entry_actual - common_entry_theory) / common_entry_theory if common_entry_theory else None

        earliest_signal_date = min(r["signal_date"] for r in rows)
        watermark = max((r["watermark"] or 0) for r in rows)
        split = _split_equal(total_qty, by_model)

        print(f"[{ticker}] 모델 {len(by_model)}개, 포지션 {len(rows)}건 → 병합 계획")
        print(f"  총수량={total_qty}  공통 entry_actual={common_entry_actual:.4f}  "
              f"공통 entry_theory={common_entry_theory:.4f}")
        for m, model_rows in by_model.items():
            print(f"  - {m}: 기존 {len(model_rows)}건(합계 {sum(r['qty'] for r in model_rows)}주) "
                  f"→ 신규 {split[m]}주")

        if not apply:
            print("  (dry-run — 실제로 반영하려면 --apply)")
            return

        async with conn.transaction():
            new_ids = {}
            for m, model_rows in by_model.items():
                ref = model_rows[0]
                buy_no = "+".join(r["kiwoom_buy_no"] for r in model_rows if r["kiwoom_buy_no"])[:20]
                tp1_rows = sorted(
                    (r for r in model_rows if r["tp1_date"] is not None),
                    key=lambda r: r["tp1_date"],
                )
                tp1 = tp1_rows[0] if tp1_rows else None

                new_id = await conn.fetchval(
                    """
                    INSERT INTO paper_positions
                        (model, ticker, signal_date, entry_theory, entry_actual, slippage_pct,
                         qty, kiwoom_buy_no, tp1_pct, tp1_ratio, trail_pct, hard_stop_pct,
                         tp1_date, tp1_price, watermark, status)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'open')
                    RETURNING id
                    """,
                    m, ticker, earliest_signal_date, common_entry_theory, common_entry_actual,
                    slippage, split[m], buy_no,
                    ref["tp1_pct"], ref["tp1_ratio"], ref["trail_pct"], ref["hard_stop_pct"],
                    tp1["tp1_date"] if tp1 else None,
                    tp1["tp1_price"] if tp1 else None,
                    watermark,
                )
                new_ids[m] = new_id

            old_ids = [r["id"] for r in rows]
            await conn.execute(
                """
                UPDATE paper_positions
                SET status='closed', exit_date=$1, exit_type='consolidated_equal',
                    exit_price=NULL, blended_return=NULL
                WHERE id = ANY($2::int[])
                """,
                date.today(), old_ids,
            )

        print(f"  신규 행: {new_ids}")
        print(f"  원본 {len(old_ids)}건 closed 처리 (id={old_ids}, exit_type=consolidated_equal)")

        chk = await conn.fetch(
            "SELECT id, model, qty, entry_actual FROM paper_positions "
            "WHERE ticker=$1 AND status='open' ORDER BY model",
            ticker,
        )
        print("  검증(현재 open):")
        s = 0
        for r in chk:
            print(f"    {dict(r)}")
            s += r["qty"]
        print(f"  open qty 합계={s} (브로커 실보유와 일치해야 함 — 직접 대조 필요)")


async def main() -> None:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")  # Windows cp949 콘솔에서 한글 로그 깨짐 방지

    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0] if __doc__ else "")
    parser.add_argument("--ticker", required=True, help="대상 티커 (예: 241710.KQ)")
    parser.add_argument("--apply", action="store_true",
                         help="실제로 DB에 반영 (기본은 dry-run — 계획만 출력, 변경 없음)")
    args = parser.parse_args()

    pool = await _db.create_pool()
    try:
        await consolidate(pool, args.ticker, args.apply)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
