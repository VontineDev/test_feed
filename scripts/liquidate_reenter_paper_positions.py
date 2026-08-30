"""
liquidate_reenter_paper_positions.py — 여러 모델이 동시보유해 모델별 귀속이
불가능해진 티커를 브로커 기준 전량 매도한 뒤, 원래 보유하던 모델들을
깨끗한 pending으로 재출발시킨다.

배경 (2026-08-30): FIFO 근사 귀속(jobs/paper_jobs.py::_reconcile_multi_model_ticker)은
qty_ordered가 기록된 이후의 신규 동시보유에만 적용된다. 그 이전 발생분
(085620.KS/036800.KQ/003230.KS/121890.KQ — 2026-08-24~08-28 동시보유)은 이
컬럼이 없어 여전히 자동 재조정 대상이 아니다.

과거 consolidate_paper_positions.py처럼 전체 수량을 모델 수만큼 "균등분배"해서
지어내는 대신, 전량 매도로 모호함 자체를 없애고 각 모델을 qty_ordered가
기록되는 새 pending으로 재출발시킨다 — 실제로 누가 얼마씩 가졌는지 추정할
필요가 없다. 재진입 자체는 새 코드가 필요 없다: 여기서 넣는 pending은
평범한 신규 신호와 동일하게 취급되어 다음 paper_open_entry_job(09:05 KST)이
시가로 사고 qty_ordered까지 정상 기록한다.

정책:
  1. 대상 티커의 status='open' 행 전부를 모델 무관하게 조회.
  2. 브로커 실보유(get_position_qty) 전량을 시장가 매도, confirm_fill()로 체결
     확인. 이 모의투자 서버의 고질적 체결 지연으로 폴링 창 안에 못 잡히면
     여기서 멈추고 아무 것도 정리하지 않는다 — 재실행은 항상 안전하다(매번
     그 시점의 실제 잔고 전체를 다시 조회해 판단하므로 중복 매도 위험 없음).
  3. 매도 후 실보유가 0으로 확인되면(또는 애초에 이미 0이었다면): 기존 open
     행 전부를 status='closed', exit_type='manual_liquidate_reentry',
     exit_price=<매도 확인 시점 현재가>, blended_return=NULL(모델별 귀속
     불가하니 통계 제외 — consolidate_paper_positions.py의 consolidated_equal과
     동일 관례)로 정리.
  4. 원래 있던 모델들 각각에 대해 새 pending 행을 insert_pending()으로 삽입
     (entry_theory=0.0 — 다음 paper_open_entry_job이 실시간 시가로 채움).
     tp1_pct 등 청산 파라미터는 그 모델의 기존 행에서 그대로 복사한다
     (모델별 튜닝값 유지 — analysis/backtest/config.py 참고).

사용법:
  python scripts/liquidate_reenter_paper_positions.py --ticker 085620.KS                    # dry-run
  python scripts/liquidate_reenter_paper_positions.py --ticker 085620.KS --apply             # 실제 반영
  python scripts/liquidate_reenter_paper_positions.py --ticker 085620.KS,036800.KQ --apply   # 여러 티커

주의: --apply는 장중(09:00~15:30 KST)에만 실행하세요. 장 마감 후/주말에는
매도 주문 자체가 브로커에서 거부되거나 무의미합니다.
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
from data.kiwoom_paper_trader import KiwoomPaperTrader, insert_pending  # noqa: E402


async def _load_open_rows(pool, ticker: str) -> list[dict]:
    async with pool.acquire() as conn:
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


async def liquidate_and_reenter(pool, trader, ticker: str, apply: bool) -> None:
    rows = await _load_open_rows(pool, ticker)
    if not rows:
        print(f"[{ticker}] open 포지션 없음 — 할 일 없음")
        return

    by_model = _group_by_model(rows)
    db_total = sum(r["qty"] or 0 for r in rows)
    actual = trader.get_position_qty(ticker)

    print(f"[{ticker}] DB open {len(rows)}건(모델 {len(by_model)}개, 합계 {db_total}주) "
          f"/ 브로커 실보유 {actual}주")
    for m, rs in by_model.items():
        print(f"  - {m}: {sum(r['qty'] or 0 for r in rs)}주 (id={[r['id'] for r in rs]})")

    if not apply:
        print("  계획: 브로커 실보유 전량 매도 → 위 open 전부 closed"
              "(exit_type=manual_liquidate_reentry) → 모델별 새 pending 삽입")
        print("  (dry-run — 실제로 반영하려면 --apply, 장중에만 실행)")
        return

    if actual > 0:
        ord_no = trader.place_sell(ticker, actual)
        filled = trader.confirm_fill(ticker, ord_no, actual, False, actual)
        if filled < actual:
            print(f"  매도 체결 미확인 {filled}/{actual}주(주문번호={ord_no}) — "
                  "지금은 아무 것도 정리하지 않음. 잠시 후 다시 실행하세요(재실행 항상 안전).")
            return
        print(f"  매도 체결 확인 {filled}/{actual}주 (주문번호={ord_no})")
    else:
        print("  브로커 실보유 이미 0 — 매도 생략, 정리만 진행")

    exit_price = trader.get_current_price(ticker)
    if not exit_price:
        print("  현재가 조회 실패 — DB 정리를 진행할 수 없음. 잠시 후 다시 실행하세요.")
        return

    old_ids = [r["id"] for r in rows]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE paper_positions
                SET status='closed', exit_date=$1, exit_type='manual_liquidate_reentry',
                    exit_price=$2, blended_return=NULL
                WHERE id = ANY($3::int[])
                """,
                date.today(), float(exit_price), old_ids,
            )

    new_ids = {}
    for m, rs in by_model.items():
        ref = rs[0]
        new_ids[m] = await insert_pending(
            pool, m, ticker, date.today(), entry_theory=0.0,
            tp1_pct=ref["tp1_pct"], tp1_ratio=ref["tp1_ratio"],
            trail_pct=ref["trail_pct"], hard_stop_pct=ref["hard_stop_pct"],
        )

    print(f"  청산 완료(exit_price={exit_price}) — 기존 {len(old_ids)}건 closed(id={old_ids})")
    print(f"  신규 pending: {new_ids}")
    print("  → 다음 paper_open_entry_job(09:05 KST)에서 시가로 재진입되며 qty_ordered까지 기록됩니다.")


async def main() -> None:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")  # Windows cp949 콘솔에서 한글 로그 깨짐 방지

    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0] if __doc__ else "")
    parser.add_argument("--ticker", required=True,
                         help="대상 티커, 쉼표로 여러 개 (예: 085620.KS,036800.KQ)")
    parser.add_argument("--apply", action="store_true",
                         help="실제로 매도/DB반영 (기본은 dry-run — 계획만 출력, 변경 없음)")
    args = parser.parse_args()

    tickers = [t.strip() for t in args.ticker.split(",") if t.strip()]

    pool = await _db.create_pool()
    trader = KiwoomPaperTrader()
    try:
        for ticker in tickers:
            await liquidate_and_reenter(pool, trader, ticker, args.apply)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
