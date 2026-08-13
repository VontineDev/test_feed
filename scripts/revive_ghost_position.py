"""
revive_ghost_position.py — 브로커엔 남아있는데 DB엔 없는(closed로 잘못 기록된)
"유령 보유" 포지션을 새 open 행으로 복원.

배경 (2026-08-10, TODOS.md "매수 오탐(buy_never_filled)으로 브로커에만 남은
유령 보유 20건" 참고): confirm_fill() 관련 버그들로 실제 체결된 매수가
"미체결"로 오판돼 status='closed'(exit_type='reconciled_no_data' 등)로
DB에서 지워진 포지션들이 있었다. 그 중 원가 단서(entry_theory)가 있는
건들은 당시 수동 복원했지만, 008470.KQ처럼 entry_theory=0.0이라 "복구할
진입가 데이터가 없다"고 결론 내려 미해결로 남은 건이 있었다.

2026-08-13 발견: 이 결론은 더 이상 유효하지 않다 — 브로커(kt00018)가 자체
보관하는 평균매입가(pur_pric)를 KiwoomPaperTrader.get_position_avg_price()로
가져올 수 있으므로, 우리 DB에 원가 기록이 없어도 브로커 쪽 기록으로 복구
가능하다.

정책:
  1. 브로커 실보유(get_position_qty)가 0이면 유령 보유가 아님 — 종료.
  2. 같은 (ticker, model)로 이미 open/pending 행이 있으면 이미 추적 중 —
     이 스크립트가 아니라 _reconcile_stale_positions()/consolidate_paper_
     positions.py 대상 — 종료.
  3. 같은 (ticker, model)의 가장 최근 closed 행을 참고 행으로 삼아
     tp1_pct/tp1_ratio/trail_pct/hard_stop_pct/signal_date를 승계 (없으면
     기본값 + 오늘 날짜).
  4. entry_actual = 브로커 평균매입가(pur_pric). entry_theory는 참고 행의
     값이 0보다 크면 그대로, 아니면 entry_actual과 동일(이론가 단서 없음).
  5. qty = 브로커 실보유. status='open'으로 신규 삽입.

참고 closed 행은 건드리지 않는다(당시 기록 그대로 유지 — 감사 추적).

사용법:
  python scripts/revive_ghost_position.py --ticker 008470.KQ --model compose-funnel1           # dry-run
  python scripts/revive_ghost_position.py --ticker 008470.KQ --model compose-funnel1 --apply    # 실제 반영
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
from data.kiwoom_paper_trader import KiwoomPaperTrader  # noqa: E402

_DEFAULTS = {"tp1_pct": 0.15, "tp1_ratio": 0.5, "trail_pct": 0.10, "hard_stop_pct": 0.10}


async def revive(pool, trader: KiwoomPaperTrader, ticker: str, model: str, apply: bool) -> None:
    qty = trader.get_position_qty(ticker)
    if qty <= 0:
        print(f"[{ticker}] 브로커 실보유 0 — 유령 보유 아님, 할 일 없음")
        return
    avg_price = trader.get_position_avg_price(ticker)
    if not avg_price:
        print(f"[{ticker}] 브로커 평균매입가 조회 실패 — 복원 불가 (다음에 재시도)")
        return

    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT id FROM paper_positions WHERE ticker=$1 AND model=$2 AND status IN ('open','pending')",
            ticker, model,
        )
        if existing:
            print(f"[{ticker}/{model}] 이미 open/pending 포지션 있음(id={existing['id']}) — "
                  f"유령 보유가 아니라 재조정 대상. 이 스크립트 대상 아님.")
            return

        ref = await conn.fetchrow(
            "SELECT * FROM paper_positions WHERE ticker=$1 AND model=$2 AND status='closed' "
            "ORDER BY exit_date DESC NULLS LAST, id DESC LIMIT 1",
            ticker, model,
        )
        ref = dict(ref) if ref else {}

        signal_date = ref.get("signal_date") or date.today()
        entry_theory = ref["entry_theory"] if ref.get("entry_theory") else avg_price
        params = {k: (ref.get(k) if ref.get(k) is not None else v) for k, v in _DEFAULTS.items()}

        print(f"[{ticker}/{model}] 복원 계획:")
        print(f"  브로커 실보유={qty}주  평균매입가={avg_price}")
        print(f"  참고 closed 행: {'id=' + str(ref['id']) if ref else '없음(기본값 사용)'}")
        print(f"  신규 open 행: signal_date={signal_date} entry_theory={entry_theory} "
              f"entry_actual={avg_price} qty={qty} params={params}")

        if not apply:
            print("  (dry-run — 실제로 반영하려면 --apply)")
            return

        new_id = await conn.fetchval(
            """
            INSERT INTO paper_positions
                (model, ticker, signal_date, entry_theory, entry_actual, slippage_pct,
                 qty, tp1_pct, tp1_ratio, trail_pct, hard_stop_pct, watermark, status)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'open')
            RETURNING id
            """,
            model, ticker, signal_date, entry_theory, avg_price,
            (avg_price - entry_theory) / entry_theory if entry_theory else None,
            qty, params["tp1_pct"], params["tp1_ratio"], params["trail_pct"],
            params["hard_stop_pct"], float(avg_price),
        )
        print(f"  신규 open 행 id={new_id} 생성 완료. 참고 closed 행은 그대로 둠(감사 추적).")


async def main() -> None:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")  # Windows cp949 콘솔에서 한글 로그 깨짐 방지

    parser = argparse.ArgumentParser(description="브로커에만 남은 유령 보유를 open 행으로 복원")
    parser.add_argument("--ticker", required=True, help="대상 티커 (예: 008470.KQ)")
    parser.add_argument("--model", required=True, help="복원 대상 모델 (예: compose-funnel1)")
    parser.add_argument("--apply", action="store_true", help="실제로 DB에 반영 (기본은 dry-run)")
    args = parser.parse_args()

    pool = await _db.create_pool()
    try:
        trader = KiwoomPaperTrader()
        await revive(pool, trader, args.ticker, args.model, args.apply)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
