"""
db_trades.py  —  trade_log 테이블 접근 함수 (거래 저널)
────────────────────────────────────────────────────────────
/buy /sell /port /pnl 텔레그램 명령 지원. core.db facade를 통해 re-export됨.
"""

from __future__ import annotations

import logging
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)


async def save_trade(
    pool: asyncpg.Pool,
    *,
    ticker: str,
    entry_date,        # datetime.date — NOT a string
    entry_price: int,
    qty: int,
    memo: Optional[str] = None,
) -> Optional[int]:
    """
    /buy 명령 처리 — trade_log INSERT.
    signal_date, stage_at_entry, after_*_at_signal 는 내부에서 자동 조회.
    반환: 저장된 row id, 실패 시 None.
    """
    try:
        async with pool.acquire() as conn:
            # 가장 최근 Stage 신호 조회 (classified_date 사용)
            sig = await conn.fetchrow(
                """
                SELECT MAX(classified_date) AS sig_date,
                       (SELECT stage FROM stage_classifications sc2
                        WHERE sc2.ticker = $1
                          AND sc2.classified_date = MAX(sc1.classified_date)
                        LIMIT 1) AS stage
                FROM stage_classifications sc1
                WHERE ticker = $1
                  AND classified_date <= $2
                  AND stage IN (1, 2, 3)
                """,
                ticker,
                entry_date,
            )
            signal_date = sig["sig_date"] if sig else None
            stage_at_entry = sig["stage"] if sig else None

            # 시간외 데이터 조회 (aftermarket_snap — same PG instance)
            ref_date = signal_date or (entry_date - __import__("datetime").timedelta(days=1))
            after_row = await conn.fetchrow(
                """
                SELECT after_close, after_chg_pct
                FROM   aftermarket_snap
                WHERE  ticker = $1 AND trade_date = $2
                """,
                ticker,
                ref_date,
            )
            after_close    = after_row["after_close"]    if after_row else None
            after_chg_pct  = after_row["after_chg_pct"] if after_row else None

            row = await conn.fetchrow(
                """
                INSERT INTO trade_log
                    (ticker, entry_date, entry_price, qty,
                     signal_date, stage_at_entry,
                     after_close_at_signal, after_chg_pct_at_signal,
                     memo)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                RETURNING id
                """,
                ticker,
                entry_date,      # date object — asyncpg requirement
                entry_price,
                qty,
                signal_date,
                stage_at_entry,
                after_close,
                after_chg_pct,
                memo,
            )
        return row["id"] if row else None
    except Exception as e:
        logger.error("[trade] save_trade 실패: %s", e)
        return None


async def close_position(
    pool: asyncpg.Pool,
    *,
    ticker: str,
    exit_date,         # datetime.date
    exit_price: int,
) -> Optional[dict]:
    """
    /sell 명령 처리 — FIFO (entry_date 가장 오래된 미청산 포지션).
    SELECT FOR UPDATE로 더블탭 레이스 컨디션 차단.
    반환: 닫힌 row dict, 없으면 None.
    """
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # 1. FIFO 포지션 선택 (SELECT FOR UPDATE)
                row = await conn.fetchrow(
                    """
                    SELECT id, entry_price, qty
                    FROM   trade_log
                    WHERE  ticker = $1
                      AND  exit_date IS NULL
                    ORDER  BY entry_date ASC
                    LIMIT  1
                    FOR UPDATE
                    """,
                    ticker,
                )
                if not row:
                    return None

                # 2. stage_at_exit 조회 (오늘 기준)
                stage_row = await conn.fetchrow(
                    """
                    SELECT stage FROM stage_classifications
                    WHERE  ticker = $1
                      AND  classified_date = $2
                    """,
                    ticker,
                    exit_date,
                )
                stage_at_exit = stage_row["stage"] if stage_row else None

                # 3. 청산 기록
                updated = await conn.fetchrow(
                    """
                    UPDATE trade_log
                    SET    exit_date     = $1,
                           exit_price    = $2,
                           stage_at_exit = $3,
                           updated_at    = now()
                    WHERE  id = $4
                    RETURNING *
                    """,
                    exit_date,
                    exit_price,
                    stage_at_exit,
                    row["id"],
                )
        return dict(updated) if updated else None
    except Exception as e:
        logger.error("[trade] close_position 실패: %s", e)
        return None


async def get_open_positions(pool: asyncpg.Pool) -> list[dict]:
    """미청산 포지션 전체 반환 (/port 명령용)."""
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, ticker, entry_date, entry_price, qty,
                       signal_date, stage_at_entry
                FROM   trade_log
                WHERE  exit_date IS NULL
                ORDER  BY entry_date ASC
                """,
            )
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("[trade] get_open_positions 실패: %s", e)
        return []


async def get_pnl_summary(
    pool: asyncpg.Pool,
    *,
    period: str = "all",   # "week" | "month" | "all"
) -> dict:
    """
    실현 P&L 요약 (/pnl 명령용).
    period:
      week  — 직전 캘린더 주 월~일 (KST 기준)
      month — 이번 달 1일~오늘
      all   — 전체
    반환: {total_pnl, trade_cnt, win_cnt, avg_win, avg_loss, by_stage}
    """
    from datetime import datetime as _datetime, timedelta as _td
    from zoneinfo import ZoneInfo
    kst = ZoneInfo("Asia/Seoul")
    today = _datetime.now(kst).date()

    where = "exit_date IS NOT NULL"
    args: list = []

    if period == "week":
        # 직전 월~일
        dow = today.weekday()  # 0=Mon
        last_mon = today - _td(days=dow + 7)
        last_sun = last_mon + _td(days=6)
        where += " AND exit_date BETWEEN $1 AND $2"
        args = [last_mon, last_sun]
    elif period == "month":
        first = today.replace(day=1)
        where += " AND exit_date BETWEEN $1 AND $2"
        args = [first, today]

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT pnl, pnl_pct, stage_at_entry
                FROM   trade_log
                WHERE  {where}
                """,
                *args,
            )
        records = [dict(r) for r in rows]

        pnls   = [r["pnl"] for r in records if r["pnl"] is not None]
        wins   = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]

        by_stage: dict = {}
        for r in records:
            s = r["stage_at_entry"]
            if s is None:
                continue
            entry = by_stage.setdefault(s, {"total": 0, "wins": 0})
            entry["total"] += 1
            if r["pnl"] and r["pnl"] > 0:
                entry["wins"] += 1

        return {
            "trade_cnt": len(records),
            "win_cnt":   len(wins),
            "total_pnl": int(sum(pnls)) if pnls else 0,
            "avg_win":   int(sum(wins)   / len(wins))   if wins   else 0,
            "avg_loss":  int(sum(losses) / len(losses)) if losses else 0,
            "by_stage":  by_stage,
        }
    except Exception as e:
        logger.error("[trade] get_pnl_summary 실패: %s", e)
        return {"trade_cnt": 0, "win_cnt": 0, "total_pnl": 0,
                "avg_win": 0, "avg_loss": 0, "by_stage": {}}
