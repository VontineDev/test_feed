"""
2026-08-05 1회성 코스닥 모의투자 신호 재확인 스크립트.

2026-07-29 investigate 세션에서 발견한 이슈들의 후속 확인:
  1. stage_classifications: 최근 7일 내 KOSDAQ(.KQ) Stage1/2/3 신규 적재 여부
     (jobs/stage_job.py의 market_map/티커캡 수정, 2026-07-24 배포분 검증)
  2. paper_positions: model='kosdaq' pending 발생 여부 (역대 0건이었음)
  3. watchlist_vol_log: KOSDAQ(.KQ) 행 발생 여부 (역대 0건, 원인 미상 — 로그 유실로
     재현 불가했던 건)
  4. run_scheduler.py 프로세스 생존 여부 (2026-07-29 재시작, PID 14092로 시작 —
     이후 재시작됐을 수 있으므로 PID가 아니라 커맨드라인 패턴으로 검색)

결과는 텔레그램으로 1회 전송하고 종료. Windows 작업 스케줄러(schtasks)로 예약된
1회성 작업에서 호출됨 — 사람이 지켜보고 있지 않으므로 예외가 나도 텔레그램으로는
반드시 뭔가 전송되도록 최상위에서 넓게 catch한다.
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
from datetime import date, timedelta

sys.path.insert(0, r"C:\Users\1\test_feed")

import asyncpg
import httpx

from core.db import get_dsn
from telegram.telegram_notify import _get_token, _get_chat_id, _post_message


async def check_stage_classifications(conn: asyncpg.pool.PoolConnectionProxy) -> str:
    cutoff = date.today() - timedelta(days=7)
    rows = await conn.fetch(
        """
        SELECT stage, COUNT(*) AS cnt, MIN(classified_date) AS first_d, MAX(classified_date) AS last_d
        FROM stage_classifications
        WHERE ticker LIKE '%.KQ' AND classified_date > $1
        GROUP BY stage
        ORDER BY stage
        """,
        cutoff,
    )
    if not rows:
        return "stage_classifications: 최근 7일 KOSDAQ 신규 0건"
    lines = [f"  stage={r['stage']}: {r['cnt']}건 ({r['first_d']}~{r['last_d']})" for r in rows]
    return "stage_classifications: 최근 7일 KOSDAQ 신규 있음\n" + "\n".join(lines)


async def check_paper_positions(conn: asyncpg.pool.PoolConnectionProxy) -> str:
    cnt = await conn.fetchval("SELECT COUNT(*) FROM paper_positions WHERE model = 'kosdaq'")
    if cnt == 0:
        return "paper_positions model='kosdaq': 여전히 0건"
    rows = await conn.fetch(
        """
        SELECT ticker, status, signal_date, created_at
        FROM paper_positions WHERE model = 'kosdaq'
        ORDER BY created_at DESC LIMIT 5
        """
    )
    lines = [f"  {r['ticker']} status={r['status']} signal={r['signal_date']}" for r in rows]
    return f"paper_positions model='kosdaq': {cnt}건 발생! (최근 5건)\n" + "\n".join(lines)


async def check_watchlist_vol_log(conn: asyncpg.pool.PoolConnectionProxy) -> str:
    cnt = await conn.fetchval(
        "SELECT COUNT(*) FROM watchlist_vol_log WHERE ticker LIKE '%.KQ'"
    )
    if cnt == 0:
        return "watchlist_vol_log: KOSDAQ 행 여전히 0건"
    rows = await conn.fetch(
        """
        SELECT ticker, trade_date FROM watchlist_vol_log
        WHERE ticker LIKE '%.KQ' ORDER BY trade_date DESC LIMIT 5
        """
    )
    lines = [f"  {r['ticker']} {r['trade_date']}" for r in rows]
    return f"watchlist_vol_log: KOSDAQ {cnt}행 발생! (최근 5건)\n" + "\n".join(lines)


def check_scheduler_alive() -> str:
    try:
        result = subprocess.run(
            [
                "powershell", "-NoProfile", "-Command",
                "Get-CimInstance Win32_Process | "
                "Where-Object { $_.CommandLine -like '*run_scheduler.py*' } | "
                "Select-Object -ExpandProperty ProcessId",
            ],
            capture_output=True, text=True, timeout=30,
        )
        pids = [p.strip() for p in result.stdout.splitlines() if p.strip()]
        if pids:
            # python.exe가 부모/자식 2개 프로세스로 보일 수 있음(2026-07-29 확인,
            # 정상 — APScheduler는 한 번만 시작됨). 0건일 때만 진짜 이상 신호.
            return f"run_scheduler.py: 실행 중 (PID {', '.join(pids)})"
        return "run_scheduler.py: 실행 중인 프로세스 없음 — 다시 죽어있음"
    except Exception as e:
        return f"run_scheduler.py 확인 실패: {e}"


async def main() -> None:
    lines = ["코스닥 모의투자 신호 재확인 (2026-08-05 예약)", ""]
    try:
        pool = await asyncpg.create_pool(dsn=get_dsn(), min_size=1, max_size=2)
        async with pool.acquire() as conn:
            lines.append(await check_stage_classifications(conn))
            lines.append("")
            lines.append(await check_paper_positions(conn))
            lines.append("")
            lines.append(await check_watchlist_vol_log(conn))
        await pool.close()
    except Exception as e:
        lines.append(f"DB 조회 실패: {e}")

    lines.append("")
    lines.append(check_scheduler_alive())

    msg = "\n".join(lines)

    try:
        async with httpx.AsyncClient() as http:
            await _post_message(
                http, _get_token(), _get_chat_id(), msg,
                label="kosdaq-followup", parse_mode=None,
            )
    except Exception as e:
        # 텔레그램 전송조차 실패하면 로그 파일에라도 남긴다 (지켜보는 사람이 없으므로)
        with open(r"C:\Users\1\test_feed\logs\kosdaq_followup_20260805_err.log", "w", encoding="utf-8") as f:
            f.write(msg + f"\n\n텔레그램 전송 실패: {e}\n")


if __name__ == "__main__":
    asyncio.run(main())
