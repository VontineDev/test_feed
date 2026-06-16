"""백필 완료 요약 확인"""
import os, sys, asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv; load_dotenv(override=True)
import asyncpg
from datetime import date

async def main():
    conn = await asyncpg.connect(
        host=os.environ["DB_HOST"], user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"], database=os.environ["DB_NAME"],
        port=int(os.environ.get("DB_PORT", 5432)),
    )
    d_start, d_end = date(2026, 5, 15), date(2026, 6, 14)
    total = await conn.fetchval(
        "SELECT COUNT(*) FROM daily_flow WHERE trade_date >= $1 AND trade_date <= $2",
        d_start, d_end,
    )
    days = await conn.fetchval(
        "SELECT COUNT(DISTINCT trade_date) FROM daily_flow WHERE trade_date >= $1 AND trade_date <= $2",
        d_start, d_end,
    )
    minc = await conn.fetchval(
        "SELECT MIN(cnt) FROM (SELECT COUNT(*) cnt FROM daily_flow WHERE trade_date >= $1 AND trade_date <= $2 GROUP BY trade_date) t",
        d_start, d_end,
    )
    maxc = await conn.fetchval(
        "SELECT MAX(cnt) FROM (SELECT COUNT(*) cnt FROM daily_flow WHERE trade_date >= $1 AND trade_date <= $2 GROUP BY trade_date) t",
        d_start, d_end,
    )
    print(f"[백필 범위: 05-15 ~ 06-14]")
    print(f"  영업일 수: {days}일")
    print(f"  총 행수:   {total:,}")
    print(f"  일별 티커: 최소 {minc} / 최대 {maxc}")
    print()
    # 오늘치
    today_cnt = await conn.fetchval(
        "SELECT COUNT(*) FROM daily_flow WHERE trade_date = $1", date(2026, 6, 15)
    )
    print(f"[오늘 06-15]: {today_cnt}개 티커")
    await conn.close()

asyncio.run(main())
