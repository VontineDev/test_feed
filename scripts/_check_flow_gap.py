"""daily_flow 5~6월 갭 확인"""
import os, sys, asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(override=True)
import asyncpg
from datetime import date

async def main():
    conn = await asyncpg.connect(
        host=os.environ["DB_HOST"], user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"], database=os.environ["DB_NAME"],
        port=int(os.environ.get("DB_PORT", 5432)),
    )
    rows = await conn.fetch(
        "SELECT trade_date, COUNT(*) FROM daily_flow "
        "WHERE trade_date >= $1 GROUP BY trade_date ORDER BY trade_date",
        date(2026, 5, 1),
    )
    print(f"{'날짜':<12} {'티커수':>7}")
    print("-" * 20)
    for r in rows:
        print(f"{str(r[0]):<12} {r[1]:>7}")
    await conn.close()

asyncio.run(main())
