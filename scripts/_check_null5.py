"""revenue_json NULL 잔여 5건 상세 확인."""
import asyncio, sys
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()
from core.db import create_pool

async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT corp_name, rcept_no, report_type, period
            FROM dart_extractions
            WHERE revenue_json IS NULL
            ORDER BY corp_name, period
        """)
    print(f"revenue_json NULL: {len(rows)}건")
    for r in rows:
        print(f"  {r['corp_name']} | {r['report_type']} | {r['period']} | {r['rcept_no']}")
    await pool.close()

asyncio.run(main())
