import asyncio, sys
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")
from core.db import create_pool

async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT stock_code, corp_name FROM dart_companies "
            "WHERE stock_code IS NOT NULL AND length(stock_code)=6 "
            "ORDER BY stock_code"
        )
        print(f"상장 종목 수: {len(rows)}")
        print("샘플 (처음 20):")
        for r in rows[:20]:
            print(f"  {r['stock_code']} {r['corp_name']}")
asyncio.run(main())
