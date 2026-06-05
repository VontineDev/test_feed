import asyncio, sys, os
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")
from core.db import create_pool

async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT corp_name, stock_code FROM dart_companies ORDER BY corp_name")
        print(f"dart_companies: {len(rows)}개")
        for r in rows:
            print(f"  {r['corp_name']} ({r['stock_code']})")
        ext = await conn.fetch(
            "SELECT corp_name, rcept_no, segments_json IS NOT NULL as has_seg, "
            "revenue_json IS NOT NULL as has_rev FROM dart_extractions ORDER BY corp_name"
        )
        print(f"\ndart_extractions: {len(ext)}개")
        for r in ext:
            print(f"  {r['corp_name']} | seg={r['has_seg']} rev={r['has_rev']}")
asyncio.run(main())
