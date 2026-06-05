import asyncio, sys
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")
from core.db import create_pool

async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        ext = await conn.fetch(
            "SELECT corp_name, report_type, period, "
            "segments_json IS NOT NULL as has_seg, "
            "revenue_json IS NOT NULL as has_rev, "
            "competitors_json IS NOT NULL as has_comp "
            "FROM dart_extractions ORDER BY corp_name"
        )
        print(f"dart_extractions: {len(ext)}행")
        for r in ext:
            print(f"  {r['corp_name']:<16} {r['period']:<10} {r['report_type']:<12} "
                  f"seg={r['has_seg']} rev={r['has_rev']} comp={r['has_comp']}")

        # DART API 잔여 한도 관련 환경변수 확인
        import os
        key = os.environ.get("DART_API_KEY", "")
        print(f"\nDART_API_KEY: {'설정됨' if key else '미설정'} ({key[:6]}...)" if key else "\nDART_API_KEY: 미설정")

asyncio.run(main())
