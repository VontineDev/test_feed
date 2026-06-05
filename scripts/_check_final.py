"""dart_extractions 최종 결과 집계."""
import asyncio, sys
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()
from core.db import create_pool

async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT corp_name,
                   COUNT(*) total,
                   COUNT(revenue_json) rev_ok,
                   COUNT(segments_json) seg_ok
            FROM dart_extractions
            GROUP BY corp_name
            ORDER BY corp_name
        """)
    print(f"기업수: {len(rows)}")
    print(f"{'기업명':<15} {'전체':>5} {'rev_ok':>6} {'seg_ok':>6}")
    print("-"*40)
    fail = []
    for r in rows:
        mark = "" if r["rev_ok"] == r["total"] else " <-- FAIL"
        print(f"{r['corp_name']:<15} {r['total']:>5} {r['rev_ok']:>6} {r['seg_ok']:>6}{mark}")
        if r["rev_ok"] == 0:
            fail.append(r["corp_name"])

    async with pool.acquire() as conn:
        totals = await conn.fetchrow("""
            SELECT COUNT(*) total, COUNT(revenue_json) rev_ok,
                   COUNT(segments_json) seg_ok
            FROM dart_extractions
        """)
    null_rev = totals["total"] - totals["rev_ok"]
    print(f"\n합계: 전체={totals['total']}, rev_ok={totals['rev_ok']}, seg_ok={totals['seg_ok']}")
    print(f"revenue_json NULL 잔여: {null_rev}건")
    if fail:
        print(f"rev_ok=0 기업: {fail}")
    await pool.close()

asyncio.run(main())
