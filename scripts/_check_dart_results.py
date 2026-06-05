import asyncio, sys
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")
from core.db import create_pool

async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        # 전체 통계
        total = await conn.fetchval("SELECT count(*) FROM dart_extractions")
        has_seg = await conn.fetchval("SELECT count(*) FROM dart_extractions WHERE segments_json IS NOT NULL")
        has_rev = await conn.fetchval("SELECT count(*) FROM dart_extractions WHERE revenue_json IS NOT NULL")
        has_comp = await conn.fetchval("SELECT count(*) FROM dart_extractions WHERE competitors_json IS NOT NULL")
        all_ok = await conn.fetchval(
            "SELECT count(*) FROM dart_extractions "
            "WHERE segments_json IS NOT NULL AND revenue_json IS NOT NULL"
        )

        print(f"=== dart_extractions 전체 통계 ===")
        print(f"총 행수          : {total:,}")
        print(f"segments_json OK : {has_seg:,}  ({has_seg/total*100:.1f}%)")
        print(f"revenue_json OK  : {has_rev:,}  ({has_rev/total*100:.1f}%)")
        print(f"competitors_json : {has_comp:,}  ({has_comp/total*100:.1f}%)")
        print(f"seg+rev 모두 OK  : {all_ok:,}  ({all_ok/total*100:.1f}%)")

        # 기업별 성공률
        print(f"\n=== 기업별 추출 현황 (segments+revenue 기준) ===")
        rows = await conn.fetch("""
            SELECT corp_name,
                   count(*) as total_reports,
                   count(*) FILTER (WHERE segments_json IS NOT NULL) as seg_ok,
                   count(*) FILTER (WHERE revenue_json IS NOT NULL) as rev_ok
            FROM dart_extractions
            GROUP BY corp_name
            ORDER BY seg_ok DESC, corp_name
        """)
        print(f"{'기업명':<20} {'보고서':>6} {'seg':>5} {'rev':>5}")
        print("-" * 40)
        for r in rows:
            mark = "✓" if r["seg_ok"] > 0 else " "
            print(f"{mark} {r['corp_name']:<18} {r['total_reports']:>6} {r['seg_ok']:>5} {r['rev_ok']:>5}")

        # 실패 기업 (seg=0) 샘플
        failed = [r for r in rows if r["seg_ok"] == 0]
        print(f"\nsegments 추출 실패 기업: {len(failed)}개")
        for r in failed[:10]:
            print(f"  {r['corp_name']} (보고서 {r['total_reports']}건)")

asyncio.run(main())
