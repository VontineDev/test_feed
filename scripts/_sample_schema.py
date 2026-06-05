"""dart_extractions 스키마 및 샘플 확인."""
import asyncio, sys, json
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()
from core.db import create_pool

async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        # 컬럼 목록
        cols = await conn.fetch("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'dart_extractions'
            ORDER BY ordinal_position
        """)
        print("=== 컬럼 ===")
        for c in cols:
            print(f"  {c['column_name']}: {c['data_type']}")

        # 보고서 수 상위 10개 기업
        top = await conn.fetch("""
            SELECT corp_name, COUNT(*) cnt,
                   COUNT(revenue_json) rev_ok,
                   COUNT(segments_json) seg_ok
            FROM dart_extractions
            GROUP BY corp_name
            ORDER BY cnt DESC, corp_name
            LIMIT 10
        """)
        print("\n=== 상위 10개 기업 ===")
        for r in top:
            print(f"  {r['corp_name']}: 보고서 {r['cnt']}건, rev={r['rev_ok']}, seg={r['seg_ok']}")

        # 삼성전자 최신 레코드 샘플
        row = await conn.fetchrow("""
            SELECT corp_name, period, report_type, narrative_text,
                   revenue_json, segments_json, competitors_json, extracted_at
            FROM dart_extractions
            WHERE corp_name = '삼성전자' AND revenue_json IS NOT NULL
            ORDER BY period DESC
            LIMIT 1
        """)
        print("\n=== 삼성전자 샘플 ===")
        print(f"  period: {row['period']}, type: {row['report_type']}")
        print(f"  narrative_text: {str(row['narrative_text'])[:200] if row['narrative_text'] else 'NULL'}")
        rev = json.loads(row['revenue_json']) if row['revenue_json'] else None
        seg = json.loads(row['segments_json']) if row['segments_json'] else None
        comp = json.loads(row['competitors_json']) if row['competitors_json'] else None
        print(f"  revenue_json keys: {list(rev.keys()) if rev else None}")
        print(f"  revenue_json.periods: {rev.get('periods') if rev else None}")
        print(f"  revenue_json.consolidated: {rev.get('consolidated') if rev else None}")
        print(f"  segments_json[0]: {seg[0] if seg else None}")
        print(f"  competitors_json[0]: {comp[0] if comp else None}")

    await pool.close()

asyncio.run(main())
