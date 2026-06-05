"""매출 유효 0인 기업들의 revenue_json 실제 내용 확인."""
import asyncio, sys, json
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()
from core.db import create_pool

TARGETS = ["SK하이닉스", "현대자동차", "NAVER", "카카오", "하나금융지주", "기아"]

async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT corp_name, period, report_type, revenue_json
            FROM dart_extractions
            WHERE corp_name = ANY($1::text[])
            ORDER BY corp_name, period DESC
        """, TARGETS)

    for r in rows:
        rv = r["revenue_json"]
        if rv is None:
            print(f"[{r['corp_name']} {r['period']}] revenue_json=NULL")
            continue
        d = json.loads(rv) if isinstance(rv, str) else rv
        if isinstance(d, list):
            print(f"[{r['corp_name']} {r['period']}] revenue_json=LIST({len(d)}개): {str(d)[:120]}")
            print()
            continue
        periods = d.get("periods", [])
        cons = d.get("consolidated", {})
        segs = d.get("segments", [])
        rev_vals = cons.get("revenue", [])
        op_vals  = cons.get("op_profit", [])
        has_rev = any(v not in (None, 0, "", "-") for v in rev_vals)
        has_op  = any(v not in (None, 0, "", "-") for v in op_vals)
        seg_cnt = len([s for s in segs if any(v not in (None, 0, "", "-") for v in s.get("revenues", []))])
        print(f"[{r['corp_name']} {r['period']} {r['report_type']}]")
        print(f"  periods={periods}, rev={rev_vals}, op={op_vals}, segs={seg_cnt}개")
        if segs:
            print(f"  segs[0]={segs[0]}")
        print()

    await pool.close()

asyncio.run(main())
