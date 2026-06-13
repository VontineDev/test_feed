"""삼성전자/SK하이닉스 DB 수치 확인 및 DART XML 대조"""
import json, asyncio, sys
from pathlib import Path
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()
from core.db import create_pool
from data.dart_extractor import (
    DART_DIR, _parse_report_dir, _pick_main_xml, extract_xbrl_quarterly
)

STOCKS = [
    ("005930", "삼성전자"),
    ("000660", "SK하이닉스"),
    ("034730", "SK"),
]

async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        for code, name in STOCKS:
            r = await conn.fetchrow("""
                SELECT de.corp_name, de.rcept_no, de.report_type, de.period, de.revenue_json
                FROM dart_companies dc
                JOIN dart_extractions de ON de.corp_name = dc.corp_name
                WHERE dc.stock_code = $1
                ORDER BY de.extracted_at DESC LIMIT 1
            """, code)
            if not r:
                print(f"[{name}] DB 없음")
                continue

            rev = json.loads(r["revenue_json"])
            c = rev.get("consolidated", {})
            unit = rev.get("unit", "원")
            revs = c.get("revenue", [])
            ops  = c.get("op_profit", [])
            periods = rev.get("periods", [])

            print(f"\n[{name}] ({code})")
            print(f"  rcept_no={r['rcept_no']} type={r['report_type']} period={r['period']}")
            print(f"  periods={periods}")
            print(f"  unit={unit}")
            rev0 = revs[0] if revs else None
            if rev0:
                eok = float(rev0) / 1e8
                print(f"  revenue[0]={rev0:,} => {eok:.0f}억원 = {eok/10000:.2f}조원")
            op0 = ops[0] if ops else None
            if op0:
                eok = float(op0) / 1e8
                print(f"  op_profit[0]={op0:,} => {eok:.0f}억원 = {eok/10000:.2f}조원")

            # DART XML 직접 재파싱으로 값 재확인
            corp_name = r["corp_name"]
            rcept_no  = r["rcept_no"]
            co_dir = DART_DIR / corp_name
            xml_path = None
            if co_dir.is_dir():
                for rd in co_dir.iterdir():
                    if not rd.is_dir(): continue
                    p_rcept, _, _ = _parse_report_dir(rd.name)
                    if p_rcept == rcept_no:
                        xml_path = _pick_main_xml(rd, rcept_no)
                        break

            if xml_path:
                xbrl = extract_xbrl_quarterly(xml_path)
                if xbrl:
                    rv = xbrl["revenue"][0] if xbrl["revenue"] else None
                    op = xbrl["op_profit"][0] if xbrl["op_profit"] else None
                    print(f"  [XML 재파싱] revenue[0]={rv:,}" if rv else "  [XML] revenue 없음")
                    if rv:
                        print(f"    => {float(rv)/1e8:.0f}억원 = {float(rv)/1e12:.2f}조원")
                    match = "OK" if rv == revs[0] else "MISMATCH"
                    print(f"  DB vs XML: {match}")
            else:
                print(f"  [XML 없음] {co_dir}")

    await pool.close()

asyncio.run(main())
