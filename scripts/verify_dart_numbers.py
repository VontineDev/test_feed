"""
종목분석 탭 종목의 dart_extractions 수치 검증
- DB 값 조회 (periods, unit, revenue, op_profit)
- 억원 환산 + 원시값 표시
"""
import asyncio, json, sys
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()
from core.db import create_pool

UNIT_TO_EOK = {
    "원": 1e-8,
    "천원": 1e-5,
    "백만원": 1e-2,
    "억원": 1.0,
    "조원": 1e4,
}

def to_eok(val, unit):
    if val is None:
        return None
    return float(val) * UNIT_TO_EOK.get(unit or "원", 1e-8)

def fmt_eok(val, unit):
    eok = to_eok(val, unit)
    if eok is None:
        return "-"
    if abs(eok) >= 10000:
        return f"{eok/10000:.2f}T"   # 조원
    if abs(eok) >= 1:
        return f"{eok:.0f}E"         # 억원
    return f"{eok*100:.1f}M"         # 백만원대

async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        # dart_companies에서 종목 전체 + dart_extractions JOIN
        rows = await conn.fetch("""
            SELECT dc.stock_code, dc.corp_name as dc_name,
                   de.corp_name as de_name, de.report_type, de.period,
                   de.revenue_json
            FROM dart_companies dc
            LEFT JOIN dart_extractions de ON de.corp_name = dc.corp_name
                AND de.period = (
                    SELECT MAX(de2.period) FROM dart_extractions de2
                    WHERE de2.corp_name = dc.corp_name
                      AND de2.revenue_json IS NOT NULL
                )
            WHERE dc.stock_code IN (
                '000150','035420','017670','003550','000660',
                '005380','189300','005930','034730','090370',
                '207940','009150','028260','010130',
                '011170','000270','000100','051900','015760'
            )
            ORDER BY dc.stock_code
        """)

    print(f"{'code':8} {'name':18} {'rtype':12} {'period':8} {'unit':6} "
          f"{'rev0(raw)':18} {'rev0(eok)':10} {'op0(eok)':10} {'periods'}")
    print("-" * 110)

    issues = []
    for r in rows:
        rev = json.loads(r["revenue_json"]) if r["revenue_json"] else {}
        c = rev.get("consolidated", {})
        unit = rev.get("unit", "none")
        periods = rev.get("periods", [])
        revs = c.get("revenue", [])
        ops = c.get("op_profit", [])
        rtype = (r["report_type"] or "-")[:12]
        period = r["period"] or "-"

        rv0 = revs[0] if revs else None
        rv0_raw = f"{rv0:,.0f}" if rv0 is not None else "-"
        rv0_eok = fmt_eok(rv0, unit)
        op0_eok = fmt_eok(ops[0] if ops else None, unit)

        name = (r["dc_name"] or "-")[:18]
        p_str = str(periods)[:30]

        print(f"{r['stock_code']:8} {name:18} {rtype:12} {period:8} {unit:6} "
              f"{rv0_raw:18} {rv0_eok:10} {op0_eok:10} {p_str}")

        # 이상 징후 탐지
        if rv0 is not None and unit == "원":
            eok = float(rv0) * 1e-8
            # 대형주는 수백억 이상이어야 함
            if eok < 10:
                issues.append(f"[이상] {r['stock_code']} {r['dc_name']}: 매출={rv0:,}원 = {eok:.1f}억원 (너무 작음?)")
        if "당기" in str(periods) or "전기" in str(periods):
            issues.append(f"[기간미확정] {r['stock_code']} {r['dc_name']}: periods={periods}")

    if issues:
        print("\n=== 이상 항목 ===")
        for issue in issues:
            print(issue)

    await pool.close()

asyncio.run(main())
