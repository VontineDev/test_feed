"""DART 추출 전체 종목 단위/수치 이상 탐지 — 분기/반기보고서 우선"""
import asyncio, json, sys
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()
from core.db import create_pool

UNIT_DIV = {"원": 1e8, "천원": 1e5, "백만원": 1e2, "억원": 1.0, "조원": 1e-4}

def parse_num(v):
    if v is None: return None
    s = str(v).replace(",", "").strip()
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()")
    try:
        return -float(s) if neg else float(s)
    except (ValueError, TypeError):
        return None

def to_eok(v, unit):
    n = parse_num(v)
    if n is None: return None
    return n / UNIT_DIV.get(unit or "원", 1e8)

async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT corp_name, rcept_no, report_type, period, revenue_json
            FROM dart_extractions
            WHERE revenue_json IS NOT NULL
            ORDER BY corp_name, period DESC
        """)

    total = len(rows)
    q_rows = [r for r in rows if "분기" in (r["report_type"] or "") or "반기" in (r["report_type"] or "")]
    a_rows = [r for r in rows if r not in q_rows]

    issues_q = []
    issues_a = []

    for r in rows:
        is_q = "분기" in (r["report_type"] or "") or "반기" in (r["report_type"] or "")
        target = issues_q if is_q else issues_a

        rev = json.loads(r["revenue_json"]) if r["revenue_json"] else {}
        if not isinstance(rev, dict):
            target.append(f"[타입이상] {r['corp_name']} {r['period']}")
            continue

        unit = rev.get("unit", "원")
        if unit not in UNIT_DIV and unit != "unknown":
            target.append(f"[단위이상] {r['corp_name']} {r['period']} unit={unit!r}")
            continue
        if unit == "unknown":
            target.append(f"[단위미확인] {r['corp_name']} {r['period']}")
            continue

        c = rev.get("consolidated", {})
        revs = c.get("revenue", [])
        ops  = c.get("op_profit", [])

        rv0 = revs[0] if revs else None
        op0 = ops[0] if ops else None

        # 원 단위인데 값이 비정상적으로 작은 경우
        if unit == "원" and rv0 is not None:
            eok = to_eok(rv0, unit)
            if eok is not None and 0 < abs(eok) < 0.1:
                target.append(f"[소액의심] {r['corp_name']} {r['period']} 매출={rv0} =>{eok:.4f}억원 (단위가 '원'이 아닐 수 있음)")

        # 당기/전기 영업이익 배율 이상 (500배 이상이면 단위 불일치 의심)
        if op0 is not None and ops and len(ops) >= 2 and ops[1] is not None:
            op1 = ops[1]
            n0, n1 = parse_num(op0), parse_num(op1)
            if n0 and n1 and n0 != 0:
                ratio = abs(n1) / abs(n0)
                if ratio > 500 or ratio < 0.002:
                    target.append(
                        f"[비율이상] {r['corp_name']} {r['period']} "
                        f"op당기={op0} op전기={op1} ratio={ratio:.0f}x unit={unit}"
                    )

    await pool.close()

    print(f"전체: {total}건 | 분기/반기: {len(q_rows)}건 | 사업보고서: {len(a_rows)}건")
    print(f"\n=== 분기/반기 이상 ({len(issues_q)}건) ===")
    for i in issues_q:
        print(" ", i)
    print(f"\n=== 사업보고서 이상 ({len(issues_a)}건, 참고용) ===")
    for i in issues_a[:30]:
        print(" ", i)
    if len(issues_a) > 30:
        print(f"  ... 외 {len(issues_a)-30}건")

asyncio.run(main())
