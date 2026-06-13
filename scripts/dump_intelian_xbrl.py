"""인텔리안테크 2025 Q1 XBRL TE 태그 덤프 + LLM 추출값 비교"""
import re, json, asyncio, sys
from pathlib import Path
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()
from data.dart_extractor import (
    DART_DIR, _parse_report_dir, _pick_main_xml, extract_xbrl_quarterly
)
from core.db import create_pool

RCEPT_NO = "20250515002486"  # 인텔리안테크 2025.1Q

co_dir = DART_DIR / "인텔리안테크"
xml_path = None
for rd in co_dir.iterdir():
    if not rd.is_dir(): continue
    p, _, _ = _parse_report_dir(rd.name)
    if p == RCEPT_NO:
        xml_path = _pick_main_xml(rd, RCEPT_NO)
        break

if not xml_path:
    print("XML 없음"); sys.exit(1)

print(f"XML: {xml_path}\n")
text = Path(xml_path).read_text(encoding="utf-8", errors="ignore")

TE_RE = re.compile(r'<TE\s[^>]*ACODE="([^"]+)"[^>]*ACONTEXT="([^"]+)"[^>]*>([^<]+)</TE>', re.IGNORECASE)
REV_CODES = {
    "ifrs-full_Revenue", "ifrs-full_OperatingRevenue",
    "ifrs-full_RevenueFromContractsWithCustomers", "dart_OperatingRevenue",
    "dart_OperatingIncomeLoss", "ifrs-full_OperatingProfit",
    "ifrs-full_ProfitLossFromOperatingActivities",
}

print("=== XBRL TE 태그 (Revenue/OP 계정) ===")
for m in TE_RE.finditer(text):
    acode, actx, raw = m.group(1), m.group(2), m.group(3).strip()
    if acode in REV_CODES:
        adec_m = re.search(r'ADECIMAL="([^"]*)"', m.group(0), re.IGNORECASE)
        adec = adec_m.group(1) if adec_m else "N/A"
        neg = "Y" if re.search(r'ANEGATED="Y"', m.group(0), re.IGNORECASE) else " "
        print(f"  {acode[:45]:45s} ACTX={actx[:50]:50s} ADEC={adec:4s} NEG={neg} VAL={raw}")

# XBRL 직접 추출 결과
xbrl = extract_xbrl_quarterly(xml_path)
print(f"\n=== extract_xbrl_quarterly 결과 ===")
if xbrl:
    rv = xbrl['revenue']
    op = xbrl['op_profit']
    print(f"  revenue  : {rv}")
    print(f"  op_profit: {op}")
    if rv[0]: print(f"  당기 매출: {float(rv[0])/1e8:.0f}억원")
    if rv[1]: print(f"  전기 매출: {float(rv[1])/1e8:.0f}억원")
    if op[0]: print(f"  당기 영업이익: {float(op[0])/1e8:.0f}억원")
else:
    print("  XBRL 추출 실패 (None)")

# DB 저장값
async def check_db():
    pool = await create_pool()
    async with pool.acquire() as conn:
        r = await conn.fetchrow(
            "SELECT revenue_json FROM dart_extractions WHERE rcept_no = $1", RCEPT_NO
        )
    await pool.close()
    if r:
        rev = json.loads(r["revenue_json"])
        c = rev.get("consolidated", {})
        print(f"\n=== DB 저장값 ===")
        print(f"  periods : {rev.get('periods')}")
        print(f"  unit    : {rev.get('unit')}")
        print(f"  revenue : {c.get('revenue')}")
        print(f"  op_profit: {c.get('op_profit')}")
        rv0 = c.get('revenue', [None])[0]
        if rv0: print(f"  당기 매출(DB): {float(rv0)/1e8:.0f}억원")

asyncio.run(check_db())
