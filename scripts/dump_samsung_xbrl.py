"""삼성전자 XBRL TE 태그 중 Revenue 계정만 덤프"""
import re, sys
from pathlib import Path
sys.path.insert(0, ".")
from data.dart_extractor import DART_DIR, _parse_report_dir, _pick_main_xml

RCEPT_NO = "20260515002181"  # 삼성전자 2026.1Q

co_dir = DART_DIR / "삼성전자"
xml_path = None
for rd in co_dir.iterdir():
    if not rd.is_dir(): continue
    p, _, _ = _parse_report_dir(rd.name)
    if p == RCEPT_NO:
        xml_path = _pick_main_xml(rd, RCEPT_NO)
        break

if not xml_path:
    print("XML 없음")
    sys.exit(1)

print(f"XML: {xml_path}")
text = Path(xml_path).read_text(encoding="utf-8", errors="ignore")

# Revenue 관련 TE 태그 전부 출력
TE_RE = re.compile(r'<TE\s[^>]*ACODE="([^"]+)"[^>]*ACONTEXT="([^"]+)"[^>]*>([^<]+)</TE>', re.IGNORECASE)

REV_CODES = {"ifrs-full_Revenue", "ifrs-full_OperatingRevenue",
             "ifrs-full_RevenueFromContractsWithCustomers", "dart_OperatingRevenue",
             "ifrs-full_OperatingProfit", "dart_OperatingIncomeLoss",
             "ifrs-full_ProfitLossFromOperatingActivities"}

print("\n=== Revenue/OP 관련 TE 태그 (ACONTEXT 포함) ===")
for m in TE_RE.finditer(text):
    acode, actx, raw = m.group(1), m.group(2), m.group(3).strip()
    if acode in REV_CODES:
        adec_m = re.search(r'ADECIMAL="([^"]*)"', m.group(0), re.IGNORECASE)
        adec = adec_m.group(1) if adec_m else "N/A"
        neg = "Y" if re.search(r'ANEGATED="Y"', m.group(0), re.IGNORECASE) else ""
        print(f"  ACODE={acode[:40]:40s} ACTX={actx:30s} ADEC={adec:4s} NEG={neg:1s} VAL={raw}")
