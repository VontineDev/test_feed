"""하나금융지주 분기보고서 XML 구조 분석."""
import re, sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")
from data.dart_extractor import _pick_main_xml, _parse_report_dir, DART_DIR

TAG_RE = re.compile(r"<[^>]+>")
def strip(l): return TAG_RE.sub("", l).strip()

corp_dir = DART_DIR / "하나금융지주"
rcept_no = "20250515002336"  # 2025.03 분기

xml_path = None
for d in corp_dir.iterdir():
    if d.is_dir() and rcept_no in d.name:
        xml_path = _pick_main_xml(d, rcept_no)
        break

print(f"XML: {xml_path}")
lines = []
with open(xml_path, encoding="utf-8", errors="ignore") as f:
    for raw in f:
        s = strip(raw)
        if s:
            lines.append(s)

print(f"총 {len(lines)}줄\n")

# 1) 영업이익 위치 (짧은 줄)
print("=== 영업이익 관련 줄 (len<80) ===")
for i, l in enumerate(lines):
    if "영업이익" in l and len(l) < 100:
        ctx = lines[max(0,i-2):i+5]
        print(f"  line {i} ({len(l)}자): {l[:80]}")
        for c in ctx:
            print(f"    >> {c[:80]}")
        print()
        if i > 10000:
            break

# 2) 요약재무정보 섹션 근처
print("\n=== 요약재무정보 @ line 49 주변 (45~100) ===")
for l in lines[45:100]:
    print(f"  {l[:100]}")
