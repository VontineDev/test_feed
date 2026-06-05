"""풍산 XML에서 요약재무정보 섹션과 영업이익 위치 확인."""
import re, sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")
from data.dart_extractor import _pick_main_xml, _parse_report_dir

TAG_RE = re.compile(r"<[^>]+>")
def strip(l): return TAG_RE.sub("", l).strip()

corp_dir = Path("reports/dart/풍산")
target = next(d for d in reversed(sorted(corp_dir.iterdir())) if "사업보고서" in d.name)
rcept_no, _, _ = _parse_report_dir(target.name)
xml_path = _pick_main_xml(target, rcept_no)

lines = []
with open(xml_path, encoding="utf-8", errors="ignore") as f:
    for raw in f:
        s = strip(raw)
        if s:
            lines.append(s)

print(f"총 {len(lines)}줄\n")

# 1) 요약재무정보 관련 헤더 탐색
print("=== 요약재무정보 관련 헤더 (줄 길이 ≤80) ===")
for i, line in enumerate(lines):
    if any(kw in line for kw in ["요약재무", "요약 재무", "재무정보"]) and len(line) <= 80:
        print(f"  line {i:5d} ({len(line)}자): {line}")

# 2) 영업이익이 헤더(짧은 줄) 형태로 등장하는 위치 + 주변
print("\n=== 영업이익 헤더 주변 ===")
for i, line in enumerate(lines):
    if line.strip() == "영업이익" or (len(line) < 20 and "영업이익" in line):
        ctx = lines[max(0,i-3):i+6]
        print(f"  --- line {i} ---")
        for c in ctx:
            print(f"  {c[:80]}")
        print()
        if i > 5000:
            break  # 처음 몇 개만
