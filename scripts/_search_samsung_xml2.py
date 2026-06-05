"""삼성전자 XML 전체 구조 탐색."""
import sys, re
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")
from data.dart_extractor import _pick_main_xml, _parse_report_dir, DART_DIR, extract_xml

corp_dir = DART_DIR / "삼성전자"
report_dirs = sorted(d for d in corp_dir.iterdir() if d.is_dir())
target = list(reversed(report_dirs))[0]
rcept_no, rtype, period = _parse_report_dir(target.name)
xml_path = _pick_main_xml(target, rcept_no)

text = extract_xml(xml_path)
lines = text.splitlines()
print(f"[{period}] 라인: {len(lines)}")

# 모든 라인 출력 (짧은 XML인 경우)
print("\n--- 전체 ---")
for i, l in enumerate(lines):
    print(f"{i:4d}: {l[:120]}")
