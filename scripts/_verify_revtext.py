"""수정 후 rev_text 변화 검증."""
import sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")

from data.dart_extractor import (
    _section_text, _REVENUE_ANCHORS, _pick_main_xml, _parse_report_dir
)

DART_DIR = Path("reports/dart")

def verify(corp_name: str):
    corp_dir = DART_DIR / corp_name
    report_dirs = sorted(d for d in corp_dir.iterdir() if d.is_dir())
    target = next((d for d in reversed(report_dirs) if "사업보고서" in d.name), report_dirs[-1])
    rcept_no, _, _ = _parse_report_dir(target.name)
    xml_path = _pick_main_xml(target, rcept_no)
    if not xml_path:
        print(f"[{corp_name}] XML 없음"); return

    rev_text = _section_text(
        xml_path, _REVENUE_ANCHORS,
        max_chars=8000, lines_per_anchor=200, priority=True,
    )
    print(f"\n{'='*60}")
    print(f"[{corp_name}]  rev_text {len(rev_text)}자")
    print(f"--- 처음 30줄 ---")
    for line in rev_text.splitlines()[:30]:
        print(f"  {line[:100]}")

for corp in ["삼표시멘트", "에코프로", "풍산"]:
    verify(corp)
