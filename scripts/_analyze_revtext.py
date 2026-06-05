"""각 기업에 실제로 전달되는 rev_text 내용 확인."""
import sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")

from data.dart_extractor import (
    _section_text, _REVENUE_ANCHORS, _pick_main_xml, _parse_report_dir
)

DART_DIR = Path("reports/dart")

def analyze(corp_name: str):
    corp_dir = DART_DIR / corp_name
    report_dirs = sorted(d for d in corp_dir.iterdir() if d.is_dir())
    # 사업보고서 최신
    target = None
    for d in reversed(report_dirs):
        if "사업보고서" in d.name:
            target = d
            break
    if not target:
        target = report_dirs[-1]

    rcept_no, rtype, period = _parse_report_dir(target.name)
    xml_path = _pick_main_xml(target, rcept_no)

    print(f"\n{'='*60}")
    print(f"[{corp_name}] {target.name}")
    print(f"XML: {xml_path.name if xml_path else 'None'}")

    if not xml_path:
        return

    rev_text = _section_text(xml_path, _REVENUE_ANCHORS, lines_per_anchor=120)
    print(f"\nrev_text 길이: {len(rev_text)}자")
    print(f"--- rev_text 전체 ---")
    # 앵커별 블록 분리 표시
    for block in rev_text.split("\n\n"):
        if block.strip():
            first = block.strip().splitlines()[0][:80]
            lines_cnt = len(block.strip().splitlines())
            print(f"  [블록 {lines_cnt}줄] {first}")
    print()
    print("--- rev_text 전문 (처음 3000자) ---")
    print(rev_text[:3000])

for corp in ["삼표시멘트", "에코프로", "풍산"]:
    analyze(corp)
