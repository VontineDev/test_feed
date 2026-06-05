"""rev_text 전체에 매출액·영업이익이 포함되는지 확인."""
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

    rev_text = _section_text(
        xml_path, _REVENUE_ANCHORS,
        max_chars=8000, lines_per_anchor=200,
        priority=True, require_keyword="영업이익",
    )
    lines = rev_text.splitlines()
    print(f"\n{'='*55}")
    print(f"[{corp_name}] rev_text {len(rev_text)}자 / {len(lines)}줄")

    # 매출액·영업이익 포함 여부
    has_rev = any("매출액" in l or "매출" == l.strip() for l in lines)
    has_op  = any("영업이익" in l for l in lines)
    print(f"  매출액 포함: {has_rev}  영업이익 포함: {has_op}")

    # 매출액·영업이익 주변 줄 출력
    for kw in ["매출액", "영업이익"]:
        for i, l in enumerate(lines):
            if kw in l:
                ctx = lines[max(0,i-1):i+5]
                print(f"\n  [{kw} @ line {i}]")
                for c in ctx:
                    print(f"    {c[:80]}")
                break

for corp in ["삼표시멘트", "에코프로", "풍산"]:
    verify(corp)
