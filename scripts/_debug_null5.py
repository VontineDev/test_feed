"""하나금융지주·현대모비스 rev_text 분석."""
import sys, re
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")

from data.dart_extractor import (
    _section_text, _anchor_best, _REVENUE_ANCHORS,
    _pick_main_xml, _parse_report_dir, DART_DIR,
)

TAG_RE = re.compile(r"<[^>]+>")
def strip(l): return TAG_RE.sub("", l).strip()

def load_lines(xml_path):
    lines = []
    with open(xml_path, encoding="utf-8", errors="ignore") as f:
        for raw in f:
            s = strip(raw)
            if s:
                lines.append(s)
    return lines

def show_anchors(corp_name, rcept_no):
    corp_dir = DART_DIR / corp_name
    xml_path = None
    for d in corp_dir.iterdir():
        if d.is_dir() and rcept_no in d.name:
            xml_path = _pick_main_xml(d, rcept_no)
            break
    if not xml_path:
        print(f"  XML 없음: {corp_name}/{rcept_no}"); return

    lines = load_lines(xml_path)
    print(f"\n{'='*60}")
    print(f"[{corp_name}] XML {len(lines)}줄 | {xml_path.name}")

    # anchor 히트 탐색
    for anchor in _REVENUE_ANCHORS:
        hits = [(i, l) for i, l in enumerate(lines) if anchor in l]
        if hits:
            i, l = hits[0]
            is_header = len(l) <= 80
            window = lines[i:i+30]
            has_op = any("영업이익" in x for x in window)
            print(f"  [{anchor}] @ line {i} (len={len(l)}, header={is_header}, op_in_30={has_op})")
            print(f"    >> {l[:100]}")

    # _section_text 결과
    rev_text = _section_text(
        xml_path, _REVENUE_ANCHORS,
        max_chars=8000, lines_per_anchor=200,
        priority=True, require_keyword="영업이익",
    )
    has_rev = "매출액" in rev_text
    has_op  = "영업이익" in rev_text
    print(f"  rev_text: {len(rev_text)}자, 매출액={has_rev}, 영업이익={has_op}")
    if not has_op:
        # 전체 XML에서 영업이익 위치
        op_lines = [(i, l) for i, l in enumerate(lines) if "영업이익" in l and len(l) < 100]
        print(f"  전체 XML 영업이익(짧은줄) {len(op_lines)}개:")
        for i, l in op_lines[:5]:
            print(f"    line {i}: {l[:80]}")

# 하나금융지주 - 분기 한 건만
CASES = [
    ("하나금융지주", "20250515002336"),
    ("현대모비스",   "20260309001878"),
]
for corp, rcept in CASES:
    show_anchors(corp, rcept)
