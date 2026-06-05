"""삼성전자 연결 손익계산서 섹션 확인."""
import sys, re
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")
from data.dart_extractor import _pick_main_xml, _parse_report_dir, DART_DIR, _section_text

CONSOL_ANCHORS = [
    "연결손익계산서",
    "연결포괄손익계산서",
    "요약연결손익계산서",
]

corp_dir = DART_DIR / "삼성전자"
report_dirs = sorted(d for d in corp_dir.iterdir() if d.is_dir())

# 최신 2개만
for target in list(reversed(report_dirs))[:2]:
    rcept_no, rtype, period = _parse_report_dir(target.name)
    xml_path = _pick_main_xml(target, rcept_no)
    print(f"\n[{period}] {rtype}")

    for anc in CONSOL_ANCHORS:
        t = _section_text(xml_path, [anc], max_chars=3000, lines_per_anchor=80, priority=False, require_keyword=None)
        if t and len(t) > 100:
            print(f"  앵커: '{anc}' | {len(t)}자")
            print(t[:600])
            break
    else:
        print("  연결 손익계산서 앵커 없음")
