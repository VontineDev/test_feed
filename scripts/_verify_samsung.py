"""삼성전자 매출실적 합계 보완 추출 검증."""
import sys, importlib
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")
import data.dart_extractor as de
importlib.reload(de)

from data.dart_extractor import (
    _pick_main_xml, _parse_report_dir, DART_DIR,
    _section_text, _REVENUE_ANCHORS,
)

_SUM_INDICATORS = ["합계", "총계", "합     계", "합  계"]
_INCOME_START_KWS = ["매출액", "영업수익", "총영업이익", "순영업이익"]

corp_dir = DART_DIR / "삼성전자"
report_dirs = sorted(d for d in corp_dir.iterdir() if d.is_dir())

for target in list(reversed(report_dirs))[:3]:
    rcept_no, rtype, period = _parse_report_dir(target.name)
    xml_path = _pick_main_xml(target, rcept_no)
    print(f"\n[{period}] {rtype}")

    # 기존 rev_text
    rev_text = _section_text(
        xml_path, _REVENUE_ANCHORS,
        max_chars=8000, lines_per_anchor=200,
        priority=True, require_keyword="영업이익",
    ) or ""

    has_sum = any(kw in rev_text for kw in _SUM_INDICATORS)
    print(f"  기존 rev_text: {len(rev_text)}자 | 합계 포함: {'예' if has_sum else '아니오'}")

    if not has_sum:
        # 보완 추출
        sales_text = _section_text(
            xml_path, ["매출실적", "부문별 매출실적", "매출현황"],
            max_chars=4000, lines_per_anchor=100,
            priority=False, require_keyword=None,
        )
        if sales_text and any(kw in sales_text for kw in _SUM_INDICATORS):
            print(f"  보완 텍스트: {len(sales_text)}자 | 합계 포함: 예")
            print("  --- 보완 텍스트 (합계 행 포함 부분) ---")
            lines = sales_text.splitlines()
            for i, l in enumerate(lines):
                if any(kw in l for kw in _SUM_INDICATORS):
                    start = max(0, i-3)
                    print("\n".join(lines[start:i+5]))
                    break
        else:
            print(f"  보완 텍스트: 합계 없음")
