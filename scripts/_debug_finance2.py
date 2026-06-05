"""금융사 기존 rev_text 전체 내용 + 금융 앵커 비교."""
import sys
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")
from data.dart_extractor import (
    _pick_main_xml, _parse_report_dir, DART_DIR,
    _section_text, _REVENUE_ANCHORS,
)

FINANCE_CORPS = ["BNK금융지주", "하나금융지주"]

_FIN_ANCHORS = ["영업수익", "총영업이익", "이자이익", "이자수익"]

_INCOME_START_KWS = [
    "매출액", "영업수익", "총영업이익", "순영업이익",
    "이자수익", "수수료수익", "보험료수익",
]

for corp in FINANCE_CORPS:
    corp_dir = DART_DIR / corp
    report_dirs = sorted(d for d in corp_dir.iterdir() if d.is_dir())
    target = next((d for d in reversed(report_dirs) if "사업보고서" in d.name), report_dirs[-1])
    rcept_no, rtype, period = _parse_report_dir(target.name)
    xml_path = _pick_main_xml(target, rcept_no)

    print(f"\n{'='*70}")
    print(f"[{corp}] {period}")

    # 기존 앵커 (트리밍 포함)
    rev_text = _section_text(
        xml_path, _REVENUE_ANCHORS,
        max_chars=8000, lines_per_anchor=200,
        priority=True, require_keyword="영업이익",
    )

    if rev_text:
        # 손익계산서 시작 트리밍
        lines = rev_text.splitlines()
        income_start = -1
        for i, l in enumerate(lines):
            s = l.strip()
            if any(kw == s or (kw in s and len(s) <= len(kw) + 5) for kw in _INCOME_START_KWS):
                income_start = i
                break
        if income_start > 5:
            rev_text = "\n".join(lines[income_start:])
        # 마지막 영업이익 후 자름
        lines = rev_text.splitlines()
        last_op = max((i for i, l in enumerate(lines) if "영업이익" in l), default=-1)
        if last_op >= 0:
            rev_text = "\n".join(lines[:last_op + 31])

    print(f"\n[기존 앵커] {len(rev_text) if rev_text else 0}자")
    print(f"  매출 지표 포함: {'예' if rev_text and any(k in rev_text for k in ['매출액','영업수익','이자수익']) else '아니오'}")
    if rev_text:
        print(rev_text[:1000])

    # 금융 앵커
    for anc in _FIN_ANCHORS:
        fin_text = _section_text(
            xml_path, [anc],
            max_chars=8000, lines_per_anchor=200,
            priority=False, require_keyword=None,
        )
        if fin_text and len(fin_text) > 200:
            print(f"\n[금융 앵커: '{anc}'] {len(fin_text)}자")
            print(fin_text[:1000])
            break
