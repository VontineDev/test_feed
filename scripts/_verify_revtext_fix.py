"""손익계산서 트리밍 후 rev_text 내용 확인."""
import sys, importlib
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")

# 모듈 갱신
import data.dart_extractor as de
importlib.reload(de)

from data.dart_extractor import (
    _section_text, _REVENUE_ANCHORS,
    _pick_main_xml, _parse_report_dir, DART_DIR,
)

_INCOME_START_KWS = [
    "매출액", "영업수익", "총영업이익", "순영업이익",
    "이자수익", "수수료수익", "보험료수익",
]

TARGETS = ["SK하이닉스", "현대자동차", "NAVER", "카카오", "기아"]

for corp in TARGETS:
    corp_dir = DART_DIR / corp
    report_dirs = sorted(d for d in corp_dir.iterdir() if d.is_dir())
    target = next((d for d in reversed(report_dirs) if "사업보고서" in d.name), report_dirs[-1])
    rcept_no, rtype, period = _parse_report_dir(target.name)
    xml_path = _pick_main_xml(target, rcept_no)

    rev_text = _section_text(
        xml_path, _REVENUE_ANCHORS,
        max_chars=8000, lines_per_anchor=200,
        priority=True, require_keyword="영업이익",
    )

    # 손익계산서 시작 트리밍
    if rev_text:
        lines = rev_text.splitlines()
        income_start = -1
        for i, l in enumerate(lines):
            stripped = l.strip()
            if any(kw == stripped or (kw in stripped and len(stripped) <= len(kw) + 5)
                   for kw in _INCOME_START_KWS):
                income_start = i
                break
        if income_start > 5:
            rev_text = "\n".join(lines[income_start:])

    # 영업이익 뒤 자름
    if rev_text:
        lines = rev_text.splitlines()
        last_op = max((i for i, l in enumerate(lines) if "영업이익" in l), default=-1)
        if last_op >= 0:
            rev_text = "\n".join(lines[:last_op + 31])

    has_rev = "매출" in rev_text
    has_op  = "영업이익" in rev_text
    print(f"\n[{corp}] {period} | rev_text={len(rev_text)}자 | 매출={has_rev} 영업이익={has_op}")
    print(rev_text[:500] if rev_text else "(비어있음)")
