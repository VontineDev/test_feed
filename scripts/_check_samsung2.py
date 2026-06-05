"""삼성전자 분기보고서 rev_text 확인."""
import asyncio, json, sys
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")
from dotenv import load_dotenv; load_dotenv()
from data.dart_extractor import (
    _pick_main_xml, _parse_report_dir, DART_DIR,
    _section_text, _REVENUE_ANCHORS,
)

_INCOME_START_KWS = [
    "매출액", "영업수익", "총영업이익", "순영업이익",
    "이자수익", "수수료수익", "보험료수익",
]

corp_dir = DART_DIR / "삼성전자"
report_dirs = sorted(d for d in corp_dir.iterdir() if d.is_dir())

for target in report_dirs:
    rcept_no, rtype, period = _parse_report_dir(target.name)
    xml_path = _pick_main_xml(target, rcept_no)
    if not xml_path:
        continue

    rev_text = _section_text(
        xml_path, _REVENUE_ANCHORS,
        max_chars=8000, lines_per_anchor=200,
        priority=True, require_keyword="영업이익",
    )

    # 손익계산서 트리밍
    if rev_text:
        lines = rev_text.splitlines()
        income_start = -1
        for i, l in enumerate(lines):
            s = l.strip()
            if any(kw == s or (kw in s and len(s) <= len(kw) + 5) for kw in _INCOME_START_KWS):
                income_start = i
                break
        if income_start > 5:
            rev_text = "\n".join(lines[income_start:])

    if rev_text:
        lines = rev_text.splitlines()
        last_op = max((i for i, l in enumerate(lines) if "영업이익" in l), default=-1)
        if last_op >= 0:
            rev_text = "\n".join(lines[:last_op + 31])

    print(f"\n{'='*60}")
    print(f"[{period}] {rtype} | rev_text={len(rev_text) if rev_text else 0}자")
    if rev_text:
        print(rev_text[:1200])
