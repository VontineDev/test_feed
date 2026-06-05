"""매출 유효 0인 기업 rev_text 내용 확인."""
import sys, re
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")

from data.dart_extractor import (
    _section_text, _REVENUE_ANCHORS,
    _pick_main_xml, _parse_report_dir, DART_DIR,
)

TARGETS = ["SK하이닉스", "현대자동차", "NAVER", "카카오", "기아"]

TAG_RE = re.compile(r"<[^>]+>")

for corp in TARGETS:
    corp_dir = DART_DIR / corp
    if not corp_dir.exists():
        print(f"[{corp}] 디렉터리 없음"); continue

    # 최신 보고서 1건만
    report_dirs = sorted(d for d in corp_dir.iterdir() if d.is_dir())
    target = next((d for d in reversed(report_dirs) if "사업보고서" in d.name), report_dirs[-1])
    rcept_no, rtype, period = _parse_report_dir(target.name)
    xml_path = _pick_main_xml(target, rcept_no)

    rev_text = _section_text(
        xml_path, _REVENUE_ANCHORS,
        max_chars=8000, lines_per_anchor=200,
        priority=True, require_keyword="영업이익",
    )

    # 트리밍 (extract_structured와 동일)
    if rev_text:
        _rv_lines = rev_text.splitlines()
        _last_op = max(
            (i for i, l in enumerate(_rv_lines) if "영업이익" in l), default=-1
        )
        if _last_op >= 0:
            rev_text = "\n".join(_rv_lines[:_last_op + 31])

    has_rev = "매출" in rev_text
    has_op  = "영업이익" in rev_text
    print(f"\n{'='*60}")
    print(f"[{corp}] {period} {rtype}")
    print(f"  rev_text={len(rev_text)}자  매출={has_rev}  영업이익={has_op}")
    print("--- rev_text[:600] ---")
    print(rev_text[:600] if rev_text else "(비어있음)")
