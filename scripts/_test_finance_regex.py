"""금융사 규칙 기반 추출 검증."""
import sys, re, asyncio
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")
from dotenv import load_dotenv; load_dotenv()
import httpx
from data.dart_extractor import (
    _pick_main_xml, _parse_report_dir, DART_DIR,
    _section_text, _REVENUE_ANCHORS, extract_structured, DEFAULT_MODEL,
)
from reports.summarizer import _ollama_is_alive

_REV_INDICATORS = ["매출액", "영업수익", "이자수익", "총영업이익", "순영업이익"]
_FIN_REVENUE_ANCHORS = ["영업수익", "총영업이익", "이자이익", "이자수익"]

_FIN_REV_PATTERNS = [
    (r"매출액\(영업수익\)\s*\n([\d,]+)\n([\d,]+)\n([\d,]+)", "revenue"),
    (r"총영업이익\s*\n([\d,]+)\s*\n([\d,]+)\s*\n([\d,]+)", "revenue"),
    (r"순영업이익\s*\n([\d,]+)\s*\n([\d,]+)\s*\n([\d,]+)", "revenue"),
]

for corp in ["BNK금융지주", "하나금융지주", "KB금융", "신한지주"]:
    corp_dir = DART_DIR / corp
    if not corp_dir.exists(): continue
    report_dirs = sorted(d for d in corp_dir.iterdir() if d.is_dir())
    target = next((d for d in reversed(report_dirs) if "사업보고서" in d.name), report_dirs[-1])
    rcept_no, rtype, period = _parse_report_dir(target.name)
    xml_path = _pick_main_xml(target, rcept_no)

    # rev_text 추출 (기존 + 폴백)
    rev_text = _section_text(
        xml_path, _REVENUE_ANCHORS, max_chars=8000, lines_per_anchor=200,
        priority=True, require_keyword="영업이익",
    ) or ""

    if not any(kw in rev_text for kw in _REV_INDICATORS):
        for anc in _FIN_REVENUE_ANCHORS:
            fin = _section_text(xml_path, [anc], max_chars=8000, lines_per_anchor=200,
                               priority=False, require_keyword=None)
            if fin and len(fin) > 200:
                rev_text = fin
                break

    print(f"\n[{corp}] {period} | rev_text={len(rev_text)}자")
    hit = False
    for pat, field in _FIN_REV_PATTERNS:
        m = re.search(pat, rev_text)
        if m:
            vals = [int(g.replace(",", "")) for g in m.groups()]
            print(f"  패턴 히트! {field}={vals}")
            hit = True
            break
    if not hit:
        # 패턴 없을 때 관련 라인 출력
        lines = rev_text.splitlines()
        for i, l in enumerate(lines):
            if any(kw in l for kw in ["매출액", "총영업이익", "순영업이익", "영업수익"]):
                print(f"  [{i}] {l.strip()}")
                if i+1 < len(lines): print(f"  [{i+1}] {lines[i+1].strip()}")
                if i+2 < len(lines): print(f"  [{i+2}] {lines[i+2].strip()}")
