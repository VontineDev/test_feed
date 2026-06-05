"""금융사 DART 보고서 rev_text 구조 분석."""
import sys, re
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")
from data.dart_extractor import (
    _pick_main_xml, _parse_report_dir, DART_DIR,
    _section_text, _REVENUE_ANCHORS, _anchor_best,
)

FINANCE_CORPS = ["BNK금융지주", "하나금융지주", "KB금융", "신한지주", "삼성생명"]

# 금융사 전용 앵커 후보
_FIN_ANCHORS = [
    "영업수익",
    "순영업이익",
    "총영업이익",
    "순이자이익",
    "이자이익",
    "핵심이익",
    "영업이익현황",
    "주요경영지표",
    "경영실적",
    "요약손익",
    "부문별 영업이익",
    "부문별 순이익",
]

for corp in FINANCE_CORPS:
    corp_dir = DART_DIR / corp
    if not corp_dir.exists():
        print(f"[{corp}] 디렉터리 없음"); continue

    # 최신 사업보고서
    report_dirs = sorted(d for d in corp_dir.iterdir() if d.is_dir())
    target = next((d for d in reversed(report_dirs) if "사업보고서" in d.name), report_dirs[-1])
    rcept_no, rtype, period = _parse_report_dir(target.name)
    xml_path = _pick_main_xml(target, rcept_no)
    if not xml_path:
        print(f"[{corp}] XML 없음"); continue

    print(f"\n{'='*60}")
    print(f"[{corp}] {period} {rtype}")

    # 1. 기존 앵커로 rev_text
    rev_text = _section_text(
        xml_path, _REVENUE_ANCHORS,
        max_chars=8000, lines_per_anchor=200,
        priority=True, require_keyword="영업이익",
    )
    print(f"  기존 앵커 rev_text: {len(rev_text) if rev_text else 0}자")

    # 2. 금융사 전용 앵커로 시도
    fin_text = None
    hit_anchor = None
    for anc in _FIN_ANCHORS:
        t = _section_text(
            xml_path, [anc],
            max_chars=4000, lines_per_anchor=100,
            priority=False, require_keyword=None,
        )
        if t and len(t) > 100:
            fin_text = t
            hit_anchor = anc
            break

    print(f"  금융 앵커 히트: '{hit_anchor}' | {len(fin_text) if fin_text else 0}자")

    if fin_text:
        print("  --- 금융 앵커 텍스트 처음 600자 ---")
        print(fin_text[:600])
    elif rev_text:
        print("  --- 기존 앵커 텍스트 처음 600자 ---")
        print(rev_text[:600])
