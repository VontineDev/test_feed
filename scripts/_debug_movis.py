"""현대모비스 extract_structured 직접 테스트."""
import asyncio, sys, json
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()

import httpx
from data.dart_extractor import (
    extract_structured, _pick_main_xml, _parse_report_dir,
    _section_text, _REVENUE_ANCHORS, DEFAULT_MODEL, DART_DIR,
)

async def main():
    corp_name = "현대모비스"
    rcept_no  = "20260309001878"

    corp_dir = DART_DIR / corp_name
    xml_path = None
    for d in corp_dir.iterdir():
        if d.is_dir() and rcept_no in d.name:
            xml_path = _pick_main_xml(d, rcept_no)
            break

    print(f"XML: {xml_path}")

    # rev_text 먼저 확인
    rev_text = _section_text(
        xml_path, _REVENUE_ANCHORS,
        max_chars=8000, lines_per_anchor=200,
        priority=True, require_keyword="영업이익",
    )
    print(f"rev_text: {len(rev_text)}자")
    print("--- rev_text 처음 2000자 ---")
    print(rev_text[:2000])
    print("--- rev_text 끝 500자 ---")
    print(rev_text[-500:])

    # extract_structured 실행
    print("\n\n--- extract_structured 실행 ---")
    async with httpx.AsyncClient(timeout=120) as http:
        result = await extract_structured(http, xml_path, DEFAULT_MODEL)

    print(f"revenue_json: {result['revenue_json']}")
    print(f"segments_json: {result['segments_json']}")

asyncio.run(main())
