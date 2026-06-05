"""현대모비스·하나금융지주에 대한 Ollama 실제 응답 확인."""
import asyncio, sys
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()

import httpx
from data.dart_extractor import (
    _section_text, _REVENUE_ANCHORS, _pick_main_xml, _parse_report_dir,
    REVENUE_ANALYZER_PROMPT, DEFAULT_MODEL, DART_DIR,
)
from reports.summarizer import _call_ollama_native

async def check(corp_name, rcept_no):
    corp_dir = DART_DIR / corp_name
    xml_path = None
    for d in corp_dir.iterdir():
        if d.is_dir() and rcept_no in d.name:
            xml_path = _pick_main_xml(d, rcept_no)
            break

    rev_text = _section_text(
        xml_path, _REVENUE_ANCHORS,
        max_chars=8000, lines_per_anchor=200,
        priority=True, require_keyword="영업이익",
    ) or ""

    prompt = REVENUE_ANALYZER_PROMPT + rev_text
    print(f"\n{'='*60}")
    print(f"[{corp_name}] rev_text {len(rev_text)}자")
    print("--- rev_text[:500] ---")
    print(rev_text[:500])

    async with httpx.AsyncClient(timeout=120) as http:
        try:
            raw = await _call_ollama_native(
                http=http, model=DEFAULT_MODEL,
                prompt=prompt, timeout=120.0,
                max_tokens=2000, enable_thinking=False,
            )
            print(f"\n--- Ollama 응답 ({len(raw)}자) ---")
            print(raw[:1000])
        except Exception as e:
            print(f"\nOllama 에러: {e}")

async def main():
    cases = [
        ("현대모비스", "20260309001878"),
        ("하나금융지주", "20250515002336"),
    ]
    for corp, rcept in cases:
        await check(corp, rcept)

asyncio.run(main())
