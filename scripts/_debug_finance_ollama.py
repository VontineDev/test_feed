"""금융사 Ollama 실제 응답 확인."""
import asyncio, json, sys
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")
from dotenv import load_dotenv; load_dotenv()
import httpx
from data.dart_extractor import (
    _pick_main_xml, _parse_report_dir, DART_DIR,
    _section_text, _REVENUE_ANCHORS, _detect_unit,
    REVENUE_ANALYZER_PROMPT, DEFAULT_MODEL,
)
from reports.summarizer import _call_ollama_native, _ollama_is_alive

_REV_INDICATORS = ["매출액", "영업수익", "이자수익", "총영업이익", "순영업이익"]
_FIN_REVENUE_ANCHORS = ["영업수익", "총영업이익", "이자이익", "이자수익"]
_INCOME_START_KWS = [
    "매출액", "영업수익", "총영업이익", "순영업이익",
    "이자수익", "수수료수익", "보험료수익",
]

async def main():
    async with httpx.AsyncClient(timeout=180) as http:
        if not await _ollama_is_alive(http):
            print("Ollama 미응답"); return

        for corp in ["BNK금융지주", "하나금융지주"]:
            corp_dir = DART_DIR / corp
            report_dirs = sorted(d for d in corp_dir.iterdir() if d.is_dir())
            target = next((d for d in reversed(report_dirs) if "사업보고서" in d.name), report_dirs[-1])
            rcept_no, rtype, period = _parse_report_dir(target.name)
            xml_path = _pick_main_xml(target, rcept_no)

            # 기존 rev_text
            rev_text = _section_text(
                xml_path, _REVENUE_ANCHORS,
                max_chars=8000, lines_per_anchor=200,
                priority=True, require_keyword="영업이익",
            ) or ""

            # 금융사 폴백
            if not any(kw in rev_text for kw in _REV_INDICATORS):
                for anc in _FIN_REVENUE_ANCHORS:
                    fin = _section_text(xml_path, [anc], max_chars=8000, lines_per_anchor=200,
                                       priority=False, require_keyword=None)
                    if fin and len(fin) > 200:
                        rev_text = fin
                        break

            print(f"\n{'='*60}")
            print(f"[{corp}] {period} | rev_text={len(rev_text)}자")
            print(f"매출 지표 포함: {any(kw in rev_text for kw in _REV_INDICATORS)}")
            print(f"rev_text 처음 300자:\n{rev_text[:300]}")

            print(f"\n→ Ollama 호출 중...")
            prompt = REVENUE_ANALYZER_PROMPT + rev_text
            result = await _call_ollama_native(http, DEFAULT_MODEL, prompt)
            print(f"응답 타입: {type(result).__name__}")
            if isinstance(result, dict):
                print(f"periods: {result.get('periods')}")
                c = result.get("consolidated", {})
                print(f"revenue: {c.get('revenue', [])[:3]}")
                print(f"op_profit: {c.get('op_profit', [])[:3]}")
            else:
                print(f"응답: {str(result)[:200]}")

asyncio.run(main())
