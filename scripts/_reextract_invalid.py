"""revenue_json이 없거나 빈 레코드 전체 재추출."""
import asyncio, json, sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()

import httpx
from data.dart_extractor import (
    extract_structured, _pick_main_xml, _parse_report_dir,
    DEFAULT_MODEL, DART_DIR,
)
from reports.summarizer import _ollama_is_alive, OLLAMA_BASE
from core.db import create_pool


def _is_valid_rev(rj) -> bool:
    """실질 숫자가 있는 revenue_json인지 판단."""
    if rj is None:
        return False
    try:
        d = json.loads(rj) if isinstance(rj, str) else rj
        if isinstance(d, list):
            return False          # [] 빈 배열
        c = d.get("consolidated", {})
        segs = d.get("segments", [])
        has_num = lambda lst: any(
            v not in (None, 0, "", "-") for v in (lst or [])
        )
        return (has_num(c.get("revenue", []))
                or has_num(c.get("op_profit", []))
                or any(has_num(s.get("revenues", [])) for s in segs))
    except Exception:
        return False


async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT corp_name, rcept_no, report_type, period, revenue_json
            FROM dart_extractions
            ORDER BY corp_name, period
        """)

    invalid = [r for r in rows if not _is_valid_rev(r["revenue_json"])]
    print(f"재추출 대상: {len(invalid)}건 / 전체 {len(rows)}건")

    async with httpx.AsyncClient(timeout=180) as http:
        if not await _ollama_is_alive(http):
            print(f"Ollama 미응답: {OLLAMA_BASE}"); return

        ok = fail = skip = 0
        for r in invalid:
            corp_name = r["corp_name"]
            rcept_no  = r["rcept_no"]

            corp_dir = DART_DIR / corp_name
            if not corp_dir.exists():
                skip += 1; continue

            xml_path = None
            for d in corp_dir.iterdir():
                if d.is_dir() and rcept_no in d.name:
                    xml_path = _pick_main_xml(d, rcept_no)
                    break
            if not xml_path:
                skip += 1; continue

            print(f"  재추출: {corp_name} / {r['report_type']} ({r['period']})", end=" ")
            try:
                structured = await extract_structured(http, xml_path, DEFAULT_MODEL)
            except Exception as e:
                print(f"[오류] {e}")
                fail += 1; continue

            new_rev = structured["revenue_json"]
            if _is_valid_rev(json.dumps(new_rev) if new_rev else None):
                async with pool.acquire() as conn:
                    await conn.execute("""
                        UPDATE dart_extractions SET
                            revenue_json     = COALESCE($1::jsonb, revenue_json),
                            segments_json    = COALESCE($2::jsonb, segments_json),
                            competitors_json = COALESCE($3::jsonb, competitors_json),
                            extracted_at     = NOW()
                        WHERE corp_name=$4 AND rcept_no=$5
                    """,
                        json.dumps(new_rev, ensure_ascii=False) if new_rev else None,
                        json.dumps(structured["segments_json"], ensure_ascii=False)
                            if structured["segments_json"] else None,
                        json.dumps(structured["competitors_json"], ensure_ascii=False)
                            if structured["competitors_json"] else None,
                        corp_name, rcept_no,
                    )
                print("[OK]"); ok += 1
            else:
                print("[FAIL]"); fail += 1

    await pool.close()
    print(f"\n완료: OK={ok}, FAIL={fail}, SKIP={skip}")

asyncio.run(main())
