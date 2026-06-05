"""revenue_json IS NULL인 레코드만 재추출."""
import asyncio, json, sys, os
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")

try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError: pass

import httpx
from data.dart_extractor import (
    extract_structured, _pick_main_xml, _parse_report_dir, DEFAULT_MODEL,
    DART_DIR,
)
from reports.summarizer import _ollama_is_alive, OLLAMA_BASE
from core.db import create_pool

async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT corp_name, rcept_no, report_type, period "
            "FROM dart_extractions "
            "WHERE revenue_json IS NULL "
            "ORDER BY corp_name, period"
        )
    print(f"revenue_json NULL 레코드: {len(rows)}건")

    async with httpx.AsyncClient() as http:
        if not await _ollama_is_alive(http):
            print(f"Ollama 미응답: {OLLAMA_BASE}"); return

        for r in rows:
            corp_name = r["corp_name"]
            rcept_no  = r["rcept_no"]

            # XML 경로 찾기
            corp_dir = DART_DIR / corp_name
            if not corp_dir.exists():
                print(f"  [스킵] 디렉터리 없음: {corp_name}"); continue

            xml_path = None
            for d in corp_dir.iterdir():
                if d.is_dir() and rcept_no in d.name:
                    xml_path = _pick_main_xml(d, rcept_no)
                    break

            if not xml_path:
                print(f"  [스킵] XML 없음: {corp_name}/{rcept_no}"); continue

            print(f"  재추출: {corp_name} / {r['report_type']} ({r['period']})")
            try:
                structured = await extract_structured(http, xml_path, DEFAULT_MODEL)
            except Exception as e:
                print(f"  [오류] {e}"); continue

            def _to_jsonb(v):
                return json.dumps(v, ensure_ascii=False) if v is not None else None

            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE dart_extractions SET
                        revenue_json     = COALESCE($1, revenue_json),
                        segments_json    = COALESCE($2, segments_json),
                        competitors_json = COALESCE($3, competitors_json),
                        extracted_at     = NOW()
                    WHERE corp_name=$4 AND rcept_no=$5
                    """,
                    _to_jsonb(structured["revenue_json"]),
                    _to_jsonb(structured["segments_json"]),
                    _to_jsonb(structured["competitors_json"]),
                    corp_name, rcept_no,
                )
            rev_ok = "OK" if structured["revenue_json"] else "FAIL"
            print(f"    revenue_json [{rev_ok}]")

    await pool.close()
    print("\n완료")

asyncio.run(main())
