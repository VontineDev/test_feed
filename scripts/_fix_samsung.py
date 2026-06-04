"""삼성전자 및 지정 기업 강제 재추출 (유효 여부 상관없이)."""
import asyncio, json, sys
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")
from dotenv import load_dotenv; load_dotenv()
import httpx
from core.db import create_pool
from data.dart_extractor import (
    extract_structured, _pick_main_xml, _parse_report_dir,
    DEFAULT_MODEL, DART_DIR,
)
from reports.summarizer import _ollama_is_alive, OLLAMA_BASE

FORCE_CORPS = ["삼성전자"]


def _is_valid_rev(rj) -> bool:
    if rj is None: return False
    try:
        d = json.loads(rj) if isinstance(rj, str) else rj
        if isinstance(d, list): return False
        c = d.get("consolidated", {})
        segs = d.get("segments", [])
        has_num = lambda lst: any(v not in (None, 0, "", "-") for v in (lst or []))
        return (has_num(c.get("revenue", []))
                or has_num(c.get("op_profit", []))
                or any(has_num(s.get("revenues", [])) for s in segs))
    except:
        return False


async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT corp_name, rcept_no, report_type, period, revenue_json "
            "FROM dart_extractions WHERE corp_name = ANY($1) ORDER BY corp_name, period",
            FORCE_CORPS,
        )

    print(f"강제 재추출 대상: {len(rows)}건")

    async with httpx.AsyncClient(timeout=180) as http:
        if not await _ollama_is_alive(http):
            print("Ollama 미응답"); return

        ok = fail = skip = 0
        for r in rows:
            corp_dir = DART_DIR / r["corp_name"]
            if not corp_dir.exists():
                skip += 1; continue

            xml_path = None
            for d in corp_dir.iterdir():
                if d.is_dir() and r["rcept_no"] in d.name:
                    xml_path = _pick_main_xml(d, r["rcept_no"])
                    break
            if not xml_path:
                skip += 1; continue

            print(f"  {r['corp_name']} / {r['report_type']} ({r['period']})", end=" ", flush=True)
            try:
                structured = await extract_structured(http, xml_path, DEFAULT_MODEL)
            except Exception as e:
                print(f"[오류] {e}"); fail += 1; continue

            new_rev = structured["revenue_json"]
            new_rev_str = json.dumps(new_rev, ensure_ascii=False) if new_rev else None

            # 기존 값과 비교
            old_rj = json.loads(r["revenue_json"]) if r["revenue_json"] else None
            old_rev = old_rj.get("consolidated", {}).get("revenue", []) if old_rj and isinstance(old_rj, dict) else []
            new_rev_vals = new_rev.get("consolidated", {}).get("revenue", []) if new_rev and isinstance(new_rev, dict) else []

            # 항상 업데이트 (강제)
            async with pool.acquire() as conn:
                await conn.execute("""
                    UPDATE dart_extractions SET
                        revenue_json     = $1::jsonb,
                        segments_json    = COALESCE($2::jsonb, segments_json),
                        competitors_json = COALESCE($3::jsonb, competitors_json),
                        extracted_at     = NOW()
                    WHERE corp_name=$4 AND rcept_no=$5
                """,
                    new_rev_str,
                    json.dumps(structured["segments_json"], ensure_ascii=False)
                        if structured["segments_json"] else None,
                    json.dumps(structured["competitors_json"], ensure_ascii=False)
                        if structured["competitors_json"] else None,
                    r["corp_name"], r["rcept_no"],
                )
            valid = _is_valid_rev(new_rev_str)
            print(f"[{'OK' if valid else 'FAIL'}] 이전={old_rev} → 이후={new_rev_vals}")
            if valid: ok += 1
            else: fail += 1

    await pool.close()
    print(f"\n완료: OK={ok}, FAIL={fail}, SKIP={skip}")

asyncio.run(main())
