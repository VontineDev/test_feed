"""삼성전자 revenue_json 확인 및 재추출 테스트."""
import asyncio, json, sys
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")
from dotenv import load_dotenv; load_dotenv()
from core.db import create_pool
from data.dart_extractor import _pick_main_xml, _parse_report_dir, DART_DIR, _section_text, _REVENUE_ANCHORS


async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT corp_name, period, report_type, rcept_no, revenue_json "
            "FROM dart_extractions WHERE corp_name='삼성전자' ORDER BY period"
        )
    await pool.close()

    print("=== DB 저장값 ===")
    for r in rows:
        rj = json.loads(r["revenue_json"]) if r["revenue_json"] else None
        if not rj:
            print(f"{r['period']} {r['report_type']}: [NULL]")
            continue
        c = rj.get("consolidated", {})
        print(f"{r['period']} {r['report_type']}")
        print(f"  periods : {rj.get('periods')}")
        print(f"  revenue : {c.get('revenue')}")
        print(f"  op_profit: {c.get('op_profit')}")

    # 최신 보고서 rev_text 확인
    print("\n=== 최신 보고서 rev_text 확인 ===")
    corp_dir = DART_DIR / "삼성전자"
    report_dirs = sorted(d for d in corp_dir.iterdir() if d.is_dir())
    print(f"보고서 디렉터리 수: {len(report_dirs)}")
    for d in report_dirs:
        print(f"  {d.name}")

    # 최신 사업보고서
    target = next((d for d in reversed(report_dirs) if "사업보고서" in d.name), report_dirs[-1])
    print(f"\n대상: {target.name}")
    rcept_no, rtype, period = _parse_report_dir(target.name)
    xml_path = _pick_main_xml(target, rcept_no)
    print(f"XML: {xml_path.name if xml_path else 'None'}")

    if xml_path:
        rev_text = _section_text(
            xml_path, _REVENUE_ANCHORS,
            max_chars=3000, lines_per_anchor=200,
            priority=True, require_keyword="영업이익",
        )
        print(f"rev_text 길이: {len(rev_text) if rev_text else 0}자")
        if rev_text:
            print("--- 처음 800자 ---")
            print(rev_text[:800])

asyncio.run(main())
