"""기존 revenue_json에 unit 필드를 백필."""
import asyncio, json, sys, re
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")
from pathlib import Path
from dotenv import load_dotenv; load_dotenv()
from core.db import create_pool
from data.dart_extractor import (
    _pick_main_xml, _parse_report_dir, DART_DIR,
    _section_text, _REVENUE_ANCHORS, _detect_unit,
)

_UNIT_RE = re.compile(r"단위\s*[:：]\s*(원|천원|백만원|억원|조원)")


async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT corp_name, rcept_no, report_type, period, revenue_json "
            "FROM dart_extractions WHERE revenue_json IS NOT NULL ORDER BY corp_name, period"
        )

    unit_counts = {}
    updated = skipped = 0

    async with pool.acquire() as conn:
        for r in rows:
            rj = json.loads(r["revenue_json"]) if r["revenue_json"] else None
            if not rj or not isinstance(rj, dict):
                skipped += 1
                continue

            # 이미 unit 있으면 skip
            if rj.get("unit") and rj["unit"] != "unknown":
                skipped += 1
                unit_counts[rj["unit"]] = unit_counts.get(rj["unit"], 0) + 1
                continue

            # XML에서 단위 파싱
            corp_dir = DART_DIR / r["corp_name"]
            xml_path = None
            if corp_dir.exists():
                for d in corp_dir.iterdir():
                    if d.is_dir() and r["rcept_no"] in d.name:
                        xml_path = _pick_main_xml(d, r["rcept_no"])
                        break

            if not xml_path:
                unit = "unknown"
            else:
                # rev_text에서 단위 감지
                rev_text = _section_text(
                    xml_path, _REVENUE_ANCHORS,
                    max_chars=8000, lines_per_anchor=200,
                    priority=True, require_keyword="영업이익",
                ) or ""
                unit = _detect_unit(rev_text)

                # rev_text에 없으면 XML 전체에서 검색
                if unit == "unknown":
                    try:
                        raw = xml_path.read_text(encoding="utf-8", errors="ignore")
                        m = _UNIT_RE.search(raw)
                        unit = m.group(1) if m else "unknown"
                    except Exception:
                        pass

            rj["unit"] = unit
            unit_counts[unit] = unit_counts.get(unit, 0) + 1

            await conn.execute(
                "UPDATE dart_extractions SET revenue_json=$1::jsonb WHERE corp_name=$2 AND rcept_no=$3",
                json.dumps(rj, ensure_ascii=False),
                r["corp_name"], r["rcept_no"],
            )
            updated += 1

    await pool.close()

    print(f"업데이트: {updated}건 / 스킵: {skipped}건")
    print("\n단위 분포:")
    for unit, cnt in sorted(unit_counts.items(), key=lambda x: -x[1]):
        print(f"  {unit}: {cnt}건")


asyncio.run(main())
