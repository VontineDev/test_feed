"""
ADECIMAL 미적용으로 인한 잘못된 수치 일괄 수정.
LLM 재실행 없이 XBRL 재파싱만 수행하여 revenue_json 업데이트.
"""
import asyncio, json, sys
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()
from core.db import create_pool
from data.dart_extractor import (
    DART_DIR, _parse_report_dir, _pick_main_xml,
    extract_xbrl_quarterly, _derive_period_labels,
)

def _unit_divisor(unit):
    return {"원": 1e8, "천원": 1e5, "백만원": 1e2, "억원": 1.0, "조원": 1e-4}.get(unit or "", 1e8)

def fmt_eok(val, unit):
    if val is None:
        return "-"
    eok = float(val) / _unit_divisor(unit)
    if abs(eok) >= 10000:
        return f"{eok/10000:.2f}T"
    return f"{eok:.0f}E"

async def main():
    pool = await create_pool()
    updated = skipped = failed = 0

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT corp_name, rcept_no, report_type, period, revenue_json
            FROM dart_extractions
            WHERE report_type LIKE '%분기%' OR report_type LIKE '%반기%'
            ORDER BY corp_name, period
        """)

    print(f"대상: {len(rows)}건 (분기/반기보고서)")

    for r in rows:
        corp_name  = r["corp_name"]
        rcept_no   = r["rcept_no"]
        report_type = r["report_type"]
        period     = r["period"]

        # XML 경로 탐색
        co_dir = DART_DIR / corp_name
        if not co_dir.is_dir():
            skipped += 1
            continue

        xml_path = None
        for rd in co_dir.iterdir():
            if not rd.is_dir():
                continue
            p_rcept, _, _ = _parse_report_dir(rd.name)
            if p_rcept == rcept_no:
                xml_path = _pick_main_xml(rd, rcept_no)
                break

        if xml_path is None:
            skipped += 1
            continue

        # XBRL 재추출
        xbrl = extract_xbrl_quarterly(xml_path)
        if xbrl is None:
            skipped += 1
            continue

        cur_lbl, pri_lbl = _derive_period_labels(report_type, period)

        # 기존 revenue_json 읽기
        old_rev = json.loads(r["revenue_json"]) if r["revenue_json"] else {}
        old_c = old_rev.get("consolidated", {})
        old_revs = old_c.get("revenue", [None, None])
        old_ops  = old_c.get("op_profit", [None, None])
        old_unit = old_rev.get("unit", "원")

        new_revs = xbrl["revenue"]
        new_ops  = xbrl["op_profit"]
        new_unit = "원"  # XBRL 추출 후 항상 원 단위

        # 변경 여부 확인
        changed = (old_revs != new_revs or old_ops != new_ops or old_unit != new_unit)
        if not changed:
            skipped += 1
            continue

        # 변경 사항 출력
        print(f"\n[{corp_name}] {report_type} {period}")
        print(f"  매출: {old_revs} ({old_unit}) → {new_revs} (원)")
        if old_revs and old_revs[0] and new_revs[0]:
            ratio = new_revs[0] / old_revs[0] if old_revs[0] != 0 else None
            if ratio and abs(ratio - 1) > 0.01:
                print(f"    배율: x{ratio:.2f}")
            before_eok = fmt_eok(old_revs[0], old_unit)
            after_eok  = fmt_eok(new_revs[0], "원")
            print(f"    당기매출: {before_eok} → {after_eok}")

        # revenue_json 업데이트
        old_rev["periods"]   = [cur_lbl, pri_lbl]
        old_rev["unit"]      = new_unit
        if "consolidated" not in old_rev:
            old_rev["consolidated"] = {}
        old_rev["consolidated"]["revenue"]   = new_revs
        old_rev["consolidated"]["op_profit"] = new_ops

        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE dart_extractions
                SET revenue_json = $1, extracted_at = NOW()
                WHERE corp_name = $2 AND rcept_no = $3
                """,
                json.dumps(old_rev, ensure_ascii=False),
                corp_name, rcept_no,
            )
        updated += 1

    print(f"\n=== 완료: 업데이트 {updated}건 | 스킵 {skipped}건 | 실패 {failed}건 ===")
    await pool.close()

asyncio.run(main())
