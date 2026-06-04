"""revenue_json 실질 유효 데이터 집계."""
import asyncio, sys, json
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()
from core.db import create_pool

async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT corp_name, period, report_type,
                   revenue_json, segments_json, competitors_json
            FROM dart_extractions
            ORDER BY corp_name, period DESC
        """)

    # 유효 여부 판단 함수
    def is_valid_rev(rj) -> bool:
        if not rj:
            return False
        try:
            d = json.loads(rj) if isinstance(rj, str) else rj
            c = d.get("consolidated", {})
            rev = c.get("revenue", [])
            op  = c.get("op_profit", [])
            segs = d.get("segments", [])
            # 연결 매출 또는 영업이익 또는 부문별 매출 중 하나라도 실제 숫자 있으면 유효
            has_num = lambda lst: any(
                v is not None and str(v).replace("-","").replace(".","").strip() != ""
                for v in lst
            )
            return has_num(rev) or has_num(op) or any(
                has_num(s.get("revenues", [])) for s in segs
            )
        except Exception:
            return False

    def is_valid_seg(sj) -> bool:
        if not sj:
            return False
        try:
            d = json.loads(sj) if isinstance(sj, str) else sj
            return isinstance(d, list) and len(d) > 0
        except Exception:
            return False

    def is_valid_comp(cj) -> bool:
        if not cj:
            return False
        try:
            d = json.loads(cj) if isinstance(cj, str) else cj
            return isinstance(d, list) and len(d) > 0
        except Exception:
            return False

    # 기업별 집계
    by_corp: dict = {}
    for r in rows:
        c = r["corp_name"]
        if c not in by_corp:
            by_corp[c] = {"total": 0, "rev": 0, "seg": 0, "comp": 0}
        by_corp[c]["total"] += 1
        if is_valid_rev(r["revenue_json"]):
            by_corp[c]["rev"] += 1
        if is_valid_seg(r["segments_json"]):
            by_corp[c]["seg"] += 1
        if is_valid_comp(r["competitors_json"]):
            by_corp[c]["comp"] += 1

    total_corps = len(by_corp)
    total_rows  = sum(v["total"] for v in by_corp.values())
    total_rev   = sum(v["rev"]   for v in by_corp.values())
    total_seg   = sum(v["seg"]   for v in by_corp.values())
    total_comp  = sum(v["comp"]  for v in by_corp.values())

    print(f"{'기업명':<16} {'전체':>4} {'매출유효':>6} {'부문유효':>6} {'경쟁사유효':>8}")
    print("-" * 50)
    for corp, v in sorted(by_corp.items(), key=lambda x: -x[1]["rev"]):
        mark_r = "" if v["rev"] == v["total"] else f"  rev={v['rev']}/{v['total']}"
        mark_s = "" if v["seg"] == v["total"] else f" seg={v['seg']}/{v['total']}"
        print(f"{corp:<16} {v['total']:>4} {v['rev']:>6} {v['seg']:>6} {v['comp']:>8}{mark_r}{mark_s}")

    print("-" * 50)
    print(f"{'합계':<16} {total_rows:>4} {total_rev:>6} {total_seg:>6} {total_comp:>8}")
    print(f"\n기업 수: {total_corps}")
    print(f"매출 유효율: {total_rev}/{total_rows} ({100*total_rev/total_rows:.1f}%)")
    print(f"부문 유효율: {total_seg}/{total_rows} ({100*total_seg/total_rows:.1f}%)")
    print(f"경쟁사 유효율: {total_comp}/{total_rows} ({100*total_comp/total_rows:.1f}%)")

    await pool.close()

asyncio.run(main())
