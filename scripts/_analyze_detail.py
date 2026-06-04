"""주요 기업 revenue_json 상세 분석 + 부문/단위 이슈 파악."""
import asyncio, json, sys
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8")
from dotenv import load_dotenv; load_dotenv()
from core.db import create_pool


def _is_valid_rev(rj):
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
        all_rows = await conn.fetch(
            "SELECT corp_name, period, report_type, revenue_json, segments_json "
            "FROM dart_extractions ORDER BY corp_name, period DESC"
        )
    await pool.close()

    # --- 1. 전체 집계 ---
    total = len(all_rows)
    rev_valid = sum(1 for r in all_rows if _is_valid_rev(r["revenue_json"]))

    # segments_json 집계: 부문이 1개 이상 있으면 유효
    seg_valid = 0
    for r in all_rows:
        sj = r["segments_json"]
        if sj:
            d = json.loads(sj) if isinstance(sj, str) else sj
            if isinstance(d, list) and any(isinstance(s, dict) and s.get("segment_name") for s in d):
                seg_valid += 1

    # revenue_json 내 segments 집계
    rev_seg_valid = 0
    for r in all_rows:
        rj = r["revenue_json"]
        if rj:
            d = json.loads(rj) if isinstance(rj, str) else rj
            if isinstance(d, dict):
                segs = d.get("segments", [])
                if any(s.get("revenues") for s in segs):
                    rev_seg_valid += 1

    print("=== 1. 전체 추출 현황 ===")
    print(f"  총 레코드       : {total}건")
    print(f"  매출 유효       : {rev_valid}건 ({rev_valid/total*100:.1f}%)")
    print(f"  매출 실패       : {total-rev_valid}건 ({(total-rev_valid)/total*100:.1f}%)")
    print(f"  부문 유효(segments_json): {seg_valid}건 ({seg_valid/total*100:.1f}%)")
    print(f"  부문 유효(revenue.segs) : {rev_seg_valid}건 ({rev_seg_valid/total*100:.1f}%)")
    print()

    # --- 2. 보고서 유형별 ---
    from collections import defaultdict
    rtype_total = defaultdict(int)
    rtype_ok    = defaultdict(int)
    for r in all_rows:
        rt = r["report_type"]
        rtype_total[rt] += 1
        if _is_valid_rev(r["revenue_json"]):
            rtype_ok[rt] += 1

    print("=== 2. 보고서 유형별 ===")
    for rt in sorted(rtype_total, key=lambda x: -rtype_total[x]):
        t = rtype_total[rt]; ok = rtype_ok[rt]
        print(f"  {rt:<34} {ok:>3}/{t:<3} ({ok/t*100:>3.0f}%)")
    print()

    # --- 3. FAIL 기업 ---
    fail_corps = defaultdict(int)
    for r in all_rows:
        if not _is_valid_rev(r["revenue_json"]):
            fail_corps[r["corp_name"]] += 1

    print(f"=== 3. FAIL 기업 ({len(fail_corps)}개) ===")
    for cn, cnt in sorted(fail_corps.items(), key=lambda x: -x[1]):
        print(f"  {cn}: {cnt}건")
    print()

    # --- 4. 주요 기업 최신 값 (단위 raw 표시) ---
    big_corps = ["삼성전자", "SK하이닉스", "현대자동차", "기아", "NAVER",
                 "카카오", "LG전자", "삼성바이오로직스", "에코프로", "셀트리온"]
    print("=== 4. 주요 기업 최신 데이터 (raw) ===")
    seen = set()
    for r in all_rows:
        cn = r["corp_name"]
        if cn not in big_corps or cn in seen: continue
        seen.add(cn)
        rj = r["revenue_json"]
        if not rj or not _is_valid_rev(rj):
            print(f"  {cn}: [FAIL]"); continue
        d = json.loads(rj) if isinstance(rj, str) else rj
        c   = d.get("consolidated", {})
        periods = d.get("periods", [])
        revs    = c.get("revenue", [])
        ops     = c.get("op_profit", [])
        segs    = d.get("segments", [])
        sj = r["segments_json"]
        seg_cnt = 0
        if sj:
            sd = json.loads(sj) if isinstance(sj, str) else sj
            seg_cnt = len([s for s in sd if isinstance(s, dict) and s.get("segment_name")])
        print(f"  [{cn}] {r['period']} {r['report_type']}")
        print(f"    periods : {periods}")
        print(f"    revenue : {revs}")
        print(f"    op_profit: {ops}")
        print(f"    rev.segs : {len(segs)}개 | seg_json : {seg_cnt}개")
    print()

    # --- 5. 금액 단위 분석 ---
    print("=== 5. 금액 단위 이슈 ===")
    print("  삼성전자 연간 매출 약 300조원, SK하이닉스 약 66-97조원")
    print("  추출값 대비 예상 단위 추정:")
    unit_samples = {"삼성전자": 300e12, "SK하이닉스": 97e12, "현대자동차": 175e12,
                    "NAVER": 9e12, "셀트리온": 3e12}
    for r in all_rows:
        cn = r["corp_name"]
        if cn not in unit_samples: continue
        rj = r["revenue_json"]
        if not rj or not _is_valid_rev(rj): continue
        d = json.loads(rj) if isinstance(rj, str) else rj
        revs = d.get("consolidated", {}).get("revenue", [])
        if not revs: continue
        try:
            raw = float(str(revs[-1]).replace(",", ""))
            expected = unit_samples[cn]
            ratio = expected / raw if raw else 0
            if ratio > 1e9: unit = f"원 (x{ratio:.0e})"
            elif ratio > 1e6: unit = f"천원 (x{ratio:.0e})"
            elif ratio > 1e3: unit = f"백만원 (x{ratio:.0e})"
            elif ratio > 1: unit = f"억원 (x{ratio:.1f})"
            else: unit = f"조원 (x{ratio:.2f})"
            print(f"  {cn}: raw={raw:.0f}  예상단위={unit}")
        except: pass

asyncio.run(main())
