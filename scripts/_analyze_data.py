"""재추출 데이터 종합 분석."""
import asyncio, json, sys
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")
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


def _is_valid_seg(sj):
    if not sj: return False
    d = json.loads(sj) if isinstance(sj, str) else sj
    return isinstance(d, list) and len(d) > 0 and any(s.get("revenues") for s in d)


async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT corp_name, report_type, period, revenue_json, segments_json, competitors_json "
            "FROM dart_extractions ORDER BY corp_name, period"
        )
    await pool.close()

    total = len(rows)
    rev_ok  = [r for r in rows if _is_valid_rev(r["revenue_json"])]
    rev_fail = [r for r in rows if not _is_valid_rev(r["revenue_json"])]
    rev_null = [r for r in rev_fail if r["revenue_json"] is None]
    rev_empty = [r for r in rev_fail if r["revenue_json"] is not None]
    seg_ok  = [r for r in rows if _is_valid_seg(r["segments_json"])]

    print("=== 전체 현황 ===")
    print(f"총 레코드: {total}건")
    print(f"매출 유효: {len(rev_ok)}건 ({len(rev_ok)/total*100:.1f}%)")
    print(f"  NULL   : {len(rev_null)}건")
    print(f"  빈값   : {len(rev_empty)}건")
    print(f"부문 유효: {len(seg_ok)}건 ({len(seg_ok)/total*100:.1f}%)")
    print()

    # 보고서 유형별
    rtype_cnt = {}
    rtype_ok  = {}
    for r in rows:
        rt = r["report_type"]
        rtype_cnt[rt] = rtype_cnt.get(rt, 0) + 1
        if _is_valid_rev(r["revenue_json"]):
            rtype_ok[rt] = rtype_ok.get(rt, 0) + 1

    print("=== 보고서 유형별 ===")
    for rt in sorted(rtype_cnt, key=lambda x: -rtype_cnt[x]):
        cnt = rtype_cnt[rt]
        ok  = rtype_ok.get(rt, 0)
        bar = "#" * int(ok/cnt*20)
        print(f"  {rt:<32} {ok:>3}/{cnt:<3} ({ok/cnt*100:>4.0f}%) {bar}")
    print()

    # FAIL 기업 목록
    fail_corps = {}
    for r in rev_fail:
        cn = r["corp_name"]
        fail_corps[cn] = fail_corps.get(cn, 0) + 1

    print(f"=== FAIL 기업 ({len(fail_corps)}개) ===")
    for cn, cnt in sorted(fail_corps.items(), key=lambda x: -x[1]):
        print(f"  {cn}: {cnt}건")
    print()

    # 주요 대형주 데이터 확인
    big_corps = ["삼성전자", "SK하이닉스", "현대자동차", "기아", "NAVER", "카카오",
                 "LG전자", "삼성바이오로직스", "에코프로", "셀트리온"]
    print("=== 주요 기업 최신 데이터 ===")
    for corp in big_corps:
        corp_rows = [r for r in rows if r["corp_name"] == corp]
        if not corp_rows: continue
        latest = sorted(corp_rows, key=lambda x: x["period"])[-1]
        rj = latest["revenue_json"]
        if rj and _is_valid_rev(rj):
            d = json.loads(rj) if isinstance(rj, str) else rj
            c = d.get("consolidated", {})
            periods = d.get("periods", [])
            revs = c.get("revenue", [])
            ops  = c.get("op_profit", [])
            segs = d.get("segments", [])
            # 최신 연도 값
            rev_latest = revs[-1] if revs else None
            op_latest  = ops[-1] if ops else None
            period_latest = periods[-1] if periods else "?"
            seg_cnt = len(segs)
            def fmt(v):
                if v is None or v in ("", "-"): return "-"
                try:
                    n = float(str(v).replace(",", ""))
                    if n >= 1e12: return f"{n/1e12:.1f}조"
                    if n >= 1e8:  return f"{n/1e8:.0f}억"
                    return str(v)
                except:
                    return str(v)
            print(f"  {corp:<16} {latest['period']} | 매출={fmt(rev_latest)} | 영업익={fmt(op_latest)} | 부문={seg_cnt}개 | 기준={period_latest}")
        else:
            print(f"  {corp:<16} {latest['period']} | [FAIL]")

asyncio.run(main())
