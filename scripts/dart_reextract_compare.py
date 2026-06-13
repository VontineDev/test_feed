"""
dart_reextract_compare.py — 재추출 전후 비교 스크립트

실행:
    python scripts/dart_reextract_compare.py --company 메타랝스
    python scripts/dart_reextract_compare.py --company NAVER
    python scripts/dart_reextract_compare.py --company 삼성전자 --save-only  # 스냅샷만 저장
    python scripts/dart_reextract_compare.py --compare-only                  # 저장된 스냅샷과 현재 비교
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)

SNAPSHOT_DIR = Path(__file__).parent.parent / "data" / "dart_snapshots"


def _unit_divisor(unit: str | None) -> float:
    """단위 → 억원 환산 제수."""
    return {"원": 1e8, "천원": 1e5, "백만원": 1e2, "억원": 1.0, "조원": 1e-4}.get(unit or "", 1e8)


def _fmt(val: float | int | None, unit: str | None) -> str:
    if val is None:
        return "—"
    try:
        eok = float(val) / _unit_divisor(unit)
        sign = "+" if eok >= 0 else ""
        return f"{sign}{eok:,.1f}억"
    except Exception:
        return str(val)


def _snapshot_path(corp_name: str) -> Path:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    safe = corp_name.replace("/", "_").replace("\\", "_")
    return SNAPSHOT_DIR / f"{safe}.json"


async def _load_db(pool, corp_name: str) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT rcept_no, report_type, period, revenue_json, segments_json, extracted_at
            FROM dart_extractions
            WHERE corp_name = $1
            ORDER BY period
            """,
            corp_name,
        )
    result = []
    for r in rows:
        result.append({
            "rcept_no":     r["rcept_no"],
            "report_type":  r["report_type"],
            "period":       r["period"],
            "revenue_json": json.loads(r["revenue_json"]) if r["revenue_json"] else None,
            "extracted_at": str(r["extracted_at"]),
        })
    return result


def _save_snapshot(corp_name: str, records: list[dict]) -> Path:
    path = _snapshot_path(corp_name)
    data = {
        "corp_name":    corp_name,
        "snapshot_at":  datetime.now().isoformat(),
        "records":      records,
    }
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("[compare] 스냅샷 저장: %s (%d건)", path, len(records))
    return path


def _load_snapshot(corp_name: str) -> dict | None:
    path = _snapshot_path(corp_name)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _print_record(label: str, rec: dict) -> None:
    rev = rec.get("revenue_json") or {}
    c = rev.get("consolidated", {})
    unit = rev.get("unit")
    periods = rev.get("periods", [])
    revenues = c.get("revenue", [])
    op_profits = c.get("op_profit", [])

    print(f"  [{label}] {rec['report_type']} {rec['period']}")
    print(f"    periods   : {periods}")
    print(f"    unit      : {unit}")
    if revenues:
        for i, (rev_val, period_lbl) in enumerate(zip(revenues, periods or ["당기", "전기"])):
            print(f"    매출({period_lbl}) : {_fmt(rev_val, unit)}")
    if op_profits:
        for i, (op_val, period_lbl) in enumerate(zip(op_profits, periods or ["당기", "전기"])):
            print(f"    영업이익({period_lbl}): {_fmt(op_val, unit)}")


def _compare(before: list[dict], after: list[dict]) -> None:
    before_map = {r["rcept_no"]: r for r in before}
    after_map  = {r["rcept_no"]: r for r in after}

    all_keys = sorted(set(before_map) | set(after_map))
    changed = 0

    for key in all_keys:
        b = before_map.get(key)
        a = after_map.get(key)

        if b is None:
            print(f"\n[신규] rcept_no={key}")
            _print_record("NEW", a)
            changed += 1
            continue
        if a is None:
            print(f"\n[삭제] rcept_no={key}")
            _print_record("DEL", b)
            changed += 1
            continue

        brev = (b.get("revenue_json") or {})
        arev = (a.get("revenue_json") or {})

        b_periods  = brev.get("periods")
        a_periods  = arev.get("periods")
        b_revenue  = (brev.get("consolidated") or {}).get("revenue")
        a_revenue  = (arev.get("consolidated") or {}).get("revenue")
        b_op       = (brev.get("consolidated") or {}).get("op_profit")
        a_op       = (arev.get("consolidated") or {}).get("op_profit")
        b_unit     = brev.get("unit")
        a_unit     = arev.get("unit")

        same = (b_periods == a_periods and b_revenue == a_revenue and
                b_op == a_op and b_unit == a_unit)
        if same:
            print(f"\n[동일] {b['report_type']} {b['period']} (rcept_no={key})")
            continue

        print(f"\n[변경] {b['report_type']} {b['period']} (rcept_no={key})")
        changed += 1
        if b_periods != a_periods:
            print(f"    periods : {b_periods!r}  →  {a_periods!r}")
        if b_unit != a_unit:
            print(f"    unit    : {b_unit!r}  →  {a_unit!r}")
        if b_revenue != a_revenue:
            print(f"    매출(before) : {b_revenue}")
            print(f"    매출(after)  : {a_revenue}")
            if b_revenue and a_revenue and len(b_revenue) == len(a_revenue):
                for bi, ai in zip(b_revenue, a_revenue):
                    if bi != ai:
                        try:
                            diff = (float(ai) - float(bi)) / abs(float(bi)) * 100
                            print(f"      차이: {_fmt(bi, b_unit)} → {_fmt(ai, a_unit)}  ({diff:+.1f}%)")
                        except Exception:
                            pass
        if b_op != a_op:
            print(f"    영업이익(before): {b_op}")
            print(f"    영업이익(after) : {a_op}")

    print(f"\n=== 비교 결과: {changed}/{len(all_keys)}건 변경 ===")


async def _main() -> None:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    ap = argparse.ArgumentParser()
    ap.add_argument("--company", help="기업명 (dart_extractions.corp_name)")
    ap.add_argument("--stock-code", help="종목코드 (dart_companies.stock_code)")
    ap.add_argument("--save-only", action="store_true", help="스냅샷만 저장 (재추출 없음)")
    ap.add_argument("--compare-only", action="store_true", help="저장된 스냅샷과 현재 DB 비교")
    ap.add_argument("--force", action="store_true", default=True, help="강제 재추출 (기본 True)")
    args = ap.parse_args()

    from core.db import create_pool
    pool = await create_pool()

    # stock_code → corp_name 변환
    if args.stock_code and not args.company:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT corp_name FROM dart_companies WHERE stock_code = $1",
                args.stock_code,
            )
        if not row:
            print(f"종목코드 {args.stock_code} 를 dart_companies에서 찾을 수 없습니다.")
            await pool.close()
            return
        args.company = row["corp_name"]
        print(f"corp_name: {args.company!r}")

    if not args.company and not args.compare_only:
        ap.print_help()
        await pool.close()
        return

    corp_name = args.company

    if args.compare_only:
        # 저장된 스냅샷과 현재 DB 비교
        snap = _load_snapshot(corp_name)
        if not snap:
            print(f"스냅샷 없음: {_snapshot_path(corp_name)}")
            await pool.close()
            return
        print(f"스냅샷: {snap['snapshot_at']} ({len(snap['records'])}건)")
        current = await _load_db(pool, corp_name)
        _compare(snap["records"], current)
        await pool.close()
        return

    # 1. Before 스냅샷
    before = await _load_db(pool, corp_name)
    snap_path = _save_snapshot(corp_name, before)
    print(f"Before 스냅샷 저장 → {snap_path} ({len(before)}건)")

    if args.save_only:
        for rec in before:
            _print_record("현재", rec)
        await pool.close()
        return

    # 2. 재추출
    import httpx
    from data.dart_extractor import extract_all_for_company
    from reports.summarizer import _ollama_is_alive

    async with httpx.AsyncClient() as http:
        alive = await _ollama_is_alive(http)
    if not alive:
        print("Ollama 미응답 — 재추출 불가")
        await pool.close()
        return

    print(f"\n재추출 중: {corp_name!r} (force={args.force}) ...")
    n = await extract_all_for_company(pool, corp_name, force=args.force)
    print(f"재추출 완료: {n}건")

    # 3. After 조회 + 비교
    after = await _load_db(pool, corp_name)
    print(f"\n=== Before vs After 비교 ===")
    _compare(before, after)

    await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
