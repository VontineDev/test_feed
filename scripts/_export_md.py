"""DART 추출 결과를 기업별 Markdown 파일로 저장."""
import asyncio, sys, json
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()
from core.db import create_pool

# 대표 섹터별 10개 기업
TARGET_CORPS = [
    "삼성전자",       # 반도체/전자
    "SK하이닉스",     # 반도체
    "현대자동차",     # 자동차
    "기아",          # 자동차
    "NAVER",         # 플랫폼
    "카카오",        # 플랫폼
    "LG전자",        # 전자
    "셀트리온",      # 바이오
    "에코프로",      # 2차전지소재
    "삼성바이오로직스", # 바이오의약
]

OUT_DIR = Path("reports/dart_md")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def fmt_num(val) -> str:
    """숫자를 억원 단위로 포맷. 단위 불명확 시 그대로."""
    if val is None:
        return "-"
    try:
        n = float(str(val).replace(",", ""))
        if abs(n) >= 1_000_000:
            return f"{n/1_000_000:,.1f}조원"
        elif abs(n) >= 10_000:
            return f"{n/10_000:,.0f}억원"
        else:
            return f"{n:,.0f}"
    except (ValueError, TypeError):
        return str(val)


def render_revenue(rev: dict | None) -> str:
    if not rev:
        return "_데이터 없음_\n"
    periods = rev.get("periods", [])
    lines = []

    # 연결 매출/영업이익
    consolidated = rev.get("consolidated", {})
    revenues = consolidated.get("revenue", [])
    op_profits = consolidated.get("op_profit", [])

    if revenues or op_profits:
        lines.append("| 구분 | " + " | ".join(str(p) for p in periods) + " |")
        lines.append("|------|" + "|".join(["------"] * len(periods)) + "|")
        if revenues:
            lines.append("| 연결 매출액 | " + " | ".join(fmt_num(v) for v in revenues) + " |")
        if op_profits:
            lines.append("| 연결 영업이익 | " + " | ".join(fmt_num(v) for v in op_profits) + " |")
        lines.append("")

    # 부문별 매출
    segments = rev.get("segments", [])
    if segments:
        lines.append("**부문별 매출**\n")
        lines.append("| 부문 | " + " | ".join(str(p) for p in periods) + " |")
        lines.append("|------|" + "|".join(["------"] * len(periods)) + "|")
        for seg in segments:
            name = seg.get("name", "-")
            revs = seg.get("revenues", [])
            lines.append("| " + name + " | " + " | ".join(fmt_num(v) for v in revs) + " |")
        lines.append("")

    return "\n".join(lines) + "\n" if lines else "_데이터 없음_\n"


def render_segments(segs: list | None) -> str:
    if not segs:
        return "_데이터 없음_\n"
    lines = ["| 부문명 | 주요 제품 | 매출 비중 | 비고 |",
             "|--------|----------|---------|------|"]
    for s in segs:
        name = s.get("segment_name", "-")
        products = ", ".join(s.get("products", [])) or "-"
        share = f"{s.get('revenue_share_pct', '-')}%"
        note = s.get("note") or "-"
        lines.append(f"| {name} | {products} | {share} | {note} |")
    return "\n".join(lines) + "\n"


def render_competitors(comps: list | None) -> str:
    if not comps:
        return "_데이터 없음_\n"
    lines = ["| 경쟁사 | 경쟁 유형 | 경쟁 부문 | 신뢰도 |",
             "|--------|---------|---------|------|"]
    for c in comps:
        name = c.get("name", "-")
        rtype = c.get("relation_type", "-")
        seg = c.get("competing_segment", "-")
        conf = c.get("confidence", "-")
        lines.append(f"| {name} | {rtype} | {seg} | {conf} |")
    return "\n".join(lines) + "\n"


def build_md(corp_name: str, rows: list) -> str:
    md = [f"# {corp_name} DART 공시 분석\n"]
    md.append(f"> 추출 보고서: {len(rows)}건 | 생성: 3-Pass Ollama 구조화 추출\n")
    md.append("---\n")

    # 최신 보고서 순서
    for row in sorted(rows, key=lambda r: r["period"], reverse=True):
        period = row["period"]
        rtype = row["report_type"]
        extracted_at = row["extracted_at"].strftime("%Y-%m-%d") if row["extracted_at"] else "-"
        rev = json.loads(row["revenue_json"]) if row["revenue_json"] else None
        seg = json.loads(row["segments_json"]) if row["segments_json"] else None
        comp = json.loads(row["competitors_json"]) if row["competitors_json"] else None

        md.append(f"## {period} {rtype}\n")
        md.append(f"_추출일: {extracted_at}_\n")

        md.append("### 매출 실적\n")
        md.append(render_revenue(rev))

        md.append("### 사업부문 현황\n")
        md.append(render_segments(seg))

        md.append("### 경쟁사\n")
        md.append(render_competitors(comp))

        md.append("---\n")

    return "\n".join(md)


async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT corp_name, period, report_type, extracted_at,
                   revenue_json, segments_json, competitors_json
            FROM dart_extractions
            WHERE corp_name = ANY($1::text[])
            ORDER BY corp_name, period DESC
        """, TARGET_CORPS)

    by_corp: dict[str, list] = {}
    for r in rows:
        by_corp.setdefault(r["corp_name"], []).append(r)

    for corp in TARGET_CORPS:
        corp_rows = by_corp.get(corp, [])
        if not corp_rows:
            print(f"  [스킵] 데이터 없음: {corp}")
            continue
        md = build_md(corp, corp_rows)
        fname = corp.replace("/", "_").replace(" ", "_") + ".md"
        out = OUT_DIR / fname
        out.write_text(md, encoding="utf-8")
        print(f"  저장: {out} ({len(corp_rows)}건)")

    await pool.close()
    print(f"\n완료: {OUT_DIR}/")

asyncio.run(main())
