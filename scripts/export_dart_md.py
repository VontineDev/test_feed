"""
export_dart_md.py — dart_extractions DB 레코드를 마크다운 파일로 출력

출력 경로: dart/YYYYMMDD_회사명.md
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import create_pool

OUTPUT_DIR = Path(__file__).parent.parent / "dart"


async def export() -> None:
    pool = await create_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT corp_name, rcept_no, report_type, period,
                   extraction_text, model, xml_chars, extracted_at
            FROM dart_extractions
            ORDER BY extracted_at DESC
            """
        )

    if not rows:
        print("dart_extractions 테이블에 데이터 없음")
        await pool.close()
        return

    OUTPUT_DIR.mkdir(exist_ok=True)

    written = 0
    for row in rows:
        date_str = row["extracted_at"].strftime("%Y%m%d") if row["extracted_at"] else "00000000"
        corp = row["corp_name"].replace("/", "_").replace(" ", "_")
        period = (row["period"] or "").replace(".", "").replace(" ", "")
        rtype_map = {
            "사업보고서": "사업",
            "사업보고서_연결사업보고서": "연결",
            "분기보고서": "분기",
            "반기보고서": "반기",
        }
        rtype = rtype_map.get(row["report_type"] or "", row["report_type"] or "")
        rtype = rtype.replace("/", "_").replace(" ", "_")
        parts = [p for p in [period, rtype] if p]
        suffix = ("_" + "_".join(parts)) if parts else ""
        filename = OUTPUT_DIR / f"{date_str}_{corp}{suffix}.md"

        content = f"""# {row['corp_name']} 공시 추출

| 항목 | 값 |
|------|-----|
| 접수번호 | {row['rcept_no']} |
| 보고서 유형 | {row['report_type'] or '-'} |
| 기간 | {row['period'] or '-'} |
| 모델 | {row['model'] or '-'} |
| XML 입력 크기 | {row['xml_chars']:,}자 |
| 추출 일시 | {row['extracted_at']} |

## 추출 결과

{row['extraction_text'] or '(내용 없음)'}
"""
        filename.write_text(content, encoding="utf-8")
        print(f"  저장: {filename.name}")
        written += 1

    await pool.close()
    print(f"\n완료: {written}건 → {OUTPUT_DIR}/")


if __name__ == "__main__":
    asyncio.run(export())
