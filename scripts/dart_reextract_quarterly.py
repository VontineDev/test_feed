"""
dart_reextract_quarterly.py — 분기/반기보고서만 선별 force 재추출

사업보고서를 건드리지 않고 분기/반기 디렉터리만 XBRL+LLM으로 재추출한다.
진행 상황은 logs/dart_quarterly_reextract_YYYYMMDD.log 에 기록된다.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

from datetime import date
log_file = LOG_DIR / f"dart_quarterly_reextract_{date.today().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


async def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    from core.db import create_pool
    import httpx
    from reports.summarizer import _ollama_is_alive
    from data.dart_extractor import (
        DART_DIR, DEFAULT_MODEL, PROMPT_TEMPLATE_PATH,
        _parse_report_dir, _pick_main_xml,
        extract_company, extract_xml, extract_structured,
    )

    pool = await create_pool()

    async with httpx.AsyncClient() as _probe:
        if not await _ollama_is_alive(_probe):
            logger.error("Ollama 미응답 — 중단")
            await pool.close()
            return

    # 분기/반기 디렉터리 수집
    targets: list[tuple[str, Path, str, str, str]] = []  # (corp_name, xml, rcept_no, rtype, period)
    for co_dir in sorted(DART_DIR.iterdir()):
        if not co_dir.is_dir():
            continue
        corp_name = co_dir.name
        for rd in sorted(co_dir.iterdir()):
            if not rd.is_dir():
                continue
            if "분기" not in rd.name and "반기" not in rd.name:
                continue
            rcept_no, report_type, period = _parse_report_dir(rd.name)
            xml_path = _pick_main_xml(rd, rcept_no)
            if xml_path is None:
                continue
            targets.append((corp_name, xml_path, rcept_no, report_type, period))

    total = len(targets)
    logger.info("재추출 대상: %d건 (분기/반기보고서만)", total)

    done = skipped = failed = 0

    async with httpx.AsyncClient() as http:
        for idx, (corp_name, xml_path, rcept_no, report_type, period) in enumerate(targets, 1):
            logger.info("[%d/%d] %s / %s (%s)", idx, total, corp_name, report_type, period)

            extraction_text = ""
            xml_chars = 0

            if PROMPT_TEMPLATE_PATH.exists():
                try:
                    extraction_text, xml_chars = await extract_company(
                        http=http,
                        corp_name=corp_name,
                        xml_path=xml_path,
                        model=DEFAULT_MODEL,
                        report_type=report_type,
                        period=period,
                    )
                except Exception as e:
                    logger.warning("서술 추출 오류 %s/%s: %s", corp_name, rcept_no, e)
                    failed += 1
                    continue
            else:
                xml_chars = len(extract_xml(xml_path))

            try:
                structured = await extract_structured(
                    http, xml_path, DEFAULT_MODEL,
                    report_type=report_type, period=period,
                )
            except Exception as e:
                logger.warning("구조화 추출 오류 %s/%s: %s", corp_name, rcept_no, e)
                failed += 1
                continue

            def _to_jsonb(v):
                return json.dumps(v, ensure_ascii=False) if v is not None else None

            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO dart_extractions
                        (corp_name, rcept_no, report_type, period,
                         extraction_text, model, xml_chars, extracted_at,
                         segments_json, revenue_json, competitors_json)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,NOW(),$8,$9,$10)
                    ON CONFLICT (corp_name, rcept_no) DO UPDATE SET
                        report_type      = EXCLUDED.report_type,
                        period           = EXCLUDED.period,
                        extraction_text  = EXCLUDED.extraction_text,
                        model            = EXCLUDED.model,
                        xml_chars        = EXCLUDED.xml_chars,
                        extracted_at     = NOW(),
                        segments_json    = COALESCE(EXCLUDED.segments_json,    dart_extractions.segments_json),
                        revenue_json     = COALESCE(EXCLUDED.revenue_json,     dart_extractions.revenue_json),
                        competitors_json = COALESCE(EXCLUDED.competitors_json, dart_extractions.competitors_json)
                    """,
                    corp_name, rcept_no, report_type, period,
                    extraction_text, DEFAULT_MODEL, xml_chars,
                    _to_jsonb(structured["segments_json"]),
                    _to_jsonb(structured["revenue_json"]),
                    _to_jsonb(structured["competitors_json"]),
                )
            done += 1
            logger.info("  저장 완료 (%d/%d) | 누계: done=%d skip=%d fail=%d",
                        idx, total, done, skipped, failed)

    logger.info("=== 완료: 총 %d건 | 저장 %d | 스킵 %d | 실패 %d ===",
                total, done, skipped, failed)
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
