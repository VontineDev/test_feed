"""
dart_extractor.py — 로컬 DART XML → Ollama 투자 판단 추출 모듈

reports/dart/{회사명}/{rcept_no}_{보고서타입} ({period})/ 의 로컬 XML을 읽어
키워드 grep + 헤더 앵커 2-트랙으로 컨텍스트를 추출하고 Ollama로 서술 텍스트를 생성한다.
결과는 dart_extractions 테이블에 저장된다.

실행 (단독 테스트):
    python data/dart_extractor.py --company LG전자
    python data/dart_extractor.py --all
    python data/dart_extractor.py --all --force
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import sys
from pathlib import Path
from typing import Optional

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))
from reports.summarizer import _call_ollama_native, _ollama_is_alive, OLLAMA_BASE

logger = logging.getLogger(__name__)

# ── 상수 ──────────────────────────────────────────────────────────────────────

KEYWORDS = [
    "AI", "인공지능", "온디바이스", "생성형",
    "데이터센터", "AIDC", "냉각", "칠러", "CDU",
    "로봇", "서비스로봇", "액추에이터",
    "매출", "영업이익", "목표",
]

ANCHORS = [
    # 실제 DART 사업보고서에 나타나는 재무/실적 표 헤더
    "부문별 매출실적",
    "사업부문별 매출",        # LG전자: "(1) 사업부문별 매출, 영업이익"
    "사업부문별 요약",        # LG전자: "바. 사업부문별 요약 재무현황"
    "사업부문별 실적",
    "매출실적",
    "매출액합계",
    "사업부문의 현황",
    "매출 및 수익구조",
    "세그먼트별",
    "사업별 현황",
    "주요 재무현황",
    "영업실적",
]

ANCHOR_LINES = 50       # 앵커 헤더 발견 시 이하 수집 행 수
ANCHOR_BUDGET = 8_000   # anchor_xml에 배정된 우선 예산 (자)
CHAR_LIMIT = 20_000     # grep + anchor 합산 상한 (자)

DART_DIR = Path(__file__).parent.parent / "reports" / "dart"
PROMPT_TEMPLATE_PATH = Path(__file__).parent.parent / "공시추출프롬프트.md"
DEFAULT_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3.5:9b")

# ── XML 필터링 ─────────────────────────────────────────────────────────────────

_TAG_RE = re.compile(r"<[^>]+>")


def _strip_tags(line: str) -> str:
    return _TAG_RE.sub("", line).strip()


def grep_xml(xml_path: str | Path, char_limit: int) -> str:
    """키워드 포함 줄 추출 (태그 strip + 키워드 매칭)."""
    path = Path(xml_path)
    kw_lower = [kw.lower() for kw in KEYWORDS]
    collected: list[str] = []
    total = 0
    with path.open(encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = _strip_tags(raw)
            if not line:
                continue
            if any(kw in line.lower() for kw in kw_lower):
                frag = line[:300]
                collected.append(frag)
                total += len(frag)
                if total >= char_limit:
                    break
    return "\n".join(collected)[:char_limit]


def anchor_xml(
    xml_path: str | Path,
    anchors: list[str] = ANCHORS,
    lines_per_anchor: int = ANCHOR_LINES,
) -> str:
    """헤더 앵커 발견 시 이하 lines_per_anchor 행 수집 (태그 strip)."""
    path = Path(xml_path)
    anchor_lower = [a.lower() for a in anchors]
    result_blocks: list[str] = []
    lines: list[str] = []
    with path.open(encoding="utf-8", errors="ignore") as f:
        for raw in f:
            stripped = _strip_tags(raw)
            if stripped:
                lines.append(stripped)

    i = 0
    while i < len(lines):
        line_lower = lines[i].lower()
        if any(anc in line_lower for anc in anchor_lower):
            block = lines[i : i + 1 + lines_per_anchor]
            result_blocks.append("\n".join(block))
            i += 1 + lines_per_anchor
        else:
            i += 1

    return "\n\n".join(result_blocks)


def extract_xml(xml_path: str | Path, char_limit: int = CHAR_LIMIT) -> str:
    """anchor 우선 + grep 나머지 조합.

    할당 전략:
    - anchor_xml 먼저 최대 ANCHOR_BUDGET=8,000자 수집
    - 남은 예산으로 grep_xml 수집
    - anchor_text + newline + grep_text 반환 (합산 char_limit 이하)

    리포트 디렉터리에 복수 XML 있을 경우 {rcept_no}.xml (suffix 없는 메인) 우선.
    이 함수는 이미 선택된 단일 xml_path를 받는다.
    """
    path = Path(xml_path)
    anchor_text = anchor_xml(path)[:ANCHOR_BUDGET]
    remaining = max(0, char_limit - len(anchor_text) - 1)
    grep_text = grep_xml(path, remaining) if remaining > 0 else ""
    combined = (anchor_text + "\n" + grep_text).strip()
    return combined[:char_limit]


# ── 디렉터리 파싱 ──────────────────────────────────────────────────────────────

_PERIOD_RE = re.compile(r"\(([^)]+)\)")
_AMEND_PREFIX = re.compile(r"^_기재정정_")


def _parse_report_dir(dir_name: str) -> tuple[str, str, str]:
    """디렉터리명에서 (rcept_no, report_type, period) 추출.

    형식 예:
      20260313000662_사업보고서 (2025.12)
      20260324000813__기재정정_반기보고서 (2024.06)
    """
    # rcept_no: 첫 _ 앞
    rcept_no = dir_name.split("_")[0]

    # period: 마지막 () 안
    m = _PERIOD_RE.search(dir_name)
    period = m.group(1) if m else "unknown"

    # report_type: rcept_no + _ 이후, period 괄호 제거, 기재정정_ prefix 제거
    after_rcept = dir_name[len(rcept_no):].lstrip("_")
    after_rcept = _AMEND_PREFIX.sub("", after_rcept)
    report_type = _PERIOD_RE.sub("", after_rcept).strip().rstrip("_").strip()

    return rcept_no, report_type, period


def _pick_main_xml(report_dir: Path, rcept_no: str) -> Optional[Path]:
    """리포트 디렉터리에서 메인 XML 선택.

    {rcept_no}.xml (suffix 없는 메인)을 우선 반환.
    없으면 가장 짧은 이름의 .xml 파일 반환 (suffix 없는 것 근사).
    """
    main = report_dir / f"{rcept_no}.xml"
    if main.exists():
        return main
    xmls = sorted(report_dir.glob("*.xml"), key=lambda p: len(p.name))
    return xmls[0] if xmls else None


# ── 프롬프트 조립 ─────────────────────────────────────────────────────────────

def build_prompt(
    context_text: str,
    company: str,
    report_type: str = "사업보고서",
    period: str = "unknown",
) -> str:
    """공시추출프롬프트.md 템플릿에 컨텍스트 삽입."""
    if not PROMPT_TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"프롬프트 템플릿 없음: {PROMPT_TEMPLATE_PATH}")
    template = PROMPT_TEMPLATE_PATH.read_text(encoding="utf-8")
    return (
        template
        .replace("[여기에 grep 결과 또는 관련 섹션 텍스트 붙여넣기]", context_text)
        .replace("{회사명}", company)
        .replace("{사업보고서 / 분기보고서}", report_type)
        .replace("{기간}", period)
    )


# ── Ollama 호출 ───────────────────────────────────────────────────────────────

async def extract_company(
    http: httpx.AsyncClient,
    corp_name: str,
    xml_path: str | Path,
    model: str,
    report_type: str,
    period: str,
) -> tuple[str, int]:
    """단일 기업 XML → Ollama 호출 → (추출 텍스트, xml_chars) 반환."""
    context = extract_xml(xml_path)
    xml_chars = len(context)
    prompt = build_prompt(context, corp_name, report_type, period)
    result = await _call_ollama_native(
        http=http,
        model=model,
        prompt=prompt,
        timeout=180.0,
        max_tokens=2000,
        enable_thinking=False,
    )
    return result, xml_chars


# ── 전체 처리 ─────────────────────────────────────────────────────────────────

async def extract_all(
    pool,
    model: str = DEFAULT_MODEL,
    force: bool = False,
    dart_dir: Path = DART_DIR,
) -> int:
    """reports/dart/ 하위 모든 기업 처리 → dart_extractions 저장.

    - 단일 httpx.AsyncClient를 생성해 모든 extract_company 호출에 공유
    - _ollama_is_alive 실패 시 전체 중단
    - 개별 기업 오류는 catch+log 후 다음 기업 계속 (전체 중단 없음)
    - force=False: 이미 저장된 (corp_name, rcept_no) 건너뜀
    - 디렉터리 순서: 파일시스템 알파벳 순
    - 기재정정 보고서: rcept_no가 달라 별도 행 저장 (ON CONFLICT DO UPDATE)
    - 예상 실행 시간: 10~60분 (기업당 Ollama 응답 시간 의존)

    반환: 저장/갱신 건수.
    """
    if not dart_dir.exists():
        logger.warning("[dart-extractor] dart_dir 없음: %s", dart_dir)
        return 0

    total = 0
    async with httpx.AsyncClient() as http:
        if not await _ollama_is_alive(http):
            logger.error(
                "[dart-extractor] Ollama 서버 미응답 — 전체 중단. "
                "OLLAMA_BASE=%s", OLLAMA_BASE
            )
            return 0

        # 회사 디렉터리: 알파벳 순
        company_dirs = sorted(d for d in dart_dir.iterdir() if d.is_dir())

        for company_dir in company_dirs:
            corp_name = company_dir.name

            # 리포트 디렉터리: 사업보고서, 분기보고서 등
            report_dirs = sorted(d for d in company_dir.iterdir() if d.is_dir())

            for report_dir in report_dirs:
                dir_name = report_dir.name
                rcept_no, report_type, period = _parse_report_dir(dir_name)

                # 이미 처리됐는지 확인 (force=False)
                if not force:
                    async with pool.acquire() as conn:
                        done = await conn.fetchval(
                            "SELECT COUNT(*) FROM dart_extractions "
                            "WHERE corp_name=$1 AND rcept_no=$2",
                            corp_name, rcept_no,
                        )
                    if done:
                        logger.debug(
                            "[dart-extractor] 건너뜀 (기존): %s / %s",
                            corp_name, rcept_no,
                        )
                        continue

                xml_path = _pick_main_xml(report_dir, rcept_no)
                if xml_path is None:
                    logger.info(
                        "[dart-extractor] XML 없음: %s / %s", corp_name, dir_name
                    )
                    continue

                logger.info(
                    "[dart-extractor] 처리 중: %s / %s (%s)",
                    corp_name, report_type, period,
                )

                try:
                    extraction_text, xml_chars = await extract_company(
                        http=http,
                        corp_name=corp_name,
                        xml_path=xml_path,
                        model=model,
                        report_type=report_type,
                        period=period,
                    )
                except Exception as e:
                    logger.warning(
                        "[dart-extractor] 오류 (건너뜀): %s / %s — %s",
                        corp_name, rcept_no, e,
                    )
                    continue

                async with pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO dart_extractions
                            (corp_name, rcept_no, report_type, period,
                             extraction_text, model, xml_chars, extracted_at)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
                        ON CONFLICT (corp_name, rcept_no) DO UPDATE SET
                            report_type     = EXCLUDED.report_type,
                            period          = EXCLUDED.period,
                            extraction_text = EXCLUDED.extraction_text,
                            model           = EXCLUDED.model,
                            xml_chars       = EXCLUDED.xml_chars,
                            extracted_at    = NOW()
                        """,
                        corp_name, rcept_no, report_type, period,
                        extraction_text, model, xml_chars,
                    )
                total += 1
                logger.info(
                    "[dart-extractor] 저장 완료: %s / %s (%d자)",
                    corp_name, rcept_no, xml_chars,
                )

    return total


# ── 단독 실행 ─────────────────────────────────────────────────────────────────

def _main_cli() -> None:
    parser = argparse.ArgumentParser(description="DART XML Ollama 추출")
    parser.add_argument("--company", help="특정 회사명 (디렉터리명과 일치)")
    parser.add_argument("--all", action="store_true", help="전체 기업 처리")
    parser.add_argument("--force", action="store_true", help="기존 결과 덮어쓰기")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.company:
        # 단일 기업 콘솔 출력 (DB 없음)
        company_dir = DART_DIR / args.company
        if not company_dir.exists():
            print(f"디렉터리 없음: {company_dir}")
            sys.exit(1)
        report_dirs = sorted(d for d in company_dir.iterdir() if d.is_dir())
        if not report_dirs:
            print(f"리포트 디렉터리 없음: {company_dir}")
            sys.exit(1)
        report_dir = report_dirs[-1]  # 가장 최근 (알파벳 마지막)
        dir_name = report_dir.name
        rcept_no, report_type, period = _parse_report_dir(dir_name)
        xml_path = _pick_main_xml(report_dir, rcept_no)
        if xml_path is None:
            print(f"XML 없음: {report_dir}")
            sys.exit(1)

        print(f"[1/3] XML 추출: {xml_path}")
        context = extract_xml(xml_path)
        print(f"      anchor+grep: {len(context):,}자")

        print("[2/3] Ollama 호출")

        async def _run():
            async with httpx.AsyncClient() as http:
                if not await _ollama_is_alive(http):
                    raise SystemExit(
                        f"Ollama 서버 미응답 — `ollama serve` 실행 후 재시도. "
                        f"OLLAMA_BASE={OLLAMA_BASE}"
                    )
                return await extract_company(http, args.company, xml_path, args.model, report_type, period)

        text, xml_chars = asyncio.run(_run())
        print(f"[3/3] 결과 ({xml_chars:,}자 컨텍스트)\n")
        import io
        out = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        out.write("=" * 72 + "\n")
        out.write(text + "\n")
        out.write("=" * 72 + "\n")
        out.flush()

    elif args.all:
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from core.db import create_pool

        async def _run_all():
            pool = await create_pool()
            n = await extract_all(pool, model=args.model, force=args.force)
            print(f"완료: {n}건 저장")

        asyncio.run(_run_all())
    else:
        parser.print_help()


if __name__ == "__main__":
    _main_cli()
