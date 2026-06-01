"""
DART XML → Ollama 공시 추출 품질 즉석 테스트 스크립트.

실행:
    python tests/test_dart_extract.py
    python tests/test_dart_extract.py --model gemma3:4b
    python tests/test_dart_extract.py --xml "reports/dart/LG전자/..." --limit 8000
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import httpx

# reports 패키지를 직접 import — OLLAMA_BASE, _ollama_is_alive, _call_ollama_native 상속
sys.path.insert(0, str(Path(__file__).parent.parent))
from reports.summarizer import _call_ollama_native, _ollama_is_alive, OLLAMA_BASE

# ── 설정 ──────────────────────────────────────────────────────────────────────

KEYWORDS = [
    "AI", "인공지능", "온디바이스", "생성형",
    "데이터센터", "AIDC", "냉각", "칠러", "CDU",
    "로봇", "서비스로봇", "액추에이터",
    "매출", "영업이익", "목표",
]

DEFAULT_XML = (
    "reports/dart/LG전자/"
    "20260313000662_사업보고서 (2025.12)/"
    "20260313000662.xml"
)
PROMPT_TEMPLATE_PATH = "공시추출프롬프트.md"
DEFAULT_MODEL = "qwen3.5:9b"
CHAR_LIMIT = 12000

# ── XML 필터링 ────────────────────────────────────────────────────────────────

def grep_xml(xml_path: str, char_limit: int = CHAR_LIMIT) -> str:
    """XML/HTML 파일에서 키워드를 포함한 줄을 추출해 char_limit 자로 잘라 반환.

    DART XML은 종종 malformed이므로 ElementTree 파싱 대신 줄 단위 텍스트 스캔을 사용한다.
    XML 태그를 strip하고 키워드 포함 줄만 수집한다.
    """
    import re

    path = Path(xml_path)
    if not path.exists():
        raise FileNotFoundError(f"XML 파일 없음: {xml_path}")

    tag_re = re.compile(r"<[^>]+>")
    kw_lower = [kw.lower() for kw in KEYWORDS]

    collected: list[str] = []
    total_chars = 0

    with path.open(encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            # XML 태그 제거 후 공백 정리
            line = tag_re.sub("", raw_line).strip()
            if not line:
                continue
            lower = line.lower()
            if any(kw in lower for kw in kw_lower):
                fragment = line[:300]  # 줄 하나당 상한
                collected.append(fragment)
                total_chars += len(fragment)
                if total_chars >= char_limit:
                    break

    joined = "\n".join(collected)[:char_limit]
    return joined


# ── 프롬프트 조립 ─────────────────────────────────────────────────────────────

def build_prompt(
    grep_text: str,
    company: str = "LG전자",
    report_type: str = "사업보고서",
    period: str = "2025.12 (제24기)",
) -> str:
    """공시추출프롬프트.md 템플릿에 grep 결과를 삽입해 최종 프롬프트를 반환."""
    template_path = Path(PROMPT_TEMPLATE_PATH)
    if not template_path.exists():
        raise FileNotFoundError(f"프롬프트 템플릿 없음: {PROMPT_TEMPLATE_PATH}")

    template = template_path.read_text(encoding="utf-8")
    return (
        template
        .replace("[여기에 grep 결과 또는 관련 섹션 텍스트 붙여넣기]", grep_text)
        .replace("{회사명}", company)
        .replace("{사업보고서 / 분기보고서}", report_type)
        .replace("{기간}", period)
    )


# ── Ollama 호출 ───────────────────────────────────────────────────────────────

async def _run(model: str, prompt: str) -> str:
    async with httpx.AsyncClient() as http:
        if not await _ollama_is_alive(http):
            raise SystemExit(
                "Ollama 서버가 응답하지 않습니다.\n"
                "  → `ollama serve` 실행 후 재시도하세요.\n"
                f"  → OLLAMA_BASE={OLLAMA_BASE}"
            )
        return await _call_ollama_native(
            http=http,
            model=model,
            prompt=prompt,
            timeout=180.0,
            max_tokens=2000,
            enable_thinking=False,
        )


# ── 메인 ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="DART XML Ollama 추출 테스트")
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"Ollama 모델 (기본: {DEFAULT_MODEL})")
    parser.add_argument("--xml", default=DEFAULT_XML, help="XML 파일 경로")
    parser.add_argument("--limit", type=int, default=CHAR_LIMIT, help="grep 텍스트 상한 자수")
    args = parser.parse_args()

    print(f"[1/4] XML 필터링: {args.xml}")
    grep_text = grep_xml(args.xml, args.limit)
    print(f"      → {len(grep_text):,}자 추출 (상한 {args.limit:,}자)")

    print("[2/4] 프롬프트 조립")
    prompt = build_prompt(grep_text)
    print(f"      → 총 {len(prompt):,}자")

    print(f"[3/4] Ollama 호출 (model={args.model}, max_tokens=2000, timeout=180s)")
    result = asyncio.run(_run(args.model, prompt))

    print("[4/4] 추출 결과\n")
    # Windows 콘솔 인코딩 문제 우회: UTF-8로 강제 출력
    import io
    out = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    out.write("=" * 72 + "\n")
    out.write(result + "\n")
    out.write("=" * 72 + "\n")
    out.write(f"\n비교 기준: 공시추출샘플클로드.md\n")
    out.flush()


if __name__ == "__main__":
    main()
