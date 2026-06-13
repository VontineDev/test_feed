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
import json

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

# ── XBRL TE태그 규칙 기반 분기 추출 ─────────────────────────────────────────

_XBRL_TE_RE = re.compile(
    r'<TE\s[^>]*ACODE="([^"]+)"[^>]*ACONTEXT="([^"]+)"[^>]*>(.+?)</TE>',
    re.IGNORECASE | re.DOTALL,
)
_XBRL_INNER_TAG_RE = re.compile(r'<[^>]+>')
_XBRL_NEGATED_RE = re.compile(r'ANEGATED="Y"', re.IGNORECASE)
_XBRL_ADECIMAL_RE = re.compile(r'ADECIMAL="([^"]*)"', re.IGNORECASE)

# ACONTEXT 패턴: CFY...FQQ = 당기 분기, PFY...FQQ = 전기 분기
_CTX_CUR_Q = re.compile(r'^CFY\d+dFQQ', re.IGNORECASE)
_CTX_PRI_Q = re.compile(r'^PFY\d+dFQQ', re.IGNORECASE)

_XBRL_REV_CODES = {
    "ifrs-full_Revenue",
    "ifrs-full_OperatingRevenue",
    "ifrs-full_RevenueFromContractsWithCustomers",
    "dart_OperatingRevenue",
}
_XBRL_OP_CODES = {
    "dart_OperatingIncomeLoss",
    "ifrs-full_OperatingProfit",
    "ifrs-full_ProfitLossFromOperatingActivities",
}


def _parse_xbrl_value(raw: str, negated: bool, adecimal: int = 0) -> int | None:
    """XBRL TE 태그 내 금액 문자열 → 정수 변환.

    adecimal: ADECIMAL 속성값 (음수이면 해당 절댓값만큼 10의 거듭제곱 곱함)
      ADECIMAL="-6" → actual = raw × 10^6 (백만 단위로 표기된 원화)
      ADECIMAL="0"  → actual = raw (이미 원 단위)
    """
    s = raw.strip().replace(",", "").replace("\xa0", "").replace(" ", "")
    negative = s.startswith("(") and s.endswith(")")
    s = s.strip("()")
    try:
        v = float(s)
        if adecimal != 0:
            v = v * (10 ** (-adecimal))
        if negative or negated:
            v = -v
        return int(round(v))
    except ValueError:
        return None


def extract_xbrl_quarterly(xml_path: str | Path) -> dict | None:
    """XBRL <TE> 태그에서 분기 손익계산서 값 직접 추출.

    성공 시 {"revenue": [cur_q, pri_q], "op_profit": [cur_q, pri_q], "unit": "원"} 반환.
    XBRL 태그가 없거나 값이 없으면 None 반환.
    """
    path = Path(xml_path)
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return None

    cur_rev = pri_rev = cur_op = pri_op = None

    for m in _XBRL_TE_RE.finditer(text):
        acode   = m.group(1)
        actx    = m.group(2)
        raw_val = _XBRL_INNER_TAG_RE.sub("", m.group(3))
        tag_str = m.group(0)
        negated = bool(_XBRL_NEGATED_RE.search(tag_str))
        adec_m  = _XBRL_ADECIMAL_RE.search(tag_str)
        _adec_s  = adec_m.group(1) if adec_m else ""
        try:
            adecimal = int(_adec_s) if _adec_s and _adec_s.upper() != "INF" else 0
        except ValueError:
            adecimal = 0

        val = _parse_xbrl_value(raw_val, negated, adecimal)
        if val is None:
            continue

        if _CTX_CUR_Q.match(actx):
            if acode in _XBRL_REV_CODES and cur_rev is None:
                cur_rev = val
            elif acode in _XBRL_OP_CODES and cur_op is None:
                cur_op = val
        elif _CTX_PRI_Q.match(actx):
            if acode in _XBRL_REV_CODES and pri_rev is None:
                pri_rev = val
            elif acode in _XBRL_OP_CODES and pri_op is None:
                pri_op = val

    if cur_rev is None and pri_rev is None:
        return None

    return {
        "revenue":   [cur_rev, pri_rev],
        "op_profit": [cur_op,  pri_op],
        "unit":      "원",
    }


# ── 금액 단위 감지 ────────────────────────────────────────────────────────────
_UNIT_RE = re.compile(r"단위\s*[:：]\s*(원|천원|백만원|억원|조원)")

def _detect_unit(text: str) -> str:
    """rev_text 에서 '(단위 : XXX)' 패턴으로 금액 단위를 감지."""
    if not text:
        return "unknown"
    m = _UNIT_RE.search(text)
    return m.group(1) if m else "unknown"

# ── 3-Pass 추출 앵커 ───────────────────────────────────────────────────────────

_SEGMENT_ANCHORS = [
    "주요 제품 및 서비스",
    "사업부문별 주요 제품",
    "주요 제품",
    "제품 및 서비스 현황",
    "주요제품",
]

_REVENUE_ANCHORS = [
    # ── Priority 0: 손익계산서 (분기/연간 공통 — 올바른 비교 기간 포함) ─────
    "연결 포괄손익계산서",          # 분기보고서 XBRL 연결손익계산서 (당기분기 vs 전기분기)
    "포괄손익계산서",               # 변형 헤더
    "연결손익계산서",
    "손익계산서",
    # ── Priority 1: 영업이익 포함 부문별 요약 ──────────────────────────────
    "사업부문별 요약 재무 현황",    # 삼성전자: 라. 사업부문별 요약 재무 현황
    "사업부문별 요약 재무현황",     # 풍산: 가. 사업부문별 요약 재무현황 (공백 없는 변형)
    "사업부문별 요약",              # LG전자 등 유사 헤더
    "요약 재무 현황",
    "사업부문별 실적",
    # ── Priority 2: 연결 요약 재무정보 (지주사·에코프로 등) ─────────────────
    "요약연결재무정보",             # III. 재무 → 가. 요약연결재무정보 (지주사 consolidated P&L)
    "요약재무정보",
    "주요재무정보",
    # ── Priority 3: 매출 전용 섹션 ──────────────────────────────────────────
    "부문별 매출실적",
    "사업부문별 매출",              # 삼표시멘트: [사업부문별 매출액 및 영업이익]
    "매출실적",
    "세그먼트별 매출",
    "사업별 매출",
    "매출현황",
]

# ── 3-Pass 프롬프트 ────────────────────────────────────────────────────────────

_JSON_ONLY = (
    "반드시 순수 JSON만 출력하세요. 설명이나 마크다운 코드블록 없이.\n"
    "추출 항목이 없으면 빈 배열 [] 를 반환하세요.\n\n"
)

SEGMENT_EXTRACTOR_PROMPT = _JSON_ONLY + """\
아래는 DART 사업보고서의 '주요 제품 및 서비스' 섹션입니다.
각 사업부문명, 주요 제품 목록, 매출 비중을 추출해 JSON 배열로만 반환하세요.

[
  {
    "segment_name": "부문명",
    "products": ["제품1", "제품2"],
    "revenue_share_pct": 43.89,
    "note": "특이사항 또는 null"
  }
]

섹션 원문:
"""

def _derive_period_labels(report_type: str, period: str) -> tuple[str, str]:
    """period 문자열("2026.03", "2025.12")에서 당기/전기 레이블 도출."""
    try:
        year_str, month_str = period.split(".")
        year = int(year_str)
        month = int(month_str)
    except (ValueError, AttributeError):
        return ("당기", "전기")

    if "분기" in report_type:
        q = (month - 1) // 3 + 1
        return (f"{year}.{q}Q", f"{year - 1}.{q}Q")
    elif "반기" in report_type:
        h = 1 if month <= 6 else 2
        return (f"{year}.{h}H", f"{year - 1}.{h}H")
    else:
        return (str(year), str(year - 1))


def _build_revenue_prompt(report_type: str = "사업보고서", period: str = "unknown") -> str:
    """보고서 유형에 맞는 매출 분석 프롬프트 생성."""
    cur_lbl, prior_lbl = _derive_period_labels(report_type, period)

    if "분기" in report_type:
        period_rule = (
            f"분기보고서입니다 (당기: {cur_lbl}, 전기: {prior_lbl}).\n"
            f"손익계산서 표는 4열 구조입니다: [당기3개월, 당기누적, 전기3개월, 전기누적].\n"
            f"반드시 1열(당기3개월={cur_lbl})과 3열(전기3개월={prior_lbl}) 값을 추출하세요.\n"
            f"2열(당기누적)은 1열과 동일하므로 무시하세요. 4열(전기누적)도 무시하세요.\n"
            f"각 항목마다 4개 숫자가 연속으로 나오면 1번째와 3번째를 사용하세요.\n"
            f'periods는 반드시 ["{cur_lbl}", "{prior_lbl}"]로 고정하세요.'
        )
    elif "반기" in report_type:
        period_rule = (
            f"반기보고서입니다 (당기: {cur_lbl}, 전기: {prior_lbl}).\n"
            f"당기 반기와 전기 동기간(반기)만 추출하세요.\n"
            f'periods는 반드시 ["{cur_lbl}", "{prior_lbl}"]로 고정하세요.'
        )
    else:
        period_rule = (
            f"사업보고서입니다 (당기: {cur_lbl}, 전기: {prior_lbl}).\n"
            f"연간(당기 vs 전기) 수치를 추출하세요.\n"
            f'periods는 반드시 ["{cur_lbl}", "{prior_lbl}"]로 고정하세요.'
        )

    return (
        "반드시 순수 JSON만 출력하세요. 설명이나 마크다운 코드블록 없이.\n"
        '매출 데이터가 없으면 {"periods":[],"segments":[],"consolidated":{"revenue":[],"op_profit":[]}} 를 반환하세요.\n\n'
        f"[보고서 유형: {report_type} | 기간: {period}]\n"
        f"{period_rule}\n\n"
        f"아래는 DART {report_type}의 매출실적 섹션입니다.\n"
        "기간별 부문별 매출을 추출해 JSON으로 반환하세요. 계산값은 computed:true로 표기.\n\n"
        '참고: "총부문수익", "외부고객으로부터의 수익", "영업수익", "순이자수익", "이자수익", "수수료수익", "총영업이익", "순영업이익", "매출액(영업수익)"도 매출액(revenue)으로 처리하세요.\n\n'
        "{\n"
        f'  "periods": ["{cur_lbl}", "{prior_lbl}"],\n'
        '  "segments": [\n'
        '    {\n'
        '      "name": "부문명",\n'
        '      "revenues": [금액, 금액],\n'
        '      "yoy_growth": [null, 0.0]\n'
        '    }\n'
        '  ],\n'
        '  "consolidated": {\n'
        '    "revenue": [금액, 금액],\n'
        '    "op_profit": [금액, 금액]\n'
        '  }\n'
        '}\n\n'
        "섹션 원문:\n"
    )

COMPETITOR_EXTRACTOR_PROMPT = _JSON_ONLY + """\
아래는 DART 사업보고서에서 경쟁 관련 내용입니다.
명시적으로 경쟁사로 언급된 회사만 추출하세요. 없으면 [] 반환.

[
  {
    "name": "회사명",
    "relation_type": "direct|indirect|global",
    "competing_segment": "경쟁 사업부문",
    "source_quote": "원문 인용 (30자 이내)",
    "confidence": "high|medium|low"
  }
]

본문:
"""

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


# ── 3-Pass 헬퍼 ───────────────────────────────────────────────────────────────

def _anchor_best(
    xml_path: str | Path,
    anchors: list[str],
    lines_per_anchor: int = 200,
    require_keyword: str | None = None,
) -> str:
    """우선순위 앵커 탐색: 앵커 목록 순서대로 시도, 최적 블록 단일 반환.

    탐색 순서:
      Pass 1: 헤더(줄 길이 ≤ 80) + require_keyword 포함
      Pass 2: 본문 포함  + require_keyword 포함  (keyword 있는 경우만)
      Pass 3: 헤더        (keyword 없이 fallback)
      Pass 4: 길이 무관   (최종 fallback)

    require_keyword: 블록에 이 키워드가 없으면 다음 앵커로 건너뜀.
    → 단일 블록만 반환하므로 max_chars 예산 경쟁 없음.
    """
    path = Path(xml_path)
    lines: list[str] = []
    with path.open(encoding="utf-8", errors="ignore") as f:
        for raw in f:
            s = _TAG_RE.sub("", raw).strip()
            if s:
                lines.append(s)

    kw = require_keyword.lower() if require_keyword else None

    def _block(i: int) -> str:
        return "\n".join(lines[i: i + 1 + lines_per_anchor])

    # DART 보고서의 처음 ~200줄은 대부분 목차. keyword 탐색 시 목차 히트를
    # 낮은 우선순위로 처리해 실제 섹션을 우선 선택한다.
    _TOC_CUTOFF = 200

    if kw:
        # Pass 1: 헤더 + keyword, 목차 외 위치 우선
        for anchor in anchors:
            al = anchor.lower()
            for i, line in enumerate(lines):
                if i <= _TOC_CUTOFF:
                    continue
                if al in line.lower() and len(line) <= 80:
                    b = _block(i)
                    if kw in b.lower():
                        return b
        # Pass 2: 본문 + keyword, 목차 외 위치 우선
        for anchor in anchors:
            al = anchor.lower()
            for i, line in enumerate(lines):
                if i <= _TOC_CUTOFF:
                    continue
                if al in line.lower():
                    b = _block(i)
                    if kw in b.lower():
                        return b
        # Pass 3: 헤더 + keyword (목차 위치 포함 fallback)
        for anchor in anchors:
            al = anchor.lower()
            for i, line in enumerate(lines):
                if al in line.lower() and len(line) <= 80:
                    b = _block(i)
                    if kw in b.lower():
                        return b
        # Pass 4: 본문 + keyword (목차 위치 포함 fallback)
        for anchor in anchors:
            al = anchor.lower()
            for i, line in enumerate(lines):
                if al in line.lower():
                    b = _block(i)
                    if kw in b.lower():
                        return b

    # Pass 3: 헤더 (keyword 무관 fallback)
    for anchor in anchors:
        al = anchor.lower()
        for i, line in enumerate(lines):
            if al in line.lower() and len(line) <= 80:
                return _block(i)

    # Pass 4: 길이 무관 (최종 fallback)
    for anchor in anchors:
        al = anchor.lower()
        for i, line in enumerate(lines):
            if al in line.lower():
                return _block(i)

    return ""


def _section_text(
    xml_path: str | Path,
    anchors: list[str],
    max_chars: int = 6000,
    lines_per_anchor: int = ANCHOR_LINES,
    priority: bool = False,
    require_keyword: str | None = None,
) -> str:
    """앵커 기반 섹션 추출. 없으면 빈 문자열 반환.

    priority=True  → _anchor_best() 단일 블록 반환 (max_chars 경쟁 없음).
    priority=False → anchor_xml() 전체 매치 합산 (기존 동작).
    """
    if priority:
        return _anchor_best(xml_path, anchors, lines_per_anchor, require_keyword)[:max_chars]
    return anchor_xml(xml_path, anchors, lines_per_anchor=lines_per_anchor)[:max_chars]


async def _call_with_retry(
    http: httpx.AsyncClient,
    model: str,
    prompt: str,
    max_retries: int = 3,
) -> list | dict | None:
    """Ollama JSON 추출 with retry. 3회 실패 시 None 반환."""
    for attempt in range(max_retries):
        try:
            raw = await _call_ollama_native(
                http=http,
                model=model,
                prompt=prompt,
                timeout=120.0,
                max_tokens=2000,
                enable_thinking=False,
            )
            text = raw.strip()
            if text.startswith("```"):
                parts = text.split("```")
                text = parts[1] if len(parts) > 1 else text
                if text.startswith("json"):
                    text = text[4:]
            text = text.strip()
            # Ollama가 설명과 함께 JSON을 반환하는 경우 JSON 블록만 추출
            if text and not text[0] in ("{", "["):
                m = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", text)
                if m:
                    text = m.group(1)
            return json.loads(text)
        except (json.JSONDecodeError, ValueError) as e:
            if attempt < max_retries - 1:
                logger.debug("[dart-extractor] JSON 파싱 실패 (시도 %d): %s", attempt + 1, e)
                await asyncio.sleep(1.0)
            else:
                logger.warning("[dart-extractor] JSON 파싱 %d회 모두 실패: %s", max_retries, e)
                return None
        except Exception as e:
            logger.warning("[dart-extractor] Ollama 호출 오류: %s", e)
            return None
    return None


async def extract_structured(
    http: httpx.AsyncClient,
    xml_path: str | Path,
    model: str,
    report_type: str = "사업보고서",
    period: str = "unknown",
) -> dict:
    """3-Pass Ollama 구조화 추출 (asyncio 병렬).

    반환: {"segments_json": list|None, "revenue_json": dict|None, "competitors_json": list|None}
    각 추출기가 실패해도 None으로 저장 — 전체 중단 없음.
    """
    full_text = extract_xml(xml_path)
    seg_text  = _section_text(xml_path, _SEGMENT_ANCHORS) or full_text[:5000]
    # revenue: 우선순위 단일 블록 + 200줄 + 8000자
    # require_keyword="영업이익" → 블록에 영업이익 없으면 다음 앵커로 계속 탐색
    rev_text  = _section_text(
        xml_path, _REVENUE_ANCHORS,
        max_chars=8000, lines_per_anchor=200,
        priority=True, require_keyword="영업이익",
    ) or full_text[:5000]
    # 단위 감지는 헤더 트리밍 전에 실행 (헤더에 단위 선언이 있을 수 있음)
    unit = _detect_unit(rev_text or "")

    # 손익계산서 시작점 탐색: 요약재무정보 섹션은 재무상태표→손익계산서 순.
    # 재무상태표(자산/부채 항목) 부분을 제거하고 손익계산서부터 사용한다.
    # 예외: 포괄손익계산서로 시작하는 섹션은 열 헤더(기간/3개월/누적)가 필요하므로 잘라내지 않는다.
    _IS_PLOCI = rev_text and any(
        kw in (rev_text.splitlines()[:3] and "\n".join(rev_text.splitlines()[:3]))
        for kw in ("포괄손익계산서", "손익계산서")
    )
    _INCOME_START_KWS = [
        "매출액", "영업수익", "총영업이익", "순영업이익",
        "이자수익", "수수료수익", "보험료수익",
    ]
    if rev_text and not _IS_PLOCI:
        _rv_lines = rev_text.splitlines()
        _income_start = -1
        for _i, _l in enumerate(_rv_lines):
            stripped = _l.strip()
            if any(kw == stripped or (kw in stripped and len(stripped) <= len(kw) + 5)
                   for kw in _INCOME_START_KWS):
                _income_start = _i
                break
        if _income_start > 5:            # 의미있는 앞부분이 있을 때만 잘라냄
            rev_text = "\n".join(_rv_lines[_income_start:])

    # 마지막 영업이익 줄 이후 30줄에서 자름 — 재무 표 뒤의 사업 설명 제거
    if rev_text:
        _rv_lines = rev_text.splitlines()
        _last_op = max(
            (i for i, l in enumerate(_rv_lines) if "영업이익" in l),
            default=-1,
        )
        if _last_op >= 0:
            rev_text = "\n".join(_rv_lines[:_last_op + 31])

    # 합계행 보완: rev_text에 "합계/총계" 행이 없으면 매출실적 표 병합
    # (삼성전자 등 부문별 요약은 내부거래 포함 — 실제 연결 합계는 매출실적 표에 있음)
    _SUM_INDICATORS = ["합계", "총계", "합     계", "합  계"]
    if not any(kw in (rev_text or "") for kw in _SUM_INDICATORS):
        _sales_text = _section_text(
            xml_path, ["매출실적", "부문별 매출실적", "매출현황"],
            max_chars=4000, lines_per_anchor=100,
            priority=False, require_keyword=None,
        )
        if _sales_text and any(kw in _sales_text for kw in _SUM_INDICATORS):
            rev_text = (rev_text or "") + "\n\n[매출실적 합계]\n" + _sales_text

    # 금융사 폴백: rev_text에 매출 지표가 없으면 금융사 전용 앵커로 재시도
    _REV_INDICATORS = ["매출액", "영업수익", "이자수익", "총영업이익", "순영업이익"]
    _FIN_REVENUE_ANCHORS = ["영업수익", "총영업이익", "이자이익", "이자수익", "핵심이익"]
    if not any(kw in (rev_text or "") for kw in _REV_INDICATORS):
        for _fin_anc in _FIN_REVENUE_ANCHORS:
            _fin_text = _section_text(
                xml_path, [_fin_anc],
                max_chars=8000, lines_per_anchor=200,
                priority=False, require_keyword=None,
            )
            if _fin_text and len(_fin_text) > 200:
                rev_text = _fin_text
                break

    # 금융사 폴백으로 rev_text가 교체된 경우 단위 재감지
    if unit == "unknown":
        unit = _detect_unit(rev_text or "")

    # 분기/반기보고서: XBRL TE 태그에서 직접 추출 시도 (LLM 오류 우회)
    _xbrl_quarterly = None
    if "분기" in report_type or "반기" in report_type:
        _xbrl_quarterly = extract_xbrl_quarterly(xml_path)

    seg_json, rev_json, comp_json = await asyncio.gather(
        _call_with_retry(http, model, SEGMENT_EXTRACTOR_PROMPT + seg_text),
        _call_with_retry(http, model, _build_revenue_prompt(report_type, period) + rev_text),
        _call_with_retry(http, model, COMPETITOR_EXTRACTOR_PROMPT + full_text[:6000]),
    )

    # XBRL 직접 추출값으로 LLM 결과의 consolidated revenue/op_profit 교체
    if _xbrl_quarterly:
        cur_lbl, pri_lbl = _derive_period_labels(report_type, period)
        if not rev_json or not isinstance(rev_json, dict):
            rev_json = {}
        rev_json["periods"]     = [cur_lbl, pri_lbl]
        rev_json["unit"]        = _xbrl_quarterly["unit"]
        xc = _xbrl_quarterly
        if not rev_json.get("consolidated"):
            rev_json["consolidated"] = {}
        rev_json["consolidated"]["revenue"]   = xc["revenue"]
        rev_json["consolidated"]["op_profit"] = xc["op_profit"]
        logger.debug("[dart-extractor] XBRL 직접 추출 적용: %s %s", report_type, period)

    # revenue_json에 단위 주입 (XBRL 미사용 경우)
    if rev_json and isinstance(rev_json, dict) and not _xbrl_quarterly:
        rev_json["unit"] = unit
        # periods가 generic("당기"/"전기"/"unknown")이면 파라미터에서 도출한 레이블로 교체
        _generic = {"당기", "전기", "unknown", "당기분기", "전기분기"}
        existing = rev_json.get("periods", [])
        if all(str(p) in _generic for p in existing):
            rev_json["periods"] = list(_derive_period_labels(report_type, period))

    # 금융사 규칙 기반 폴백: Ollama가 revenue를 비워두면 regex로 직접 추출
    def _is_empty_rev(rj) -> bool:
        if not rj or not isinstance(rj, dict): return True
        c = rj.get("consolidated", {})
        has_num = lambda lst: any(v not in (None, 0, "", "-") for v in (lst or []))
        return not (has_num(c.get("revenue", [])) or has_num(c.get("op_profit", [])))

    if _is_empty_rev(rev_json) and rev_text:
        _FIN_REV_PATTERNS = [
            (r"매출액\(영업수익\)\s*\n([\d,]+)\n([\d,]+)\n([\d,]+)", "revenue"),
            (r"총영업이익\s*\n([\d,]+)\s*\n([\d,]+)\s*\n([\d,]+)", "revenue"),
            (r"순영업이익\s*\n([\d,]+)\s*\n([\d,]+)\s*\n([\d,]+)", "revenue"),
        ]
        for _pat, _field in _FIN_REV_PATTERNS:
            _m = re.search(_pat, rev_text)
            if _m:
                _vals = [int(g.replace(",", "")) for g in _m.groups()]
                if not rev_json or not isinstance(rev_json, dict):
                    rev_json = {"periods": [], "segments": [], "consolidated": {"revenue": [], "op_profit": []}}
                rev_json["consolidated"][_field] = _vals
                rev_json["unit"] = unit
                break

    return {
        "segments_json":    seg_json,
        "revenue_json":     rev_json,
        "competitors_json": comp_json,
    }


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
    corp_filter: str | None = None,
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
        if corp_filter:
            company_dirs = [d for d in company_dirs if d.name == corp_filter]

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

                # 서술 추출 — 프롬프트 템플릿 없으면 스킵 (3-Pass는 계속 실행)
                extraction_text = ""
                xml_chars = 0
                if PROMPT_TEMPLATE_PATH.exists():
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
                            "[dart-extractor] 서술 추출 오류 (건너뜀): %s / %s — %s",
                            corp_name, rcept_no, e,
                        )
                        continue
                else:
                    context = extract_xml(xml_path)
                    xml_chars = len(context)

                # 3-Pass 구조화 추출
                try:
                    structured = await extract_structured(http, xml_path, model, report_type=report_type, period=period)
                except Exception as e:
                    logger.warning("[dart-extractor] 구조화 추출 오류: %s", e)
                    structured = {"segments_json": None, "revenue_json": None, "competitors_json": None}

                def _to_jsonb(v) -> Optional[str]:
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
                        extraction_text, model, xml_chars,
                        _to_jsonb(structured["segments_json"]),
                        _to_jsonb(structured["revenue_json"]),
                        _to_jsonb(structured["competitors_json"]),
                    )
                total += 1
                logger.info(
                    "[dart-extractor] 저장 완료: %s / %s (%d자)",
                    corp_name, rcept_no, xml_chars,
                )

    return total


# ── 단일 기업 추출 (dart_screened_sync 용) ───────────────────────────────────

async def extract_all_for_company(
    pool,
    corp_name: str,
    http: httpx.AsyncClient | None = None,
    model: str = DEFAULT_MODEL,
    force: bool = False,
    dart_dir: Path = DART_DIR,
) -> int:
    """특정 기업 디렉터리만 3-Pass 추출. extract_all의 단일 기업 버전.

    http: 외부에서 공유 클라이언트를 넘길 수 있음. None이면 내부에서 생성.
    반환: 저장/갱신 건수.
    """
    company_dir = dart_dir / corp_name
    if not company_dir.exists():
        logger.warning("[dart-extractor] 디렉터리 없음: %s", company_dir)
        return 0

    async def _run(http_client: httpx.AsyncClient) -> int:
        total = 0
        report_dirs = sorted(d for d in company_dir.iterdir() if d.is_dir())
        for report_dir in report_dirs:
            dir_name = report_dir.name
            rcept_no, report_type, period = _parse_report_dir(dir_name)

            if not force:
                async with pool.acquire() as conn:
                    done = await conn.fetchval(
                        "SELECT COUNT(*) FROM dart_extractions "
                        "WHERE corp_name=$1 AND rcept_no=$2",
                        corp_name, rcept_no,
                    )
                if done:
                    logger.debug("[dart-extractor] 건너뜀(기존): %s/%s", corp_name, rcept_no)
                    continue

            xml_path = _pick_main_xml(report_dir, rcept_no)
            if xml_path is None:
                continue

            logger.info("[dart-extractor] 처리: %s / %s (%s)", corp_name, report_type, period)

            extraction_text = ""
            xml_chars = 0
            if PROMPT_TEMPLATE_PATH.exists():
                try:
                    extraction_text, xml_chars = await extract_company(
                        http=http_client, corp_name=corp_name, xml_path=xml_path,
                        model=model, report_type=report_type, period=period,
                    )
                except Exception as e:
                    logger.warning("[dart-extractor] 서술 추출 오류: %s / %s — %s", corp_name, rcept_no, e)
                    continue
            else:
                xml_chars = len(extract_xml(xml_path))

            try:
                structured = await extract_structured(http_client, xml_path, model, report_type=report_type, period=period)
            except Exception as e:
                logger.warning("[dart-extractor] 구조화 추출 오류: %s", e)
                structured = {"segments_json": None, "revenue_json": None, "competitors_json": None}

            def _to_jsonb(v) -> Optional[str]:
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
                    extraction_text, model, xml_chars,
                    _to_jsonb(structured["segments_json"]),
                    _to_jsonb(structured["revenue_json"]),
                    _to_jsonb(structured["competitors_json"]),
                )
            total += 1
            logger.info("[dart-extractor] 저장: %s/%s (%d자)", corp_name, rcept_no, xml_chars)
        return total

    if http is not None:
        return await _run(http)
    async with httpx.AsyncClient() as http_client:
        return await _run(http_client)


# ── 단독 실행 ─────────────────────────────────────────────────────────────────

def _main_cli() -> None:
    parser = argparse.ArgumentParser(description="DART XML Ollama 추출")
    parser.add_argument("--company", help="특정 회사명 (디렉터리명과 일치)")
    parser.add_argument("--all", action="store_true", help="전체 기업 처리")
    parser.add_argument("--force", action="store_true", help="기존 결과 덮어쓰기")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--migrate-schema", action="store_true",
                        help="dart_extractions에 segments_json/revenue_json/competitors_json 컬럼 추가")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.migrate_schema:
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from core.db import create_pool

        async def _migrate():
            pool = await create_pool()
            async with pool.acquire() as conn:
                await conn.execute("""
                    ALTER TABLE dart_extractions
                        ADD COLUMN IF NOT EXISTS segments_json    JSONB,
                        ADD COLUMN IF NOT EXISTS revenue_json     JSONB,
                        ADD COLUMN IF NOT EXISTS competitors_json JSONB
                """)
            print("마이그레이션 완료: segments_json / revenue_json / competitors_json 컬럼 추가")

        asyncio.run(_migrate())

    elif args.company:
        # 단일 기업 콘솔 출력 (DB 없음) — 기존 서술 + 3-Pass 구조화 JSON
        company_dir = DART_DIR / args.company
        if not company_dir.exists():
            print(f"디렉터리 없음: {company_dir}")
            sys.exit(1)
        report_dirs = sorted(d for d in company_dir.iterdir() if d.is_dir())
        if not report_dirs:
            print(f"리포트 디렉터리 없음: {company_dir}")
            sys.exit(1)
        report_dir = report_dirs[-1]  # 가장 최근
        dir_name = report_dir.name
        rcept_no, report_type, period = _parse_report_dir(dir_name)
        xml_path = _pick_main_xml(report_dir, rcept_no)
        if xml_path is None:
            print(f"XML 없음: {report_dir}")
            sys.exit(1)

        print(f"[1/4] XML 추출: {xml_path}")
        context = extract_xml(xml_path)
        print(f"      anchor+grep: {len(context):,}자")

        print("[2/4] 서술 추출 (기존)")

        async def _run():
            async with httpx.AsyncClient() as http:
                if not await _ollama_is_alive(http):
                    raise SystemExit(
                        f"Ollama 서버 미응답 — `ollama serve` 실행 후 재시도. "
                        f"OLLAMA_BASE={OLLAMA_BASE}"
                    )
                # 서술 추출 — 프롬프트 파일 없으면 스킵
                narrative_result = None
                if PROMPT_TEMPLATE_PATH.exists():
                    print("[2/4] 서술 추출 (기존)")
                    narrative_result = await extract_company(
                        http, args.company, xml_path, args.model, report_type, period
                    )
                else:
                    print("[2/4] 서술 추출 스킵 (프롬프트 파일 없음)")

                print("[3/4] 3-Pass 구조화 추출 (병렬)")
                structured_result = await extract_structured(
                    http, xml_path, args.model, report_type=report_type, period=period
                )
                return narrative_result, structured_result

        (narrative, structured) = asyncio.run(_run())

        import io
        out = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        out.write(f"\n[4/4] 결과 ({len(context):,}자 컨텍스트)\n")
        if narrative:
            text, xml_chars = narrative
            out.write("─" * 72 + "\n[서술 텍스트]\n" + "─" * 72 + "\n")
            out.write(text + "\n")
        out.write("─" * 72 + "\n[구조화 JSON]\n" + "─" * 72 + "\n")
        for key, val in structured.items():
            status = "OK" if val is not None else "FAIL(None)"
            out.write(f"\n## {key} [{status}]\n")
            if val is not None:
                out.write(json.dumps(val, ensure_ascii=False, indent=2)[:2000] + "\n")
        out.write("─" * 72 + "\n")
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
