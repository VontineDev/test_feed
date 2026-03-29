"""
summarizer.py  —  로컬 LLM 한글 요약 모듈
Fallback 체인: Ollama → LM Studio → 요약 실패 표시

Ollama  기본 포트: 11434  (ollama serve)
LM Studio 기본 포트: 1234   (Local Server 탭에서 Start)
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import os

import httpx

# ── .env 파일 자동 로드 ──────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)


# ── 설정 ─────────────────────────────────────────────────────

class Backend(str, Enum):
    OLLAMA    = "ollama"
    LM_STUDIO = "lm_studio"
    FAILED    = "failed"


@dataclass
class SummaryResult:
    text: str           # 한글 요약 (실패 시 빈 문자열)
    backend: Backend    # 어떤 백엔드가 처리했는지
    success: bool


OLLAMA_BASE    = os.environ.get("OLLAMA_BASE",    "http://localhost:11434")
LM_STUDIO_BASE = os.environ.get("LM_STUDIO_BASE", "http://localhost:1234")

# LM Studio는 OpenAI 호환 엔드포인트 사용
# Ollama도 /api/chat (네이티브) 와 /v1/chat/completions (OpenAI 호환) 둘 다 지원

OLLAMA_MODEL    = os.environ.get("OLLAMA_MODEL",    "Qwen3.5-9B:latest")
LM_STUDIO_MODEL = os.environ.get("LM_STUDIO_MODEL", "eeve-korean-instruct-10.8b-v1.0")

SYSTEM_PROMPT_SUMMARY = (
    "너는 한국 주식 트레이더를 위한 금융 뉴스 요약 전문가야. "
    "핵심 수치(%, 금액, EPS 등)와 종목명을 반드시 포함해서 "
    "2~3문장으로 간결하게 한글 요약해. "
    "서론 없이 바로 요약 결과만 출력해. "
    "반드시 순수 한글만 사용해. 한자나 중국어를 절대 섞지 마. "
    "원문의 수치와 통화 단위를 그대로 사용해. 임의로 환산하거나 변환하지 마. "
    "국가명·통화명·기관명은 정확히 번역해. (예: 인도 루피=₹·INR, 인도네시아 루피아=Rp·IDR, 혼동 금지)"
)

SYSTEM_PROMPT_SIGNAL = (
    "You are a 20-year veteran Wall Street news analyst. "
    "Analyze news with logical reasoning: news → market mechanism → price impact. "
    "Think step by step about causal chains before concluding. "
    "Output ONLY structured JSON results. No preamble, no explanation outside JSON. "
    "reason field must be written in Korean only. Do not use Chinese characters."
)

SUMMARY_PROMPT = """You are a Korean financial news summarizer.
Summarize the following English news in 2-3 Korean sentences.
Include key numbers, stock names, and index names.
Output ONLY the Korean summary sentences. Do not repeat the title. Do not include labels like '제목:' or '내용:'.
Write in Korean only. Do not use Chinese characters or Chinese language.
IMPORTANT: Even if the content is short, you MUST produce a Korean summary. Never output an empty response.

Title: {title}
Content: {body}

Korean summary:"""

# ── 공통 OpenAI 호환 호출 ────────────────────────────────────
async def _call_openai_compat(
    http: httpx.AsyncClient,
    base_url: str,
    model: str,
    prompt: str,
    timeout: float = 30.0,
    max_tokens: int = 300,
    system_prompt: str = "",
    enable_thinking=None,
) -> str:
    """OpenAI /v1/chat/completions 호환 엔드포인트 호출"""
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": 0.3,
        "stream": False,
    }
    if enable_thinking is not None:
        payload["enable_thinking"] = enable_thinking
    resp = await http.post(
        f"{base_url}/v1/chat/completions",
        json=payload,
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    text = data["choices"][0]["message"]["content"].strip()
    if not text:
        raise ValueError("LLM이 빈 응답 반환")
    return text
# ── 추론 블록 제거 유틸 ─────────────────────────────────────

def _strip_thinking(raw: str) -> str:
    """Qwen3 추론 블록 + EOS 토큰 제거."""
    import re as _re
    text = _re.sub(r'<think>.*?</think>', '', raw, flags=_re.DOTALL)
    text = _re.sub(r'Thinking Process:[\s\S]*?(?=\n\n|$)', '', text)
    text = _re.sub(r'<\|[^|]+\|>.*', '', text, flags=_re.DOTALL)  # EOS 토큰 제거
    text = _re.sub(r'</think>', '', text)   # 쌍 없이 남은 닫는 태그 제거
    return text.strip()

# ── Ollama 네이티브 호출 (/api/chat) ────────────────────────
async def _call_ollama_native(
    http: httpx.AsyncClient,
    model: str,
    prompt: str,
    timeout: float = 60.0,
    max_tokens: int = 800,
    system_prompt: str = "",
    enable_thinking: bool = False,
) -> str:
    """Ollama 네이티브 /api/chat 엔드포인트 호출.
    /v1/chat/completions의 enable_thinking 파라미터 문제를 우회.
    """
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    resp = await http.post(
        f"{OLLAMA_BASE}/api/chat",
        json={
            "model": model,
            "messages": messages,
            "stream": False,
            "options": {"num_predict": max_tokens, "temperature": 0.3},
            "think": enable_thinking,
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    text = data.get("message", {}).get("content", "").strip()
    if not text:
        raise ValueError("Ollama 네이티브 빈 응답 반환")
    return text


# ── Ollama 헬스체크 ───────────────────────────────────────────

async def _ollama_is_alive(http: httpx.AsyncClient) -> bool:
    try:
        r = await http.get(f"{OLLAMA_BASE}/api/tags", timeout=3.0)
        return r.status_code == 200
    except Exception:
        return False


# ── LM Studio 헬스체크 ────────────────────────────────────────

async def _lmstudio_is_alive(http: httpx.AsyncClient) -> bool:
    try:
        r = await http.get(f"{LM_STUDIO_BASE}/v1/models", timeout=3.0)
        return r.status_code == 200
    except Exception:
        return False


# ── Ollama 요약 ───────────────────────────────────────────────

async def _summarize_ollama(
    http: httpx.AsyncClient,
    title: str,
    body: str,
) -> str:
    prompt = SUMMARY_PROMPT.format(title=title, body=body[:800])
    # OpenAI 호환 엔드포인트는 enable_thinking 파라미터를 지원하지 않아 400 에러 발생
    # → 네이티브 /api/chat 엔드포인트 사용
    raw = await _call_ollama_native(
        http, OLLAMA_MODEL, prompt,
        timeout=120.0, max_tokens=300,
        system_prompt=SYSTEM_PROMPT_SUMMARY,
        enable_thinking=False,
    )
    text = _strip_thinking(raw)
    if not text:
        raise ValueError(f"빈 응답 (원본 {len(raw)}자)")
    return text


# ── LM Studio 요약 ────────────────────────────────────────────

async def _summarize_lmstudio(
    http: httpx.AsyncClient,
    title: str,
    body: str,
) -> str:
    # EEVE 모델은 system role을 지원하지 않을 수 있으므로 system prompt를 user 메시지에 합침
    base_prompt = SUMMARY_PROMPT.format(title=title, body=body[:800])
    prompt = f"{SYSTEM_PROMPT_SUMMARY}\n\n{base_prompt}"
    raw = await _call_openai_compat(
        http, LM_STUDIO_BASE, LM_STUDIO_MODEL, prompt,
        timeout=120.0, max_tokens=500,
        system_prompt="",   # system role 제거
        enable_thinking=None,
    )
    text = _strip_thinking(raw)
    if not text:
        raise ValueError(f"빈 응답 (원본 {len(raw)}자)")
    return text


# ── Fallback 체인 메인 함수 ───────────────────────────────────

async def summarize(
    title: str,
    body: str,
    http: Optional[httpx.AsyncClient] = None,
) -> SummaryResult:
    """
    Ollama → LM Studio → 실패 순서로 한글 요약 시도.
    http 클라이언트를 외부에서 주입하면 커넥션 풀을 재사용합니다.
    """
    if len(body.strip()) < 200:
        body = f"{title}. {body}".strip()
        logger.debug("[요약] 본문 짧음 — 제목 보강 후 %d자", len(body))

    _own_client = http is None
    if _own_client:
        http = httpx.AsyncClient()

    try:
        # ── 1차: Ollama ──────────────────────────────
        if await _ollama_is_alive(http):
            try:
                text = await _summarize_ollama(http, title, body)
                logger.debug("[요약] Ollama 성공 (%d자): %s", len(text), text[:40])
                return SummaryResult(text=text, backend=Backend.OLLAMA, success=True)
            except Exception as e:
                logger.warning("[요약] Ollama 실패 → LM Studio 시도: %s", e)
        else:
            logger.debug("[요약] Ollama 미실행 → LM Studio 시도")

        # ── 2차: LM Studio ───────────────────────────
        if await _lmstudio_is_alive(http):
            try:
                text = await _summarize_lmstudio(http, title, body)
                logger.debug("[요약] LM Studio 성공 (%d자): %s", len(text), text[:40])
                return SummaryResult(text=text, backend=Backend.LM_STUDIO, success=True)
            except Exception as e:
                logger.warning("[요약] LM Studio 실패: %s", e)
        else:
            logger.debug("[요약] LM Studio 미실행")

        # ── 3차: 실패 ────────────────────────────────
        logger.info("[요약] 모든 백엔드 실패 — 요약 없이 저장")
        return SummaryResult(text="", backend=Backend.FAILED, success=False)

    finally:
        if _own_client:
            await http.aclose()


# ── 배치 요약 (크롤러에서 여러 기사 한번에 처리) ──────────────

async def summarize_batch(
    articles: list[dict],  # [{"title": ..., "summary": ...}, ...]
    concurrency: int = 3,  # 동시 요청 수 (로컬 LLM 부하 고려)
) -> list[SummaryResult]:
    """
    여러 기사를 concurrency 제한을 두고 병렬 요약.
    로컬 LLM은 동시 요청이 많으면 느려지므로 기본 3으로 제한.
    """
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient() as http:
        async def _limited(art: dict) -> SummaryResult:
            async with sem:
                return await summarize(
                    title=art.get("title", ""),
                    body=art.get("summary", ""),
                    http=http,
                )

        tasks = [_limited(art) for art in articles]
        return await asyncio.gather(*tasks)


# ── 단독 테스트 ───────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    TEST_ARTICLES = [
        {
            "title": "Federal Reserve holds interest rates steady, signals two cuts in 2025",
            "summary": (
                "The Federal Reserve kept its benchmark interest rate unchanged at 5.25%-5.5% "
                "on Wednesday, while signaling it still expects two quarter-point cuts by the end "
                "of 2025. Fed Chair Jerome Powell said the central bank needs more confidence that "
                "inflation is moving sustainably toward its 2% goal before reducing borrowing costs."
            ),
        },
        {
            "title": "Samsung Electronics shares fall 3% after weak Q1 earnings guidance",
            "summary": (
                "Samsung Electronics shares dropped 3.2% in Seoul trading after the company issued "
                "weaker-than-expected guidance for Q1 2026 earnings, citing sluggish memory chip "
                "demand and intensifying competition from SK Hynix in the HBM segment."
            ),
        },
        {
            "title": "S&P 500 rises 0.8% as tech stocks lead broad market rally",
            "summary": (
                "U.S. stocks advanced on Thursday, with the S&P 500 gaining 0.8% and the Nasdaq "
                "Composite climbing 1.2%, driven by strong gains in mega-cap technology stocks "
                "including Nvidia, Apple, and Microsoft amid optimism over AI spending."
            ),
        },
    ]

    async def run_test():
        print("\n" + "="*60)
        print("로컬 LLM 한글 요약 테스트")
        print("="*60)

        # 헬스체크 먼저
        async with httpx.AsyncClient() as http:
            ollama_ok    = await _ollama_is_alive(http)
            lmstudio_ok  = await _lmstudio_is_alive(http)

        print(f"\n  Ollama    ({OLLAMA_BASE}): {'✓ 실행 중' if ollama_ok else '✗ 미실행'}")
        print(f"  LM Studio ({LM_STUDIO_BASE}): {'✓ 실행 중' if lmstudio_ok else '✗ 미실행'}")

        if not ollama_ok and not lmstudio_ok:
            print("\n[!] 두 백엔드 모두 미실행 상태입니다.")
            print("    Ollama:    ollama serve  &  ollama pull qwen2.5:7b")
            print("    LM Studio: Local Server 탭 → Start Server")
            sys.exit(1)

        print(f"\n{'─'*60}")
        print("배치 요약 시작 (3건)...")
        print(f"{'─'*60}\n")

        results = await summarize_batch(TEST_ARTICLES)

        for i, (art, res) in enumerate(zip(TEST_ARTICLES, results)):
            print(f"[{i+1}] {art['title'][:65]}")
            if res.success:
                print(f"     백엔드: {res.backend.value}")
                print(f"     요약:   {res.text}")
            else:
                print(f"     ⚠ 요약 실패 (모든 백엔드 응답 없음)")
            print()

    asyncio.run(run_test())
