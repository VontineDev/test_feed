"""
signal_detector.py  —  LLM 기반 매매 신호 감지 모듈
────────────────────────────────────────────────────────────
한글 요약된 뉴스를 LLM에게 넘겨 매매 신호 여부를 판단.
키워드 하드코딩 없이 LLM이 직접 판단하므로 유연성 높음.

신호 구조:
    direction : "BUY" | "SELL" | "WATCH" | "NONE"
    strength  : 1~5 (1=약, 5=강)
    reason    : 판단 근거 한 줄 요약
    tickers   : 관련 종목/지수 리스트 (없으면 빈 리스트)
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Optional

import httpx

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from market_data import MacroContext
from summarizer import (
    _call_openai_compat,
    _call_ollama_native,
    _ollama_is_alive,
    _lmstudio_is_alive,
    OLLAMA_BASE, OLLAMA_MODEL,
    LM_STUDIO_BASE, LM_STUDIO_MODEL,
    Backend,
    SYSTEM_PROMPT_SIGNAL,
)

logger = logging.getLogger(__name__)


# ── 신호 데이터 클래스 ────────────────────────────────────────
@dataclass
class TradeSignal:
    direction: str               # BUY | SELL | WATCH | NONE
    strength: int                # 1~5
    reason: str                  # 판단 근거
    tickers: list[str]           # 관련 종목/지수 표시명 (로깅/Telegram용)
    ticker_symbols: dict[str, str]  # 표시명 → yfinance 심볼 (예: {"삼성전자": "005930.KS"})
    backend: Backend
    success: bool                # LLM 판단 성공 여부

    @property
    def is_actionable(self) -> bool:
        """BUY/SELL/WATCH 이고 strength 2 이상이면 유효 신호"""
        return self.direction in ("BUY", "SELL", "WATCH") and self.strength >= 2


NONE_SIGNAL = TradeSignal(
    direction="NONE", strength=0, reason="", tickers=[],
    ticker_symbols={}, backend=Backend.FAILED, success=False,
)


# ── 프롬프트 ─────────────────────────────────────────────────
SIGNAL_PROMPT = """You are a Korean stock market trading signal analyzer.

Analyze the following Korean financial news summary and determine if it contains a trading signal.

News title: {title}
Korean summary: {summary_ko}

Respond ONLY with a JSON object in this exact format (no markdown, no explanation):
{{
  "direction": "BUY" or "SELL" or "WATCH" or "NONE",
  "strength": <integer 1-5>,
  "reason": "<one sentence in Korean explaining why>",
  "tickers": [
    {{"name": "<display name in Korean or English>", "symbol": "<yfinance ticker symbol>"}}
  ]
}}

yfinance ticker symbol format:
- Korean stocks  : 6-digit KRX code + .KS  (e.g. 삼성전자→"005930.KS", SK하이닉스→"000660.KS", 현대차→"005380.KS")
- Korean indices : ^KS11 (KOSPI), ^KQ11 (KOSDAQ)
- US stocks      : standard ticker           (e.g. NVDA, AAPL, TSLA, MSFT, AMZN, META, GOOGL)
- US indices     : ^GSPC (S&P500), ^IXIC (Nasdaq), ^DJI (Dow Jones)
- Commodities    : GC=F (gold), CL=F (oil/WTI), SI=F (silver), HG=F (copper)
- If symbol is unknown, use empty string ""

Guidelines:
- BUY  : Clearly positive news (rate cut, earnings beat, policy support, index surge)
- SELL : Clearly negative news (rate hike, earnings miss, sanctions, index crash)
- WATCH: Ambiguous but market-moving news (geopolitical tension, Fed minutes, macro data)
- NONE : Irrelevant to trading (sports, entertainment, unrelated politics)
- strength 1-2: weak/indirect signal
- strength 3  : moderate signal
- strength 4-5: strong/direct signal
- If no specific ticker, use empty list []
- reason: Write in Korean only. Do not mix Chinese characters or Chinese language.
{macro_section}"""

# Qwen3 사고 억제: enable_thinking=False 시 _call_ollama_native가 /no_think\n\n 을 prepend


def _build_macro_section(macro: Optional[MacroContext]) -> str:
    """Return macro context block for the LLM prompt. Returns empty string if nothing available."""
    parts = []
    if macro and macro.usd_krw is not None:
        parts.append(f"- USD/KRW exchange rate: {macro.usd_krw:.1f}")
        # Note: weekend/holiday data may be up to 3 days stale (last close)
    if macro and macro.korea_base_rate is not None:
        parts.append(f"- Korea base rate: {macro.korea_base_rate}%")
    if not parts:
        return ""
    header = "\nMacro context (use this to adjust your signal assessment):"
    note = (
        "Note: High USD/KRW (weak KRW) benefits exporters (Samsung, Hyundai, LG Electronics). "
        "Low USD/KRW (strong KRW) benefits importers and rate-sensitive sectors. "
        "Rising base rate suppresses construction, real estate, and high-debt companies."
    )
    return "\n".join([header] + parts + [note]) + "\n"


# ── JSON 파싱 ─────────────────────────────────────────────────
def _parse_signal_json(raw: str, backend: Backend) -> TradeSignal:
    """LLM 응답에서 JSON 추출 → TradeSignal 변환"""
    try:
        # <think>...</think> 추론 토큰 제거 (DeepSeek-R1 / Qwen3)
        text = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL)
        # 닫히지 않은 <think> — 이후 전부 제거
        text = re.sub(r"<think>.*", "", text, flags=re.DOTALL)
        text = re.sub(r"</think>", "", text)
        # 코드블록 제거
        text = re.sub(r"```(?:json)?|```", "", text).strip()
        # 첫 번째 완성된 JSON 객체만 추출 (중복 출력 방지)
        start = text.find("{")
        if start == -1:
            raise ValueError("JSON 없음")
        # 중괄호 depth 추적으로 첫 번째 JSON 객체 끝 위치 탐색
        depth = 0
        end = -1
        for i, ch in enumerate(text[start:], start):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        if end == -1:
            raise ValueError("JSON 없음")
        data = json.loads(text[start:end+1])

        direction = str(data.get("direction", "NONE")).upper()
        if direction not in ("BUY", "SELL", "WATCH", "NONE"):
            direction = "NONE"

        strength = int(data.get("strength", 0))
        strength = max(0, min(5, strength))

        tickers_raw = data.get("tickers", [])
        if not isinstance(tickers_raw, list):
            tickers_raw = []

        # 새 형식 [{"name": ..., "symbol": ...}] 와 구형식 ["string"] 모두 처리
        tickers: list[str] = []
        ticker_symbols: dict[str, str] = {}
        for item in tickers_raw:
            if isinstance(item, dict):
                name = str(item.get("name", "")).strip()
                symbol = str(item.get("symbol", "")).strip()
                if name:
                    tickers.append(name)
                    if symbol:
                        ticker_symbols[name] = symbol
            elif isinstance(item, str) and item.strip():
                tickers.append(item.strip())

        if ticker_symbols:
            logger.debug("[신호감지] LLM 심볼 %d개 수신: %s", len(ticker_symbols), ticker_symbols)

        return TradeSignal(
            direction=direction,
            strength=strength,
            reason=str(data.get("reason", "")),
            tickers=tickers,
            ticker_symbols=ticker_symbols,
            backend=backend,
            success=True,
        )
    except Exception as e:
        logger.warning("[신호감지] JSON 파싱 실패: %s | raw: %s", e, raw[:100])
        return NONE_SIGNAL

# ── 메인 감지 함수 ────────────────────────────────────────────
async def detect_signal(
    title: str,
    summary_ko: str,
    http: Optional[httpx.AsyncClient] = None,
    macro: Optional[MacroContext] = None,
) -> TradeSignal:
    """
    뉴스 제목 + 한글 요약 → 매매 신호 판단.
    요약이 없으면 NONE 즉시 반환.
    macro: MacroContext — LLM 프롬프트에 매크로 컨텍스트 주입 (없으면 생략)
    """
    if not summary_ko.strip():
        return NONE_SIGNAL

    prompt = SIGNAL_PROMPT.format(
        title=title,
        summary_ko=summary_ko,
        macro_section=_build_macro_section(macro),
    )

    _own_client = http is None
    if _own_client:
        http = httpx.AsyncClient()

    try:
        # Ollama 우선 (실패 시 1회 재시도)
        if await _ollama_is_alive(http):
            for attempt in range(2):
                try:
                    raw = await _call_ollama_native(
                        http, OLLAMA_MODEL, prompt,
                        timeout=60.0,
                        max_tokens=800,
                        system_prompt=SYSTEM_PROMPT_SIGNAL,
                        enable_thinking=False,
                    )
                    sig = _parse_signal_json(raw, Backend.OLLAMA)
                    if sig.success:
                        return sig
                    if attempt == 0:
                        logger.info("[신호감지] Ollama JSON 파싱 실패 → 재시도")
                        continue
                except Exception as e:
                    if attempt == 0:
                        logger.info("[신호감지] Ollama 오류 → 재시도: %s", e)
                        continue
                    logger.warning("[신호감지] Ollama 실패 → LM Studio: %s", e)
                    break

        # LM Studio fallback
        if await _lmstudio_is_alive(http):
            try:
                raw = await _call_openai_compat(
                    http, LM_STUDIO_BASE, LM_STUDIO_MODEL, prompt,
                    timeout=60.0,
                    max_tokens=800,
                    system_prompt=SYSTEM_PROMPT_SIGNAL,
                    enable_thinking=None,   # LM Studio는 파라미터 미전송 (Reasoning OFF)
                )
                return _parse_signal_json(raw, Backend.LM_STUDIO)
            except Exception as e:
                logger.warning("[신호감지] LM Studio 실패: %s", e)

        logger.warning("[신호감지] 모든 백엔드 실패")
        return NONE_SIGNAL

    finally:
        if _own_client:
            await http.aclose()


# ── 단독 테스트 ───────────────────────────────────────────────
if __name__ == "__main__":
    import asyncio
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
    )

    TESTS = [
        {
            "title": "Federal Reserve cuts interest rates by 25bp",
            "summary_ko": "연준이 기준금리를 0.25%p 인하했습니다. 이는 2년 만의 금리 인하로, 증시에 강한 호재로 작용할 전망입니다.",
        },
        {
            "title": "Samsung Electronics misses Q1 earnings estimates",
            "summary_ko": "삼성전자가 1분기 실적이 시장 예상치를 하회했습니다. 메모리 반도체 수요 부진과 HBM 경쟁 심화가 원인으로 지목됩니다.",
        },
        {
            "title": "Oil prices surge 8% on Middle East tensions",
            "summary_ko": "중동 지역 긴장 고조로 국제유가가 8% 급등했습니다. 에너지 관련주와 항공주에 영향을 줄 수 있어 주목됩니다.",
        },
        {
            "title": "US payrolls unexpectedly fell by 92,000 in February",
            "summary_ko": "미국 2월 비농업 고용이 예상 외로 9만2천 명 감소했습니다. 경기 침체 우려가 커지며 증시 하락 압력이 예상됩니다.",
        },
    ]

    async def run():
        print("\n" + "=" * 60)
        print("매매 신호 감지 테스트")
        print("=" * 60)
        for t in TESTS:
            sig = await detect_signal(t["title"], t["summary_ko"])
            icon = {"BUY": "🟢", "SELL": "🔴", "WATCH": "🟡", "NONE": "⚪"}.get(sig.direction, "⚪")
            print(f"\n{icon} [{sig.direction}] 강도: {sig.strength}/5")
            print(f"   제목: {t['title'][:60]}")
            print(f"   근거: {sig.reason}")
            print(f"   종목: {sig.tickers}")

    asyncio.run(run())
