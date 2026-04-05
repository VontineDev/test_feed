"""
article_fetcher.py  —  기사 본문 크롤링 모듈
────────────────────────────────────────────────────────────
소스별 파서 + 범용 fallback 구조.

소스별 전략:
    cnbc      : div.ArticleBody-articleBody > p 태그
    investing : div.WYSIWYG.articlePage > p 태그
    reuters   : div[class*="article-body"] > p 태그 (Google News 우회라 실패 多)
    fallback  : <article> 또는 <main> 안의 p 태그 모두 수집

본문 길이 제한: 3,000자 (LLM 컨텍스트 고려)
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Optional

import httpx
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)

MAX_BODY_CHARS = 3000  # LLM에 넘길 최대 본문 길이

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


# ── 소스별 파서 ───────────────────────────────────────────────

def _parse_cnbc(soup: BeautifulSoup) -> str:
    """CNBC 본문 파서"""
    selectors = [
        "div.ArticleBody-articleBody",
        "div[class*='ArticleBody']",
        "div.article-body",
    ]
    for sel in selectors:
        container = soup.select_one(sel)
        if container:
            paras = container.find_all("p")
            text = " ".join(p.get_text(strip=True) for p in paras if p.get_text(strip=True))
            if len(text) > 200:
                return text
    return ""


def _parse_investing(soup: BeautifulSoup) -> str:
    """Investing.com 본문 파서"""
    selectors = [
        "div.WYSIWYG.articlePage",
        "div[class*='articlePage']",
        "div.articlePage",
    ]
    for sel in selectors:
        container = soup.select_one(sel)
        if container:
            paras = container.find_all("p")
            text = " ".join(p.get_text(strip=True) for p in paras if p.get_text(strip=True))
            if len(text) > 200:
                return text
    return ""


def _parse_reuters(soup: BeautifulSoup) -> str:
    """Reuters 본문 파서 (Google News 우회 URL은 실패 가능)"""
    selectors = [
        "div[class*='article-body']",
        "div[class*='ArticleBody']",
        "div[data-testid='ArticleBody']",
    ]
    for sel in selectors:
        container = soup.select_one(sel)
        if container:
            paras = container.find_all("p")
            text = " ".join(p.get_text(strip=True) for p in paras if p.get_text(strip=True))
            if len(text) > 200:
                return text
    return ""



def _parse_yahoo(soup: BeautifulSoup) -> str:
    """Yahoo Finance 본문 파서"""
    selectors = [
        "div.caas-body",
        "div[class*='caas-body']",
        "article",
    ]
    for sel in selectors:
        container = soup.select_one(sel)
        if container:
            paras = container.find_all("p")
            text = " ".join(p.get_text(strip=True) for p in paras if p.get_text(strip=True))
            if len(text) > 200:
                return text
    return ""


def _parse_marketwatch(soup: BeautifulSoup) -> str:
    """MarketWatch 본문 파서"""
    selectors = [
        "div.article__body",
        "div[class*='article__body']",
        "div.region.region--primary",
    ]
    for sel in selectors:
        container = soup.select_one(sel)
        if container:
            paras = container.find_all("p")
            text = " ".join(p.get_text(strip=True) for p in paras if p.get_text(strip=True))
            if len(text) > 200:
                return text
    return ""



def _parse_bloomberg(soup: BeautifulSoup) -> str:
    """Bloomberg 본문 파서"""
    selectors = [
        "div.body-content",
        "div[class*='body-content']",
        "article",
    ]
    for sel in selectors:
        container = soup.select_one(sel)
        if container:
            paras = container.find_all("p")
            text = " ".join(p.get_text(strip=True) for p in paras if p.get_text(strip=True))
            if len(text) > 200:
                return text
    return ""


def _parse_yonhap(soup: BeautifulSoup) -> str:
    """연합뉴스 본문 파서"""
    container = soup.find("div", class_="story-news")
    if container:
        article = container.find("article")
        if article:
            paras = article.find_all("p")
            return " ".join(p.get_text(strip=True) for p in paras if len(p.get_text(strip=True)) > 20)
    return ""


def _parse_hankyung(soup: BeautifulSoup) -> str:
    """한국경제 본문 파서"""
    container = soup.find("div", class_="article-body")
    if container:
        paras = container.find_all("p")
        text = " ".join(p.get_text(strip=True) for p in paras if len(p.get_text(strip=True)) > 20)
        if text:
            return text
    # fallback: outer wrapper
    container = soup.find("div", class_="article-body-wrap")
    if container:
        paras = container.find_all("p")
        return " ".join(p.get_text(strip=True) for p in paras if len(p.get_text(strip=True)) > 20)
    return ""


def _parse_mk(soup: BeautifulSoup) -> str:
    """매일경제 본문 파서"""
    container = soup.find("div", class_="art_txt")
    if container:
        paras = container.find_all("p")
        text = " ".join(p.get_text(strip=True) for p in paras if len(p.get_text(strip=True)) > 20)
        if text:
            return text
    return ""


def _parse_fallback(soup: BeautifulSoup) -> str:
    """
    범용 fallback 파서.
    <article> → <main> → <body> 순서로 시도.
    광고/메뉴 텍스트를 최소화하기 위해 p 태그만 수집.
    """
    for tag in ["article", "main"]:
        container = soup.find(tag)
        if container:
            paras = container.find_all("p")
            text = " ".join(p.get_text(strip=True) for p in paras if len(p.get_text(strip=True)) > 30)
            if len(text) > 200:
                return text

    # 최후 수단: 전체 p 태그 (길이 30자 이상만)
    paras = soup.find_all("p")
    text = " ".join(p.get_text(strip=True) for p in paras if len(p.get_text(strip=True)) > 30)
    return text


# ── JSON-LD에서 articleBody 추출 (가장 정확) ──────────────────

def _extract_json_ld_body(soup: BeautifulSoup) -> str:
    """
    뉴스 사이트들은 SEO용으로 JSON-LD 스키마에 articleBody를 포함하는 경우가 많음.
    HTML 파싱보다 안정적.
    """
    import json
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            # 리스트인 경우 순회
            items = data if isinstance(data, list) else [data]
            for item in items:
                body = item.get("articleBody", "")
                if body and len(body) > 200:
                    return body
        except Exception:
            continue
    return ""


# ── 텍스트 정제 ───────────────────────────────────────────────

def _clean(text: str) -> str:
    """공백 정리, 광고성 문구 제거"""
    text = re.sub(r"\s+", " ", text).strip()
    # 짧은 문장(광고/네비 잔재) 제거
    sentences = [s.strip() for s in text.split(".") if len(s.strip()) > 20]
    return ". ".join(sentences)


# ── 메인 크롤링 함수 ──────────────────────────────────────────

async def fetch_article_body(
    url: str,
    source: str,
    http: Optional[httpx.AsyncClient] = None,
) -> str:
    """
    기사 URL → 본문 텍스트 반환.
    실패 시 빈 문자열 반환 (예외 미전파).
    MAX_BODY_CHARS 로 잘라서 반환.
    """
    _own_client = http is None
    if _own_client:
        http = httpx.AsyncClient(headers=HEADERS, follow_redirects=True)

    # Yahoo 등 속도제한(429) 대비 — 최대 2회 재시도, 지수 백오프
    RETRY_ON_429 = 2
    for attempt in range(1 + RETRY_ON_429):
        try:
            resp = await http.get(url, timeout=15, follow_redirects=True)
            if resp.status_code == 429 and attempt < RETRY_ON_429:
                wait = 3 * (2 ** attempt)   # 3s → 6s
                logger.debug("[본문] 429 속도제한 — %d초 후 재시도 (%d/%d)", wait, attempt+1, RETRY_ON_429)
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            break
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429 and attempt < RETRY_ON_429:
                wait = 3 * (2 ** attempt)
                logger.debug("[본문] 429 예외 — %d초 후 재시도", wait)
                await asyncio.sleep(wait)
                continue
            logger.warning("[본문] HTTP %d: %s", e.response.status_code, url[:60])
            return ""
        except Exception as e:
            logger.warning("[본문] 크롤링 실패 (%s): %s", source, e)
            return ""

    try:
        soup = BeautifulSoup(resp.text, "html.parser")

        # 1순위: JSON-LD articleBody (가장 신뢰도 높음)
        body = _extract_json_ld_body(soup)

        # 2순위: 소스별 파서
        if not body:
            if source == "cnbc":
                body = _parse_cnbc(soup)
            elif source == "investing":
                body = _parse_investing(soup)
            elif source == "reuters":
                body = _parse_reuters(soup)
            elif source == "yahoo":
                body = _parse_yahoo(soup)
            elif source == "marketwatch":
                body = _parse_marketwatch(soup)
            elif source == "bloomberg":
                body = _parse_bloomberg(soup)
            elif source == "yonhap":
                body = _parse_yonhap(soup)
            elif source == "hankyung":
                body = _parse_hankyung(soup)
            elif source == "mk":
                body = _parse_mk(soup)

        # 3순위: 범용 fallback
        if not body:
            body = _parse_fallback(soup)

        body = _clean(body)

        if body:
            logger.info("[본문] %s — %d자 수집", source, len(body))
        else:
            logger.info("[본문] %s — 본문 추출 실패", source)

        return body[:MAX_BODY_CHARS]

    except Exception as e:
        logger.warning("[본문] 크롤링 실패 (%s): %s", source, e)
        return ""
    finally:
        if _own_client:
            await http.aclose()


# ── 단독 테스트 ───────────────────────────────────────────────
if __name__ == "__main__":
    import asyncio
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    TEST_URLS = [
        ("cnbc",      "https://www.cnbc.com/2026/03/21/iran-targeted-but-did-not-hit-diego-garcia-base-with-missiles-wsj.html"),
        ("investing", "https://www.investing.com/news/economy-news/beijing-courts-eli-lilly-as-weightloss-drug-race-drives-3bn-china-commitment-4574031"),
    ]

    async def run():
        print("\n" + "=" * 60)
        print("기사 본문 크롤링 테스트")
        print("=" * 60)
        async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as http:
            for source, url in TEST_URLS:
                print(f"\n[{source}] {url[:60]}")
                body = await fetch_article_body(url, source, http=http)
                if body:
                    print(f"  ✓ {len(body)}자 수집")
                    print(f"  미리보기: {body[:200]}...")
                else:
                    print("  ✗ 본문 추출 실패")

    asyncio.run(run())
