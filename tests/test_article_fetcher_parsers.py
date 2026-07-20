"""
test_article_fetcher_parsers.py — core/article_fetcher.py 파서 특성화 테스트

리팩토링 전: cnbc/investing/reuters/yahoo/marketwatch/bloomberg 6개
소스별 파서(셀렉터 리스트만 다르고 순회 로직은 동일)의 현재 동작을 고정.
셀렉터 맵 + 공용 파서로 축약한 뒤에도 이 테스트가 그대로 통과해야 한다.
"""
from __future__ import annotations

import pytest
from bs4 import BeautifulSoup

from core.article_fetcher import (
    _parse_cnbc,
    _parse_investing,
    _parse_reuters,
    _parse_yahoo,
    _parse_marketwatch,
    _parse_bloomberg,
    fetch_article_body,
)


def _long_para_html(container_open: str, container_close: str, n_paras: int = 3) -> str:
    """200자 초과 텍스트를 만드는 <p> n개를 컨테이너 안에 채운 HTML."""
    para = "<p>" + ("가" * 80) + "</p>"
    return f"{container_open}{para * n_paras}{container_close}"


PARSER_CASES = [
    (_parse_cnbc, '<div class="ArticleBody-articleBody">', "</div>"),
    (_parse_investing, '<div class="WYSIWYG articlePage">', "</div>"),
    (_parse_reuters, '<div class="article-body-x">', "</div>"),
    (_parse_yahoo, '<div class="caas-body">', "</div>"),
    (_parse_marketwatch, '<div class="article__body">', "</div>"),
    (_parse_bloomberg, '<div class="body-content">', "</div>"),
]


@pytest.mark.parametrize("parser,open_tag,close_tag", PARSER_CASES)
def test_parser_matches_primary_selector(parser, open_tag, close_tag):
    html = _long_para_html(open_tag, close_tag)
    soup = BeautifulSoup(html, "html.parser")
    text = parser(soup)
    assert len(text) > 200


@pytest.mark.parametrize("parser,_open_tag,_close_tag", PARSER_CASES)
def test_parser_returns_empty_when_no_match(parser, _open_tag, _close_tag):
    soup = BeautifulSoup("<div class='unrelated'><p>짧은 텍스트</p></div>", "html.parser")
    assert parser(soup) == ""


@pytest.mark.parametrize("parser,open_tag,close_tag", PARSER_CASES)
def test_parser_returns_empty_when_text_too_short(parser, open_tag, close_tag):
    """컨테이너는 매치되지만 텍스트가 200자 미만이면 빈 문자열."""
    html = f"{open_tag}<p>짧음</p>{close_tag}"
    soup = BeautifulSoup(html, "html.parser")
    assert parser(soup) == ""


# ── fetch_article_body 소스별 디스패치 스모크 ───────────────────

@pytest.mark.asyncio
@pytest.mark.parametrize("source,open_tag,close_tag", [
    ("cnbc", '<div class="ArticleBody-articleBody">', "</div>"),
    ("investing", '<div class="WYSIWYG articlePage">', "</div>"),
    ("reuters", '<div class="article-body-x">', "</div>"),
    ("yahoo", '<div class="caas-body">', "</div>"),
    ("marketwatch", '<div class="article__body">', "</div>"),
    ("bloomberg", '<div class="body-content">', "</div>"),
])
async def test_fetch_article_body_dispatches_by_source(source, open_tag, close_tag):
    """소스 문자열에 맞는 파서가 여전히 호출되는지 — 셀렉터맵 축약 후에도 불변이어야 함."""
    from unittest.mock import AsyncMock, MagicMock

    html = _long_para_html(open_tag, close_tag)
    resp = MagicMock()
    resp.status_code = 200
    resp.text = html
    resp.raise_for_status = MagicMock()

    http = AsyncMock()
    http.get = AsyncMock(return_value=resp)

    body = await fetch_article_body("https://example.com/a", source, http=http)
    assert len(body) > 200
