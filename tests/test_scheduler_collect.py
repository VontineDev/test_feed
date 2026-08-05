"""jobs/scheduler_collect.py 단위 테스트 — 특히 사이트맵 파서(_parse_sitemap).

2026-08-05: 한국경제 RSS(/feed/*)가 Cloudflare JS 챌린지로 3일+ 100% 403 차단된 것을
확인 — 같은 도메인의 뉴스 사이트맵(/sitemap/latest-article.xml)으로 대체했다.
fetch_feed()가 cfg["type"]로 rss/sitemap 파서를 올바르게 디스패치하는지,
사이트맵 XML에서 url/title/날짜를 정확히 뽑아내는지 검증한다.
"""
from datetime import datetime, timezone

import httpx
import pytest

from jobs.scheduler_collect import (
    _is_fresh,
    _parse_sitemap,
    _parse_sitemap_dt,
    fetch_feed,
)

_SITEMAP_XML = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
        xmlns:news="http://www.google.com/schemas/sitemap-news/0.9"
        xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">
    <url>
        <loc>https://www.hankyung.com/article/2026080570601</loc>
        <news:news>
            <news:publication>
                <news:name>한국경제</news:name>
                <news:language>ko</news:language>
            </news:publication>
            <news:publication_date>2026-08-05T20:10:21+09:00</news:publication_date>
            <news:title><![CDATA[삼성전자 신고가 경신]]></news:title>
        </news:news>
        <image:image>
            <image:loc>https://img.hankyung.com/photo/x.jpg</image:loc>
        </image:image>
    </url>
    <url>
        <loc>https://www.hankyung.com/article/no-title-entry</loc>
    </url>
</urlset>
"""


class TestParseSitemapDt:
    def test_parses_iso8601_with_positive_offset(self):
        dt = _parse_sitemap_dt("2026-08-05T20:10:21+09:00")
        assert dt is not None
        assert dt.tzinfo is not None
        # KST 20:10 -> UTC 11:10
        assert dt.astimezone(timezone.utc).hour == 11

    def test_empty_string_returns_none(self):
        assert _parse_sitemap_dt("") is None

    def test_malformed_string_returns_none(self):
        assert _parse_sitemap_dt("not-a-date") is None


class TestParseSitemap:
    def test_extracts_url_title_and_date(self):
        entries = _parse_sitemap(_SITEMAP_XML)
        assert len(entries) == 2
        url, title, dt, summary = entries[0]
        assert url == "https://www.hankyung.com/article/2026080570601"
        assert title == "삼성전자 신고가 경신"
        assert dt is not None
        assert summary == ""  # 사이트맵엔 summary 없음

    def test_entry_without_news_block_has_empty_title_and_none_date(self):
        entries = _parse_sitemap(_SITEMAP_XML)
        url, title, dt, summary = entries[1]
        assert url == "https://www.hankyung.com/article/no-title-entry"
        assert title == ""
        assert dt is None

    def test_entry_without_loc_is_skipped(self):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url></url>
        </urlset>"""
        assert _parse_sitemap(xml) == []


class TestFetchFeedDispatch:
    @pytest.mark.asyncio
    async def test_sitemap_type_uses_sitemap_parser(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, text=_SITEMAP_XML)

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as http:
            cfg = {
                "source": "hankyung", "category": "korea", "type": "sitemap",
                "url": "https://www.hankyung.com/sitemap/latest-article.xml",
            }
            articles = await fetch_feed(http, cfg)

        # 날짜 없는 entry(2번째)는 _is_fresh(None)=True라 통과, 둘 다 포함됨
        assert len(articles) == 2
        assert articles[0]["source"] == "hankyung"
        assert articles[0]["title"] == "삼성전자 신고가 경신"
        assert articles[0]["url"] == "https://www.hankyung.com/article/2026080570601"

    @pytest.mark.asyncio
    async def test_missing_type_defaults_to_rss(self):
        rss_xml = """<?xml version="1.0"?>
        <rss version="2.0"><channel>
            <item><title>t</title><link>https://example.com/a</link></item>
        </channel></rss>"""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, text=rss_xml)

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as http:
            cfg = {"source": "yonhap", "category": "korea",
                   "url": "https://example.com/rss.xml"}
            articles = await fetch_feed(http, cfg)

        assert len(articles) == 1
        assert articles[0]["url"] == "https://example.com/a"

    @pytest.mark.asyncio
    async def test_stale_sitemap_entry_filtered_out(self):
        stale_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
                xmlns:news="http://www.google.com/schemas/sitemap-news/0.9">
            <url>
                <loc>https://www.hankyung.com/article/old</loc>
                <news:news>
                    <news:publication_date>2020-01-01T00:00:00+09:00</news:publication_date>
                    <news:title>old news</news:title>
                </news:news>
            </url>
        </urlset>"""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, text=stale_xml)

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as http:
            cfg = {
                "source": "hankyung", "category": "korea", "type": "sitemap",
                "url": "https://www.hankyung.com/sitemap/latest-article.xml",
            }
            articles = await fetch_feed(http, cfg)

        assert articles == []

    @pytest.mark.asyncio
    async def test_403_retries_then_returns_empty(self):
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(403, text="Just a moment...")

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as http:
            cfg = {
                "source": "hankyung", "category": "korea", "type": "sitemap",
                "url": "https://www.hankyung.com/sitemap/latest-article.xml",
            }
            from jobs import scheduler_collect
            orig_delay = scheduler_collect.FETCH_RETRY_DELAY
            scheduler_collect.FETCH_RETRY_DELAY = 0  # 테스트 속도
            try:
                articles = await fetch_feed(http, cfg)
            finally:
                scheduler_collect.FETCH_RETRY_DELAY = orig_delay

        assert articles == []
        assert calls["n"] == 3  # FETCH_RETRY_COUNT


def test_is_fresh_none_datetime_passes():
    assert _is_fresh(None) is True


def test_is_fresh_recent_datetime_passes():
    assert _is_fresh(datetime.now(timezone.utc)) is True
