"""
test_feeds.py  —  로컬에서 실행: python test_feeds.py
pip install feedparser httpx 으로 의존성 설치 후 실행하세요.
DB/Claude API 키 불필요 — 순수 크롤링만 테스트합니다.
"""

import asyncio
import feedparser
import httpx
from datetime import datetime, timezone
import calendar

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

FEEDS = [
    {"source": "reuters", "category": "markets", "url": "https://news.google.com/rss/search?q=when:24h+allinurl:reuters.com+markets&ceid=US:en&hl=en-US&gl=US"},
    {"source": "reuters", "category": "macro",   "url": "https://news.google.com/rss/search?q=when:24h+allinurl:reuters.com+economy+fed&ceid=US:en&hl=en-US&gl=US"},
    {"source": "investing",  "category": "markets", "url": "https://www.investing.com/rss/news_25.rss"},
    {"source": "investing",  "category": "macro",   "url": "https://www.investing.com/rss/news_14.rss"},
    {"source": "investing",  "category": "korea",   "url": "https://www.investing.com/rss/news_285.rss"},
    {"source": "cnbc",      "category": "markets", "url": "https://www.cnbc.com/id/10001147/device/rss/rss.html"},
    {"source": "cnbc",      "category": "macro",   "url": "https://www.cnbc.com/id/20910258/device/rss/rss.html"},
    {"source": "cnbc",      "category": "korea",   "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100727362"},
]


def parse_published(entry):
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            ts = calendar.timegm(entry.published_parsed)
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            pass
    return "날짜 없음"


async def fetch_one(http: httpx.AsyncClient, cfg: dict):
    try:
        r = await http.get(cfg["url"], timeout=15)
        r.raise_for_status()
        parsed = feedparser.parse(r.text)
        return cfg["source"], cfg["category"], "OK", parsed.entries
    except httpx.HTTPStatusError as e:
        return cfg["source"], cfg["category"], f"HTTP {e.response.status_code}", []
    except Exception as e:
        return cfg["source"], cfg["category"], f"ERR: {e}", []


async def main():
    print("=" * 60)
    print("뉴스 크롤러 피드 테스트")
    print("=" * 60)

    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as http:
        tasks = [fetch_one(http, cfg) for cfg in FEEDS]
        results = await asyncio.gather(*tasks)

    # ── 결과 요약표 ──────────────────────────────────
    print(f"\n{'소스':<12} {'카테고리':<10} {'상태':<8} {'기사수':>6}")
    print("─" * 44)

    all_ok = []
    for source, cat, status, entries in results:
        flag = "✓" if status == "OK" else "✗"
        print(f"{flag} {source:<10} {cat:<10} {status:<8} {len(entries):>5}건")
        if status == "OK":
            all_ok.append((source, cat, entries))

    # ── 성공 피드의 기사 샘플 출력 ───────────────────
    if not all_ok:
        print("\n[!] 수집된 기사가 없습니다.")
        print("    → 403이면 IP 차단: VPN 해제 후 재시도")
        print("    → 타임아웃이면 네트워크 확인")
        return

    print(f"\n\n{'═'*60}")
    print("기사 샘플 (소스별 상위 3건)")
    print('═'*60)

    for source, cat, entries in all_ok:
        print(f"\n▶ [{source.upper()} / {cat}] — 총 {len(entries)}건")
        for i, e in enumerate(entries[:3]):
            title   = getattr(e, "title",   "제목 없음")
            link    = getattr(e, "link",    "URL 없음")
            summary = getattr(e, "summary", "") or getattr(e, "description", "")
            pub     = parse_published(e)
            print(f"\n  [{i+1}] {title[:72]}")
            print(f"       날짜: {pub}")
            print(f"       URL:  {link[:70]}")
            if summary:
                clean = summary.replace("\n", " ").strip()
                print(f"       요약: {clean[:100]}{'...' if len(clean)>100 else ''}")

    print(f"\n\n{'='*60}")
    print(f"테스트 완료 — 성공 {len(all_ok)}/{len(FEEDS)}개 피드")
    print('='*60)


if __name__ == "__main__":
    asyncio.run(main())
