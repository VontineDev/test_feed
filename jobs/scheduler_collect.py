"""jobs/scheduler_collect.py — run_scheduler.py의 피드 수집 순수 헬퍼.

전역 상태(_db_pool/_summary_queue 등)와 완전히 무관 — http/cfg 매개변수만
사용하므로 byte-identical로 분리 가능. collect_job 자체(전역 상태 R/W)는
run_scheduler.py에 그대로 남는다.
"""

from __future__ import annotations

import asyncio
import calendar
import hashlib
import logging
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

import feedparser
import httpx

logger = logging.getLogger(__name__)

MAX_AGE_HOURS = 24  # 이 시간보다 오래된 기사는 수집 제외


# ── 유틸 ─────────────────────────────────────────────────────
def _url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]

def _parse_dt(entry) -> datetime | None:
    """RSS entry → timezone-aware datetime. 파싱 실패 시 None."""
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            ts = calendar.timegm(entry.published_parsed)
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            pass
    return None

def _fmt_date(dt: datetime | None) -> str:
    return dt.strftime("%m-%d %H:%M") if dt else "??-?? ??:??"

def _is_fresh(dt: datetime | None) -> bool:
    """published_at이 None이면 통과(날짜 없는 피드 허용), 있으면 MAX_AGE_HOURS 이내만 허용."""
    if dt is None:
        return True
    age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
    return age_hours <= MAX_AGE_HOURS


# ── 피드 형식별 파서: (url, title, published_dt, summary) 튜플 리스트 반환 ──
def _parse_rss(text: str) -> list[tuple[str, str, datetime | None, str]]:
    parsed = feedparser.parse(text)
    out = []
    for e in parsed.entries:
        url = getattr(e, "link", None)
        if not url:
            continue
        out.append((
            url, getattr(e, "title", ""), _parse_dt(e),
            (getattr(e, "summary", "") or "")[:200],
        ))
    return out


_SITEMAP_NS = {
    "sm":   "http://www.sitemaps.org/schemas/sitemap/0.9",
    "news": "http://www.google.com/schemas/sitemap-news/0.9",
}

def _parse_sitemap_dt(raw: str) -> datetime | None:
    """'2026-08-05T20:10:21+09:00' → timezone-aware UTC datetime."""
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw).astimezone(timezone.utc)
    except ValueError:
        return None

def _parse_sitemap(text: str) -> list[tuple[str, str, datetime | None, str]]:
    """구글 뉴스 사이트맵 XML(news:news 확장) 파서.

    한국경제 RSS(/feed/*)가 Cloudflare JS 챌린지로 막혀(2026-08-05 확인,
    3일 이상 100% 403) 도입 — 같은 도메인의 사이트맵(/sitemap/*.xml)은
    차단되지 않는다. summary는 사이트맵에 없어 항상 빈 문자열.
    """
    root = ET.fromstring(text)
    out = []
    for url_el in root.findall("sm:url", _SITEMAP_NS):
        loc = url_el.findtext("sm:loc", default="", namespaces=_SITEMAP_NS)
        if not loc:
            continue
        news_el = url_el.find("news:news", _SITEMAP_NS)
        title, dt = "", None
        if news_el is not None:
            title = news_el.findtext("news:title", default="", namespaces=_SITEMAP_NS)
            dt = _parse_sitemap_dt(
                news_el.findtext("news:publication_date", default="", namespaces=_SITEMAP_NS)
            )
        out.append((loc, title, dt, ""))
    return out


_PARSERS = {"rss": _parse_rss, "sitemap": _parse_sitemap}

FETCH_RETRY_COUNT = 3      # 최대 재시도 횟수
FETCH_RETRY_DELAY = 2.0    # 초기 대기 시간 (초) — 지수 백오프

# ── 단일 피드 수집 (재시도 포함) ─────────────────────────────
async def fetch_feed(http: httpx.AsyncClient, cfg: dict) -> list[dict]:
    parse = _PARSERS[cfg.get("type", "rss")]
    last_error = None
    for attempt in range(1, FETCH_RETRY_COUNT + 1):
        try:
            r = await http.get(cfg["url"], timeout=15)
            r.raise_for_status()
            entries = parse(r.text)

            articles = []
            skipped = 0
            for url, title, dt, summary in entries:
                if not _is_fresh(dt):
                    skipped += 1
                    continue
                articles.append({
                    "source":       cfg["source"],
                    "category":     cfg["category"],
                    "title":        title,
                    "url":          url,
                    "url_hash":     _url_hash(url),
                    "summary":      summary,
                    "published":    _fmt_date(dt),
                    "published_dt": dt,
                })

            if skipped:
                logger.debug(
                    "  [%s/%s] 낡은 기사 %d건 제외 (24시간 초과)",
                    cfg["source"], cfg["category"], skipped,
                )
            return articles

        except Exception as e:
            last_error = e
            if attempt < FETCH_RETRY_COUNT:
                delay = FETCH_RETRY_DELAY * (2 ** (attempt - 1))  # 2s → 4s → 8s
                logger.warning(
                    "  [%s/%s] 수집 실패 (%d/%d회) — %.0f초 후 재시도: %s",
                    cfg["source"], cfg["category"],
                    attempt, FETCH_RETRY_COUNT, delay, e,
                )
                await asyncio.sleep(delay)
            else:
                logger.warning(
                    "  [%s/%s] 수집 최종 실패 (%d회 시도): %s",
                    cfg["source"], cfg["category"],
                    FETCH_RETRY_COUNT, last_error,
                )
    return []
