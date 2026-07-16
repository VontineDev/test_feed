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


FETCH_RETRY_COUNT = 3      # 최대 재시도 횟수
FETCH_RETRY_DELAY = 2.0    # 초기 대기 시간 (초) — 지수 백오프

# ── 단일 피드 수집 (재시도 포함) ─────────────────────────────
async def fetch_feed(http: httpx.AsyncClient, cfg: dict) -> list[dict]:
    last_error = None
    for attempt in range(1, FETCH_RETRY_COUNT + 1):
        try:
            r = await http.get(cfg["url"], timeout=15)
            r.raise_for_status()
            parsed = feedparser.parse(r.text)

            articles = []
            skipped = 0
            for e in parsed.entries:
                if not getattr(e, "link", None):
                    continue
                dt = _parse_dt(e)
                if not _is_fresh(dt):
                    skipped += 1
                    continue
                url = getattr(e, "link", "")
                articles.append({
                    "source":       cfg["source"],
                    "category":     cfg["category"],
                    "title":        getattr(e, "title", ""),
                    "url":          url,
                    "url_hash":     _url_hash(url),
                    "summary":      (getattr(e, "summary", "") or "")[:200],
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
