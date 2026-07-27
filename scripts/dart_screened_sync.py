"""
dart_screened_sync.py — 스크리닝된 종목의 DART XML 다운로드 + 3-Pass 추출

stage_classifications / chart_signals 에 최근 등장한 종목을 dart_companies 와
매핑하여, XML이 없으면 DART API로 다운로드하고 Ollama 3-Pass로 추출한다.
결과는 dart_extractions 테이블에 upsert 된다.

실행:
    python scripts/dart_screened_sync.py              # 최근 30일, 최대 30개사
    python scripts/dart_screened_sync.py --days 60    # 기간 확대
    python scripts/dart_screened_sync.py --limit 50   # 처리 한도 확대
    python scripts/dart_screened_sync.py --dry-run    # 대상 목록만 출력
    python scripts/dart_screened_sync.py --force      # 이미 추출된 것도 재추출

lib 함수로 import해서 스케줄러에서도 사용 가능:
    from scripts.dart_screened_sync import dart_screened_sync_job
    await dart_screened_sync_job(db_pool, days=30, limit=30)
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)

_DART_DIR = Path(__file__).parent.parent / "reports" / "dart"


# ── 우선순위 점수 ─────────────────────────────────────────────

def _priority_score(
    stage: int | None,
    is_enhanced: bool,
    has_gapjum: bool,
    appearance_count: int,
    attention_q: int | None,
) -> int:
    """낮을수록 먼저 처리.
    트리플콤보(S1·S2 + 스크리너 + attention Q2+) > S1+스크리너 > S1 > S2 > 스크리너 전용.
    """
    base            = {1: 0, 2: 100, None: 200}.get(stage, 200)
    screener_bonus  = -10 if (is_enhanced or has_gapjum) else 0
    attention_bonus = {2: -10, 3: -20, 4: -20, 5: -20}.get(attention_q or 0, 0)
    return base + screener_bonus + attention_bonus - min(appearance_count, 10)


# ── 대상 종목 수집 ────────────────────────────────────────────

async def _collect_targets(pool, days: int, force: bool) -> list[dict]:
    """스크리닝 종목 중 DART 추출 대상을 우선순위 순으로 반환."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH screened AS (
                -- Stage 분류 종목
                SELECT ticker,
                       MAX(stage)           AS stage,
                       COUNT(*)             AS appearance_count,
                       MAX(classified_date) AS last_seen,
                       FALSE                AS is_enhanced,
                       FALSE                AS has_gapjum
                FROM   stage_classifications
                WHERE  classified_date >= CURRENT_DATE - ($1 * INTERVAL '1 day')
                GROUP  BY ticker
                UNION ALL
                -- 차트 스크리너 종목
                SELECT ticker,
                       NULL                                              AS stage,
                       COUNT(*)                                         AS appearance_count,
                       to_date(MAX(week_of) || ' 1', 'IYYY-"W"IW ID') AS last_seen,
                       BOOL_OR(is_enhanced)                             AS is_enhanced,
                       BOOL_OR(has_gapjum)                              AS has_gapjum
                FROM   chart_signals
                WHERE  week_of >= to_char(CURRENT_DATE - ($1 * INTERVAL '1 day'), 'IYYY-"W"IW')
                GROUP  BY ticker
            ),
            deduped AS (
                SELECT ticker,
                       MIN(stage)            AS stage,
                       SUM(appearance_count) AS appearance_count,
                       MAX(last_seen)        AS last_seen,
                       BOOL_OR(is_enhanced)  AS is_enhanced,
                       BOOL_OR(has_gapjum)   AS has_gapjum
                FROM   screened
                GROUP  BY ticker
            ),
            yt AS (
                SELECT ticker,
                       NTILE(5) OVER (ORDER BY attention_score) AS attention_q
                FROM   youtube_attention_scores
                WHERE  window_end = (SELECT MAX(window_end) FROM youtube_attention_scores)
            )
            SELECT d.ticker,
                   COALESCE(tn.name_ko, SPLIT_PART(d.ticker, '.', 1)) AS name,
                   d.stage, d.appearance_count, d.last_seen,
                   d.is_enhanced, d.has_gapjum,
                   yt.attention_q,
                   dc.corp_code, dc.corp_name, dc.stock_code
            FROM   deduped d
            JOIN   dart_companies dc
                   ON dc.stock_code = SPLIT_PART(d.ticker, '.', 1)
                   AND dc.stock_code IS NOT NULL
            LEFT JOIN ticker_names tn ON tn.ticker = d.ticker
            LEFT JOIN yt ON yt.ticker = SPLIT_PART(d.ticker, '.', 1)
            WHERE  ($2::boolean OR NOT EXISTS (
                       SELECT 1 FROM dart_extractions de
                       WHERE  de.corp_name = dc.corp_name
                         AND  de.revenue_json IS NOT NULL
                   ))
            ORDER  BY last_seen DESC
            """,
            days, force,
        )

    targets = []
    for r in rows:
        targets.append({
            "ticker":           r["ticker"],
            "name":             r["name"],
            "stage":            r["stage"],
            "appearance_count": r["appearance_count"],
            "is_enhanced":      r["is_enhanced"],
            "has_gapjum":       r["has_gapjum"],
            "attention_q":      r["attention_q"],
            "corp_code":        r["corp_code"],
            "corp_name":        r["corp_name"],
            "stock_code":       r["stock_code"],
        })

    # 우선순위 정렬
    targets.sort(key=lambda t: _priority_score(
        t["stage"], t["is_enhanced"], t["has_gapjum"],
        t["appearance_count"], t["attention_q"],
    ))
    return targets


# ── 단일 기업 처리 ────────────────────────────────────────────

async def _process_one(
    pool,
    target: dict,
    api_key: str,
    http_client,
    year: str,
) -> str:
    """XML 다운로드(없을 때만) + 3-Pass 추출. 결과 상태 문자열 반환."""
    corp_code = target["corp_code"]
    corp_name = target["corp_name"]
    corp_dir  = _DART_DIR / corp_name

    # ── 1. XML 다운로드 (없을 때만) ─────────────────────────────
    has_xml = corp_dir.exists() and any(corp_dir.rglob("*.xml"))
    if not has_xml:
        logger.info("[dart-screened] 다운로드: %s (%s)", corp_name, corp_code)
        try:
            from data.dart_download import download_reports
            stats = await download_reports(
                corp_map={corp_code: corp_name},
                year=year,
                api_key=api_key,
            )
            if stats["downloaded"] == 0 and stats["failed"] > 0:
                return "download_failed"
            if stats["downloaded"] == 0:
                return "no_report"
        except Exception as e:
            logger.warning("[dart-screened] 다운로드 실패 %s: %s", corp_name, e)
            return "download_error"

        # 다운로드 후 재확인
        has_xml = corp_dir.exists() and any(corp_dir.rglob("*.xml"))
        if not has_xml:
            return "no_xml_after_download"

    # ── 2. 3-Pass Ollama 추출 ────────────────────────────────────
    logger.info("[dart-screened] 추출: %s", corp_name)
    try:
        from data.dart_extractor import extract_all_for_company
        n = await extract_all_for_company(pool, corp_name, http_client)
        return f"extracted:{n}"
    except Exception as e:
        logger.warning("[dart-screened] 추출 실패 %s: %s", corp_name, e)
        return "extract_error"


# ── 메인 진입점 ───────────────────────────────────────────────

async def dart_screened_sync_job(
    pool,
    days:  int  = 30,
    limit: int  = 30,
    force: bool = False,
    dry_run: bool = False,
    year: str | None = None,
) -> dict:
    """
    스크리닝 종목 DART 동기화 잡.

    반환: {"total": N, "extracted": N, "skipped": N, "failed": N}
    """
    api_key = os.environ.get("DART_API_KEY", "")
    if not api_key and not dry_run:
        logger.error("[dart-screened] DART_API_KEY 미설정")
        return {"total": 0, "extracted": 0, "skipped": 0, "failed": 0}

    if year is None:
        year = str(date.today().year)

    # Ollama 가용 확인
    if not dry_run:
        import httpx as _httpx
        from reports.summarizer import _ollama_is_alive
        async with _httpx.AsyncClient() as _probe:
            if not await _ollama_is_alive(_probe):
                logger.error("[dart-screened] Ollama 미응답 — 중단")
                return {"total": 0, "extracted": 0, "skipped": 0, "failed": 0}

    targets = await _collect_targets(pool, days, force)
    targets = targets[:limit]

    stats = {"total": len(targets), "extracted": 0, "skipped": 0, "failed": 0}

    logger.info(
        "[dart-screened] 대상 %d개사 (days=%d limit=%d force=%s dry_run=%s)",
        len(targets), days, limit, force, dry_run,
    )

    for t in targets:
        tag = f"{t['corp_name']}({t['stock_code']}) S{t['stage'] or '-'}"
        if dry_run:
            badges = ""
            if t["is_enhanced"]: badges += " [강화]"
            if t["has_gapjum"]:  badges += " [갭]"
            if t["attention_q"]: badges += f" Q{t['attention_q']}"
            score = _priority_score(
                t["stage"], t["is_enhanced"], t["has_gapjum"],
                t["appearance_count"], t["attention_q"],
            )
            print(f"  {tag}{badges} 등장{t['appearance_count']}회  (score={score})")
            continue

        import httpx as _httpx
        async with _httpx.AsyncClient() as http:
            status = await _process_one(pool, t, api_key, http, year)

        if status.startswith("extracted"):
            n = status.split(":")[1]
            logger.info("[dart-screened] ✓ %s → %s건 추출", tag, n)
            stats["extracted"] += 1
        elif status in ("no_report", "no_xml_after_download"):
            logger.info("[dart-screened] - %s → 보고서 없음", tag)
            stats["skipped"] += 1
        else:
            logger.warning("[dart-screened] ✗ %s → %s", tag, status)
            stats["failed"] += 1

        await asyncio.sleep(2.0)  # DART API 부하 방지

    logger.info(
        "[dart-screened] 완료 — 추출:%d 스킵:%d 실패:%d",
        stats["extracted"], stats["skipped"], stats["failed"],
    )
    return stats


# ── CLI ───────────────────────────────────────────────────────

async def _main() -> None:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    ap = argparse.ArgumentParser(description="스크리닝 종목 DART 동기화")
    ap.add_argument("--days",    type=int, default=30,    help="스크리닝 참조 기간 (일)")
    ap.add_argument("--limit",   type=int, default=30,    help="최대 처리 기업 수")
    ap.add_argument("--year",    type=str, default=None,  help="보고서 연도 (기본: 올해)")
    ap.add_argument("--force",   action="store_true",     help="이미 추출된 것도 재추출")
    ap.add_argument("--dry-run", action="store_true",     help="대상 목록만 출력")
    args = ap.parse_args()

    from core.db import create_pool
    pool = await create_pool()
    try:
        await dart_screened_sync_job(
            pool,
            days=args.days,
            limit=args.limit,
            force=args.force,
            dry_run=args.dry_run,
            year=args.year,
        )
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
