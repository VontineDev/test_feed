"""
youtube_backfill_monthly.py — 삼프로TV 내러티브 월별 순차 백필

단계:
  sync          각 월 영상 수집 + LLM 추출 → youtube_mention_raw
  fill-returns  yfinance로 forward return 채우기 → youtube_mention_forward_returns
  scores        날짜별 attention_score 집계 → youtube_attention_scores
  all           세 단계 순서대로 실행 (기본값)

사용법:
  python scripts/youtube_backfill_monthly.py
  python scripts/youtube_backfill_monthly.py --from 2026-02 --to 2026-05
  python scripts/youtube_backfill_monthly.py --step sync
  python scripts/youtube_backfill_monthly.py --step fill-returns
  python scripts/youtube_backfill_monthly.py --step scores --from 2026-01 --to 2026-05

순서 의존성:
  sync는 반드시 1월 → 2월 → ... 순서로 실행.
  2월 attention_score rolling window가 1월 데이터를 참조하기 때문.

재실행 안전:
  sync: UNIQUE(video_id, stock_name_raw, source_quote) → 중복 스킵
  fill-returns: ON CONFLICT DO UPDATE SET ... COALESCE → 부분 채움 보존
  scores: ON CONFLICT DO UPDATE → 덮어쓰기 무해
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from calendar import monthrange
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=True)
except ImportError:
    pass

logger = logging.getLogger(__name__)

_DEFAULT_FROM = "2026-01"
_DEFAULT_TO   = "2026-05"


# ── 유틸 ──────────────────────────────────────────────────────────────

def _iter_months(from_ym: str, to_ym: str):
    """YYYY-MM 범위 내 각 월의 (시작일, 종료일) 생성."""
    y, m = map(int, from_ym.split("-"))
    ey, em = map(int, to_ym.split("-"))
    while (y, m) <= (ey, em):
        last_day = monthrange(y, m)[1]
        yield date(y, m, 1), date(y, m, last_day)
        m += 1
        if m > 12:
            m, y = 1, y + 1


def _get_api_keys() -> tuple[str, str]:
    api_key    = os.environ.get("YOUTUBE_API_KEY", "")
    gemini_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise SystemExit("YOUTUBE_API_KEY 미설정")
    if not gemini_key:
        raise SystemExit("GEMINI_API_KEY 미설정")
    return api_key, gemini_key


def _get_dsn() -> str:
    try:
        from core.db import get_dsn
        return get_dsn()
    except Exception:
        dsn = os.environ.get("DATABASE_URL", "")
        if not dsn:
            raise SystemExit("DATABASE_URL 미설정")
        return dsn


# ── 단계별 함수 ───────────────────────────────────────────────────────

def step_sync(dsn: str, from_ym: str, to_ym: str) -> int:
    """월별 영상 수집 + LLM 추출 → youtube_mention_raw."""
    from data.youtube_narrative_sync import run_sync, ensure_tables
    api_key, gemini_key = _get_api_keys()
    ensure_tables(dsn)

    months = list(_iter_months(from_ym, to_ym))
    total  = 0
    failed = []

    for i, (start, end) in enumerate(months, 1):
        label = start.strftime("%Y-%m")
        logger.info("━" * 55)
        logger.info("[sync] %d/%d  %s  (%s ~ %s)", i, len(months), label, start, end)
        try:
            n = run_sync(dsn, api_key, gemini_key, start, end)
            total += n
            logger.info("[sync] %s 완료 — %d건 저장", label, n)
        except Exception as e:
            logger.error("[sync] %s 실패: %s — 다음 달로 계속", label, e)
            failed.append(label)

    logger.info("━" * 55)
    logger.info("[sync] 전체 완료: %d건 저장", total)
    if failed:
        logger.warning("[sync] 실패 월: %s", ", ".join(failed))
    return total


def step_fill_returns(dsn: str) -> int:
    """미채움 레코드에 yfinance forward return 채우기."""
    from data.youtube_narrative_sync import fill_forward_returns
    logger.info("━" * 55)
    logger.info("[fill-returns] forward return 채우기 시작")
    n = fill_forward_returns(dsn)
    logger.info("[fill-returns] 완료: %d건", n)

    # 500건 배치 제한 — 미채움 잔여분 있으면 재실행
    if n > 0:
        logger.info("[fill-returns] 잔여분 확인 중...")
        n2 = fill_forward_returns(dsn)
        if n2:
            logger.info("[fill-returns] 추가 %d건. 완전히 채우려면 --step fill-returns 재실행.", n2)
    return n


def step_scores(dsn: str, from_ym: str, to_ym: str) -> int:
    """날짜별 rolling attention_score 집계."""
    from data.youtube_narrative_sync import compute_attention_scores
    months  = list(_iter_months(from_ym, to_ym))
    start_d = months[0][0]
    end_d   = months[-1][1]

    logger.info("━" * 55)
    logger.info("[scores] attention_score 집계: %s ~ %s", start_d, end_d)

    current = start_d
    count   = 0
    while current <= end_d:
        try:
            compute_attention_scores(dsn, window_end=current)
            count += 1
        except Exception as e:
            logger.warning("[scores] %s 실패: %s", current, e)
        current += timedelta(days=1)

    logger.info("[scores] 완료: %d일 집계", count)
    return count


# ── 진입점 ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="YouTube 내러티브 월별 순차 백필",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python scripts/youtube_backfill_monthly.py                      # 1~5월 전체
  python scripts/youtube_backfill_monthly.py --from 2026-02       # 2~5월
  python scripts/youtube_backfill_monthly.py --step sync          # sync만
  python scripts/youtube_backfill_monthly.py --step fill-returns  # return만
  python scripts/youtube_backfill_monthly.py --step scores        # 집계만
        """,
    )
    parser.add_argument("--from", dest="from_ym", default=_DEFAULT_FROM,
                        metavar="YYYY-MM", help=f"시작 월 (기본: {_DEFAULT_FROM})")
    parser.add_argument("--to",   dest="to_ym",   default=_DEFAULT_TO,
                        metavar="YYYY-MM", help=f"종료 월 (기본: {_DEFAULT_TO})")
    parser.add_argument("--step", default="all",
                        choices=["sync", "fill-returns", "scores", "all"],
                        help="실행 단계 (기본: all = sync → fill-returns → scores)")
    args = parser.parse_args()

    dsn = _get_dsn()

    logger.info("백필 범위: %s ~ %s  /  단계: %s", args.from_ym, args.to_ym, args.step)

    if args.step == "sync":
        step_sync(dsn, args.from_ym, args.to_ym)
    elif args.step == "fill-returns":
        step_fill_returns(dsn)
    elif args.step == "scores":
        step_scores(dsn, args.from_ym, args.to_ym)
    else:
        step_sync(dsn, args.from_ym, args.to_ym)
        step_fill_returns(dsn)
        step_scores(dsn, args.from_ym, args.to_ym)
