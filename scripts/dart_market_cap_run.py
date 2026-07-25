"""
dart_market_cap_run.py — 시가총액 순위 기준 DART 수집 + 추출

1. Naver Finance에서 KOSPI/KOSDAQ 시가총액 순위 수집
2. dart_companies와 매핑하여 reports/dart/ 미존재 기업 파악
3. 시가총액 순으로 DART XML 다운로드 (사업보고서/반기/분기)
4. Ollama 3-Pass 추출

실행:
    python scripts/dart_market_cap_run.py
    python scripts/dart_market_cap_run.py --dry-run        # 대상 목록만 출력
    python scripts/dart_market_cap_run.py --limit 50       # 최대 50개사
    python scripts/dart_market_cap_run.py --years 2024 2025 2026
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from pathlib import Path

import httpx
import psycopg2
from bs4 import BeautifulSoup, Tag

sys.path.insert(0, str(Path(__file__).parent.parent))
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

_DART_DIR = Path(__file__).parent.parent / "reports" / "dart"


# ── Naver Finance 시총 순위 ────────────────────────────────────

def _fetch_naver_market_cap(sosok: int, pages: int = 5) -> list[dict]:
    """Naver Finance 시총 순위. sosok=0:KOSPI, 1:KOSDAQ"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://finance.naver.com/",
    }
    result = []
    for page in range(1, pages + 1):
        try:
            resp = httpx.get(
                "https://finance.naver.com/sise/sise_market_sum.nhn",
                params={"sosok": str(sosok), "page": str(page)},
                headers=headers,
                timeout=15,
            )
            soup = BeautifulSoup(resp.text, "html.parser")
            table = soup.find("table", class_="type_2")
            if not table:
                break
            for row in table.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) < 7:
                    continue
                a = row.find("a", href=True)
                if not isinstance(a, Tag):
                    continue
                href = str(a.get("href", ""))
                if "code=" not in href:
                    continue
                code = href.split("code=")[-1][:6]
                name = a.text.strip()
                cap_str = cols[6].text.strip().replace(",", "")
                try:
                    cap = int(cap_str)
                except ValueError:
                    continue
                result.append({"code": code, "name": name, "market_cap_억": cap})
        except Exception as e:
            logger.warning("Naver 페이지 %d 실패: %s", page, e)
        time.sleep(0.2)
    return result


def get_market_cap_ranking(kospi_pages: int = 5, kosdaq_pages: int = 4) -> list[dict]:
    """KOSPI + KOSDAQ 시총 순위 합산 정렬."""
    logger.info("KOSPI 시가총액 순위 수집 중...")
    kospi = _fetch_naver_market_cap(0, kospi_pages)
    logger.info("KOSDAQ 시가총액 순위 수집 중...")
    kosdaq = _fetch_naver_market_cap(1, kosdaq_pages)
    all_stocks = kospi + kosdaq
    all_stocks.sort(key=lambda x: -x["market_cap_억"])
    logger.info("총 %d개 종목 수집 완료", len(all_stocks))
    return all_stocks


# ── DB 매핑 ───────────────────────────────────────────────────

def _get_pg_conn():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )


def get_download_targets(all_stocks: list[dict], limit: int) -> list[dict]:
    """시총 순위 기업 중 reports/dart/ 미존재 기업을 반환."""
    conn = _get_pg_conn()
    cur = conn.cursor()

    codes = tuple(s["code"] for s in all_stocks[:limit * 2])  # 여유분
    cur.execute(
        "SELECT corp_code, corp_name, stock_code FROM dart_companies WHERE stock_code IN %s",
        (codes,),
    )
    rows = cur.fetchall()
    conn.close()

    code_to_corp = {r[2]: (r[0], r[1]) for r in rows}
    existing_dirs = set(d.name for d in _DART_DIR.iterdir() if d.is_dir()) if _DART_DIR.exists() else set()

    targets = []
    for s in all_stocks:
        code = s["code"]
        if code not in code_to_corp:
            continue
        corp_code, corp_name = code_to_corp[code]
        if corp_name in existing_dirs:
            continue
        targets.append({
            "code": code,
            "corp_code": corp_code,
            "corp_name": corp_name,
            "name": s["name"],
            "market_cap_조": round(s["market_cap_억"] / 10000, 1),
        })
        if len(targets) >= limit:
            break

    return targets


# ── 다운로드 + 추출 ───────────────────────────────────────────

async def run_download_and_extract(
    targets: list[dict],
    years: list[str],
    dry_run: bool = False,
) -> None:
    from data.dart_download import download_reports
    from data.dart_extractor import extract_all_for_company, _ollama_is_alive as _ext_ollama_check
    from core.db import create_pool, init_db

    api_key = os.environ.get("DART_API_KEY")
    if not api_key:
        logger.error("DART_API_KEY 미설정")
        return

    pool = await create_pool()
    await init_db(pool)

    total_dl = 0
    total_ext = 0

    async with httpx.AsyncClient() as http_client:
        # Ollama 상태 확인
        from reports.summarizer import _ollama_is_alive
        try:
            ollama_ok = await _ollama_is_alive(http_client)
        except Exception:
            ollama_ok = False
        if not ollama_ok:
            logger.warning("Ollama 미응답 — 다운로드만 수행 (추출 건너뜀)")

        for i, t in enumerate(targets, 1):
            corp_code = t["corp_code"]
            corp_name = t["corp_name"]
            mc = t["market_cap_조"]
            logger.info(
                "[%d/%d] %s (%s) — 시총 %.1f조",
                i, len(targets), corp_name, t["code"], mc,
            )

            if dry_run:
                continue

            # ── 다운로드 ──────────────────────────────────────
            corp_map = {corp_code: corp_name}
            dl_results = {"downloaded": 0, "skipped": 0, "failed": 0}
            for year in years:
                try:
                    r = await download_reports(
                        corp_map=corp_map,
                        year=year,
                        api_key=api_key,
                        base_dir=_DART_DIR,
                    )
                    for k in dl_results:
                        dl_results[k] += r.get(k, 0)
                except Exception as e:
                    logger.error("  [다운로드 오류] %s %s: %s", corp_name, year, e)

            logger.info(
                "  다운로드 결과: 신규=%d, 스킵=%d, 실패=%d",
                dl_results["downloaded"], dl_results["skipped"], dl_results["failed"],
            )
            total_dl += dl_results["downloaded"]

            # ── 추출 ─────────────────────────────────────────
            if not ollama_ok:
                continue

            if not (_DART_DIR / corp_name).exists():
                logger.info("  보고서 디렉터리 없음 (다운로드 없었음) — 추출 건너뜀")
                continue

            try:
                n = await extract_all_for_company(
                    pool=pool,
                    corp_name=corp_name,
                    http=http_client,
                )
                logger.info("  추출 완료: %d건", n)
                total_ext += n
            except Exception as e:
                logger.error("  [추출 오류] %s: %s", corp_name, e)

    logger.info(
        "=== 완료 — 다운로드 %d건, 추출 %d건 ===",
        total_dl, total_ext,
    )
    await pool.close()


# ── CLI ───────────────────────────────────────────────────────

async def _main() -> None:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

    parser = argparse.ArgumentParser(description="시가총액 순 DART 수집 + 추출")
    parser.add_argument("--limit", type=int, default=100, help="최대 처리 기업 수 (기본 100)")
    parser.add_argument("--years", nargs="+", default=["2024", "2025", "2026"], help="수집 연도")
    parser.add_argument("--dry-run", action="store_true", help="대상 목록만 출력, 실행 안 함")
    args = parser.parse_args()

    all_stocks = get_market_cap_ranking(kospi_pages=5, kosdaq_pages=4)
    targets = get_download_targets(all_stocks, limit=args.limit)

    logger.info("다운로드 대상: %d개 기업", len(targets))
    for i, t in enumerate(targets[:20], 1):
        logger.info("  %2d. %s (%s) %.1f조", i, t["corp_name"], t["code"], t["market_cap_조"])
    if len(targets) > 20:
        logger.info("  ... 이하 %d개 생략", len(targets) - 20)

    if args.dry_run:
        return

    await run_download_and_extract(
        targets=targets,
        years=args.years,
        dry_run=False,
    )


if __name__ == "__main__":
    asyncio.run(_main())
