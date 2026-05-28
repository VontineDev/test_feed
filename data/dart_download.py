"""
dart_download.py — DART 사업보고서 원문 로컬 다운로드

기업별 폴더 생성 후 정기보고서(사업보고서·반기보고서·분기보고서) 원문을 압축 해제하여 저장.
이미 다운로드된 보고서는 자동 스킵.

저장 구조:
  reports/dart/
    {기업명}/
      {rcept_no}_{보고서명}/      ← 압축 해제 파일들
        BIZ_YYYYMMDD_XXXXXX.xml
        ...
      {rcept_no}_meta.json        ← 보고서 메타데이터

사용법:
  python data/dart_download.py                           # 2026년 Top 20 전체
  python data/dart_download.py --year 2025               # 2025년
  python data/dart_download.py --year 2026 --corp 005930  # 삼성전자만
  python data/dart_download.py --year 2026 --type 사업보고서  # 사업보고서만
  python data/dart_download.py --year 2026 --dry-run      # 다운로드 없이 목록만
  python data/dart_download.py --year 2026 --zip          # 압축 해제 대신 ZIP 저장
"""
from __future__ import annotations

import argparse
import asyncio
import io
import json
import logging
import os
import re
import sys
import zipfile
from datetime import date
from pathlib import Path
from typing import Optional

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)

# 기본 저장 경로
_BASE_DIR = Path(__file__).parent.parent / "reports" / "dart"

# 수집 대상 정기보고서 키워드 (부분 일치)
_REPORT_KEYWORDS = {"사업보고서", "반기보고서", "분기보고서"}

# Top 20 코스피 종목코드 (6자리)
_TOP20_STOCK_CODES = [
    "005930",  # 삼성전자
    "000660",  # SK하이닉스
    "207940",  # 삼성바이오로직스
    "005380",  # 현대차
    "068270",  # 셀트리온
    "005490",  # POSCO홀딩스
    "035420",  # NAVER
    "051910",  # LG화학
    "000270",  # 기아
    "028260",  # 삼성물산
    "012330",  # 현대모비스
    "066570",  # LG전자
    "003670",  # 포스코퓨처엠
    "032830",  # 삼성생명
    "055550",  # 신한지주
    "105560",  # KB금융
    "035720",  # 카카오
    "096770",  # SK이노베이션
    "017670",  # SK텔레콤
    "030200",  # KT
]


# ── 유틸 ──────────────────────────────────────────────────────

def _safe_name(name: str) -> str:
    """파일/디렉터리명에 쓸 수 없는 문자 제거."""
    return re.sub(r'[\\/:*?"<>|\[\]]', "_", name).strip().strip(".")


def _report_type(report_nm: str) -> Optional[str]:
    """보고서명에서 종류 추출. 해당 없으면 None."""
    for kw in _REPORT_KEYWORDS:
        if kw in report_nm:
            return kw
    return None


# ── 다운로드 핵심 ─────────────────────────────────────────────

async def download_one(
    client,
    rcept_no: str,
    corp_name: str,
    report_nm: str,
    corp_code: str,
    rcept_dt: str,
    target_dir: Path,
    meta_path: Path,
    save_zip: bool,
) -> bool:
    """
    단일 보고서 다운로드 + 저장.
    반환: True=성공, False=실패
    """
    try:
        zip_bytes = await client.fetch_document_zip(rcept_no)
    except Exception as e:
        logger.error("  [오류] %s — 다운로드 실패: %s", report_nm, e)
        return False

    if not zip_bytes:
        logger.warning("  [경고] %s — 빈 응답", report_nm)
        return False

    try:
        if save_zip:
            # ZIP 파일 그대로 저장
            target_dir.parent.mkdir(parents=True, exist_ok=True)
            zip_path = target_dir.with_suffix(".zip")
            zip_path.write_bytes(zip_bytes)
            file_list = [zip_path.name]
            logger.info("  [완료] %s → %s (%.1f MB)", report_nm, zip_path.name, len(zip_bytes) / 1_048_576)
        else:
            # 압축 해제하여 저장
            target_dir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                # 파일명 인코딩 처리 (EUC-KR 레거시 ZIP 대응)
                for member in zf.infolist():
                    try:
                        fname = member.filename.encode("cp437").decode("cp949")
                    except Exception:
                        fname = member.filename
                    out_path = target_dir / _safe_name(fname)
                    out_path.write_bytes(zf.read(member))
            file_list = [p.name for p in sorted(target_dir.iterdir())]
            logger.info(
                "  [완료] %s → %s (%d개 파일, %.1f MB)",
                report_nm, target_dir.name, len(file_list), len(zip_bytes) / 1_048_576,
            )
    except zipfile.BadZipFile:
        logger.error("  [오류] %s — ZIP 파싱 실패 (DART API 응답 오류)", report_nm)
        return False

    # 메타데이터 저장
    meta = {
        "rcept_no": rcept_no,
        "corp_code": corp_code,
        "corp_name": corp_name,
        "report_nm": report_nm,
        "rcept_dt": rcept_dt,
        "downloaded_at": date.today().isoformat(),
        "size_bytes": len(zip_bytes),
        "files": file_list,
    }
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


async def download_reports(
    corp_map: dict[str, str],          # {corp_code: corp_name}
    year: str,
    api_key: str,
    report_filter: Optional[set[str]] = None,
    dry_run: bool = False,
    save_zip: bool = False,
    base_dir: Path = _BASE_DIR,
) -> dict:
    """
    corp_map 기업에 대해 year년 정기보고서를 base_dir에 저장.
    반환: {"downloaded": N, "skipped": N, "failed": N}
    """
    from data.dart_sync import DartClient

    client = DartClient(api_key)
    stats = {"downloaded": 0, "skipped": 0, "failed": 0, "total": 0}

    try:
        for corp_code, corp_name in corp_map.items():
            logger.info("▶ %s (%s)", corp_name, corp_code)

            try:
                items = await client.fetch_disclosures(
                    corp_code,
                    bgn_de=f"{year}0101",
                    end_de=f"{year}1231",
                    pblntf_ty="A",
                )
            except Exception as e:
                logger.warning("  [경고] 공시 목록 조회 실패: %s", e)
                continue

            # 정기보고서만 필터
            reports = [
                item for item in items
                if _report_type(item.get("report_nm", "")) is not None
                and (report_filter is None or _report_type(item.get("report_nm", "")) in report_filter)
            ]

            if not reports:
                logger.info("  해당 보고서 없음")
                continue

            safe_corp = _safe_name(corp_name)
            corp_dir = base_dir / safe_corp

            for item in reports:
                rcept_no  = (item.get("rcept_no") or "").strip()
                report_nm = (item.get("report_nm") or "").strip()
                rcept_dt  = (item.get("rcept_dt") or "").strip()
                if not rcept_no:
                    continue

                stats["total"] += 1
                safe_report = _safe_name(report_nm)
                target_dir = corp_dir / f"{rcept_no}_{safe_report}"
                meta_path  = corp_dir / f"{rcept_no}_meta.json"

                # 이미 존재하면 스킵
                already = (save_zip and target_dir.with_suffix(".zip").exists()) or \
                          (not save_zip and target_dir.exists() and meta_path.exists())
                if already:
                    logger.info("  [스킵] %s (이미 존재)", report_nm)
                    stats["skipped"] += 1
                    continue

                logger.info("  [시작] %s (rcept=%s %s)", report_nm, rcept_no, rcept_dt)

                if dry_run:
                    stats["downloaded"] += 1
                    continue

                ok = await download_one(
                    client=client,
                    rcept_no=rcept_no,
                    corp_name=corp_name,
                    report_nm=report_nm,
                    corp_code=corp_code,
                    rcept_dt=rcept_dt,
                    target_dir=target_dir,
                    meta_path=meta_path,
                    save_zip=save_zip,
                )
                if ok:
                    stats["downloaded"] += 1
                else:
                    stats["failed"] += 1

                await asyncio.sleep(1.5)  # DART API 부하 방지

    finally:
        await client.aclose()

    return stats


# ── corp_code 조회 헬퍼 ───────────────────────────────────────

async def _resolve_corp_map(
    corp_arg: Optional[str],
    pool=None,
) -> dict[str, str]:
    """
    CLI --corp 인수 → {corp_code: corp_name} 매핑.
    인수 없으면 Top 20 전체.
    """
    if corp_arg:
        code = corp_arg.strip()
        # DB에서 corp_name 조회
        if pool:
            async with pool.acquire() as conn:
                if len(code) == 6:
                    row = await conn.fetchrow(
                        "SELECT corp_code, corp_name FROM dart_companies WHERE stock_code=$1", code
                    )
                else:
                    row = await conn.fetchrow(
                        "SELECT corp_code, corp_name FROM dart_companies WHERE corp_code=$1", code
                    )
            if row:
                return {row["corp_code"]: row["corp_name"]}
        # DB 없거나 미발견 → 임시 이름
        return {code: code}

    # Top 20 전체
    if pool:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT corp_code, corp_name FROM dart_companies
                WHERE stock_code = ANY($1)
                ORDER BY stock_code
                """,
                _TOP20_STOCK_CODES,
            )
        if rows:
            return {r["corp_code"]: r["corp_name"] for r in rows}

    # DB 없으면 종목코드를 corp_code로 사용 (API 응답에서 corp_name 보완됨)
    logger.warning("DB 없음 — API 응답에서 기업명 자동 수집")
    return {code: code for code in _TOP20_STOCK_CODES}


# ── CLI ───────────────────────────────────────────────────────

async def _main():
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    parser = argparse.ArgumentParser(
        description="DART 정기보고서 원문 로컬 다운로드",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python data/dart_download.py                           # 2026년 Top 20 전체
  python data/dart_download.py --year 2025               # 2025년
  python data/dart_download.py --year 2026 --corp 005930  # 삼성전자만
  python data/dart_download.py --year 2026 --type 사업보고서  # 사업보고서만
  python data/dart_download.py --year 2026 --dry-run      # 목록 확인
  python data/dart_download.py --year 2026 --zip          # ZIP 파일 그대로 저장
        """,
    )
    parser.add_argument(
        "--year", default=str(date.today().year),
        help="수집 연도 (기본: 올해)",
    )
    parser.add_argument(
        "--corp", metavar="CODE",
        help="단일 기업 지정 (6자리 종목코드 또는 8자리 DART 코드)",
    )
    parser.add_argument(
        "--type", metavar="TYPE", dest="report_type",
        choices=["사업보고서", "반기보고서", "분기보고서"],
        help="보고서 종류 필터 (미지정: 전체)",
    )
    parser.add_argument(
        "--out", metavar="DIR", default=str(_BASE_DIR),
        help=f"저장 경로 (기본: {_BASE_DIR})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="다운로드 없이 수집 대상 목록만 출력",
    )
    parser.add_argument(
        "--zip", action="store_true", dest="save_zip",
        help="압축 해제 대신 ZIP 파일 그대로 저장",
    )
    args = parser.parse_args()

    api_key = os.environ.get("DART_API_KEY")
    if not api_key:
        print("DART_API_KEY 환경변수가 설정되지 않았습니다.", file=sys.stderr)
        sys.exit(1)

    base_dir = Path(args.out)
    report_filter = {args.report_type} if args.report_type else None

    # DB 연결 시도 (선택적)
    pool = None
    try:
        from core.db import create_pool, init_db
        pool = await create_pool()
        await init_db(pool)
    except Exception as e:
        logger.warning("DB 연결 실패 (%s) — DB 없이 계속", e)

    # 기업 목록 결정
    corp_map = await _resolve_corp_map(args.corp, pool)
    if not corp_map:
        print("대상 기업 없음.", file=sys.stderr)
        sys.exit(1)

    if args.corp:
        logger.info("대상 기업: %s", list(corp_map.values()))
    else:
        logger.info("대상 기업: Top 20 (%d개)", len(corp_map))

    logger.info("수집 연도: %s  |  보고서 종류: %s  |  저장 경로: %s",
                args.year, args.report_type or "전체", base_dir)

    if args.dry_run:
        logger.info("[DRY-RUN] 실제 다운로드 없이 목록만 확인합니다.")

    stats = await download_reports(
        corp_map=corp_map,
        year=args.year,
        api_key=api_key,
        report_filter=report_filter,
        dry_run=args.dry_run,
        save_zip=args.save_zip,
        base_dir=base_dir,
    )

    if pool:
        await pool.close()

    label = "예정" if args.dry_run else "완료"
    prefix = "DRY-RUN " if args.dry_run else ""
    print(
        f"\n{prefix}결과 -- "
        f"다운로드 {label}: {stats['downloaded']}건  |  "
        f"스킵: {stats['skipped']}건  |  "
        f"실패: {stats['failed']}건  |  "
        f"합계: {stats['total']}건"
    )
    print(f"저장 경로: {base_dir.resolve()}")


if __name__ == "__main__":
    asyncio.run(_main())
