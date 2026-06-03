"""
dart_expand.py — 거래대금 상위 순으로 DART 보고서 다운로드 + 3-Pass 추출

사용:
    python scripts/dart_expand.py --top 80          # 거래대금 Top80 신규 기업
    python scripts/dart_expand.py --top 80 --dry-run  # 대상 목록만 출력
    python scripts/dart_expand.py --top 80 --skip-download  # 추출만 (이미 다운로드됨)

흐름:
    1. dart_companies DB: 상장 종목코드 전체 조회 (약 3,900개)
    2. yfinance 배치 다운로드: 최근 5거래일 거래대금(종가×거래량) 계산
    3. 거래대금 내림차순 정렬 → 이미 reports/dart/ 존재하는 기업 제외
    4. 상위 --top 개 기업: 2025·2026년 보고서 DART API 다운로드
    5. dart_extractor.py 3-Pass 추출 → dart_extractions 저장

DART API 소모량 (기업당 ~4~6 req):
    - list.json × 2 (2025·2026)   = 2 req
    - document.xml × 2~4          = 2~4 req
    한도 10,000 req/day → --top 200 이하 권장
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s")

_BATCH = 400   # yfinance 1회 배치 크기


# ── 1. DB에서 전체 상장 종목 조회 ────────────────────────────────

async def fetch_listed_stocks(pool) -> list[dict]:
    """dart_companies에서 6자리 stock_code가 있는 종목 전체 반환."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT stock_code, corp_code, corp_name FROM dart_companies "
            "WHERE stock_code IS NOT NULL AND length(stock_code) = 6 "
            "ORDER BY stock_code"
        )
    return [{"stock_code": r["stock_code"],
             "corp_code":  r["corp_code"],
             "corp_name":  r["corp_name"]} for r in rows]


# ── 2. yfinance 배치 거래대금 계산 ───────────────────────────────

def calc_trading_value(stocks: list[dict]) -> list[dict]:
    """
    yfinance로 최근 5거래일 평균 거래대금 계산.
    KOSPI(.KS) 시도 후 데이터 없으면 KOSDAQ(.KQ) 재시도.
    반환: stocks에 avg_value(원) 필드 추가, 내림차순 정렬.
    """
    import yfinance as yf
    import pandas as pd

    logger.info("[yfinance] %d개 종목 거래대금 조회 시작 (배치=%d)", len(stocks), _BATCH)

    # .KS 심볼로 우선 시도
    ks_syms = [f"{s['stock_code']}.KS" for s in stocks]
    close_ks: dict[str, float] = {}
    vol_ks:   dict[str, float] = {}

    for i in range(0, len(ks_syms), _BATCH):
        batch = ks_syms[i:i + _BATCH]
        batch_no = i // _BATCH + 1
        total_batches = (len(ks_syms) + _BATCH - 1) // _BATCH
        logger.info("[yfinance] .KS 배치 %d/%d (%d종목)", batch_no, total_batches, len(batch))
        try:
            df = yf.download(batch, period="5d", auto_adjust=True,
                             progress=False, timeout=60, threads=True)
            if df.empty:
                continue
            # MultiIndex: (Metric, Ticker) or single-level
            if isinstance(df.columns, pd.MultiIndex):
                close_vals = df["Close"].mean()
                vol_vals   = df["Volume"].mean()
            else:
                # 단일 종목인 경우
                close_vals = pd.Series({"Close": df["Close"].mean()})
                vol_vals   = pd.Series({"Volume": df["Volume"].mean()})
            for sym in batch:
                c = close_vals.get(sym, 0) if sym in close_vals.index else 0
                v = vol_vals.get(sym, 0)   if sym in vol_vals.index   else 0
                if c and v and not pd.isna(c) and not pd.isna(v):
                    close_ks[sym] = float(c)
                    vol_ks[sym]   = float(v)
        except Exception as e:
            logger.warning("[yfinance] 배치 %d 오류: %s", batch_no, e)

    logger.info("[yfinance] .KS 유효 데이터: %d종목", len(close_ks))

    # .KS 데이터 없는 종목 → .KQ 재시도
    no_data_codes = [s["stock_code"] for s in stocks
                     if f"{s['stock_code']}.KS" not in close_ks]
    close_kq: dict[str, float] = {}
    vol_kq:   dict[str, float] = {}

    if no_data_codes:
        kq_syms = [f"{c}.KQ" for c in no_data_codes]
        logger.info("[yfinance] .KQ 재시도: %d종목", len(kq_syms))
        for i in range(0, len(kq_syms), _BATCH):
            batch = kq_syms[i:i + _BATCH]
            try:
                df = yf.download(batch, period="5d", auto_adjust=True,
                                 progress=False, timeout=60, threads=True)
                if df.empty:
                    continue
                if isinstance(df.columns, pd.MultiIndex):
                    close_vals = df["Close"].mean()
                    vol_vals   = df["Volume"].mean()
                else:
                    close_vals = pd.Series({"Close": df["Close"].mean()})
                    vol_vals   = pd.Series({"Volume": df["Volume"].mean()})
                for sym in batch:
                    c = close_vals.get(sym, 0) if sym in close_vals.index else 0
                    v = vol_vals.get(sym, 0)   if sym in vol_vals.index   else 0
                    if c and v and not pd.isna(c) and not pd.isna(v):
                        close_kq[sym] = float(c)
                        vol_kq[sym]   = float(v)
            except Exception as e:
                logger.warning("[yfinance] .KQ 배치 오류: %s", e)
        logger.info("[yfinance] .KQ 유효 데이터: %d종목", len(close_kq))

    # 거래대금 계산 및 결과 조립
    result = []
    for s in stocks:
        code = s["stock_code"]
        ks = f"{code}.KS"
        kq = f"{code}.KQ"
        if ks in close_ks:
            value = int(close_ks[ks] * vol_ks[ks])
            mkt = "KOSPI"
        elif kq in close_kq:
            value = int(close_kq[kq] * vol_kq[kq])
            mkt = "KOSDAQ"
        else:
            value = 0
            mkt = "?"
        result.append({**s, "avg_value": value, "market": mkt})

    result.sort(key=lambda x: x["avg_value"], reverse=True)
    logger.info("[yfinance] 거래대금 산출 완료: 유효 %d / 전체 %d",
                sum(1 for r in result if r["avg_value"] > 0), len(result))
    return result


# ── 3. 이미 다운로드된 기업 확인 ─────────────────────────────────

def already_downloaded(base_dir: Path) -> set[str]:
    if not base_dir.exists():
        return set()
    return {d.name for d in base_dir.iterdir() if d.is_dir()}


# ── 4. 다운로드 ──────────────────────────────────────────────────

async def download_for_corps(corp_map: dict[str, str], api_key: str) -> int:
    from data.dart_download import download_reports
    base_dir = Path(__file__).parent.parent / "reports" / "dart"
    total = 0
    for year in ["2025", "2026"]:
        logger.info("[download] %s년 시작 (%d기업)", year, len(corp_map))
        stats = await download_reports(
            corp_map=corp_map, year=year, api_key=api_key, base_dir=base_dir
        )
        logger.info("[download] %s년 완료: 다운 %d / 스킵 %d / 실패 %d",
                    year, stats["downloaded"], stats["skipped"], stats["failed"])
        total += stats["downloaded"]
    return total


# ── 5. 추출 ──────────────────────────────────────────────────────

async def run_extractor(pool) -> int:
    from data.dart_extractor import extract_all
    logger.info("[extract] 3-Pass 추출 시작 (신규 기업만)")
    n = await extract_all(pool)
    logger.info("[extract] 완료: %d건 저장", n)
    return n


# ── 메인 ─────────────────────────────────────────────────────────

async def main(top_n: int, dry_run: bool, skip_download: bool) -> None:
    from core.db import create_pool
    dart_dir = Path(__file__).parent.parent / "reports" / "dart"
    api_key  = os.environ.get("DART_API_KEY", "")

    if not api_key and not dry_run and not skip_download:
        logger.error("DART_API_KEY 환경변수 미설정")
        return

    pool = await create_pool()

    # ── Step 1: DB 전체 상장 종목 ────────────────────────────────
    logger.info("Step 1: dart_companies 상장 종목 조회")
    all_stocks = await fetch_listed_stocks(pool)
    logger.info("  상장 종목: %d개", len(all_stocks))

    # ── Step 2: yfinance 거래대금 계산 ───────────────────────────
    logger.info("Step 2: yfinance 거래대금 계산 (2~3분 소요)")
    ranked = calc_trading_value(all_stocks)

    # ── Step 3: 기존 다운로드 제외 + top_n 선택 ─────────────────
    downloaded = already_downloaded(dart_dir)
    logger.info("Step 3: 기존 %d기업 제외 후 상위 %d 선택", len(downloaded), top_n)

    ordered: list[dict] = []
    for c in ranked:
        if c["corp_name"] in downloaded:
            continue
        ordered.append(c)
        if len(ordered) >= top_n:
            break

    logger.info("  신규 처리 대상: %d개", len(ordered))
    if not ordered:
        logger.info("처리할 신규 기업 없음")
        await pool.close()
        return

    # ── 대상 출력 ─────────────────────────────────────────────
    logger.info("")
    logger.info("%-5s %-12s %-22s %-8s %s", "순위", "종목코드", "기업명", "시장", "거래대금(억)")
    logger.info("-" * 65)
    for i, c in enumerate(ordered, 1):
        val_eok = c["avg_value"] // 100_000_000 if c["avg_value"] else 0
        logger.info("%-5s %-12s %-22s %-8s %s억",
                    str(i), c["stock_code"], c["corp_name"],
                    c["market"], f"{val_eok:,}")

    est_calls = len(ordered) * 6
    logger.info("")
    logger.info("DART API 예상 소모: ~%d req (한도 10,000/day)", est_calls)

    if dry_run:
        logger.info("[dry-run] 종료")
        await pool.close()
        return

    # ── Step 4: 다운로드 ─────────────────────────────────────
    if not skip_download:
        corp_map = {c["corp_code"]: c["corp_name"] for c in ordered}
        logger.info("Step 4: 보고서 다운로드 (%d기업 × 2년)", len(corp_map))
        await download_for_corps(corp_map, api_key)
    else:
        logger.info("Step 4: --skip-download — 건너뜀")

    # ── Step 5: 추출 ─────────────────────────────────────────
    logger.info("Step 5: 3-Pass 추출")
    await run_extractor(pool)

    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="거래대금 상위 기업 DART 확장 수집")
    parser.add_argument("--top", type=int, default=80,
                        help="거래대금 상위 N개 신규 기업 (기본: 80)")
    parser.add_argument("--dry-run", action="store_true",
                        help="대상 목록만 출력")
    parser.add_argument("--skip-download", action="store_true",
                        help="다운로드 건너뛰고 추출만 실행")
    args = parser.parse_args()
    asyncio.run(main(args.top, args.dry_run, args.skip_download))
