"""
krx_sync.py — KRX 전체 종목 리스트를 krx_listings 테이블에 동기화.
출처: KRX Open API (data-dbg.krx.co.kr) — 공식 REST API, scraping 불필요.
      환경변수 KRX_OPENAPI_KEY 필요 (openapi.krx.co.kr 가입 후 발급).

krx_listings 테이블은 db.py의 init_db()에서 생성됩니다.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Optional

import asyncpg

logger = logging.getLogger(__name__)

SUPPORTED_MARKETS: frozenset[str] = frozenset({"KOSPI", "KOSDAQ"})

UPSERT_SQL = """
INSERT INTO krx_listings
    (isin_code, short_code, name_ko, name_ko_abbr, name_en,
     listed_at, market, security_type, sector, stock_type,
     par_value, listed_shares, yfinance_symbol, updated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW())
ON CONFLICT (isin_code) DO UPDATE SET
    short_code      = EXCLUDED.short_code,
    name_ko         = EXCLUDED.name_ko,
    name_ko_abbr    = EXCLUDED.name_ko_abbr,
    name_en         = EXCLUDED.name_en,
    listed_at       = EXCLUDED.listed_at,
    market          = EXCLUDED.market,
    security_type   = EXCLUDED.security_type,
    sector          = EXCLUDED.sector,
    stock_type      = EXCLUDED.stock_type,
    par_value       = EXCLUDED.par_value,
    listed_shares   = EXCLUDED.listed_shares,
    yfinance_symbol = EXCLUDED.yfinance_symbol,
    updated_at      = NOW()
"""


def _derive_yfinance_symbol(short_code: str, market: str) -> str:
    if "KOSDAQ" in market.upper():
        return f"{short_code}.KQ"
    return f"{short_code}.KS"


def _parse_listed_at(raw: str) -> Optional[date]:
    """KRX 날짜 문자열(예: '20050101') 파싱. 빈 문자열은 None 반환."""
    raw = raw.strip()
    if len(raw) == 8 and raw.isdigit():
        try:
            return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
        except ValueError:
            pass
    return None


def _parse_listed_shares(raw: str) -> Optional[int]:
    raw = raw.strip().replace(",", "")
    if raw.isdigit():
        return int(raw)
    return None


def _row_to_params(item: dict[str, Any]) -> Optional[tuple]:
    """
    KRX API 행 → upsert 파라미터 튜플.
    필수 필드 누락 또는 지원하지 않는 시장(KONEX 등)이면 None 반환.
    """
    isin = item.get("ISU_CD", "").strip()
    short = item.get("ISU_SRT_CD", "").strip()
    name_ko = item.get("ISU_NM", "").strip()  # 10자 표시명 (짧을 수 있음)
    if not isin or not short or not name_ko:
        return None

    market_raw = item.get("MKT_NM", "").strip()
    if market_raw not in SUPPORTED_MARKETS:
        # KONEX, ETF 전용 보드 등 yfinance 심볼이 없는 시장 제외
        return None

    yf_symbol = _derive_yfinance_symbol(short, market_raw)

    # ISU_ABBRV는 전체 약식명 (예: "LG에너지솔루션"). ISU_NM이 잘릴 때 ISU_ABBRV로 조회됨.
    return (
        isin,
        short,
        name_ko,
        item.get("ISU_ABBRV", "").strip() or None,       # name_ko_abbr (전체 약식명)
        item.get("ISU_ENG_NM", "").strip() or None,      # name_en
        _parse_listed_at(item.get("LIST_DD", "")),        # listed_at
        market_raw or None,                               # market
        item.get("SECUGRP_NM", "").strip() or None,      # security_type
        item.get("SECT_TP_NM", "").strip() or None,      # sector
        item.get("KIND_STKCERT_TP_NM", "").strip() or None,  # stock_type
        item.get("PAR_VAL", "").strip() or None,          # par_value
        _parse_listed_shares(item.get("LIST_SHRS", "")), # listed_shares
        yf_symbol,
    )


async def sync_krx_listings(pool: asyncpg.Pool) -> int:
    """KRX Open API에서 전체 종목 리스트를 가져와 krx_listings에 upsert.

    data.krx.co.kr scraping 대신 공식 REST API 사용.
    반환값: upsert된 행 수.
    전제: init_db()가 먼저 호출되어 krx_listings 테이블이 생성돼 있어야 합니다.
    """
    from data.krx_openapi import KRXOpenAPIClient

    client = KRXOpenAPIClient()

    # 휴장일(주말·공휴일)에는 OutBlock_1이 빈 배열로 옴 — 데이터가 나올 때까지
    # 최근 영업일을 거꾸로 탐색 (최대 10일 — 최장 추석/설 연휴 커버)
    rows_kospi: list[dict] = []
    rows_kosdaq: list[dict] = []
    bas_dd = ""
    for days_back in range(1, 11):
        bas_dd = (date.today() - timedelta(days=days_back)).strftime("%Y%m%d")
        logger.info("[krx_sync] KRX Open API 종목 조회 시작 (%s) ...", bas_dd)
        rows_kospi  = client.get_kospi_tickers(bas_dd)
        rows_kosdaq = client.get_kosdaq_tickers(bas_dd)
        if rows_kospi or rows_kosdaq:
            break
        logger.info("[krx_sync] %s 휴장일로 추정 (응답 0건) — 이전 영업일 조회", bas_dd)

    rows_raw = [
        (row, "KOSPI",  ".KS") for row in rows_kospi
    ] + [
        (row, "KOSDAQ", ".KQ") for row in rows_kosdaq
    ]

    if not rows_raw:
        raise ValueError("[krx_sync] KRX Open API 응답 없음 — KRX_OPENAPI_KEY 확인")

    params_list: list[tuple] = []
    skipped = 0

    for row, market_raw, suffix in rows_raw:
        p = _row_to_params_openapi(row, market_raw, suffix)
        if p is None:
            skipped += 1
        else:
            params_list.append(p)

    if skipped:
        logger.debug("[krx_sync] %d행 건너뜀 (필수 필드 누락)", skipped)

    if not params_list:
        raise ValueError(
            f"[krx_sync] 유효한 종목이 없음 (전체 {len(rows_raw)}행 필터링됨)"
        )

    async with pool.acquire() as conn:
        async with conn.transaction():
            sync_start_ts = await conn.fetchval("SELECT NOW()")
            await conn.executemany(UPSERT_SQL, params_list)
            deleted_status = await conn.execute(
                "DELETE FROM krx_listings WHERE updated_at < $1",
                sync_start_ts,
            )
            try:
                deleted_count = int(str(deleted_status).split()[-1])
            except (ValueError, IndexError):
                deleted_count = 0
            if deleted_count:
                logger.info("[krx_sync] 상장폐지 종목 %d행 삭제", deleted_count)

    logger.info("[krx_sync] %d행 upsert 완료 (건너뜀 %d)", len(params_list), skipped)
    return len(params_list)


def _row_to_params_openapi(
    row: dict[str, Any], market: str, suffix: str
) -> Optional[tuple]:
    """KRX Open API 행 → krx_listings upsert 파라미터 튜플."""
    isin  = row.get("ISU_CD", "").strip()
    short = row.get("ISU_SRT_CD", "").strip().zfill(6)
    name  = row.get("ISU_NM", "").strip()

    if not isin or not short or not name:
        return None

    yf_symbol = f"{short}{suffix}"

    return (
        isin,
        short,
        name,
        row.get("ISU_ABBRV", "").strip() or None,
        row.get("ISU_ENG_NM", "").strip() or None,
        _parse_listed_at(row.get("LIST_DD", "")),
        market,
        row.get("SECUGRP_NM", "").strip() or None,
        row.get("SECT_TP_NM", "").strip() or None,
        row.get("KIND_STKCERT_TP_NM", "").strip() or None,
        row.get("PARVAL", "").strip() or None,
        _parse_listed_shares(row.get("LIST_SHRS", "")),
        yf_symbol,
    )
