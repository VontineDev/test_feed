"""
krx_sync.py — KRX 전체 종목 리스트를 krx_listings 테이블에 동기화.
출처: data.krx.co.kr (공개 API, 인증 불필요)
"""
from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any, Optional

import asyncpg
import httpx

logger = logging.getLogger(__name__)

KRX_API_URL = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
KRX_PAYLOAD = {
    "bld": "dbms/MDC/STAT/standard/MDCSTAT01901",
    "mktId": "ALL",
    "share": "1",
    "money": "1",
    "csvxls_isNo": "false",
}

# KOSPI와 KOSDAQ만 yfinance로 조회 가능. KONEX 등은 심볼이 없으므로 제외.
SUPPORTED_MARKETS: frozenset[str] = frozenset({"KOSPI", "KOSDAQ"})

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS krx_listings (
    isin_code       TEXT PRIMARY KEY,
    short_code      TEXT NOT NULL,
    name_ko         TEXT NOT NULL,
    name_ko_abbr    TEXT,
    name_en         TEXT,
    listed_at       DATE,
    market          TEXT,
    security_type   TEXT,
    sector          TEXT,
    stock_type      TEXT,
    par_value       TEXT,
    listed_shares   BIGINT,
    yfinance_symbol TEXT NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_krx_listings_name_ko
    ON krx_listings (name_ko);
CREATE INDEX IF NOT EXISTS idx_krx_listings_name_ko_abbr
    ON krx_listings (name_ko_abbr);
CREATE INDEX IF NOT EXISTS idx_krx_listings_short_code
    ON krx_listings (short_code);
"""

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
    """
    data.krx.co.kr에서 전체 종목 리스트를 가져와 krx_listings에 upsert.
    반환값: upsert된 행 수. 오류 시 예외 발생.
    """
    # 테이블이 없으면 생성
    async with pool.acquire() as conn:
        await conn.execute(CREATE_TABLE_SQL)

    logger.info("[krx_sync] data.krx.co.kr 전체 종목 조회 시작 ...")
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(KRX_API_URL, data=KRX_PAYLOAD)
        resp.raise_for_status()
        # KRX는 EUC-KR 응답을 반환할 수 있음. 명시적으로 디코딩.
        try:
            data = json.loads(resp.content.decode("euc-kr"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            data = resp.json()  # UTF-8 fallback

    rows_raw = data.get("OutBlock_1", [])
    if not rows_raw:
        raise ValueError(
            f"[krx_sync] OutBlock_1이 비어 있음 — 응답 키: {list(data)}"
        )

    # 총 건수 검증 (페이지네이션 발생 감지)
    i_tot = data.get("iTotCnt")
    if i_tot is not None and int(i_tot) != len(rows_raw):
        logger.warning(
            "[krx_sync] iTotCnt=%s이지만 수신 %d행 — 응답이 페이지네이션됐을 수 있음",
            i_tot, len(rows_raw),
        )

    params_list = [p for item in rows_raw if (p := _row_to_params(item)) is not None]
    skipped = len(rows_raw) - len(params_list)
    if skipped:
        logger.debug("[krx_sync] %d행 건너뜀 (KONEX/미지원 시장 또는 필수 필드 누락)", skipped)

    async with pool.acquire() as conn:
        await conn.executemany(UPSERT_SQL, params_list)

    logger.info("[krx_sync] %d행 upsert 완료 (건너뜀 %d)", len(params_list), skipped)
    return len(params_list)
