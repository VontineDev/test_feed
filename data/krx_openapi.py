"""
krx_openapi.py — KRX Open API 클라이언트 (공식 REST API)

Base URL : https://data-dbg.krx.co.kr/svc/apis
인증     : AUTH_KEY: {KRX_OPENAPI_KEY}  (환경변수)
요청 형식 : GET ?basDd=YYYYMMDD
응답 형식 : {"OutBlock_1": [...]}

제공 데이터:
  종목기본정보  — KOSPI/KOSDAQ 전 종목 (코드, 종목명, 상장일 등)
  일별 OHLCV   — KOSPI/KOSDAQ 전 종목 (시가/고가/저가/종가/거래량)
  지수 시세    — KOSPI 시리즈

미제공 (별도 소스 필요):
  투자자별 순매수(외국인·기관) → krx_flow_sync.py 참조

KRX OpenAPI 가입: https://openapi.krx.co.kr
"""
from __future__ import annotations

import logging
import os
from datetime import date
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_BASE = "https://data-dbg.krx.co.kr/svc/apis"

# ISIN → 단축코드 변환 (KRX ISIN 형식: KR{type}{6digit}{3check})
# 예) "KR7005930003"[3:9] → "005930"
def _isin_to_short(isin: str) -> str:
    return isin[3:9] if len(isin) == 12 else isin


class KRXOpenAPIClient:
    """KRX Open API REST 클라이언트.

    인증:
      환경변수 KRX_OPENAPI_KEY 설정 또는 생성자에서 appkey 직접 전달.
      openapi.krx.co.kr 가입 후 인증키 발급 필요.

    Rate limit: 10 req/s (pykrx-openapi 기준)
    """

    def __init__(self, appkey: Optional[str] = None) -> None:
        self._appkey = appkey or os.environ.get("KRX_OPENAPI_KEY", "")
        if not self._appkey:
            logger.warning(
                "[krx-api] KRX_OPENAPI_KEY 미설정 — openapi.krx.co.kr 가입 후 .env에 추가"
            )
        self._session = requests.Session()

    def _get(self, path: str, bas_dd: str, timeout: int = 30) -> list[dict]:
        """GET 요청. 실패 시 빈 리스트 반환."""
        url = f"{_BASE}{path}"
        headers = {"AUTH_KEY": self._appkey}
        params = {"basDd": bas_dd}
        try:
            resp = self._session.get(url, headers=headers, params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            rows = data.get("OutBlock_1", [])
            logger.debug("[krx-api] %s %s → %d행", path, bas_dd, len(rows))
            return rows
        except Exception as e:
            logger.warning("[krx-api] %s %s 실패: %s", path, bas_dd, e)
            return []

    # ── 종목기본정보 ─────────────────────────────────────────────

    def get_kospi_tickers(self, bas_dd: str) -> list[dict]:
        """유가증권 종목기본정보 (ISU_CD, ISU_SRT_CD, ISU_NM 등)."""
        return self._get("/sto/stk_isu_base_info", bas_dd)

    def get_kosdaq_tickers(self, bas_dd: str) -> list[dict]:
        """코스닥 종목기본정보."""
        return self._get("/sto/ksq_isu_base_info", bas_dd)

    def get_all_tickers(
        self, bas_dd: str
    ) -> list[tuple[str, str, str]]:
        """KOSPI + KOSDAQ 전 종목 → [(yfinance_symbol, name, market), ...]."""
        result: list[tuple[str, str, str]] = []

        for rows, suffix, market in [
            (self.get_kospi_tickers(bas_dd),  ".KS", "KOSPI"),
            (self.get_kosdaq_tickers(bas_dd), ".KQ", "KOSDAQ"),
        ]:
            for row in rows:
                short = row.get("ISU_SRT_CD", "").strip().zfill(6)
                name  = row.get("ISU_NM", short).strip()
                if short:
                    result.append((f"{short}{suffix}", name, market))

        logger.info("[krx-api] 종목 %d개 로드 (%s)", len(result), bas_dd)
        return result

    # ── 일별 OHLCV ─────────────────────────────────────────────

    def get_kospi_ohlcv(self, bas_dd: str) -> list[dict]:
        """유가증권 일별매매정보 (전 종목, 단일 날짜)."""
        return self._get("/sto/stk_bydd_trd", bas_dd)

    def get_kosdaq_ohlcv(self, bas_dd: str) -> list[dict]:
        """코스닥 일별매매정보 (전 종목, 단일 날짜)."""
        return self._get("/sto/ksq_bydd_trd", bas_dd)

    def get_daily_ohlcv_all(
        self, bas_dd: str
    ) -> list[dict]:
        """KOSPI + KOSDAQ 전 종목 OHLCV (단일 날짜).

        반환 형식 (정규화):
          {symbol, market, date, open, high, low, close, volume}
          symbol: yfinance 심볼 (005930.KS / 035720.KQ)
        """
        result: list[dict] = []
        d = _parse_date(bas_dd)

        for rows, suffix, market in [
            (self.get_kospi_ohlcv(bas_dd),  ".KS", "KOSPI"),
            (self.get_kosdaq_ohlcv(bas_dd), ".KQ", "KOSDAQ"),
        ]:
            for row in rows:
                isin  = row.get("ISU_CD", "")
                short = _isin_to_short(isin).zfill(6)
                if not short or not isin:
                    continue

                close = _to_float(row.get("TDD_CLSPRC"))
                if close is None or close <= 0:
                    continue

                result.append({
                    "symbol": f"{short}{suffix}",
                    "market": market,
                    "date":   d,
                    "open":   _to_float(row.get("TDD_OPNPRC")),
                    "high":   _to_float(row.get("TDD_HGPRC")),
                    "low":    _to_float(row.get("TDD_LWPRC")),
                    "close":  close,
                    "volume": _to_int(row.get("ACC_TRDVOL")),
                })

        logger.info("[krx-api] OHLCV %d종목 (%s)", len(result), bas_dd)
        return result

    # ── 지수 시세 ────────────────────────────────────────────────

    def get_kospi_index(self, bas_dd: str) -> list[dict]:
        """KOSPI 지수 시세 (IDX_NM, CLSPRC_IDX 등)."""
        return self._get("/idx/kospi_dd_trd", bas_dd)

    def get_kospi_index_ohlcv(self, bas_dd: str) -> Optional[dict]:
        """KOSPI (종합지수) 하루 OHLCV + prev_close → market_index 용.

        반환: {symbol, market, date, open, high, low, close, volume, prev_close}
        """
        rows = self.get_kospi_index(bas_dd)
        for row in rows:
            if row.get("IDX_NM", "") == "KOSPI":
                close = _to_float(row.get("CLSPRC_IDX"))
                if close is None:
                    return None
                return {
                    "symbol":     "^KS11",
                    "market":     "IDX",
                    "date":       _parse_date(bas_dd),
                    "open":       _to_float(row.get("OPNPRC_IDX")),
                    "high":       _to_float(row.get("HGPRC_IDX")),
                    "low":        _to_float(row.get("LWPRC_IDX")),
                    "close":      close,
                    "volume":     _to_int(row.get("ACC_TRDVOL")),
                    "prev_close": _to_float(row.get("BASPRC_IDX")),
                }
        return None

    def get_kosdaq_index(self, bas_dd: str) -> list[dict]:
        """KOSDAQ 지수 시세 (IDX_NM, CLSPRC_IDX 등)."""
        return self._get("/idx/ksq_dd_trd", bas_dd)

    def get_kosdaq_index_ohlcv(self, bas_dd: str) -> Optional[dict]:
        """KOSDAQ (종합지수) 하루 OHLCV + prev_close → market_index 용.

        반환: {symbol, market, date, open, high, low, close, volume, prev_close}
        """
        rows = self.get_kosdaq_index(bas_dd)
        for row in rows:
            if row.get("IDX_NM", "") in ("코스닥", "코스닥종합", "KOSDAQ"):
                close = _to_float(row.get("CLSPRC_IDX"))
                if close is None:
                    return None
                return {
                    "symbol":     "^KQ11",
                    "market":     "IDX",
                    "date":       _parse_date(bas_dd),
                    "open":       _to_float(row.get("OPNPRC_IDX")),
                    "high":       _to_float(row.get("HGPRC_IDX")),
                    "low":        _to_float(row.get("LWPRC_IDX")),
                    "close":      close,
                    "volume":     _to_int(row.get("ACC_TRDVOL")),
                    "prev_close": _to_float(row.get("BASPRC_IDX")),
                }
        return None


# ── 유틸 ────────────────────────────────────────────────────────

def _to_float(s: object) -> Optional[float]:
    if s is None:
        return None
    try:
        return float(str(s).replace(",", ""))
    except (ValueError, TypeError):
        return None


def _to_int(s: object) -> Optional[int]:
    if s is None:
        return None
    try:
        return int(str(s).replace(",", ""))
    except (ValueError, TypeError):
        return None


def _parse_date(bas_dd: str) -> date:
    """'YYYYMMDD' → date."""
    return date(int(bas_dd[:4]), int(bas_dd[4:6]), int(bas_dd[6:8]))


# ── 모듈 수준 싱글턴 ──────────────────────────────────────────────

_client: Optional[KRXOpenAPIClient] = None


def get_client() -> KRXOpenAPIClient:
    """애플리케이션 수준 싱글턴. 환경변수에서 자동으로 키 로드."""
    global _client
    if _client is None:
        _client = KRXOpenAPIClient()
    return _client
