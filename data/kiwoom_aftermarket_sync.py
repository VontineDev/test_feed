"""
kiwoom_aftermarket_sync.py — 키움 REST API로 시간외 단일가 스냅샷 수집

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
설계 의도:
  기존 krx_aftermarket_sync.py (data.krx.co.kr BLD 방식) 대신
  키움 REST API를 사용하는 버전.

  ka10098 (시간외단일가등락률조회)로 KOSPI/KOSDAQ 전 종목
  시간외 데이터를 2회 요청으로 수집.

  ⚠️  키움 REST API는 당일 실시간 데이터만 제공.
      과거 날짜 backfill은 krx_aftermarket_sync.py 사용.

  수집 데이터 (aftermarket_snap):
    reg_close     : 정규장 종가    (tdy_close_pric)
    after_close   : 시간외 체결가   (cur_prc)
    after_volume  : 시간외 누적 거래량 (acc_trde_qty)
    after_value   : 시간외 누적 거래대금 (acc_trde_prica × 10000 원)
    after_chg_pct : 시간외 등락률   (flu_rt)

  ⚠️  acc_trde_prica 단위는 만원(10,000원)으로 추정.
      실제 값이 다를 경우 _VALUE_UNIT 상수를 조정하세요.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
인증:
  .env 에 KIWOOM_APPKEY + KIWOOM_SECRETKEY 설정 (필수)
  또는 KIWOOM_TOKEN (미리 발급된 토큰 직접 주입)

API 레퍼런스:
  au10001  POST /oauth2/token          토큰 발급
  au10002  POST /oauth2/revoke         토큰 폐기
  ka10098  POST /api/dostk/rkinfo      시간외단일가등락률조회 (bulk)
  ka10087  POST /api/dostk/mrkcond     시간외단일호가조회 (per-stock)

사용법:
  # 당일 수집 (15:40~16:00 사이 실행 권장):
  python kiwoom_aftermarket_sync.py --today

  # 특정 날짜로 적재 (데이터는 당일 것, 날짜만 override):
  python kiwoom_aftermarket_sync.py --today --trade-date 2026-05-09

  # 시장 필터:
  python kiwoom_aftermarket_sync.py --today --market KOSPI

  # 응답 구조 확인 (--probe):
  python kiwoom_aftermarket_sync.py --probe

  # 증분 (어제 날짜로 당일 데이터 적재 — 스케줄러 16:05 실행):
  python kiwoom_aftermarket_sync.py --incremental
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
from __future__ import annotations

import argparse
import json as _json
import logging
import os
import sys
import time as _time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import psycopg2
import psycopg2.extras
import requests as _req
from dotenv import load_dotenv

from core.dates import last_trading_day

load_dotenv()
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── 상수 ─────────────────────────────────────────────────────────

_BASE_URL = "https://api.kiwoom.com"
_MOCK_URL = "https://mockapi.kiwoom.com"  # KRX 시장 운영 시간 외 테스트용

# acc_trde_prica 단위 보정 계수 (만원 → 원)
# ⚠️  실제 단위가 다를 경우 이 값을 조정:
#     만원    → 10_000
#     백만원  → 1_000_000
#     원(그대로) → 1
_VALUE_UNIT = 1_000_000

# ka10098 mrkt_tp 매핑
_MARKET_CODES = {
    "KOSPI":  "001",
    "KOSDAQ": "101",
    "ALL":    "000",  # 전체 (한 번에 수집, 시장 구분 불명확)
}


# ── DDL ──────────────────────────────────────────────────────────

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS aftermarket_snap (
    trade_date    DATE            NOT NULL,
    ticker        VARCHAR(12)     NOT NULL,
    reg_close     NUMERIC(12, 0),
    after_close   NUMERIC(12, 0),
    after_volume  BIGINT,
    after_value   BIGINT,
    reg_value     BIGINT,
    after_chg_pct NUMERIC(6, 2),
    fetched_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (trade_date, ticker)
);
CREATE INDEX IF NOT EXISTS idx_aftermarket_date
    ON aftermarket_snap (trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_aftermarket_ticker_date
    ON aftermarket_snap (ticker, trade_date DESC);
"""


# ── 레코드 ───────────────────────────────────────────────────────

@dataclass
class AftermarketRecord:
    trade_date:    date
    ticker:        str
    reg_close:     Optional[int]
    after_close:   Optional[int]
    after_volume:  Optional[int]
    after_value:   Optional[int]
    after_chg_pct: Optional[float]
    reg_value:     Optional[int] = None   # 정규장 거래대금 (close × volume)


# ── 키움 REST API 클라이언트 ──────────────────────────────────────

class KiwoomClient:
    """키움 REST API OAuth2 클라이언트.

    au10001 토큰 발급 → Bearer 토큰으로 API 호출.
    """

    def __init__(self, use_mock: bool = False) -> None:
        self._base = _MOCK_URL if use_mock else _BASE_URL
        self._session = _req.Session()
        self._token: Optional[str] = None
        self._token_expires: Optional[datetime] = None

    # ── 인증 ───────────────────────────────────────────────────

    def issue_token(self, appkey: str, secretkey: str) -> str:
        """au10001: 토큰 발급. 발급된 토큰 문자열 반환."""
        resp = self._session.post(
            f"{self._base}/oauth2/token",
            json={
                "grant_type": "client_credentials",
                "appkey": appkey,
                "secretkey": secretkey,
            },
            headers={"Content-Type": "application/json;charset=UTF-8"},
            timeout=15,
        )
        resp.raise_for_status()
        if "text/html" in resp.headers.get("Content-Type", ""):
            raise RuntimeError("키움 API 점검 중 (HTML 응답) — 시스템작업알림 페이지 수신")
        data = resp.json()
        if data.get("return_code", -1) != 0:
            raise RuntimeError(f"토큰 발급 실패: {data.get('return_msg')}")

        self._token = data["token"]
        # expires_dt: "20241107083713" → datetime
        exp_str = data.get("expires_dt", "")
        if len(exp_str) == 14:
            self._token_expires = datetime.strptime(exp_str, "%Y%m%d%H%M%S")
        logger.info("[kiwoom] 토큰 발급 완료 (만료: %s)", exp_str)
        return self._token

    def inject_token(self, token: str) -> None:
        """KIWOOM_TOKEN 환경변수로 토큰 직접 주입."""
        self._token = token
        logger.info("[kiwoom] 토큰 주입 완료 (외부 제공)")

    def revoke_token(self, appkey: str, secretkey: str) -> None:
        """au10002: 토큰 폐기."""
        if not self._token:
            return
        try:
            self._session.post(
                f"{self._base}/oauth2/revoke",
                json={"appkey": appkey, "secretkey": secretkey, "token": self._token},
                headers=self._auth_headers("au10002"),
                timeout=10,
            )
        except Exception as e:
            logger.debug("[kiwoom] 토큰 폐기 중 오류 (무시): %s", e)

    def _auth_headers(self, api_id: str) -> dict:
        if not self._token:
            raise RuntimeError("토큰 미발급 — issue_token() 또는 inject_token() 먼저 호출")
        return {
            "Content-Type":  "application/json;charset=UTF-8",
            "authorization": f"Bearer {self._token}",
            "api-id":        api_id,
        }

    # ── API 호출 공통 ──────────────────────────────────────────

    def _post(self, path: str, api_id: str, body: dict,
              cont_yn: str = "N", next_key: str = "") -> tuple[dict, dict]:
        """(json_body, resp_headers) 반환. cont-yn/next-key 는 응답 헤더에 있음."""
        headers = self._auth_headers(api_id)
        if cont_yn == "Y":
            headers["cont-yn"] = "Y"
            headers["next-key"] = next_key
        resp = self._session.post(
            f"{self._base}{path}",
            json=body,
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        if "text/html" in resp.headers.get("Content-Type", ""):
            raise RuntimeError("키움 API 점검 중 (HTML 응답) — 시스템작업알림 페이지 수신")
        data = resp.json()
        if data.get("return_code", -1) != 0:
            raise RuntimeError(
                f"API 오류 [{api_id}]: {data.get('return_msg', '알 수 없음')}"
            )
        return data, dict(resp.headers)

    # ── ka10098: 시간외단일가등락률조회 ────────────────────────

    def fetch_aftermarket_bulk(
        self,
        mrkt_tp: str,
        sort_base: str = "5",
    ) -> list[dict]:
        """ka10098: 시장 전체 시간외단일가 등락률 목록.

        페이지네이션(cont-yn/next-key)을 처리해 전 종목을 반환.
        """
        body = {
            "mrkt_tp":     mrkt_tp,
            "sort_base":   sort_base,  # 5:등락률
            "stk_cnd":     "0",        # 0:전체조회
            "trde_qty_cnd": "0",       # 0:전체조회
            "crd_cnd":     "0",        # 0:전체조회
            "trde_prica":  "0",        # 0:전체조회
        }
        all_rows: list[dict] = []
        cont_yn, next_key = "N", ""

        while True:
            data, resp_hdrs = self._post(
                "/api/dostk/rkinfo", "ka10098", body, cont_yn, next_key
            )
            rows = data.get("ovt_sigpric_flu_rt_rank") or []
            all_rows.extend(rows)
            logger.debug("[kiwoom] ka10098 mrkt_tp=%s — %d행 수신 (누적 %d)",
                         mrkt_tp, len(rows), len(all_rows))

            # cont-yn / next-key 는 HTTP 응답 헤더에 있음
            resp_cont = resp_hdrs.get("cont-yn", "N")
            resp_key  = resp_hdrs.get("next-key", "")
            if resp_cont != "Y" or not resp_key:
                break
            cont_yn, next_key = "Y", resp_key

        return all_rows

    # ── ka10087: 시간외단일호가조회 (단일 종목) ────────────────

    def fetch_aftermarket_single(self, stk_cd: str) -> dict:
        """ka10087: 특정 종목 시간외단일가 호가 현황."""
        data, _ = self._post(
            "/api/dostk/mrkcond", "ka10087", {"stk_cd": stk_cd}
        )
        return data

    # ── 계좌 포트폴리오 조회 ──────────────────────────────────────

    def fetch_portfolio_balance(
        self,
        qry_tp: str = "1",
        dmst_stex_tp: str = "KRX",
    ) -> dict:
        """kt00018: 계좌평가잔고내역요청.

        qry_tp: "1"=합산, "2"=개별
        응답: 총매입금액/총평가금액/총손익/총수익률 + 종목별 잔고 리스트
        """
        data, _ = self._post(
            "/api/dostk/acnt",
            "kt00018",
            {"qry_tp": qry_tp, "dmst_stex_tp": dmst_stex_tp},
        )
        return data

    def fetch_cash_detail(self, qry_tp: str = "3") -> dict:
        """kt00001: 예수금상세현황요청.

        qry_tp: "3"=추정조회, "2"=일반조회
        응답: 예수금/출금가능금액/주문가능금액 등 현금 상세
        """
        data, _ = self._post(
            "/api/dostk/acnt",
            "kt00001",
            {"qry_tp": qry_tp},
        )
        return data

    # ── 거래대금 상위 조회 (대시보드 /top 탭) ─────────────────────

    def fetch_top_volume(
        self,
        n: int = 20,
        market: str = "000",
    ) -> list[dict]:
        """장중 거래대금 상위 N 종목 조회 — ka10032 거래대금상위요청.

        market: "000"=전체, "001"=KOSPI, "101"=KOSDAQ

        API 스키마 (REST API 문서 p.102-103):
          URL:   /api/dostk/rkinfo
          API ID: ka10032
          응답 배열 키: trde_prica_upper
          거래대금 필드: trde_prica (단위: 백만원 × 1_000_000 → 원)
          검증: 삼성전자 152,000원 × 3,495만주 ≈ 5.31조원 = trde_prica(5,308,092) × 1,000,000 ✓
        """
        data, _ = self._post(
            "/api/dostk/rkinfo",
            "ka10032",
            {
                "mrkt_tp":       market,   # 000:전체, 001:KOSPI, 101:KOSDAQ
                "mang_stk_incls": "1",     # 1:관리종목 포함
                "stex_tp":       "3",      # 3:전체(KRX+NXT)
            },
        )

        rows = data.get("trde_prica_upper") or []
        result: list[dict] = []
        for i, r in enumerate(rows[:n]):
            raw_amt = _parse_int(r.get("trde_prica"))
            if raw_amt is not None and raw_amt == 0:
                # 거래대금 명시적 0 = 장 미개장 또는 무의미한 행 → 건너뜀
                continue
            result.append({
                "rank":       _parse_int(r.get("now_rank")) or (i + 1),
                "ticker":     str(r.get("stk_cd", "")).strip().zfill(6),
                "name":       str(r.get("stk_nm") or r.get("stk_cd") or "").strip(),
                "price":      abs(_parse_int(r.get("cur_prc")) or 0),
                "change_pct": _parse_float(r.get("flu_rt")) or 0.0,
                "amount":     (raw_amt or 0) * _VALUE_UNIT,
            })
        return result


# ── 파싱 ─────────────────────────────────────────────────────────

def _strip_sign(s: str) -> str:
    """'+95400' → '95400', '-3000' → '-3000'"""
    if s and s[0] in ("+",):
        return s[1:]
    return s


def _parse_int(s) -> Optional[int]:
    if s is None:
        return None
    s = str(s).replace(",", "").strip()
    s = _strip_sign(s)
    if s in ("-", "", "N/A", "nan", "0"):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _parse_float(s) -> Optional[float]:
    if s is None:
        return None
    s = str(s).replace(",", "").strip()
    s = _strip_sign(s)
    if s in ("-", "", "N/A", "nan"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_bulk_rows(
    rows: list[dict],
    trade_date: date,
    suffix: str,          # ".KS" for KOSPI, ".KQ" for KOSDAQ
    market_filter: Optional[str] = None,
) -> list[AftermarketRecord]:
    """ka10098 응답 행 목록 → AftermarketRecord 목록."""
    records: list[AftermarketRecord] = []

    for row in rows:
        try:
            krx_code = str(row.get("stk_cd", "")).strip().zfill(6)
            if not krx_code or krx_code == "000000":
                continue

            ticker = f"{krx_code}{suffix}"

            # 시장 필터
            if market_filter == "KOSPI"  and suffix != ".KS":
                continue
            if market_filter == "KOSDAQ" and suffix != ".KQ":
                continue

            after_close  = _parse_int(row.get("cur_prc"))
            after_volume = _parse_int(row.get("acc_trde_qty"))
            reg_close    = _parse_int(row.get("tdy_close_pric"))

            # 거래량 0 제외 (시간외 거래 없음)
            if after_volume is None or after_volume == 0:
                continue

            # 거래대금: acc_trde_prica × _VALUE_UNIT → 원
            raw_val = _parse_int(row.get("acc_trde_prica"))
            after_value = (raw_val * _VALUE_UNIT) if raw_val is not None else None

            # 등락률
            flu_raw = _parse_float(row.get("flu_rt"))
            if flu_raw is not None:
                after_chg_pct = round(flu_raw, 2)
            elif reg_close and after_close and reg_close > 0:
                after_chg_pct = round((after_close / reg_close - 1.0) * 100, 2)
            else:
                after_chg_pct = None

            records.append(AftermarketRecord(
                trade_date=trade_date,
                ticker=ticker,
                reg_close=reg_close,
                after_close=after_close,
                after_volume=after_volume,
                after_value=after_value,
                after_chg_pct=after_chg_pct,
            ))
        except Exception as e:
            logger.debug("[kiwoom] 행 파싱 실패: %s — %s", row, e)

    return records


# ── DB 유틸 ──────────────────────────────────────────────────────

# core/db_sync.py로 통합 (2026-07 Phase B) — 기존 이름 유지
from core.db_sync import connect as _connect


def ensure_table(dsn: str) -> None:
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            for stmt in _CREATE_TABLE.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)
            cur.execute(
                "ALTER TABLE aftermarket_snap ADD COLUMN IF NOT EXISTS reg_value BIGINT"
            )
            cur.execute("ALTER TABLE aftermarket_snap ENABLE ROW LEVEL SECURITY;")
            cur.execute("""
                DO $$ BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM pg_policies
                    WHERE schemaname='public' AND tablename='aftermarket_snap' AND policyname='backend_all'
                  ) THEN
                    CREATE POLICY backend_all ON aftermarket_snap FOR ALL TO service_role USING (true) WITH CHECK (true);
                  ELSE
                    ALTER POLICY backend_all ON aftermarket_snap TO service_role;
                  END IF;
                END $$;
            """)
        conn.commit()
    finally:
        conn.close()


def upsert_records(dsn: str, records: list[AftermarketRecord]) -> int:
    if not records:
        return 0
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO aftermarket_snap
                    (trade_date, ticker, reg_close, after_close,
                     after_volume, after_value, reg_value, after_chg_pct)
                VALUES %s
                ON CONFLICT (trade_date, ticker) DO UPDATE SET
                    reg_close     = EXCLUDED.reg_close,
                    after_close   = EXCLUDED.after_close,
                    after_volume  = EXCLUDED.after_volume,
                    after_value   = EXCLUDED.after_value,
                    reg_value     = EXCLUDED.reg_value,
                    after_chg_pct = EXCLUDED.after_chg_pct,
                    fetched_at    = now()
                """,
                [
                    (r.trade_date, r.ticker, r.reg_close, r.after_close,
                     r.after_volume, r.after_value, r.reg_value, r.after_chg_pct)
                    for r in records
                ],
            )
        conn.commit()
        return len(records)
    finally:
        conn.close()


def _enrich_with_reg_value(client: "KiwoomClient", records: list[AftermarketRecord]) -> None:
    """ka10032(거래대금상위)로 KRX+NXT 합산 거래대금을 reg_value에 채움.

    ka10032는 장 마감 후에도 당일 최종 합산값을 반환.
    stex_tp="3" (전체=KRX+NXT) 기준 상위 500종목 조회.

    ka10032 stk_cd 형식: "005930_AL"(KOSPI) / "035720_AQ"(KOSDAQ)
    aftermarket_snap ticker 형식: "005930.KS" / "035720.KQ"
    → _AL→.KS, _AQ→.KQ 변환 후 매칭.
    """
    if not records:
        return
    try:
        top_items = client.fetch_top_volume(n=500)
    except Exception as e:
        logger.warning("[kiwoom] reg_value 조회(ka10032) 실패 (무시): %s", e)
        return

    # _AL/.KS / _AQ/.KQ 변환 → aftermarket_snap ticker와 동일 형식으로 정규화
    from core.tickers import kiwoom_to_yfinance

    def _to_snap_ticker(raw: str) -> str:
        # 매치 안 되면(접미사도 '.'도 없는 원시 코드) raw 그대로 — 알 수 없는
        # 케이스를 조용히 스킵하기보다 원본 유지해 디버깅 여지를 남긴다.
        return kiwoom_to_yfinance(raw) or raw

    amount_map: dict[str, int] = {
        _to_snap_ticker(it["ticker"]): it["amount"]
        for it in top_items
        if it.get("amount")
    }

    n = 0
    for r in records:
        amt = amount_map.get(r.ticker)
        if amt:
            r.reg_value = amt
            n += 1
    logger.info("[kiwoom] reg_value 보강 완료: %d/%d건 (ka10032 top500 매칭)", n, len(records))


# ── daily_market_snap DDL + 저장 ─────────────────────────────────

_DDL_DAILY_SNAP = """
CREATE TABLE IF NOT EXISTS daily_market_snap (
    trade_date  DATE         NOT NULL,
    ticker      VARCHAR(12)  NOT NULL,
    name        VARCHAR(100),
    price       INTEGER,
    change_pct  NUMERIC(6, 2),
    amount      BIGINT,
    market      VARCHAR(10),
    fetched_at  TIMESTAMPTZ  DEFAULT now(),
    PRIMARY KEY (trade_date, ticker)
);
CREATE INDEX IF NOT EXISTS idx_daily_snap_date
    ON daily_market_snap (trade_date DESC);
"""


def ensure_daily_snap_table(dsn: str) -> None:
    """daily_market_snap 테이블 생성 + RLS 활성화."""
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            for stmt in _DDL_DAILY_SNAP.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)
            cur.execute("ALTER TABLE daily_market_snap ENABLE ROW LEVEL SECURITY")
            cur.execute("""
                DO $$ BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM pg_policies
                    WHERE schemaname='public'
                      AND tablename='daily_market_snap'
                      AND policyname='backend_all'
                  ) THEN
                    CREATE POLICY backend_all ON daily_market_snap
                      FOR ALL TO service_role USING (true) WITH CHECK (true);
                  ELSE
                    ALTER POLICY backend_all ON daily_market_snap TO service_role;
                  END IF;
                END $$
            """)
        conn.commit()
    finally:
        conn.close()


def _raw_to_yf(raw: str) -> str | None:
    """'000660_AL' → '000660.KS', '035720_AQ' → '035720.KQ'. 변환 불가 시 None."""
    from core.tickers import kiwoom_to_yfinance
    return kiwoom_to_yfinance(raw)


def save_daily_market_snap(dsn: str, items: list[dict], trade_date: date) -> int:
    """ka10032 items → daily_market_snap upsert."""
    if not items:
        return 0
    records = []
    for it in items:
        ticker = _raw_to_yf(it.get("ticker", ""))
        if not ticker:
            continue
        market = "KOSPI" if ticker.endswith(".KS") else "KOSDAQ"
        records.append((
            trade_date,
            ticker,
            it.get("name") or "",
            it.get("price"),
            it.get("change_pct"),
            it.get("amount"),
            market,
        ))
    if not records:
        return 0
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO daily_market_snap
                    (trade_date, ticker, name, price, change_pct, amount, market)
                VALUES %s
                ON CONFLICT (trade_date, ticker) DO UPDATE SET
                    name       = EXCLUDED.name,
                    price      = EXCLUDED.price,
                    change_pct = EXCLUDED.change_pct,
                    amount     = EXCLUDED.amount,
                    market     = EXCLUDED.market,
                    fetched_at = now()
                """,
                records,
            )
        conn.commit()
        return len(records)
    finally:
        conn.close()


def run_daily_snap(dsn: str, client: "KiwoomClient", trade_date: date) -> int:
    """ka10032 top100 조회 후 daily_market_snap 저장. 저장 건수 반환."""
    ensure_daily_snap_table(dsn)
    items = client.fetch_top_volume(n=100)
    if not items:
        logger.warning("[daily-snap] ka10032 응답 없음")
        return 0
    saved = save_daily_market_snap(dsn, items, trade_date)
    logger.info("[daily-snap] %s %d건 저장", trade_date, saved)
    return saved


def already_loaded(dsn: str, trade_date: date) -> bool:
    """해당 날짜 데이터가 이미 DB에 있는지 확인."""
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM aftermarket_snap WHERE trade_date = %s",
                (trade_date,),
            )
            return cur.fetchone()[0] > 0
    finally:
        conn.close()


# ── probe: 응답 구조 출력 ─────────────────────────────────────────

def probe(client: KiwoomClient) -> None:
    print("\n[probe] ka10098 KOSPI 응답 구조 확인...")
    rows = client.fetch_aftermarket_bulk(mrkt_tp="001")
    if not rows:
        print("[probe] 데이터 없음 (장 마감 전이거나 시간외 거래 없음)")
        return
    print(f"[probe] KOSPI {len(rows)}행 수신")
    print(f"[probe] 컬럼: {list(rows[0].keys())}")
    print("\n[probe] 첫 3행 (KOSPI):")
    for r in rows[:3]:
        print(" ", r)

    _time.sleep(0.5)

    print("\n[probe] ka10098 KOSDAQ 응답 구조 확인...")
    rows_kq = client.fetch_aftermarket_bulk(mrkt_tp="101")
    print(f"[probe] KOSDAQ {len(rows_kq)}행 수신")
    if rows_kq:
        print("\n[probe] 첫 3행 (KOSDAQ):")
        for r in rows_kq[:3]:
            print(" ", r)

    print(
        "\n[probe] 필드 해석:\n"
        "  cur_prc        → 시간외 현재가 (after_close)\n"
        "  tdy_close_pric → 당일 정규장 종가 (reg_close)\n"
        "  acc_trde_qty   → 시간외 누적 거래량 (after_volume)\n"
        f"  acc_trde_prica → 시간외 거래대금 × {_VALUE_UNIT} = after_value (원)\n"
        "  flu_rt         → 시간외 등락률 (after_chg_pct)\n"
        "\n  단위가 맞지 않으면 _VALUE_UNIT 상수를 수정하세요."
    )


# ── 메인 실행 ────────────────────────────────────────────────────

def _build_client(use_mock: bool = False) -> KiwoomClient:
    appkey    = os.environ.get("KIWOOM_APPKEY")
    secretkey = os.environ.get("KIWOOM_SECRETKEY")
    token_env = os.environ.get("KIWOOM_TOKEN")

    client = KiwoomClient(use_mock=use_mock)

    if token_env:
        client.inject_token(token_env)
    elif appkey and secretkey:
        client.issue_token(appkey, secretkey)
    else:
        logger.warning(
            "[kiwoom] 인증 정보 없음. "
            ".env 에 KIWOOM_APPKEY+KIWOOM_SECRETKEY 또는 KIWOOM_TOKEN 설정 필요."
        )
    return client


def _collect(
    client: KiwoomClient,
    trade_date: date,
    markets: list[tuple[str, str]],  # [(mrkt_tp, suffix), ...]
) -> list[AftermarketRecord]:
    """markets 목록에 대해 ka10098 수집 후 파싱."""
    all_records: list[AftermarketRecord] = []
    for mrkt_tp, suffix in markets:
        label = "KOSPI" if suffix == ".KS" else "KOSDAQ"
        logger.info("[kiwoom] %s 시간외 수집 중 (mrkt_tp=%s)...", label, mrkt_tp)
        rows = client.fetch_aftermarket_bulk(mrkt_tp=mrkt_tp)
        records = parse_bulk_rows(rows, trade_date, suffix)
        logger.info("[kiwoom] %s — %d건 파싱", label, len(records))
        all_records.extend(records)
        _time.sleep(0.3)
    return all_records


def main() -> None:
    parser = argparse.ArgumentParser(description="키움 REST API 시간외 단일가 수집기")

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--today",       action="store_true",
                      help="당일 시간외 데이터 수집")
    mode.add_argument("--incremental", action="store_true",
                      help="전일 날짜로 당일 데이터 적재 (스케줄러 16:05 실행)")
    mode.add_argument("--probe",       action="store_true",
                      help="응답 구조 확인 (DB 저장 없음)")

    parser.add_argument("--trade-date", metavar="DATE",
                        help="적재 trade_date override (YYYY-MM-DD). "
                             "--today 와 함께 사용")
    parser.add_argument("--market", choices=["KOSPI", "KOSDAQ", "ALL"],
                        default="ALL", help="시장 필터 (기본: ALL)")
    parser.add_argument("--mock", action="store_true",
                        help="mockapi.kiwoom.com 사용 (장외 테스트)")
    parser.add_argument("--force", action="store_true",
                        help="이미 수집된 날짜도 재수집")

    args = parser.parse_args()

    client = _build_client(use_mock=args.mock)

    # ── probe 모드 ──────────────────────────────────────────────
    if args.probe:
        probe(client)
        return

    if not (args.today or args.incremental):
        parser.error("--today | --incremental | --probe 중 하나 필요")

    # ── 날짜 결정 ───────────────────────────────────────────────
    if args.trade_date:
        trade_date = date.fromisoformat(args.trade_date)
    elif args.incremental:
        trade_date = last_trading_day(date.today())
    else:
        trade_date = date.today()

    # ── DB 준비 ─────────────────────────────────────────────────
    # get_dsn() 사용 (2026-07 Phase B): DSN 조립 중복 제거 + 특수문자
    # 비밀번호 URL 인코딩을 얻음 (기존 f-string은 인코딩 없이 깨졌음).
    from core.db import get_dsn
    dsn = get_dsn()
    ensure_table(dsn)

    if not args.force and already_loaded(dsn, trade_date):
        logger.info("[kiwoom] %s 이미 수집됨 — 스킵 (--force 로 재수집 가능)", trade_date)
        return

    # ── 시장 목록 결정 ──────────────────────────────────────────
    if args.market == "KOSPI":
        markets = [("001", ".KS")]
    elif args.market == "KOSDAQ":
        markets = [("101", ".KQ")]
    else:
        # ALL: KOSPI + KOSDAQ 각각 호출해 suffix 확정
        markets = [("001", ".KS"), ("101", ".KQ")]

    # ── 수집 + 저장 ─────────────────────────────────────────────
    records = _collect(client, trade_date, markets)

    if not records:
        logger.warning("[kiwoom] 수집된 데이터 없음 — 시간외 거래가 없거나 장 진행 중")
        return

    # reg_value: ka10032(KRX+NXT 합산)로 당일 최종 거래대금 보강
    _enrich_with_reg_value(client, records)

    saved = upsert_records(dsn, records)
    logger.info("[kiwoom] 완료 — %s %d건 저장", trade_date, saved)


if __name__ == "__main__":
    main()
