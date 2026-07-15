"""
krx_aftermarket_sync.py — 시간외 단일가 스냅샷을 aftermarket_snap 테이블에 적재

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
설계 의도:

  한국 정규장(09:00~15:30) 마감 후 시간외 단일가(15:40~16:00)에서
  가격이 역전되는 종목을 추적. 정규장에서 밀렸다가 시간외에서 회복하는
  패턴을 감지하거나, 시간외 거래량을 기관/외인 잔량 소화 신호로 해석.

  수집 데이터 (aftermarket_snap):
    reg_close     : 정규장 종가
    after_close   : 시간외 마지막 단일가 체결가
    after_volume  : 시간외 총 거래량 (주)
    after_value   : 시간외 총 거래대금 (원)
    after_chg_pct : (after_close / reg_close − 1) × 100  — 시간외 등락률

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
데이터 소스:

  data.krx.co.kr → 주식 → 시간외 단일가 → 일별 체결
  endpoint BLD: dbms/MDC/STAT/standard/MDCSTAT01501

  ⚠️  BLD 문자열은 KRX 페이지 변경에 따라 바뀔 수 있음.
      첫 실행 전 반드시 확인:
        python krx_aftermarket_sync.py --probe 2026-05-08

  인증: .env의 KRX_SESSION (우선) 또는 KRX_ID/KRX_PW.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
사용법:

  # BLD 응답 구조 확인 (첫 실행 시 필수):
  python krx_aftermarket_sync.py --probe 2026-05-08

  # 단일 날짜:
  python krx_aftermarket_sync.py --date 2026-05-08

  # 날짜 범위 (벌크 적재):
  python krx_aftermarket_sync.py --start 2025-01-02 --end 2026-05-08

  # 증분 (전일 데이터만, 스케줄러용):
  python krx_aftermarket_sync.py --incremental

  # 시장 필터 + 건수 제한 (테스트):
  python krx_aftermarket_sync.py --date 2026-05-08 --market KOSPI --max 50
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

KRX 응답 예상 구조 (--probe로 확인):
  MKSC_SHRN_ISCD : 단축 종목코드 (6자리)
  ISU_ABBRV      : 종목 약명
  TDD_CLSPRC     : 시간외 종가 (마지막 단일가)
  ACC_TRDVOL     : 시간외 누적 거래량
  ACC_TRDVAL     : 시간외 누적 거래대금
  CMPPREVDD_PRC  : 전일 대비 (또는 정규장 종가 대비)

  컬럼명은 실제 응답과 다를 수 있음 — --probe로 반드시 검증.
"""
from __future__ import annotations

import argparse
import json as _json
import logging
import os
import sys
import time as _time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import psycopg2
import psycopg2.extras
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


# ── DDL ──────────────────────────────────────────────────────────

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS aftermarket_snap (
    trade_date    DATE            NOT NULL,
    ticker        VARCHAR(12)     NOT NULL,   -- yfinance 심볼 (005930.KS / 035720.KQ)
    reg_close     NUMERIC(12, 0),             -- 정규장 종가 (원)
    after_close   NUMERIC(12, 0),             -- 시간외 마지막 단일가 (원)
    after_volume  BIGINT,                     -- 시간외 거래량 (주)
    after_value   BIGINT,                     -- 시간외 거래대금 (원)
    after_chg_pct NUMERIC(6, 2),              -- (after_close/reg_close − 1)×100 (%)
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
    ticker:        str             # yfinance 심볼 (005930.KS)
    krx_code:      str             # KRX 6자리 코드 (005930)
    reg_close:     Optional[int]   # 정규장 종가
    after_close:   Optional[int]   # 시간외 종가
    after_volume:  Optional[int]   # 시간외 거래량
    after_value:   Optional[int]   # 시간외 거래대금
    after_chg_pct: Optional[float] # 시간외 등락률 (%)


# ── KRX 요청 ─────────────────────────────────────────────────────

class _KrxAftermarketFetcher:
    """data.krx.co.kr 시간외 단일가 수집기.

    ⚠️  BLD 문자열은 --probe 로 실제 응답 구조를 먼저 확인하세요.
        KRX 페이지 개편 시 변경될 수 있습니다.
    """

    _BASE    = "https://data.krx.co.kr"
    _WARMUP  = _BASE + "/contents/MDC/MDI/index.cmd"
    _LOGIN   = _BASE + "/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
    _DATA    = _BASE + "/comm/bldAttendant/getJsonData.cmd"

    # 시간외 단일가 일별 체결현황
    # ⚠️  첫 실행 전 --probe 로 검증 필요
    _BLD = "dbms/MDC/STAT/standard/MDCSTAT01501"

    # KRX 응답 컬럼 매핑 (--probe 결과에 따라 수정)
    # key = 내부 필드명, value = KRX 응답 컬럼명
    _COL_CODE      = "MKSC_SHRN_ISCD"   # 단축 종목코드
    _COL_MKT       = "MKT_ID"            # 시장 구분 (STK=KOSPI, KSQ=KOSDAQ)
    _COL_CLOSE     = "TDD_CLSPRC"        # 시간외 종가
    _COL_VOLUME    = "ACC_TRDVOL"        # 누적 거래량
    _COL_VALUE     = "ACC_TRDVAL"        # 누적 거래대금
    _COL_REG_CLOSE = "CMPPREVDD_PRC"     # 정규장 종가 (전일 대비 기준가)

    _HEADERS = {
        "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer":          _BASE + "/contents/MDC/MDI/outerLoader/index.cmd",
        "X-Requested-With": "XMLHttpRequest",
        "Accept":           "application/json, text/javascript, */*; q=0.01",
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
    }

    def __init__(self) -> None:
        import requests as _req
        self._session = _req.Session()
        self._authenticated = False

    def _post(self, url: str, data: dict, timeout: int = 30) -> bytes:
        resp = self._session.post(url, data=data, headers=self._HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp.content

    @staticmethod
    def _decode(raw: bytes) -> str:
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return raw.decode("euc-kr", errors="replace")

    def warmup(self) -> None:
        try:
            self._session.get(self._WARMUP, headers=self._HEADERS, timeout=15)
        except Exception as e:
            logger.debug("[aftermarket] warmup 실패: %s", e)

    def login(self, krx_id: str, krx_pw: str) -> bool:
        try:
            raw  = self._post(self._LOGIN, {"userId": krx_id, "userPw": krx_pw})
            text = self._decode(raw)
            if "success" in text.lower() or text.strip().upper() == "OK":
                self._authenticated = True
                logger.info("[aftermarket] 로그인 성공")
                return True
            logger.warning("[aftermarket] 로그인 실패: %s", text[:120])
            return False
        except Exception as e:
            logger.warning("[aftermarket] 로그인 실패: %s", e)
            return False

    def inject_session(self, jsessionid: str, visitor_id: Optional[str] = None) -> None:
        self._session.cookies.set("JSESSIONID", jsessionid, domain=".krx.co.kr")
        self._session.cookies.set("mdc.client_session", "true", domain="data.krx.co.kr")
        if visitor_id:
            self._session.cookies.set("__smVisitorID", visitor_id, domain="data.krx.co.kr")
        self._authenticated = True
        logger.info("[aftermarket] KRX_SESSION 쿠키 주입 완료")

    def fetch_raw(self, target_date: date) -> list[dict]:
        """target_date 하루 전 종목의 시간외 체결 원본 행 반환.

        하루 1 요청으로 전 종목 데이터를 수신 (per-date 방식).
        """
        date_str = target_date.strftime("%Y%m%d")
        payload = {
            "bld":      self._BLD,
            "trdDd":    date_str,   # 조회 날짜
            "share":    "1",        # 주수 기준
            "money":    "1",        # 거래대금 원 기준
            "csvxls_isNo": "false",
        }
        try:
            raw  = self._post(self._DATA, payload)
            text = self._decode(raw)
            parsed = _json.loads(text)

            # KRX 표준 응답: {"OutBlock_1": [...]} 또는 {"output": [...]}
            rows = (
                parsed.get("OutBlock_1")
                or parsed.get("output")
                or parsed.get("block1")
                or []
            )
            logger.info("[aftermarket] %s — %d행 수신", date_str, len(rows))
            return rows
        except Exception as e:
            logger.warning("[aftermarket] %s 수집 실패: %s", date_str, e)
            return []

    def probe(self, target_date: date) -> None:
        """응답 구조 출력 — BLD 및 컬럼명 검증용."""
        rows = self.fetch_raw(target_date)
        if not rows:
            print(f"[probe] {target_date}: 데이터 없음 (BLD={self._BLD!r} 재확인 필요)")
            return
        print(f"\n[probe] {target_date}: {len(rows)}행 수신")
        print(f"[probe] BLD: {self._BLD}")
        print(f"\n[probe] 컬럼 목록: {list(rows[0].keys())}")
        print("\n[probe] 첫 3행:")
        for row in rows[:3]:
            print(" ", row)
        print(
            "\n[probe] 확인 후 _COL_* 상수를 실제 컬럼명으로 수정하세요:\n"
            f"  _COL_CODE      = {self._COL_CODE!r}\n"
            f"  _COL_MKT       = {self._COL_MKT!r}\n"
            f"  _COL_CLOSE     = {self._COL_CLOSE!r}\n"
            f"  _COL_VOLUME    = {self._COL_VOLUME!r}\n"
            f"  _COL_VALUE     = {self._COL_VALUE!r}\n"
            f"  _COL_REG_CLOSE = {self._COL_REG_CLOSE!r}\n"
        )

    def parse_rows(
        self,
        rows: list[dict],
        target_date: date,
        market_filter: Optional[str] = None,
    ) -> list[AftermarketRecord]:
        """원본 행 목록 → AftermarketRecord 목록 변환."""
        records: list[AftermarketRecord] = []
        for row in rows:
            try:
                krx_code = str(row.get(self._COL_CODE, "")).strip().zfill(6)
                if not krx_code or krx_code == "000000":
                    continue

                # 시장 구분
                mkt_raw = str(row.get(self._COL_MKT, "")).upper()
                if "KSQ" in mkt_raw or "KOSDAQ" in mkt_raw:
                    suffix = ".KQ"
                else:
                    suffix = ".KS"

                if market_filter == "KOSPI"  and suffix != ".KS":
                    continue
                if market_filter == "KOSDAQ" and suffix != ".KQ":
                    continue

                ticker = f"{krx_code}{suffix}"

                after_close  = _parse_int(row.get(self._COL_CLOSE,     ""))
                after_volume = _parse_int(row.get(self._COL_VOLUME,     ""))
                after_value  = _parse_int(row.get(self._COL_VALUE,      ""))
                reg_close    = _parse_int(row.get(self._COL_REG_CLOSE,  ""))

                # 시간외 거래량 0인 종목 제외 (시간외 거래 없음)
                if after_volume is None or after_volume == 0:
                    continue

                after_chg_pct: Optional[float] = None
                if reg_close and after_close and reg_close > 0:
                    after_chg_pct = round((after_close / reg_close - 1.0) * 100, 2)

                records.append(AftermarketRecord(
                    trade_date=target_date,
                    ticker=ticker,
                    krx_code=krx_code,
                    reg_close=reg_close,
                    after_close=after_close,
                    after_volume=after_volume,
                    after_value=after_value,
                    after_chg_pct=after_chg_pct,
                ))
            except Exception as e:
                logger.debug("[aftermarket] 행 파싱 실패: %s — %s", row, e)

        return records


# ── DB 유틸 ──────────────────────────────────────────────────────

def _connect(dsn: str):
    return psycopg2.connect(dsn)


def ensure_table(dsn: str) -> None:
    """aftermarket_snap 테이블 및 인덱스 생성 + RLS 활성화 (멱등)."""
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            for stmt in _CREATE_TABLE.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)
            cur.execute("ALTER TABLE aftermarket_snap ENABLE ROW LEVEL SECURITY;")
        conn.commit()
    finally:
        conn.close()


def upsert_records(dsn: str, records: list[AftermarketRecord]) -> int:
    """aftermarket_snap 테이블에 upsert. 저장된 행 수 반환."""
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
                     after_volume, after_value, after_chg_pct)
                VALUES %s
                ON CONFLICT (trade_date, ticker) DO UPDATE SET
                    reg_close     = EXCLUDED.reg_close,
                    after_close   = EXCLUDED.after_close,
                    after_volume  = EXCLUDED.after_volume,
                    after_value   = EXCLUDED.after_value,
                    after_chg_pct = EXCLUDED.after_chg_pct,
                    fetched_at    = now()
                """,
                [
                    (r.trade_date, r.ticker, r.reg_close, r.after_close,
                     r.after_volume, r.after_value, r.after_chg_pct)
                    for r in records
                ],
            )
        conn.commit()
        return len(records)
    finally:
        conn.close()


def load_missing_dates(dsn: str, start: date, end: date) -> list[date]:
    """start~end 범위 중 DB에 없는 영업일 목록 반환 (증분 모드용)."""
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT trade_date FROM aftermarket_snap "
                "WHERE trade_date BETWEEN %s AND %s",
                (start, end),
            )
            have = {row[0] for row in cur.fetchall()}
    finally:
        conn.close()

    missing = []
    d = start
    while d <= end:
        if d.weekday() < 5 and d not in have:  # 월~금, DB 미보유
            missing.append(d)
        d += timedelta(days=1)
    return missing


# ── 파싱 유틸 ────────────────────────────────────────────────────

def _parse_int(s) -> Optional[int]:
    if s is None:
        return None
    s = str(s).replace(",", "").strip()
    if s in ("-", "", "N/A", "nan"):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


# ── 날짜 범위 헬퍼 ───────────────────────────────────────────────

def _business_days(start: date, end: date) -> list[date]:
    result, d = [], start
    while d <= end:
        if d.weekday() < 5:
            result.append(d)
        d += timedelta(days=1)
    return result


# ── 메인 실행 ────────────────────────────────────────────────────

def _run(
    fetcher: _KrxAftermarketFetcher,
    dsn: str,
    dates: list[date],
    market_filter: Optional[str],
    max_tickers: int,
    delay: float = 1.5,
) -> None:
    """날짜 목록에 대해 수집 → upsert 반복."""
    total = 0
    for i, d in enumerate(dates):
        rows    = fetcher.fetch_raw(d)
        records = fetcher.parse_rows(rows, d, market_filter)
        if max_tickers > 0:
            records = records[:max_tickers]
        saved = upsert_records(dsn, records)
        total += saved
        logger.info("[aftermarket] %s — %d건 저장 (%d/%d일)", d, saved, i + 1, len(dates))
        if i < len(dates) - 1:
            _time.sleep(delay)

    logger.info("[aftermarket] 완료 — 총 %d건 저장", total)


def main() -> None:
    parser = argparse.ArgumentParser(description="KRX 시간외 단일가 수집기")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--probe",        metavar="DATE",
                     help="응답 구조 출력 (YYYY-MM-DD). BLD/컬럼명 검증용")
    grp.add_argument("--date",         metavar="DATE",
                     help="단일 날짜 수집 (YYYY-MM-DD)")
    grp.add_argument("--incremental",  action="store_true",
                     help="전일 데이터만 수집 (스케줄러용)")
    parser.add_argument("--start",     metavar="DATE",
                        help="시작일 (YYYY-MM-DD), --end 와 함께 사용")
    parser.add_argument("--end",       metavar="DATE",
                        help="종료일 (YYYY-MM-DD)")
    parser.add_argument("--market",    choices=["KOSPI", "KOSDAQ", "ALL"],
                        default="ALL",  help="시장 필터 (기본: ALL)")
    parser.add_argument("--max",       type=int, default=0,
                        help="최대 종목 수 (0=전종목, 테스트용)")
    parser.add_argument("--delay",     type=float, default=1.5,
                        help="요청 간 딜레이(초, 기본 1.5)")
    args = parser.parse_args()

    dsn         = os.environ.get("DATABASE_URL") or (
        f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ.get('DB_PORT', 5432)}"
        f"/{os.environ['DB_NAME']}"
    )
    krx_id      = os.environ.get("KRX_ID")
    krx_pw      = os.environ.get("KRX_PW")
    krx_session = os.environ.get("KRX_SESSION")
    krx_visitor = os.environ.get("KRX_VISITOR")
    market_filter = None if args.market == "ALL" else args.market

    fetcher = _KrxAftermarketFetcher()
    fetcher.warmup()
    if krx_session:
        fetcher.inject_session(krx_session, krx_visitor)
    elif krx_id and krx_pw:
        fetcher.login(krx_id, krx_pw)
    else:
        logger.warning(
            "[aftermarket] KRX 인증 정보 없음. .env에 KRX_SESSION 또는 KRX_ID/KRX_PW 설정 필요."
        )

    # ── probe 모드 ──────────────────────────────────────────────
    if args.probe:
        fetcher.probe(date.fromisoformat(args.probe))
        return

    # ── DB 테이블 준비 ──────────────────────────────────────────
    ensure_table(dsn)

    # ── 날짜 결정 ───────────────────────────────────────────────
    if args.incremental:
        yesterday = last_trading_day(date.today())  # 주말이면 전 영업일로
        dates = load_missing_dates(dsn, yesterday, yesterday)
        if not dates:
            logger.info("[aftermarket] %s 이미 수집됨 — 스킵", yesterday)
            return
    elif args.date:
        dates = [date.fromisoformat(args.date)]
    elif args.start and args.end:
        start = date.fromisoformat(args.start)
        end   = date.fromisoformat(args.end)
        dates = load_missing_dates(dsn, start, end)
        logger.info("[aftermarket] 미수집 날짜 %d일", len(dates))
    else:
        parser.error("--probe | --date | --incremental | (--start + --end) 중 하나 필요")
        return

    _run(fetcher, dsn, dates, market_filter, args.max, args.delay)


if __name__ == "__main__":
    main()
