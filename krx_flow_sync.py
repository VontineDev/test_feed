"""
krx_flow_sync.py — 외국인/기관 순매수 이력을 daily_flow 테이블에 적재

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
백엔드 (--backend 옵션으로 선택, 기본: krx-direct):

  krx-direct (기본, 권장)
    data.krx.co.kr에 직접 HTTP 요청. pykrx 의존성 없음.
    Python 3.14에서도 동작 (pykrx EUC-KR 파싱 버그 없음).
    .env에 KRX_ID=아이디, KRX_PW=비밀번호 필요.
    한국 ISP 또는 VPN 환경 필요 (해외 IP 차단).

  pykrx
    pykrx 라이브러리 경유. Python 3.12 이하 권장.
    동일 자격증명(KRX_ID/KRX_PW) 사용.

  csv
    --csv <파일> 로 수동 임포트.
    data.krx.co.kr → 주식 → 투자자별 거래실적 → CSV 다운로드

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
참고: KRX OpenAPI (openapi.krx.co.kr / pykrx-openapi)는
      투자자 순매수 데이터를 제공하지 않습니다 (OHLCV/지수만 제공).
      투자자 데이터는 data.krx.co.kr에서만 제공됩니다.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

사용법:
  # 벌크 적재 (초기 1회):
  python krx_flow_sync.py --start 2023-01-01 --end 2026-05-03

  # 특정 백엔드:
  python krx_flow_sync.py --start 2025-01-01 --backend pykrx
  python krx_flow_sync.py --csv /path/to/data.csv --backend csv

  # 응답 구조 확인 (첫 실행 시 권장):
  python krx_flow_sync.py --probe 005930

  # 증분 모드 (스케줄러에서 매일):
  python krx_flow_sync.py --incremental

  # 시장/티커 수 제한 (테스트):
  python krx_flow_sync.py --start 2025-01-01 --end 2026-05-03 --market KOSPI --max 50
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CSV 형식 (--csv):
  컬럼: date, ticker, foreign_net, inst_net
  date 형식: YYYY-MM-DD 또는 YYYYMMDD
  ticker: yfinance 심볼 (005930.KS) 또는 KRX 6자리 코드 (005930)
  예:
    date,ticker,foreign_net,inst_net
    2025-01-02,005930.KS,123456,-78900
    2025-01-02,035720.KQ,-5000,12000

  또는 KRX 다운로드 CSV (자동 감지):
    날짜,종목코드,종목명,외국인순매수,기관합계
    20250102,005930,삼성전자,123456,-78900
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json as _json
import logging
import os
import sys
import time as _time
from datetime import date, timedelta
from typing import Optional

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── 수급 레코드 ────────────────────────────────────────────────
from dataclasses import dataclass


@dataclass
class FlowRecord:
    ticker: str        # yfinance 심볼 (005930.KS / 035720.KQ)
    trade_date: date
    foreign_net: Optional[int]   # 외국인 순매수 (주). 음수 = 순매도
    inst_net: Optional[int]      # 기관 순매수 (주). 음수 = 순매도
    foreign_streak: Optional[int] = None
    inst_streak: Optional[int] = None


# ── 스트릭 계산 ────────────────────────────────────────────────

def _next_streak(prev: Optional[int], net: Optional[int]) -> Optional[int]:
    """순매수 연속일 계산.
    prev=None → 초기값으로 취급 (net 부호만 반영).
    net=None  → 데이터 없음, streak 0 반환.
    """
    if net is None:
        return 0
    p = prev or 0
    if net > 0:
        return max(1, p + 1)
    if net < 0:
        return min(-1, p - 1)
    return 0


# ── KRX Direct 백엔드 ─────────────────────────────────────────
#
# pykrx 없이 data.krx.co.kr에 직접 HTTP 요청.
# pykrx의 Python 3.14 EUC-KR 파싱 버그를 우회하며,
# 동일한 KRX_ID/KRX_PW 자격증명을 사용합니다.
#
# data.krx.co.kr 투자자 거래실적 엔드포인트:
#   bld=dbms/MDC/STAT/standard/MDCSTAT02302  (일자별 투자자 거래실적)
#   isuSrtCd=005930  (6자리 종목코드)
#   strtDd/endDd=YYYYMMDD
#   inqTpCd=1 (순매수 수량 기준)
#   trdVolVal=1 (거래량)
#   askBid=3 (순매수=매수-매도)
#
# 응답 구조 (행당 투자자 유형 1개):
#   INVST_TP_NM: "외국인합계" | "기관합계" | "금융투자" | ...
#   TDD_STR_DD:  날짜 YYYYMMDD
#   NETBID_TRDVOL: 순매수 주수 (음수=순매도)
#
# --probe <종목코드> 옵션으로 실제 응답 구조를 먼저 확인하세요.


class _KrxDirectFetcher:
    """pykrx 없이 data.krx.co.kr를 직접 호출하는 수급 수집기."""

    _BASE      = "https://data.krx.co.kr"
    _WARMUP    = _BASE + "/contents/MDC/MDI/index.cmd"
    _LOGIN     = _BASE + "/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
    _DATA      = _BASE + "/comm/bldAttendant/getJsonData.cmd"
    _BLD_DAILY = "dbms/MDC/STAT/standard/MDCSTAT02302"  # 일자별 투자자 거래실적

    _HEADERS = {
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer":         _BASE + "/contents/MDC/MDI/outerLoader/index.cmd",
        "X-Requested-With": "XMLHttpRequest",
        "Accept":          "application/json, text/javascript, */*; q=0.01",
        "Content-Type":    "application/x-www-form-urlencoded; charset=UTF-8",
    }

    # 투자자 유형명 (EUC-KR 디코딩 후 정확한 문자열)
    _FOREIGN_KEY = "외국인합계"
    _INST_KEY    = "기관합계"

    def __init__(self) -> None:
        import requests as _req
        self._session = _req.Session()
        self._authenticated = False

    def _post(self, url: str, data: dict, timeout: int = 30) -> bytes:
        resp = self._session.post(url, data=data, headers=self._HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp.content

    def warmup(self) -> bool:
        """KRX 세션 초기화 (JSESSIONID 쿠키 획득)."""
        try:
            self._session.get(self._WARMUP, headers=self._HEADERS, timeout=15)
            return True
        except Exception as e:
            logger.debug("[krx-direct] warmup 실패: %s", e)
            return False

    def login(self, krx_id: str, krx_pw: str) -> bool:
        """KRX 계정 로그인. 성공 시 True."""
        try:
            raw = self._post(self._LOGIN, {"userId": krx_id, "userPw": krx_pw})
            text = raw.decode("euc-kr", errors="replace")
            # "LOGOUT" (6B) 등 짧은 오류 응답과 구분하기 위해 길이 조건 제거.
            # KRX 로그인 성공 응답에는 "success" 또는 JSON redirect가 포함됨.
            if "success" in text.lower() or ("OK" in text and "LOGOUT" not in text):
                self._authenticated = True
                logger.info("[krx-direct] 로그인 성공")
                return True
            logger.warning("[krx-direct] 로그인 응답 (실패로 처리): %s", text[:80])
            return False
        except Exception as e:
            logger.warning("[krx-direct] 로그인 실패: %s", e)
            return False

    def fetch_raw(
        self,
        krx_code: str,
        start: date,
        end: date,
    ) -> list[dict]:
        """단일 종목 투자자별 거래실적 원본 행 목록 반환.

        반환 형식: [{"TDD_STR_DD": "20250102", "INVST_TP_NM": "외국인합계",
                      "NETBID_TRDVOL": "12345", ...}, ...]
        """
        payload = {
            "bld":       self._BLD_DAILY,
            "isuSrtCd": krx_code,
            "strtDd":   start.strftime("%Y%m%d"),
            "endDd":    end.strftime("%Y%m%d"),
            "inqTpCd":  "1",
            "trdVolVal": "1",
            "askBid":   "3",
            "csvxls_isNo": "false",
        }
        try:
            raw = self._post(self._DATA, payload)
        except Exception as e:
            logger.debug("[krx-direct] %s 요청 실패: %s", krx_code, e)
            return []

        # 응답이 LOGOUT/빈값이면 인증 실패
        if raw in (b"LOGOUT", b""):
            logger.warning("[krx-direct] 인증 필요 — KRX_ID/KRX_PW를 확인하세요.")
            return []

        try:
            text = raw.decode("euc-kr", errors="replace")
            data = _json.loads(text)
        except Exception as e:
            logger.debug("[krx-direct] JSON 파싱 실패 (%s): %s", krx_code, e)
            return []

        # KRX 응답 키: "output" 또는 "OutBlock_1"
        rows = data.get("output") or data.get("OutBlock_1") or []
        return rows if isinstance(rows, list) else []

    def fetch_records(
        self,
        yf_symbol: str,
        start: date,
        end: date,
        prev_f_streak: Optional[int],
        prev_i_streak: Optional[int],
        rate_delay: float = 0.5,
    ) -> list[FlowRecord]:
        """단일 종목 수급 FlowRecord 목록 반환."""
        krx_code = yf_symbol.split(".")[0]
        rows = self.fetch_raw(krx_code, start, end)
        if not rows:
            return []

        # 날짜별로 외국인/기관 순매수 취합
        # 행 구조: 날짜 × 투자자유형 (행이 (날짜, 유형) 조합이거나
        #           날짜별로 그룹핑된 경우 모두 처리)
        from collections import defaultdict
        by_date: dict[str, dict[str, Optional[int]]] = defaultdict(dict)

        for row in rows:
            raw_date = (
                row.get("TDD_STR_DD")
                or row.get("TRD_DD")
                or row.get("BAS_DD")
                or ""
            )
            raw_date = raw_date.replace("-", "").replace("/", "").strip()
            if len(raw_date) != 8 or not raw_date.isdigit():
                continue

            inv_type = row.get("INVST_TP_NM", "").strip()
            raw_vol = (
                row.get("NETBID_TRDVOL")
                or row.get("NET_BID_TRDVOL")
                or row.get("NETBIDVOL")
                or ""
            )
            vol = _parse_int(raw_vol)

            if inv_type == self._FOREIGN_KEY:
                by_date[raw_date]["foreign"] = vol
            elif inv_type == self._INST_KEY:
                by_date[raw_date]["inst"] = vol

        records: list[FlowRecord] = []
        f_streak = prev_f_streak
        i_streak = prev_i_streak

        for raw_date in sorted(by_date):
            try:
                d = date(int(raw_date[:4]), int(raw_date[4:6]), int(raw_date[6:8]))
            except ValueError:
                continue
            if not (start <= d <= end):
                continue

            day = by_date[raw_date]
            f_net = day.get("foreign")
            i_net = day.get("inst")
            f_streak = _next_streak(f_streak, f_net)
            i_streak = _next_streak(i_streak, i_net)

            records.append(FlowRecord(
                ticker=yf_symbol,
                trade_date=d,
                foreign_net=f_net,
                inst_net=i_net,
                foreign_streak=f_streak,
                inst_streak=i_streak,
            ))

        if rate_delay > 0:
            _time.sleep(rate_delay)

        return records


def _parse_int(s: str) -> Optional[int]:
    """숫자 문자열 파싱. 빈값/'-'/NaN → None."""
    if not s:
        return None
    s = str(s).replace(",", "").strip()
    if s in ("-", "N/A", "nan", ""):
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _make_krx_direct(krx_id: Optional[str], krx_pw: Optional[str]) -> "_KrxDirectFetcher":
    """KrxDirectFetcher 초기화: warmup → login."""
    fetcher = _KrxDirectFetcher()
    fetcher.warmup()
    if krx_id and krx_pw:
        fetcher.login(krx_id, krx_pw)
    else:
        logger.warning(
            "[krx-direct] KRX_ID/KRX_PW 미설정 — 일부 엔드포인트는 인증 필요.\n"
            "  data.krx.co.kr 회원가입 후 .env에 KRX_ID/KRX_PW 추가하세요."
        )
    return fetcher


# ── pykrx 백엔드 ──────────────────────────────────────────────

def _pykrx_available() -> bool:
    try:
        import pykrx  # noqa: F401
        return True
    except ImportError:
        return False


def _fetch_pykrx(
    krx_code: str,
    start: date,
    end: date,
) -> Optional["pd.DataFrame"]:
    """pykrx로 단일 종목 투자자별 거래량(주) 조회.

    반환: index=날짜, columns=[..., 기관합계, 외국인합계, ...] DataFrame.
          실패 시 None.
    컬럼 위치 (pykrx 표준):
      idx 7 = 기관합계, idx 8 = 외국인합계
    """
    try:
        from pykrx import stock
        import pandas as pd

        fromdate = start.strftime("%Y%m%d")
        todate   = end.strftime("%Y%m%d")

        df = stock.get_market_trading_volume_by_investor(fromdate, todate, krx_code)

        if df is None or df.empty:
            return None

        return df

    except Exception as e:
        logger.debug("[pykrx] %s 조회 실패: %s", krx_code, e)
        return None


def _extract_nets(row: "pd.Series") -> tuple[Optional[int], Optional[int]]:
    """DataFrame 행에서 외국인/기관 순매수(주) 추출.

    컬럼명이 한글로 있으면 직접 접근, KeyError 시 위치 기반 접근.
    pykrx 표준 컬럼 순서:
      [금융투자, 보험, 투신, 사모, 은행, 기타금융, 연기금, 기관합계, 외국인합계, 기타법인, 개인]
    """
    import pandas as pd

    def _get(col_name: str, fallback_pos: int) -> Optional[int]:
        try:
            v = row[col_name]
        except KeyError:
            cols = row.index.tolist()
            if len(cols) > fallback_pos:
                v = row.iloc[fallback_pos]
            else:
                return None
        if pd.isna(v):
            return None
        return int(v)

    inst    = _get("기관합계",   7)
    foreign = _get("외국인합계", 8)
    return foreign, inst


def fetch_pykrx_records(
    yf_symbol: str,
    start: date,
    end: date,
    prev_f_streak: Optional[int],
    prev_i_streak: Optional[int],
) -> list[FlowRecord]:
    """단일 종목 수급 레코드 목록 반환 (스트릭 포함)."""
    import pandas as pd

    krx_code = yf_symbol.split(".")[0]  # "005930.KS" → "005930"
    df = _fetch_pykrx(krx_code, start, end)
    if df is None or df.empty:
        return []

    records: list[FlowRecord] = []
    f_streak = prev_f_streak
    i_streak = prev_i_streak

    for ts, row in df.iterrows():
        if isinstance(ts, str):
            try:
                d = date.fromisoformat(ts[:10])
            except ValueError:
                continue
        else:
            d = ts.date() if hasattr(ts, "date") else ts

        if not (start <= d <= end):
            continue

        foreign_net, inst_net = _extract_nets(row)
        f_streak = _next_streak(f_streak, foreign_net)
        i_streak = _next_streak(i_streak, inst_net)

        records.append(FlowRecord(
            ticker=yf_symbol,
            trade_date=d,
            foreign_net=foreign_net,
            inst_net=inst_net,
            foreign_streak=f_streak,
            inst_streak=i_streak,
        ))

    return records


# ── CSV 백엔드 ────────────────────────────────────────────────

def _is_krx_style(header: list[str]) -> bool:
    """KRX 다운로드 CSV 형식 감지 (날짜, 종목코드, 종목명, ...)."""
    h = [c.strip() for c in header]
    return "종목코드" in h or "날짜" in h


def _code_to_symbol(code: str, market_hint: str = "") -> str:
    """KRX 6자리 코드 → yfinance 심볼 (시장 힌트 없으면 .KS 기본)."""
    code = code.strip().zfill(6)
    if ".KS" in market_hint or ".KQ" in market_hint:
        return market_hint.strip()
    if code.endswith(".KS") or code.endswith(".KQ"):
        return code
    # KOSDAQ: 첫자리 0~9, 구분 불가 → 입력에 시장 정보 없으면 KOSPI로 가정
    return f"{code}.KS"


def load_csv_records(csv_path: str) -> list[FlowRecord]:
    """CSV 파일에서 FlowRecord 목록 로드.

    지원 형식:
      1. 표준: date, ticker, foreign_net, inst_net
      2. KRX: 날짜, 종목코드, 종목명, 외국인순매수, 기관합계
    """
    records: list[FlowRecord] = []

    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        krx_style = _is_krx_style(header)

        for i, row in enumerate(reader, start=2):
            try:
                if krx_style:
                    raw_date = row.get("날짜", "").strip()
                    code     = row.get("종목코드", "").strip()
                    f_raw    = row.get("외국인순매수", row.get("외국인합계", "0"))
                    i_raw    = row.get("기관합계", row.get("기관순매수", "0"))
                    ticker   = _code_to_symbol(code)
                else:
                    raw_date = row.get("date", "").strip()
                    ticker   = row.get("ticker", "").strip()
                    f_raw    = row.get("foreign_net", "0")
                    i_raw    = row.get("inst_net", "0")

                # 날짜 파싱 (YYYY-MM-DD 또는 YYYYMMDD)
                raw_date = raw_date.replace("-", "")
                if len(raw_date) != 8 or not raw_date.isdigit():
                    continue
                d = date(int(raw_date[:4]), int(raw_date[4:6]), int(raw_date[6:8]))

                def _to_int(s: str) -> Optional[int]:
                    s = s.replace(",", "").strip()
                    return int(s) if s and s != "-" else None

                records.append(FlowRecord(
                    ticker=ticker,
                    trade_date=d,
                    foreign_net=_to_int(f_raw),
                    inst_net=_to_int(i_raw),
                ))
            except Exception as e:
                logger.warning("[csv] line %d 건너뜀: %s", i, e)

    logger.info("[csv] %s — %d행 로드", csv_path, len(records))
    return records


def _compute_streaks(records: list[FlowRecord]) -> list[FlowRecord]:
    """레코드 목록에서 종목별 스트릭 계산 (날짜 오름차순 전제)."""
    from collections import defaultdict
    f_streaks: dict[str, Optional[int]] = defaultdict(lambda: None)
    i_streaks: dict[str, Optional[int]] = defaultdict(lambda: None)

    records.sort(key=lambda r: (r.ticker, r.trade_date))
    for rec in records:
        rec.foreign_streak = _next_streak(f_streaks[rec.ticker], rec.foreign_net)
        rec.inst_streak    = _next_streak(i_streaks[rec.ticker], rec.inst_net)
        f_streaks[rec.ticker] = rec.foreign_streak
        i_streaks[rec.ticker] = rec.inst_streak

    return records


# ── DB 저장 ───────────────────────────────────────────────────

async def _save_batch(pool, records: list[FlowRecord]) -> int:
    """FlowRecord 목록을 daily_flow에 upsert. 저장 건수 반환."""
    from db import save_daily_flow

    saved = 0
    for rec in records:
        try:
            await save_daily_flow(
                pool,
                ticker=rec.ticker,
                trade_date=rec.trade_date.isoformat(),
                foreign_net=rec.foreign_net,
                inst_net=rec.inst_net,
                foreign_streak=rec.foreign_streak,
                inst_streak=rec.inst_streak,
            )
            saved += 1
        except Exception as e:
            logger.debug("[db] %s %s 저장 실패: %s", rec.ticker, rec.trade_date, e)
    return saved


async def _get_existing_dates(pool, ticker: str, start: date, end: date) -> set[date]:
    """daily_flow에서 이미 저장된 날짜 집합 반환 (증분 스킵용)."""
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT trade_date FROM daily_flow
                WHERE ticker = $1 AND trade_date BETWEEN $2 AND $3
                """,
                ticker, start, end,
            )
        return {r["trade_date"] for r in rows}
    except Exception:
        return set()


# ── 메인 워크플로우 ───────────────────────────────────────────

async def run_pykrx(
    pool,
    start: date,
    end: date,
    market: str,
    max_tickers: int,
) -> None:
    """pykrx 백엔드: 전 종목 순차 수집 후 저장."""
    from chart_screener import get_all_tickers
    from db import get_prev_streak

    all_tickers = get_all_tickers()
    if market == "KOSPI":
        tickers = [(t, n) for t, n, _ in all_tickers if t.endswith(".KS")]
    elif market == "KOSDAQ":
        tickers = [(t, n) for t, n, _ in all_tickers if t.endswith(".KQ")]
    else:
        tickers = [(t, n) for t, n, _ in all_tickers]

    if max_tickers > 0:
        tickers = tickers[:max_tickers]

    logger.info("[flow] 대상 %d개 종목  %s ~ %s", len(tickers), start, end)

    total_saved = 0
    for i, (yf_sym, name) in enumerate(tickers, 1):
        # 이미 저장된 날짜 확인 (증분)
        existing = await _get_existing_dates(pool, yf_sym, start, end)
        # 달력일 기준이 아닌 거래일 기준으로 비교 (KRX 연간 거래일 ≈ 250일).
        # 달력일(~365) × 0.9 = 328 > 실제 저장 가능한 거래일(~250) 이므로
        # 달력일 비교 시 증분 스킵이 절대 동작하지 않음.
        expected_trading_days = int((end - start).days * 250 / 365)
        if len(existing) >= max(1, expected_trading_days * 0.9):
            logger.debug("[flow] %s 이미 저장됨, 건너뜀", yf_sym)
            continue

        # 전일 스트릭 로드
        prev_f, prev_i = await get_prev_streak(pool, yf_sym, start)

        # pykrx 수집 (동기 → executor)
        loop = asyncio.get_running_loop()
        records = await loop.run_in_executor(
            None, fetch_pykrx_records, yf_sym, start, end, prev_f, prev_i
        )

        if not records:
            logger.debug("[flow] %s 데이터 없음", yf_sym)
            continue

        saved = await _save_batch(pool, records)
        total_saved += saved

        if i % 50 == 0 or i == len(tickers):
            logger.info("[flow] %d/%d 처리 완료  저장 누계: %d건", i, len(tickers), total_saved)

    logger.info("[flow] 완료 — 총 저장: %d건", total_saved)


async def run_krx_direct(
    pool,
    start: date,
    end: date,
    market: str,
    max_tickers: int,
    krx_id: Optional[str],
    krx_pw: Optional[str],
) -> None:
    """krx-direct 백엔드: pykrx 없이 data.krx.co.kr 직접 호출."""
    from chart_screener import get_all_tickers
    from db import get_prev_streak

    fetcher = _make_krx_direct(krx_id, krx_pw)

    all_tickers = get_all_tickers()
    if market == "KOSPI":
        tickers = [(t, n) for t, n, _ in all_tickers if t.endswith(".KS")]
    elif market == "KOSDAQ":
        tickers = [(t, n) for t, n, _ in all_tickers if t.endswith(".KQ")]
    else:
        tickers = [(t, n) for t, n, _ in all_tickers]

    if max_tickers > 0:
        tickers = tickers[:max_tickers]

    logger.info("[flow] [krx-direct] 대상 %d개 종목  %s ~ %s", len(tickers), start, end)

    total_saved = 0
    for i, (yf_sym, _name) in enumerate(tickers, 1):
        existing = await _get_existing_dates(pool, yf_sym, start, end)
        # 달력일 기준이 아닌 거래일 기준으로 비교 (KRX 연간 거래일 ≈ 250일).
        # 달력일(~365) × 0.9 = 328 > 실제 저장 가능한 거래일(~250) 이므로
        # 달력일 비교 시 증분 스킵이 절대 동작하지 않음.
        expected_trading_days = int((end - start).days * 250 / 365)
        if len(existing) >= max(1, expected_trading_days * 0.9):
            logger.debug("[flow] %s 이미 저장됨, 건너뜀", yf_sym)
            continue

        prev_f, prev_i = await get_prev_streak(pool, yf_sym, start)

        loop = asyncio.get_running_loop()
        records = await loop.run_in_executor(
            None,
            lambda sym=yf_sym, pf=prev_f, pi=prev_i: fetcher.fetch_records(
                sym, start, end, pf, pi
            ),
        )

        if not records:
            logger.debug("[flow] %s 데이터 없음", yf_sym)
            continue

        saved = await _save_batch(pool, records)
        total_saved += saved

        if i % 50 == 0 or i == len(tickers):
            logger.info("[flow] %d/%d 처리 완료  저장 누계: %d건", i, len(tickers), total_saved)

    logger.info("[flow] 완료 — 총 저장: %d건", total_saved)


async def run_csv(pool, csv_path: str) -> None:
    """CSV 백엔드: 파일 로드 → 스트릭 계산 → 저장."""
    records = load_csv_records(csv_path)
    if not records:
        logger.warning("[csv] 저장할 레코드 없음")
        return

    records = _compute_streaks(records)

    saved = await _save_batch(pool, records)
    logger.info("[csv] 완료 — 저장: %d/%d건", saved, len(records))


def probe_raw_response(krx_code: str, krx_id: Optional[str], krx_pw: Optional[str]) -> None:
    """--probe: 단일 종목 응답 구조를 JSON으로 출력 (필드명 확인용)."""
    fetcher = _make_krx_direct(krx_id, krx_pw)
    yesterday = date.today() - timedelta(days=1)
    start = yesterday - timedelta(days=5)
    rows = fetcher.fetch_raw(krx_code.split(".")[0], start, yesterday)
    if not rows:
        print(f"[probe] 응답 없음 — 인증 실패 또는 네트워크 차단")
        print("  확인: KRX_ID/KRX_PW 설정, 한국 ISP 또는 VPN 사용 여부")
        return
    print(f"[probe] {len(rows)}행 수신. 첫 3행:")
    import json
    for row in rows[:3]:
        print(json.dumps(row, ensure_ascii=False, indent=2))
    print("\n[probe] 투자자 유형 목록:")
    types = sorted({r.get("INVST_TP_NM", "?") for r in rows})
    for t in types:
        print(f"  {t}")
    print("\n[probe] 날짜 키 후보:", [k for k in rows[0] if "DD" in k or "DT" in k or "DATE" in k])
    print("[probe] 순매수 키 후보:", [k for k in rows[0] if "NET" in k or "NETBID" in k])


# ── CLI ───────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="외국인/기관 순매수 이력을 daily_flow 테이블에 적재"
    )
    parser.add_argument("--start",   default=None, help="시작일 YYYY-MM-DD (기본: 2년 전)")
    parser.add_argument("--end",     default=None, help="종료일 YYYY-MM-DD (기본: 어제)")
    parser.add_argument("--market",  default="ALL", choices=["KOSPI", "KOSDAQ", "ALL"])
    parser.add_argument("--max",     type=int, default=0,
                        help="최대 티커 수 (기본 0=전종목, 테스트용)")
    parser.add_argument("--incremental", action="store_true",
                        help="어제 데이터만 적재 (스케줄러용)")
    parser.add_argument("--backend", default="krx-direct",
                        choices=["krx-direct", "pykrx", "csv"],
                        help="수집 백엔드 (기본: krx-direct)")
    parser.add_argument("--csv",     default=None, metavar="FILE",
                        help="CSV 파일 임포트 경로 (--backend csv 시 필수)")
    parser.add_argument("--probe",   default=None, metavar="CODE",
                        help="응답 구조 확인용 단일 종목 테스트 (예: --probe 005930)")
    return parser.parse_args()


async def main() -> None:
    args = _parse_args()

    krx_id = os.environ.get("KRX_ID")
    krx_pw = os.environ.get("KRX_PW")

    # --probe: DB 없이 응답 구조만 확인
    if args.probe:
        probe_raw_response(args.probe, krx_id, krx_pw)
        return

    yesterday = date.today() - timedelta(days=1)

    if args.incremental:
        start = yesterday
        end   = yesterday
    else:
        end   = date.fromisoformat(args.end)   if args.end   else yesterday
        start = date.fromisoformat(args.start) if args.start else end - timedelta(days=730)

    if start > end:
        logger.error("start(%s) > end(%s)", start, end)
        sys.exit(1)

    # DB 풀 생성
    import db as _db
    pool = await _db.create_pool()
    await _db.init_db(pool)

    logger.info("[flow] 시작  backend=%s  %s ~ %s  %s  최대티커: %s",
                args.backend, start, end, args.market, args.max or "전종목")

    try:
        if args.backend == "csv":
            if not args.csv or not os.path.isfile(args.csv):
                logger.error("--backend csv 사용 시 --csv <파일경로> 필수")
                sys.exit(1)
            await run_csv(pool, args.csv)

        elif args.backend == "pykrx":
            if not _pykrx_available():
                logger.error("pykrx 미설치. pip install pykrx 후 재실행.")
                sys.exit(1)
            if not krx_id or not krx_pw:
                logger.warning(
                    "KRX_ID/KRX_PW 미설정 — 인증 없이 시도합니다. 실패 시 .env 확인."
                )
            await run_pykrx(pool, start, end, args.market, args.max)

        else:  # krx-direct (기본)
            await run_krx_direct(
                pool, start, end, args.market, args.max, krx_id, krx_pw
            )
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
