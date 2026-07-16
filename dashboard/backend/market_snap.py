"""
dashboard/backend/market_snap.py
시세 스냅샷 데이터 계층 — Kiwoom 토큰 관리 + 거래대금 상위 조회.

routers_heatmap과 routers_top이 공유하는 데이터 소스 헬퍼:
  _fetch_top_kiwoom / _fetch_nxt_live          — Kiwoom REST 실시간
  _fetch_daily_snap_top_async                  — daily_market_snap (장마감 1순위)
  _fetch_aftermarket_snap_top_async            — aftermarket_snap (폴백)

의존 방향: routers_* → market_snap → database/data.* (main/routers import 금지).
"""
from __future__ import annotations

import logging
import os
import time as _time_module
from pathlib import Path

from database import get_pool

import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from data.kiwoom_aftermarket_sync import KiwoomClient, _parse_int, _parse_float, _VALUE_UNIT  # noqa: E402

logger = logging.getLogger(__name__)

# ── 키움 토큰 캐시 (au10001 반복 호출 방지, 토큰 유효기간 24h) ──
_KIWOOM_TOKEN: str | None = None
_KIWOOM_TOKEN_TS: float = 0.0
_KIWOOM_TOKEN_TTL = 82800  # 23시간


# ── daily_market_snap에서 거래대금 상위 N 조회 (장마감 1순위) ──────
async def _fetch_daily_snap_top_async(n: int) -> dict | None:
    """daily_market_snap 최신 영업일 거래대금 상위 N 종목.

    aftermarket_snap 대비 장점:
      - NXT 거래 여부와 무관하게 전 종목 커버 (ka10032 top100)
      - amount = KRX+NXT 합산 당일 최종값
      - change_pct = 정규장 기준 당일 등락률
    데이터 없으면 None 반환.
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT d.ticker,
                       COALESCE(tn.name_ko, d.name,
                                SPLIT_PART(d.ticker, '.', 1)) AS name,
                       d.price, d.change_pct, d.amount,
                       d.market, d.trade_date
                FROM   daily_market_snap d
                LEFT JOIN ticker_names tn ON tn.ticker = d.ticker
                WHERE  d.trade_date = (SELECT MAX(trade_date) FROM daily_market_snap)
                  AND  d.amount > 0
                ORDER  BY d.amount DESC
                LIMIT  $1
                """,
                n,
            )
        if not rows:
            return None
        trade_date = str(rows[0]["trade_date"])
        items = []
        for i, r in enumerate(rows, 1):
            items.append({
                "rank":       i,
                "ticker":     r["ticker"],
                "name":       r["name"] or r["ticker"],
                "price":      int(r["price"]) if r["price"] else 0,
                "change_pct": float(r["change_pct"]) if r["change_pct"] is not None else 0.0,
                "amount":     int(r["amount"]),
                "market":     r["market"] or "",
            })
        return {"items": items, "fetched_at": trade_date, "is_aftermarket": True}
    except Exception as e:
        logger.warning("[daily-snap] 조회 실패: %s", e)
        return None


# ── aftermarket_snap에서 합산(KRX+NXT) 거래대금 상위 N 조회 ──────
async def _fetch_aftermarket_snap_top_async(n: int) -> dict | None:
    """aftermarket_snap 최근 영업일 거래대금 상위 N 종목 반환.

    정렬/표시 기준:
      reg_value 있음 → reg_value (ka10032 KRX+NXT 당일 최종, NXT 시간외 포함 여부 미확정이므로
                        after_value를 더하지 않아 이중계산 위험 제거)
      reg_value NULL → after_value (NXT 시간외 전용 폴백)
    데이터 없으면 None 반환.
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT a.ticker,
                       COALESCE(tn.name_ko, k.name_ko, SPLIT_PART(a.ticker, '.', 1)) AS name,
                       a.reg_close,
                       a.after_close,
                       a.after_value,
                       a.reg_value,
                       COALESCE(a.reg_value, a.after_value, 0) AS total_value,
                       a.after_chg_pct,
                       a.trade_date,
                       CASE WHEN a.ticker LIKE '%.KS' THEN 'KOSPI'
                            WHEN a.ticker LIKE '%.KQ' THEN 'KOSDAQ'
                            ELSE '' END AS market
                FROM   aftermarket_snap a
                LEFT JOIN ticker_names tn ON tn.ticker = a.ticker
                LEFT JOIN krx_listings k  ON k.yfinance_symbol = a.ticker
                WHERE  a.trade_date = (SELECT MAX(trade_date) FROM aftermarket_snap)
                  AND  COALESCE(a.reg_value, a.after_value, 0) > 0
                ORDER  BY total_value DESC
                LIMIT  $1
                """,
                n,
            )
        if not rows:
            return None
        trade_date = str(rows[0]["trade_date"])
        items = []
        for i, r in enumerate(rows, 1):
            price = int(r["after_close"]) if r["after_close"] else (int(r["reg_close"]) if r["reg_close"] else 0)
            change_pct = float(r["after_chg_pct"]) if r["after_chg_pct"] is not None else 0.0
            items.append({
                "rank":       i,
                "ticker":     r["ticker"],
                "name":       r["name"] or r["ticker"],
                "price":      price,
                "change_pct": change_pct,
                "amount":     int(r["total_value"]),
                "market":     r["market"] or "",
            })
        return {"items": items, "fetched_at": trade_date, "is_aftermarket": True}
    except Exception as e:
        logger.warning("[aftermarket] snap 조회 실패: %s", e)
        return None


# ── 키움 토큰 관리 ────────────────────────────────────────────

def _get_kiwoom_token() -> str:
    """키움 OAuth 토큰 반환 (23h 캐시, 만료 시 재발급)."""
    global _KIWOOM_TOKEN, _KIWOOM_TOKEN_TS
    now = _time_module.time()
    if _KIWOOM_TOKEN and now - _KIWOOM_TOKEN_TS < _KIWOOM_TOKEN_TTL:
        return _KIWOOM_TOKEN
    appkey = os.environ.get("KIWOOM_APPKEY")
    secretkey = os.environ.get("KIWOOM_SECRETKEY")
    if not appkey or not secretkey:
        raise RuntimeError("KIWOOM_APPKEY / KIWOOM_SECRETKEY 환경변수 미설정")
    client = KiwoomClient(use_mock=False)
    _KIWOOM_TOKEN = client.issue_token(appkey, secretkey)
    _KIWOOM_TOKEN_TS = now
    logger.info("[top] 키움 토큰 재발급 완료")
    return _KIWOOM_TOKEN


def _invalidate_kiwoom_token() -> None:
    global _KIWOOM_TOKEN, _KIWOOM_TOKEN_TS
    _KIWOOM_TOKEN = None
    _KIWOOM_TOKEN_TS = 0.0


def _fetch_nxt_live(n: int) -> dict:
    """ka10098으로 NXT 시간외 실시간 상위 N 종목 조회 (동기 — asyncio.to_thread에서 호출).

    401 수신 시 토큰을 무효화하고 1회 재시도합니다.
    반환 형식은 _fetch_top_kiwoom과 동일 (rank, ticker, name, price, change_pct, amount, market).
    """
    import requests as _requests
    for attempt in range(2):
        try:
            client = KiwoomClient(use_mock=False)
            client.inject_token(_get_kiwoom_token())
            all_items: list[dict] = []
            for mrkt_tp, suffix, market_label in [
                ("001", ".KS", "KOSPI"),
                ("101", ".KQ", "KOSDAQ"),
            ]:
                rows = client.fetch_aftermarket_bulk(mrkt_tp=mrkt_tp)
                for row in rows:
                    krx_code = str(row.get("stk_cd", "")).strip().zfill(6)
                    if not krx_code or krx_code == "000000":
                        continue
                    after_volume = _parse_int(row.get("acc_trde_qty"))
                    if not after_volume:
                        continue
                    raw_val    = _parse_int(row.get("acc_trde_prica"))
                    after_value = (raw_val * _VALUE_UNIT) if raw_val else 0
                    if not after_value:
                        continue
                    reg_close   = _parse_int(row.get("tdy_close_pric"))
                    after_close = abs(_parse_int(row.get("cur_prc")) or 0)
                    flu_raw     = _parse_float(row.get("flu_rt"))
                    if flu_raw is not None:
                        change_pct = round(flu_raw, 2)
                    elif reg_close and after_close and reg_close > 0:
                        change_pct = round((after_close / reg_close - 1.0) * 100, 2)
                    else:
                        change_pct = 0.0
                    all_items.append({
                        "ticker":     f"{krx_code}{suffix}",
                        "name":       str(row.get("stk_nm") or krx_code).strip(),
                        "price":      after_close,
                        "change_pct": change_pct,
                        "amount":     after_value,
                        "market":     market_label,
                    })
            all_items.sort(key=lambda x: x["amount"], reverse=True)
            for i, it in enumerate(all_items[:n], 1):
                it["rank"] = i
            return {
                "items":      all_items[:n],
                "fetched_at": _time_module.strftime("%H:%M:%S"),
                "is_nxt":     True,
            }
        except _requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 401 and attempt == 0:
                logger.warning("[nxt] 401 수신 — 토큰 무효화 후 재시도")
                _invalidate_kiwoom_token()
                continue
            raise


def _fetch_top_kiwoom(n: int) -> dict:
    """Kiwoom REST API로 거래대금 상위 N 조회 (동기 — asyncio.to_thread에서 호출).

    401 수신 시 토큰을 무효화하고 1회 재시도합니다.
    """
    import requests as _requests
    for attempt in range(2):
        try:
            client = KiwoomClient(use_mock=False)
            client.inject_token(_get_kiwoom_token())
            items = client.fetch_top_volume(n=n)
            return {"items": items, "fetched_at": _time_module.strftime("%H:%M:%S")}
        except _requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 401 and attempt == 0:
                logger.warning("[top] 401 수신 — 토큰 무효화 후 재시도")
                _invalidate_kiwoom_token()
                continue
            raise
