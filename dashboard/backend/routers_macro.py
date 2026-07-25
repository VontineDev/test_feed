"""
dashboard/backend/routers_macro.py
매크로 팩터 분석 + 시장 지수 라우터.

  GET /api/market_index — KOSPI/KOSDAQ 등락률 + 시장 감성 (5분 캐시)
  GET /api/macro        — MacroTracker 팩터 분석 (10분 캐시)

의존 방향: routers_* → common/database/core.*/data.*/analysis.* 만 허용 (main import 금지).
캐시 dict(_MACRO_CACHE/_MARKET_INDEX_CACHE)는 재대입 금지 — main의
health/_warmup_caches가 참조를 공유함. _HEATMAP_CACHE는 common 소유를 읽기만 함.
"""
from __future__ import annotations

import asyncio
import logging
import time as _time_module
from datetime import datetime
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, HTTPException

from common import (
    _HEATMAP_CACHE,
    _KST,
    _bg_refresh,
    _ext_thread,
    _is_market_open,
)

import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from analysis.macro_tracker import MacroTracker, DEFAULT_TICKERS as _MACRO_TICKERS  # noqa: E402
from core.tickers import kiwoom_to_yfinance as _kiwoom_to_yfinance  # noqa: E402

logger = logging.getLogger(__name__)

router = APIRouter()

# ── 매크로 캐시 (10분) ───────────────────────────────────────
_MACRO_CACHE: dict = {"data": None, "expires": 0.0}
_MACRO_TTL = 600  # 10분 (yfinance 다운로드 비용 고려)
_MACRO_LOCK = asyncio.Lock()

# ── 시장 지수 캐시 (5분) ─────────────────────────────────────
_MARKET_INDEX_CACHE: dict = {"data": None, "expires": 0.0}
_MARKET_INDEX_TTL = 300  # 5분
_MARKET_INDEX_LOCK = asyncio.Lock()


# ── GET /api/macro ────────────────────────────────────────────

def _fetch_prev_top20_sync() -> dict[str, str] | None:
    """최근 영업일 거래대금 TOP 20 → {ticker: name}. 실패 시 None.

    1순위: daily_market_snap (ka10032 top100, 전 종목)
    2순위: aftermarket_snap  (NXT 거래 종목만, 폴백)
    """
    try:
        from core.db_sync import connect as _db_connect
        conn = _db_connect()
        try:
            with conn.cursor() as cur:
                # 1순위: daily_market_snap
                cur.execute("""
                    SELECT d.ticker,
                           COALESCE(tn.name_ko, d.name,
                                    SPLIT_PART(d.ticker, '.', 1)) AS name
                    FROM   daily_market_snap d
                    LEFT JOIN ticker_names tn ON tn.ticker = d.ticker
                    WHERE  d.trade_date = (SELECT MAX(trade_date) FROM daily_market_snap)
                      AND  d.amount > 0
                    ORDER  BY d.amount DESC
                    LIMIT  20
                """)
                rows = cur.fetchall()
                if rows:
                    result = {row[0]: row[1] for row in rows if row[0]}
                    logger.info("[macro] daily_market_snap 전일 TOP %d 종목 로드", len(result))
                    return result if result else None

                # 2순위: aftermarket_snap 폴백
                cur.execute("""
                    SELECT a.ticker,
                           COALESCE(tn.name_ko, SPLIT_PART(a.ticker, '.', 1)) AS name
                    FROM   aftermarket_snap a
                    LEFT JOIN ticker_names tn ON tn.ticker = a.ticker
                    WHERE  a.trade_date = (SELECT MAX(trade_date) FROM aftermarket_snap)
                      AND  COALESCE(a.reg_value, a.after_value, 0) > 0
                    ORDER  BY COALESCE(a.reg_value, a.after_value, 0) DESC
                    LIMIT  20
                """)
                rows = cur.fetchall()
        finally:
            conn.close()
        if not rows:
            return None
        result = {row[0]: row[1] for row in rows if row[0]}
        logger.info("[macro] aftermarket_snap 전일 TOP %d 종목 로드 (폴백)", len(result))
        return result if result else None
    except Exception as e:
        logger.warning("[macro] 전일 TOP 조회 실패: %s", e)
        return None


def _run_macro_analysis() -> dict:
    """MacroTracker 분석 실행 (동기, asyncio.to_thread에서 호출)."""
    # 1순위: 오늘 실시간 히트맵 캐시 — 전체 풀에서 변환 가능한 것 모두 분석 후 거래대금 상위 20개 선별
    heatmap_items: list[dict] = (_HEATMAP_CACHE.get("data") or {}).get("items") or []
    heatmap_rank: dict[str, int] = {}  # yf_ticker → 거래대금 순위 (1-based)
    if len(heatmap_items) >= 5:
        live_tickers: dict[str, str] | None = {}
        rank = 0
        for item in heatmap_items:
            if not item.get("ticker") or not item.get("name"):
                continue
            yf_tk = _kiwoom_to_yfinance(item["ticker"], item.get("market", ""))
            if yf_tk:
                rank += 1
                heatmap_rank[yf_tk] = rank
                live_tickers[yf_tk] = item["name"]
        if live_tickers:
            logger.info("[macro] 히트맵 %d 종목 분석 (거래대금 상위 20 선별)", len(live_tickers))
        else:
            logger.info("[macro] 히트맵 티커 변환 불가 — aftermarket_snap 폴백")
            live_tickers = _fetch_prev_top20_sync()
            if live_tickers is None:
                logger.info("[macro] 전일 aftermarket 없음 — DEFAULT_TICKERS 사용")
    else:
        # 2순위: aftermarket_snap 전날 TOP 20
        live_tickers = _fetch_prev_top20_sync()
        if live_tickers is None:
            logger.info("[macro] 전일 aftermarket 없음 — DEFAULT_TICKERS 사용")

    tracker = MacroTracker(period="2y", min_obs=60)
    tracker.fit(live_tickers)

    snapshot = tracker.snapshot()

    # 종목별 결과 + 팩터별 5일 기여 계산
    stocks = []
    for r in tracker._results:
        factor_contribs: dict[str, float] = {}
        for f in ["rate", "fx", "oil", "vix", "dxy", "export"]:
            beta = r["betas"].get(f, 0.0)
            delta5 = snapshot.get(f, {}).get("change_5d", 0.0)
            factor_contribs[f] = round(beta * delta5, 4)

        stocks.append({
            "ticker":              r["ticker"],
            "name":                r["name"],
            "n_obs":               r["n_obs"],
            "r_squared":           r["r_squared"],
            "adj_r_squared":       r["adj_r_squared"],
            "residual_std":        r["residual_std"],
            "macro_score":         r["macro_score"],
            "macro_score_5d":      r["macro_score_5d"],
            "macro_score_20d":     r["macro_score_20d"],
            "significant_factors": r["significant_factors"],
            "betas":               {k: round(v, 5) for k, v in r["betas"].items() if k != "alpha"},
            "alpha":               round(r["betas"].get("alpha", 0.0), 6),
            "t_stats":             {k: v for k, v in r["t_stats"].items() if k != "alpha"},
            "p_values":            {k: v for k, v in r["p_values"].items() if k != "alpha"},
            "factor_contribs_5d":  factor_contribs,
        })

    # 거래대금 순서 정렬 후 상위 20개 선별 (히트맵 경로), fallback은 macro_score 내림차순
    if heatmap_rank:
        stocks.sort(key=lambda s: heatmap_rank.get(s["ticker"], 9999))
        stocks = stocks[:20]
    else:
        stocks.sort(key=lambda s: s["macro_score"], reverse=True)

    return {
        "snapshot":   snapshot,
        "stocks":     stocks,
        "fetched_at": _time_module.strftime("%H:%M:%S"),
    }


def _market_sentiment(
    kospi_pct: float | None,
    kosdaq_pct: float | None,
) -> tuple[str, str]:
    """KOSPI/KOSDAQ 등락률 → (sentiment, detail) 규칙 기반 분류."""
    if kospi_pct is None and kosdaq_pct is None:
        return "정보없음", "지수 데이터 로딩 실패"

    available = [p for p in (kospi_pct, kosdaq_pct) if p is not None]
    avg = sum(available) / len(available)
    both = kospi_pct is not None and kosdaq_pct is not None

    if avg >= 2.0:
        detail = "코스피/코스닥 모두 급등 — 매수세 강함" if both else "지수 급등 — 매수세 강함 (코스피 기준)"
        return "강세", detail
    elif avg >= 0.5:
        return "상승", f"시장 전반 오름세{'' if both else ' (코스피 기준)'}"
    elif avg >= -0.5:
        return "보합", f"큰 방향성 없이 혼조{'' if both else ' (코스피 기준)'}"
    elif avg >= -2.0:
        return "하락", f"시장 전반 내림세{'' if both else ' (코스피 기준)'}"
    else:
        detail = "코스피/코스닥 모두 하락폭 커짐" if both else "지수 급락 — 낙폭 확대 (코스피 기준)"
        return "급락", detail


async def _fetch_market_index_data() -> dict:
    """KOSPI/KOSDAQ 지수 데이터를 실제로 조회하는 순수 fetch 함수."""
    now_kst = datetime.now(_KST)
    is_open = _is_market_open()
    bas_dd = now_kst.strftime("%Y%m%d")

    def _fetch_krx():
        try:
            from data.krx_openapi import get_client as _krx_client
            client = _krx_client()
            return (
                client.get_kospi_index_ohlcv(bas_dd),
                client.get_kosdaq_index_ohlcv(bas_dd),
            )
        except Exception as e:
            logger.warning("[market_index] KRX 조회 실패: %s", e)
            return None, None

    def _fetch_yf_daily():
        """yfinance 10일 일별 데이터 → (close, prev_close) 쌍 반환.
        KRX 실패 시 current+prev_close 모두 yfinance로 커버.
        오늘 부분 데이터가 마지막 행에 있을 수 있으므로
        오늘 이전(today) 마지막 완결 행을 prev_close로 사용."""
        try:
            import yfinance as _yf
            from datetime import date as _date
            hist = _yf.download(["^KS11", "^KQ11"], period="10d", interval="1d",
                                auto_adjust=True, progress=False, threads=True)
            if hist is None or hist.empty:
                return None, None, None, None
            close_df = hist["Close"]
            today_str = _date.today().isoformat()

            def _close_prev(s):
                if s is None:
                    return None, None
                s = s.dropna()
                if s.empty:
                    return None, None
                # 오늘 날짜 행과 그 이전 행 분리
                idx_strs = [str(i.date()) if hasattr(i, "date") else str(i)[:10] for i in s.index]
                past = [v for dt, v in zip(idx_strs, s) if dt < today_str]
                current = float(s.iloc[-1])
                prev = float(past[-1]) if past else None
                return current, prev

            ks_s = close_df["^KS11"] if "^KS11" in close_df.columns else None
            kq_s = close_df["^KQ11"] if "^KQ11" in close_df.columns else None
            ks_close, ks_prev = _close_prev(ks_s)
            kq_close, kq_prev = _close_prev(kq_s)
            return ks_close, ks_prev, kq_close, kq_prev
        except Exception as e:
            logger.warning("[market_index] yfinance daily 조회 실패: %s", e)
            return None, None, None, None

    def _fetch_realtime():
        try:
            import yfinance as _yf
            hist = _yf.download(["^KS11", "^KQ11"], period="1d", interval="1m",
                                auto_adjust=True, progress=False, threads=True)
            if hist is None or hist.empty:
                return None, None
            close_df = hist["Close"]
            ks = float(pd.Series(close_df["^KS11"]).dropna().iloc[-1]) if "^KS11" in close_df.columns else None
            kq = float(pd.Series(close_df["^KQ11"]).dropna().iloc[-1]) if "^KQ11" in close_df.columns else None
            return ks, kq
        except Exception as e:
            logger.warning("[market_index] yfinance 실시간 조회 실패: %s", e)
            return None, None

    try:
        krx_kospi, krx_kosdaq = await _ext_thread(_fetch_krx, timeout=15.0)
    except asyncio.TimeoutError:
        logger.warning("[market_index] KRX 타임아웃 (15s)")
        krx_kospi, krx_kosdaq = None, None

    is_realtime = False
    kospi_close = krx_kospi["close"] if krx_kospi else None
    kosdaq_close = krx_kosdaq["close"] if krx_kosdaq else None
    kospi_prev  = krx_kospi["prev_close"]  if krx_kospi  else None
    kosdaq_prev = krx_kosdaq["prev_close"] if krx_kosdaq else None

    # KRX 지수 조회 실패 시 yfinance daily로 close + prev_close 보완
    if not krx_kospi and not krx_kosdaq:
        try:
            yf_ks, yf_ks_prev, yf_kq, yf_kq_prev = await _ext_thread(_fetch_yf_daily, timeout=20.0)
        except asyncio.TimeoutError:
            logger.warning("[market_index] yfinance daily 타임아웃 (20s)")
            yf_ks = yf_ks_prev = yf_kq = yf_kq_prev = None
        if yf_ks:
            kospi_close, kospi_prev = yf_ks, yf_ks_prev
        if yf_kq:
            kosdaq_close, kosdaq_prev = yf_kq, yf_kq_prev

    if is_open and (kospi_close or kosdaq_close):
        try:
            rt_ks, rt_kq = await _ext_thread(_fetch_realtime, timeout=20.0)
        except asyncio.TimeoutError:
            logger.warning("[market_index] yfinance 실시간 타임아웃 (20s)")
            rt_ks, rt_kq = None, None
        if rt_ks:
            kospi_close = rt_ks
            is_realtime = True
        if rt_kq:
            kosdaq_close = rt_kq
            is_realtime = True

    def _pct(close, prev):
        if close and prev and prev > 0:
            return round((close - prev) / prev * 100, 2)
        return None
    kospi_pct   = _pct(kospi_close,  kospi_prev)
    kosdaq_pct  = _pct(kosdaq_close, kosdaq_prev)

    sentiment, sentiment_detail = _market_sentiment(kospi_pct, kosdaq_pct)

    return {
        "market_status":    "open" if is_open else "closed",
        "is_realtime":      is_realtime,
        "kospi":  {"change_pct": kospi_pct,  "close": kospi_close,  "prev_close": kospi_prev}  if kospi_close  else None,
        "kosdaq": {"change_pct": kosdaq_pct, "close": kosdaq_close, "prev_close": kosdaq_prev} if kosdaq_close else None,
        "sentiment":        sentiment,
        "sentiment_detail": sentiment_detail,
        "as_of":            now_kst.isoformat(),
    }


@router.get("/api/market_index")
async def get_market_index():
    """KOSPI / KOSDAQ 지수 등락률 + 시장 감성 (5분 캐시).

    장중: yfinance ^KS11/^KQ11 현재가 + KRX BASPRC_IDX(기준가) → change_pct 계산.
    장마감/주말: KRX OpenAPI 확정값.
    응답: {market_status, is_realtime, kospi, kosdaq, sentiment, sentiment_detail, as_of}
    """
    now = _time_module.time()
    if _MARKET_INDEX_CACHE["data"] and now < _MARKET_INDEX_CACHE["expires"]:
        return _MARKET_INDEX_CACHE["data"]
    if _MARKET_INDEX_CACHE["data"]:
        # stale 데이터 있음 — 즉시 반환하고 백그라운드에서 갱신
        if not _MARKET_INDEX_LOCK.locked():
            asyncio.create_task(_bg_refresh(
                _MARKET_INDEX_CACHE, _MARKET_INDEX_LOCK,
                _fetch_market_index_data, _MARKET_INDEX_TTL, "market_index"
            ))
        return _MARKET_INDEX_CACHE["data"]
    # 최초 기동: 한 번만 대기
    async with _MARKET_INDEX_LOCK:
        if _MARKET_INDEX_CACHE["data"] and _time_module.time() < _MARKET_INDEX_CACHE["expires"]:
            return _MARKET_INDEX_CACHE["data"]
        result = await _fetch_market_index_data()
        _MARKET_INDEX_CACHE["data"] = result
        _MARKET_INDEX_CACHE["expires"] = _time_module.time() + _MARKET_INDEX_TTL
        return result


@router.get("/api/macro")
async def get_macro(refresh: bool = False):
    """
    매크로 팩터 분석 결과 (10분 캐시).

    최초 호출 시 yfinance 다운로드로 30~60초 소요.
    이후 캐시에서 즉시 반환.
    """
    now = _time_module.time()
    if not refresh and _MACRO_CACHE["data"] and now < _MACRO_CACHE["expires"]:
        return {**_MACRO_CACHE["data"], "cached": True}
    if not refresh and _MACRO_CACHE["data"]:
        # stale 데이터 있음 — 즉시 반환하고 백그라운드에서 갱신
        if not _MACRO_LOCK.locked():
            asyncio.create_task(_bg_refresh(
                _MACRO_CACHE, _MACRO_LOCK,
                lambda: _ext_thread(_run_macro_analysis, timeout=90.0),
                _MACRO_TTL, "macro"
            ))
        return {**_MACRO_CACHE["data"], "cached": True, "stale": True}
    # 최초 기동 또는 강제 refresh: 한 번만 대기
    async with _MACRO_LOCK:
        now = _time_module.time()
        if not refresh and _MACRO_CACHE["data"] and now < _MACRO_CACHE["expires"]:
            return {**_MACRO_CACHE["data"], "cached": True}
        try:
            data = await _ext_thread(_run_macro_analysis, timeout=90.0)
            _MACRO_CACHE["data"] = data
            _MACRO_CACHE["expires"] = _time_module.time() + _MACRO_TTL
            return {**data, "cached": False}
        except Exception as e:
            logger.error("[macro] 분석 실패: %s", e)
            if _MACRO_CACHE["data"]:
                return {**_MACRO_CACHE["data"], "cached": True, "stale": True,
                        "error": "분석 오류 — 이전 데이터 표시 중"}
            raise HTTPException(status_code=500, detail=str(e))
