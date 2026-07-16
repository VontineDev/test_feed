"""
dashboard/backend/common.py
대시보드 공용 인프라 — 외부 API 스레드 풀, 거래일 캘린더, SWR 캐시 헬퍼, 공유 캐시.

의존 방향: routers_* → common/database/core.*/data.* 만 허용.
common은 main/routers_*를 절대 import하지 않는다.

주의: 이 모듈의 캐시 dict(_POS_PRICE_CACHE, _SSE_CONNECTIONS 등)는
재대입 금지 — main의 health/_warmup_caches가 참조를 공유한다.
"""
from __future__ import annotations

import asyncio
import logging
import time as _time_module
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")

# ── 외부 API 전용 스레드 풀 ──────────────────────────────────
# yfinance/Kiwoom/KRX 호출을 기본 executor와 분리.
# max_workers=4: 외부 API가 느려도 이벤트 루프와 일반 요청에 영향 없음.
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor
_EXT_EXECUTOR = _ThreadPoolExecutor(max_workers=4, thread_name_prefix="ext-api")


async def _ext_thread(fn, *args, timeout: float):
    """외부 API 전용 풀에서 동기 함수를 실행한다. timeout 초 초과 시 TimeoutError."""
    loop = asyncio.get_running_loop()
    return await asyncio.wait_for(
        loop.run_in_executor(_EXT_EXECUTOR, fn, *args),
        timeout=timeout,
    )

# ── 한국 휴장일 캐시 ─────────────────────────────────────────
# 네이버 Finance siseJson(005930)으로 실제 영업일 목록을 조회해 당일 휴장 여부 확인.
# 미래 날짜는 API 데이터가 없으므로 고정 법정공휴일 fallback 병용.
#
# 캐시 구조:
#   _HOLIDAY_CACHE: {date → bool}  — True=휴장일
#   _HOLIDAY_CACHE_DATE: 마지막 갱신 날짜 (당일 1회만 조회)
_HOLIDAY_CACHE: dict[date, bool] = {}
_HOLIDAY_CACHE_DATE: date | None = None

# 고정 법정공휴일 (연도 무관, 대체공휴일·선거일·임시공휴일 제외)
_FIXED_HOLIDAYS: set[tuple[int, int]] = {
    (1, 1), (3, 1), (5, 1), (5, 5), (6, 6),
    (8, 15), (10, 3), (10, 9), (12, 25),
}


def _fetch_trading_days_naver(start: date, end: date) -> set[date] | None:
    """네이버 Finance siseJson으로 기간 내 실제 영업일 반환. 실패 시 None."""
    try:
        import requests as _req
        resp = _req.get(
            "https://api.finance.naver.com/siseJson.naver",
            params={
                "symbol":      "005930",
                "requestType": 1,
                "startTime":   start.strftime("%Y%m%d"),
                "endTime":     end.strftime("%Y%m%d"),
                "timeframe":   "day",
            },
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer":    "https://finance.naver.com/",
            },
            timeout=8,
        )
        resp.raise_for_status()
        trading: set[date] = set()
        for line in resp.text.splitlines():
            line = line.strip()
            if line.startswith('[\"20') and len(line) > 11:
                ds = line[2:10]
                try:
                    trading.add(date(int(ds[:4]), int(ds[4:6]), int(ds[6:])))
                except ValueError:
                    pass
        return trading if trading else None
    except Exception as e:
        logger.debug("[holidays] 네이버 API 실패: %s", e)
        return None


def _is_holiday(d: date) -> bool:
    """KST 날짜 d가 한국 주식시장 휴장일이면 True.

    판단 순서:
    1. 캐시 히트 → 즉시 반환
    2. 네이버 siseJson으로 ±15일 영업일 조회 → 당일 포함 여부로 판단
       (과거 확정 데이터이므로 정확)
    3. API 실패 → 고정 법정공휴일 fallback
    """
    global _HOLIDAY_CACHE, _HOLIDAY_CACHE_DATE

    if d in _HOLIDAY_CACHE:
        return _HOLIDAY_CACHE[d]

    today = datetime.now(_KST).date()

    # 오늘 처음 조회 시 ±15일 윈도우 일괄 갱신 (API 1회 호출)
    if _HOLIDAY_CACHE_DATE != today:
        win_start = today - timedelta(days=15)
        win_end   = today + timedelta(days=3)   # 근미래 소폭 포함
        trading = _fetch_trading_days_naver(win_start, win_end)
        if trading is not None:
            for offset in range(-15, 4):
                cd = today + timedelta(days=offset)
                if cd.weekday() < 5:   # 평일만 판단
                    if cd < today:
                        # 과거 날짜: 거래 데이터 부재 = 휴장
                        _HOLIDAY_CACHE[cd] = cd not in trading
                    elif cd in trading:
                        # 오늘/미래: 데이터가 있을 때만 영업일로 확정
                        # (장 개시 전엔 데이터 없음 → 캐시 미설정, fallback으로 고정공휴일만 체크)
                        _HOLIDAY_CACHE[cd] = False
            _HOLIDAY_CACHE_DATE = today
            logger.info("[holidays] 영업일 캐시 갱신 (±15일, %d일 반영)", len(trading))
            if d in _HOLIDAY_CACHE:
                return _HOLIDAY_CACHE[d]

    # API 실패 또는 범위 밖 → 고정 법정공휴일 fallback
    result = (d.month, d.day) in _FIXED_HOLIDAYS
    _HOLIDAY_CACHE[d] = result
    return result


# ── 공용 TTL 상수 ────────────────────────────────────────────
_PRICE_TTL = 300     # 5분
_NXT_TTL   = 120     # NXT 시간외 2분
_AFTERMARKET_TTL = 1800   # 장 마감 후 30분 (aftermarket_snap은 하루 종일 불변)

# ── 포지션 현재가 캐시 — {ticker: current_price_float} (5분) ──
_POS_PRICE_CACHE: dict = {"data": {}, "expires": 0.0}

# ── 히트맵 캐시 (5분) ────────────────────────────────────────
# market_open: 캐시 생성 시점의 _is_market_open() 값 — 케이스 전환 감지용
# routers_heatmap이 채우고, routers_macro(_run_macro_analysis)가 오늘 TOP 종목
# 소스로 읽으며, main의 health/_warmup_caches도 참조 — 그래서 common 소유.
_HEATMAP_CACHE: dict = {"data": None, "expires": 0.0, "market_open": None, "is_nxt": None}
_HEATMAP_LOCK = asyncio.Lock()

# ── SSE 연결 카운터 ──────────────────────────────────────────
_SSE_CONNECTIONS: dict[str, int] = {"signals": 0, "scheduler": 0}


def _cache_is_valid(cache: dict) -> bool:
    """캐시 유효 여부: TTL + market_open/is_nxt 상태 일치 확인.
    market_open 또는 is_nxt 상태가 바뀌면 TTL이 남아 있어도 무효 처리.
    """
    if not cache["data"]:
        return False
    if _time_module.time() >= cache["expires"]:
        return False
    if cache.get("market_open") is not None and cache["market_open"] != _is_market_open():
        return False
    if cache.get("is_nxt") is not None and cache["is_nxt"] != _is_nxt_open():
        return False
    return True


async def _bg_refresh(cache: dict, lock: asyncio.Lock, fetch_fn, ttl, label: str) -> None:
    """stale-while-revalidate: 백그라운드에서 캐시를 갱신한다. 실패 시 stale 유지.
    ttl: float 또는 Callable[[data], float] — 데이터에 따라 TTL을 동적 결정할 때 callable 사용.
    """
    async with lock:
        if _cache_is_valid(cache):
            return  # 락 대기 중 이미 다른 태스크가 갱신 완료
        try:
            data = await fetch_fn()
            cache["data"] = data
            cache["expires"] = _time_module.time() + (ttl(data) if callable(ttl) else ttl)
            cache["market_open"] = _is_market_open()
            cache["is_nxt"] = _is_nxt_open()
            logger.info("[cache] %s 갱신 완료", label)
        except Exception as e:
            logger.warning("[cache] %s 백그라운드 갱신 실패 — stale 유지: %s", label, e)


# ── 포지션 현재가 조회 (yfinance 1d 1m 인터벌, 5분 캐시) ────
async def _fetch_current_prices(
    tickers: list[str], *, update_cache: bool = True
) -> dict[str, float]:
    """종목 리스트의 최신 종가를 yfinance로 조회. {ticker: price} 반환.

    update_cache=False: 단일 종목 조회 시 공유 캐시 오염 방지용.
    """
    if not tickers:
        return {}
    now = _time_module.time()
    if _POS_PRICE_CACHE["data"] and now < _POS_PRICE_CACHE["expires"]:
        return _POS_PRICE_CACHE["data"]

    def _fetch() -> dict[str, float]:
        result: dict[str, float] = {}
        try:
            import yfinance as _yf
            import pandas as _pd
            hist = _yf.download(
                tickers, period="1d", interval="1m",
                auto_adjust=True, progress=False, threads=True,
            )
            if hist.empty:
                return result
            close_df = hist["Close"] if isinstance(hist.columns, _pd.MultiIndex) else hist
            for t in tickers:
                try:
                    if t not in close_df.columns:
                        continue
                    series = close_df[t].dropna()
                    if len(series) >= 1:
                        result[t] = float(series.iloc[-1])
                except Exception:
                    pass
        except Exception as e:
            logger.warning("[prices] 현재가 조회 실패: %s", e)
        return result

    try:
        prices = await _ext_thread(_fetch, timeout=20.0)
    except asyncio.TimeoutError:
        logger.warning("[prices] yfinance 타임아웃 (20s) — 빈 결과 반환")
        prices = {}
    if update_cache:
        _POS_PRICE_CACHE["data"] = prices
        _POS_PRICE_CACHE["expires"] = now + _PRICE_TTL
        logger.info("[prices] 포지션 현재가 갱신: %d종목", len(prices))
    return prices


# ── 시장 개장 여부 ─────────────────────────────────────────────
# TODO [엣지 11] 금요일 20:00 → 토요일 00:00 경계:
#   캐시 market_open 태그로 대부분 처리되지만, 토요일 00:00 직후
#   첫 요청까지는 금요일 캐시가 살아있을 수 있음.
#   캐시 TTL이 만료되면 자동 해소 — 허용 범위로 판단.
def _is_market_open() -> bool:
    """평일 비공휴일 09:00~15:39 KST — 정규장 + 마감 동시호가 종료까지.

    15:30 마감 동시호가 체결 이후 종가가 확정되므로 15:40 직전까지 ka10032를
    사용한다. 15:31~15:39 구간은 NXT 애프터마켓 단일가가 시작되기 전이며,
    ka10032가 당일 최종 정규장 가격을 반영한 상태이다.
    """
    now_kst = datetime.now(_KST)
    if now_kst.weekday() >= 5:
        return False
    if _is_holiday(now_kst.date()):
        return False
    return time(9, 0) <= now_kst.time() < time(15, 40)


def _is_nxt_open() -> bool:
    """평일 비공휴일 15:40~16:09 KST — NXT 애프터마켓 실시간 구간.

    16:10에 daily_market_snap_job이 당일 최종 스냅샷을 수집하므로,
    그 직전까지 ka10098 실시간 데이터를 사용해 갭을 방지한다.
    """
    now_kst = datetime.now(_KST)
    if now_kst.weekday() >= 5:
        return False
    if _is_holiday(now_kst.date()):
        return False
    return time(15, 40) <= now_kst.time() < time(16, 10)


def _compute_cache_ttl(data: dict) -> float:
    """데이터 소스에 따라 적절한 캐시 TTL 반환."""
    if data.get("is_nxt"):
        return _NXT_TTL
    fetched = str(data.get("fetched_at", ""))
    if fetched and "-" in fetched:   # YYYY-MM-DD 형식 → aftermarket snap
        return _AFTERMARKET_TTL
    return _PRICE_TTL
