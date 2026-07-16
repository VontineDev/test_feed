"""
dashboard/backend/routers_portfolio.py
수동 포트폴리오 라우터.

  GET    /api/portfolio                        — 보유 종목 평가 (admin/special)
  POST   /api/portfolio/holdings               — 종목 추가 (admin)
  PUT    /api/portfolio/holdings/{holding_id}  — 종목 수정 (admin)
  DELETE /api/portfolio/holdings/{holding_id}  — 종목 삭제 (admin)

종목코드 조회는 routers_ticker.py, DART 요약은 routers_dart.py로 분리됨.

의존 방향: routers_* → common/database/core.*/data.* 만 허용 (main import 금지).
"""
from __future__ import annotations

import asyncio
import logging
import time as _time_module

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from database import get_pool
from common import _ext_thread

logger = logging.getLogger(__name__)

router = APIRouter()

# ── USD/KRW 환율 캐시 (10분) ──────────────────────────────────
# 캐시 dict는 재대입 금지 — 참조 공유 가능성 고려.
_USDKRW_CACHE: dict = {"rate": None, "expires": 0.0}
_USDKRW_TTL = 600  # 10분


# ── 수동 포트폴리오 (manual_portfolio 테이블) ──────────────────

class _HoldingInput(BaseModel):
    ticker:    str
    name:      str
    avg_price: float
    qty:       float


async def _get_current_prices(pool, tickers: list[str]) -> dict[str, float]:
    """aftermarket_snap 최신 종가 조회 → 미수록 종목은 yfinance 폴백.

    한국주식: aftermarket_snap (reg_close) → yfinance .KS/.KQ
    미국주식: yfinance 직접 조회 (숫자 없는 티커 = US 주식 판별)
    """
    if not tickers:
        return {}
    prices: dict[str, float] = {}

    # aftermarket_snap은 한국주식 전용
    kr_tickers = [t for t in tickers if any(c.isdigit() for c in t)]
    if kr_tickers:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker) ticker, reg_close
                FROM aftermarket_snap
                WHERE ticker = ANY($1::text[])
                  AND reg_close IS NOT NULL
                ORDER BY ticker, trade_date DESC
                """,
                kr_tickers,
            )
        for r in rows:
            if r["reg_close"]:
                prices[r["ticker"]] = float(r["reg_close"])

    missing = [t for t in tickers if t not in prices]
    if missing:
        def _yf_fetch():
            import yfinance as yf
            result: dict[str, float] = {}
            for t in missing:
                # 미국주식 판별: 티커에 숫자가 없으면 US
                if not any(c.isdigit() for c in t):
                    try:
                        info = yf.Ticker(t).fast_info
                        price = getattr(info, "last_price", None) or getattr(info, "regular_market_price", None)
                        if price:
                            result[t] = float(price)
                            continue
                    except Exception:
                        pass
                else:
                    for suffix in (".KS", ".KQ"):
                        try:
                            info = yf.Ticker(t + suffix).fast_info
                            price = getattr(info, "last_price", None) or getattr(info, "regular_market_price", None)
                            if price:
                                result[t] = float(price)
                                break
                        except Exception:
                            continue
            return result
        try:
            yf_prices = await _ext_thread(_yf_fetch, timeout=15.0)
            prices.update(yf_prices)
        except Exception as e:
            logger.warning("[portfolio] yfinance 폴백 실패: %s", e)

    return prices


async def _get_usdkrw_rate() -> float:
    """USD/KRW 환율 (yfinance USDKRW=X, 10분 캐시). 실패 시 최근 캐시 또는 1350 반환."""
    now = _time_module.time()
    if _USDKRW_CACHE["rate"] and now < _USDKRW_CACHE["expires"]:
        return float(_USDKRW_CACHE["rate"])

    def _fetch() -> float | None:
        import yfinance as yf
        try:
            fi = yf.Ticker("USDKRW=X").fast_info
            rate = getattr(fi, "last_price", None)
            if rate and float(rate) > 100:
                return float(rate)
        except Exception:
            pass
        return None

    try:
        rate = await _ext_thread(_fetch, timeout=8.0)
        if rate:
            _USDKRW_CACHE["rate"] = rate
            _USDKRW_CACHE["expires"] = now + _USDKRW_TTL
            logger.info("[portfolio] USD/KRW 환율 갱신: %.2f", rate)
            return rate
    except Exception as e:
        logger.warning("[portfolio] USD/KRW 환율 조회 실패: %s", e)

    return float(_USDKRW_CACHE.get("rate") or 1350.0)


def _calc_holdings(rows, prices: dict[str, float], usd_krw: float) -> tuple[list[dict], dict]:
    """DB 행 + 현재가 + 환율 → holdings 리스트 + summary (합계는 모두 원화 환산 기준)."""
    holdings = []
    for r in rows:
        ticker = r["ticker"]
        is_us  = not any(c.isdigit() for c in ticker)
        rate   = usd_krw if is_us else 1.0
        avg_p  = float(r["avg_price"])
        qty    = float(r["qty"])
        cur_prc = prices.get(ticker)

        # 원화 환산 금액 (KR 주식은 rate=1 이므로 그대로)
        pur_amt_krw    = round(avg_p * qty * rate)
        evlt_amt_krw   = round(cur_prc * qty * rate) if cur_prc is not None else None
        evltv_prft_krw = (evlt_amt_krw - pur_amt_krw) if evlt_amt_krw is not None else None

        # 네이티브 통화 금액 (US: USD 소수점 유지, KR: KRW 정수)
        if is_us:
            pur_amt    = round(avg_p * qty, 2)
            evlt_amt   = round(cur_prc * qty, 2) if cur_prc is not None else None
            evltv_prft = round((cur_prc - avg_p) * qty, 2) if cur_prc is not None else None
        else:
            pur_amt    = pur_amt_krw
            evlt_amt   = evlt_amt_krw
            evltv_prft = evltv_prft_krw

        prft_rt = (
            round(evltv_prft_krw / pur_amt_krw * 100, 2)
            if evltv_prft_krw is not None and pur_amt_krw else None
        )

        holdings.append({
            "id":             r["id"],
            "stk_cd":         ticker,
            "stk_nm":         r["name"],
            "market":         "US" if is_us else "KR",
            "avg_price":      round(avg_p, 2) if is_us else int(avg_p),
            "qty":            qty,
            "cur_prc":        cur_prc,
            "pur_amt":        pur_amt,
            "evlt_amt":       evlt_amt,
            "evltv_prft":     evltv_prft,
            "pur_amt_krw":    pur_amt_krw,
            "evlt_amt_krw":   evlt_amt_krw,
            "evltv_prft_krw": evltv_prft_krw,
            "prft_rt":        prft_rt,
            "poss_rt":        None,
        })

    # 총계·비중 모두 원화 기준으로 계산
    tot_pur  = sum(h["pur_amt_krw"] for h in holdings)
    tot_evlt = sum(h["evlt_amt_krw"] for h in holdings if h["evlt_amt_krw"] is not None)
    tot_pl   = tot_evlt - tot_pur if holdings else 0
    tot_rt   = round(tot_pl / tot_pur * 100, 2) if tot_pur else None

    for h in holdings:
        if h["evlt_amt_krw"] is not None and tot_evlt:
            h["poss_rt"] = round(h["evlt_amt_krw"] / tot_evlt * 100, 1)

    summary = {
        "tot_pur_amt":  tot_pur,
        "tot_evlt_amt": tot_evlt if holdings else None,
        "tot_evlt_pl":  tot_pl if holdings else None,
        "tot_prft_rt":  tot_rt,
    }
    return holdings, summary


@router.get("/api/portfolio")
async def get_portfolio(request: Request):
    """수동 입력 포트폴리오 조회 (admin + special 전용)."""
    role = getattr(request.state, "role", "user")
    if role not in ("admin", "special"):
        raise HTTPException(status_code=403, detail="포트폴리오 조회 권한이 없습니다")

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, ticker, name, avg_price, qty FROM manual_portfolio ORDER BY created_at"
        )

    tickers = [r["ticker"] for r in rows]
    prices, usd_krw = await asyncio.gather(
        _get_current_prices(pool, tickers),
        _get_usdkrw_rate(),
    )
    holdings, summary = _calc_holdings(rows, prices, usd_krw)
    return {"summary": summary, "holdings": holdings, "usd_krw": round(usd_krw, 2)}


@router.post("/api/portfolio/holdings", status_code=201)
async def add_holding(request: Request, body: _HoldingInput):
    """종목 추가 (admin 전용)."""
    if getattr(request.state, "role", "user") != "admin":
        raise HTTPException(status_code=403, detail="관리자만 종목을 추가할 수 있습니다")
    if body.qty <= 0 or body.avg_price <= 0:
        raise HTTPException(status_code=422, detail="수량·단가는 양수여야 합니다")
    ticker = body.ticker.strip().upper()
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO manual_portfolio (ticker, name, avg_price, qty)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (ticker) DO UPDATE
                  SET name=$2, avg_price=$3, qty=$4, updated_at=NOW()
                RETURNING id, ticker, name, avg_price, qty
                """,
                ticker, body.name.strip(), body.avg_price, body.qty,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    return {"id": row["id"], "ticker": row["ticker"], "name": row["name"],
            "avg_price": float(row["avg_price"]), "qty": row["qty"]}


@router.put("/api/portfolio/holdings/{holding_id}")
async def update_holding(request: Request, holding_id: int, body: _HoldingInput):
    """종목 수정 (admin 전용)."""
    if getattr(request.state, "role", "user") != "admin":
        raise HTTPException(status_code=403, detail="관리자만 종목을 수정할 수 있습니다")
    if body.qty <= 0 or body.avg_price <= 0:
        raise HTTPException(status_code=422, detail="수량·단가는 양수여야 합니다")
    ticker = body.ticker.strip().upper()
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE manual_portfolio
            SET ticker=$1, name=$2, avg_price=$3, qty=$4, updated_at=NOW()
            WHERE id=$5
            RETURNING id
            """,
            ticker, body.name.strip(), body.avg_price, body.qty, holding_id,
        )
    if not row:
        raise HTTPException(status_code=404, detail="해당 종목을 찾을 수 없습니다")
    return {"ok": True}


@router.delete("/api/portfolio/holdings/{holding_id}", status_code=204)
async def delete_holding(request: Request, holding_id: int):
    """종목 삭제 (admin 전용)."""
    if getattr(request.state, "role", "user") != "admin":
        raise HTTPException(status_code=403, detail="관리자만 종목을 삭제할 수 있습니다")
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM manual_portfolio WHERE id=$1", holding_id
        )
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="해당 종목을 찾을 수 없습니다")
