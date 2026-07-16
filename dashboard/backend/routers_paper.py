"""
dashboard/backend/routers_paper.py
모의투자 조회 라우터.

  GET /api/paper/history — 종목별 모의투자 전체 이력
  GET /api/paper/curve   — 모델별 누적 P&L 시계열 + 통계 + 미실현
  GET /api/paper/export  — 포지션 전체 CSV 다운로드

의존 방향: routers_* → common/database/core.*/data.* 만 허용 (main import 금지).
테스트의 monkeypatch는 이 모듈을 대상으로 해야 함 (main의 재수출 아님).
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

from database import get_pool
from common import _fetch_current_prices

logger = logging.getLogger(__name__)

router = APIRouter()


# ── GET /api/paper/history ────────────────────────────────────
@router.get("/api/paper/history")
async def get_paper_ticker_history(ticker: str):
    """특정 종목의 모의투자 전체 이력 (모든 포지션, 신호일 역순)."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    p.id, p.model, p.ticker,
                    COALESCE(tn.name_ko, k.name_ko,
                             cs.name, SPLIT_PART(p.ticker, '.', 1)) AS name,
                    p.signal_date, p.entry_theory, p.entry_actual,
                    p.slippage_pct, p.qty, p.status,
                    p.tp1_pct, p.tp1_ratio, p.tp1_date, p.tp1_price,
                    p.trail_pct, p.hard_stop_pct, p.watermark,
                    p.exit_date, p.exit_price, p.exit_type,
                    p.blended_return, p.created_at,
                    o.close AS current_price
                FROM   paper_positions p
                LEFT JOIN ticker_names tn ON tn.ticker = p.ticker
                LEFT JOIN krx_listings k  ON k.yfinance_symbol = p.ticker
                LEFT JOIN LATERAL (
                    SELECT name FROM chart_signals
                    WHERE  ticker = p.ticker ORDER BY screened_at DESC LIMIT 1
                ) cs ON TRUE
                LEFT JOIN LATERAL (
                    SELECT close FROM daily_ohlcv
                    WHERE  symbol = p.ticker ORDER BY date DESC LIMIT 1
                ) o ON TRUE
                WHERE  p.ticker = $1
                ORDER  BY p.signal_date DESC, p.id DESC
                """,
                ticker,
            )

        # 오픈 포지션이 있으면 현재가 갱신
        active_rows = [r for r in rows if r["status"] in ("open", "pending")]
        prices: dict[str, float] = {}
        if active_rows:
            # update_cache=False: 단일 종목이 공유 포지션 캐시를 오염시키지 않도록
            prices = await _fetch_current_prices([ticker], update_cache=False)

        def _fmt(r) -> dict:
            d = dict(r)
            for k in ("entry_actual", "entry_theory", "slippage_pct",
                      "tp1_pct", "tp1_ratio", "trail_pct", "hard_stop_pct",
                      "tp1_price", "watermark", "exit_price", "blended_return"):
                if d.get(k) is not None:
                    d[k] = float(d[k])
            # current_price: daily_ohlcv 대신 yfinance 캐시 사용
            d.pop("current_price", None)
            curr = prices.get(ticker) if d["status"] in ("open", "pending") else None
            d["current_price"] = curr
            for k in ("signal_date", "tp1_date", "exit_date"):
                if d.get(k) is not None:
                    d[k] = str(d[k])
            if d.get("created_at") is not None:
                d["created_at"] = d["created_at"].isoformat()
            entry = d.get("entry_actual")
            d["unrealized_pct"] = (
                round((curr / entry - 1) * 100, 2)
                if curr and entry else None
            )
            return d

        positions = [_fmt(r) for r in rows]
        name = positions[0]["name"] if positions else ticker
        return {"data": positions, "ticker": ticker, "name": name}

    except Exception as e:
        logger.error("[paper/history] 조회 실패: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ── GET /api/paper/curve ──────────────────────────────────────
@router.get("/api/paper/curve")
async def get_paper_curve():
    """모델별 누적 P&L 시계열 + 모델 통계 + open 포지션 미실현 반환."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            # Query 1: 누적 실현 P&L 시계열
            series_rows = await conn.fetch(
                """
                SELECT model,
                       exit_date::text AS date,
                       SUM(blended_return) OVER (
                           PARTITION BY model
                           ORDER BY exit_date, id
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                       ) AS cumulative
                FROM   paper_positions
                WHERE  status = 'closed' AND blended_return IS NOT NULL
                ORDER  BY model, exit_date, id
                """
            )

            # Query 2: 모델별 집계 통계
            stats_rows = await conn.fetch(
                """
                SELECT
                    model,
                    COUNT(*)                                                   AS n_trades,
                    COUNT(*) FILTER (WHERE blended_return > 0)                 AS n_wins,
                    AVG(blended_return) FILTER (WHERE blended_return > 0)      AS avg_win,
                    AVG(blended_return) FILTER (WHERE blended_return < 0)      AS avg_loss,
                    SUM(blended_return)                                         AS total_realized
                FROM   paper_positions
                WHERE  status = 'closed' AND blended_return IS NOT NULL
                GROUP  BY model
                """
            )

            # Query 3: 청산 종목명 맵 (krx_listings는 이 환경에서 항상 비어 있으므로 제외)
            name_rows = await conn.fetch(
                """
                SELECT DISTINCT
                    p.ticker,
                    COALESCE(tn.name_ko, SPLIT_PART(p.ticker, '.', 1)) AS name
                FROM   paper_positions p
                LEFT JOIN ticker_names tn ON tn.ticker = p.ticker
                WHERE  p.status = 'closed'
                """
            )

            # Query 4: open 포지션 (미실현 계산용)
            open_rows = await conn.fetch(
                """
                SELECT p.ticker, p.model, p.entry_actual, p.qty,
                       COALESCE(tn.name_ko, k.name_ko,
                                cs.name, SPLIT_PART(p.ticker, '.', 1)) AS name
                FROM   paper_positions p
                LEFT JOIN ticker_names tn ON tn.ticker = p.ticker
                LEFT JOIN krx_listings k  ON k.yfinance_symbol = p.ticker
                LEFT JOIN LATERAL (
                    SELECT name FROM chart_signals
                    WHERE  ticker = p.ticker ORDER BY screened_at DESC LIMIT 1
                ) cs ON TRUE
                WHERE  p.status IN ('open', 'pending')
                ORDER  BY p.signal_date
                """
            )

        # 시계열 구조화
        series: dict = {}
        for r in series_rows:
            m = r["model"]
            if m not in series:
                series[m] = []
            series[m].append({
                "date": r["date"],
                "cumulative": round(float(r["cumulative"]), 4),
            })

        # 모델 통계 구조화
        model_stats: dict = {}
        for r in stats_rows:
            m = r["model"]
            n = int(r["n_trades"])
            nw = int(r["n_wins"])
            model_stats[m] = {
                "n_trades": n,
                "n_wins": nw,
                "win_rate": round(nw / n, 3) if n else 0.0,
                "avg_win": round(float(r["avg_win"]), 4) if r["avg_win"] is not None else None,
                "avg_loss": round(float(r["avg_loss"]), 4) if r["avg_loss"] is not None else None,
                "total_realized": round(float(r["total_realized"]), 4) if r["total_realized"] is not None else 0.0,
                "total_unrealized": 0.0,  # 아래에서 채움
            }

        # 종목명 맵
        ticker_name_map = {r["ticker"]: r["name"] for r in name_rows}

        # open 포지션 미실현 계산
        open_list = [dict(r) for r in open_rows]
        open_positions = []
        if open_list:
            unique_tickers = list({r["ticker"] for r in open_list})
            prices = await _fetch_current_prices(unique_tickers, update_cache=False)
            model_unrealized: dict[str, float] = {}
            for r in open_list:
                ticker = r["ticker"]
                entry = r["entry_actual"]
                curr = prices.get(ticker)
                unrealized_pct: float | None = None
                if curr and entry:
                    unrealized_pct = round((curr / float(entry) - 1), 4)
                open_positions.append({
                    "ticker": ticker,
                    "name": r["name"],
                    "model": r["model"],
                    "unrealized_pct": unrealized_pct,
                })
                if unrealized_pct is not None:
                    model_unrealized[r["model"]] = (
                        model_unrealized.get(r["model"], 0.0) + unrealized_pct
                    )
            for m, v in model_unrealized.items():
                if m in model_stats:
                    model_stats[m]["total_unrealized"] = round(v, 4)

        return {
            "data": {
                "series": series,
                "model_stats": model_stats,
                "ticker_name_map": ticker_name_map,
                "open_positions": open_positions,
            }
        }
    except Exception as e:
        logger.error("[paper/curve] 조회 실패: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ── GET /api/paper/export ─────────────────────────────────────
@router.get("/api/paper/export")
async def get_paper_export():
    """모의투자 포지션 전체 CSV 다운로드 (full dump, 필터링은 Python에서).

    utf-8-sig BOM 인코딩 — Excel에서 한글 종목명 깨짐 방지.
    StreamingResponse 대신 Response — 수백 행 규모에서 async generator 불필요.
    """
    import csv, io
    from datetime import date as _date
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    p.model, p.ticker,
                    COALESCE(tn.name_ko, SPLIT_PART(p.ticker, '.', 1)) AS name,
                    p.signal_date, p.entry_theory, p.entry_actual, p.slippage_pct,
                    p.qty, p.status,
                    p.tp1_pct, p.tp1_ratio, p.tp1_date, p.tp1_price,
                    p.trail_pct, p.hard_stop_pct, p.watermark,
                    p.exit_date, p.exit_price, p.exit_type,
                    p.blended_return, p.created_at
                FROM   paper_positions p
                LEFT JOIN ticker_names tn ON tn.ticker = p.ticker
                ORDER  BY p.model, p.exit_date DESC NULLS LAST, p.id DESC
                """
            )

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "model", "ticker", "name",
            "signal_date", "entry_theory", "entry_actual", "slippage_pct",
            "qty", "status",
            "tp1_pct", "tp1_ratio", "tp1_date", "tp1_price",
            "trail_pct", "hard_stop_pct", "watermark",
            "exit_date", "exit_price", "exit_type",
            "blended_return", "created_at",
        ])
        for row in rows:
            writer.writerow(["" if v is None else str(v) for v in row])

        today = _date.today().strftime("%Y%m%d")
        return Response(
            content=buf.getvalue().encode("utf-8-sig"),
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="paper_positions_{today}.csv"'
            },
        )
    except Exception as e:
        logger.error("[paper/export] 조회 실패: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
