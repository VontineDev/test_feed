"""
backtest.py  —  교차분석 백테스팅 모듈
────────────────────────────────────────────────────────────
교차분석(CONFIRM/CAUTION/FILTER/NEUTRAL) 판정의 정확도를 검증.

기능:
    track     — 미채움 체크포인트에 실제 가격 채우기
    backfill  — 기존 신호에 대해 과거 교차분석 + 체크포인트 백필
    report    — 판정 적중률, 수익률, 점수 상관관계 리포트

실행:
    python backtest.py track
    python backtest.py backfill [--since 2025-03-01]
    python backtest.py report [--verdict CONFIRM] [--checkpoint 1d]
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import math
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import yfinance as yf

from db import (
    create_pool,
    fetch_pending_outcomes,
    init_db,
    save_cross_analysis,
    update_outcome,
)
from market_data import (
    CrossAnalysis,
    PriceContext,
    _calc_rsi,
    _fetch_yfinance,
    cross_analyze,
    get_price_context,
    YFINANCE_MAP,
)

logger = logging.getLogger(__name__)

# ── 체크포인트 오프셋 ────────────────────────────────────────
CHECKPOINT_OFFSETS = {
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "1d": timedelta(days=1),
    "3d": timedelta(days=3),
}


# ═════════════════════════════════════════════════════════════
# Component A: 가격 트래커
# ═════════════════════════════════════════════════════════════

def _fetch_historical_price(
    symbol: str,
    target_time: datetime,
) -> Optional[float]:
    """
    yfinance에서 target_time에 가장 가까운 종가를 조회.
    장중 데이터(~60일)가 있으면 시간봉, 없으면 일봉 사용.
    """
    try:
        t = yf.Ticker(symbol)
        now = datetime.now(timezone.utc)
        days_ago = (now - target_time).days

        if days_ago <= 55:
            # 시간봉 시도
            start = (target_time - timedelta(hours=2)).strftime("%Y-%m-%d")
            end = (target_time + timedelta(hours=6)).strftime("%Y-%m-%d")
            hist = t.history(start=start, end=end, interval="1h")
            if not hist.empty:
                hist.index = hist.index.tz_convert("UTC") if hist.index.tz else hist.index.tz_localize("UTC")
                target_utc = target_time if target_time.tzinfo else target_time.replace(tzinfo=timezone.utc)
                diffs = abs(hist.index - target_utc)
                closest_idx = diffs.argmin()
                price = float(hist["Close"].iloc[closest_idx])
                if math.isnan(price):
                    return None
                return round(price, 2)

        # 일봉 폴백
        start = (target_time - timedelta(days=2)).strftime("%Y-%m-%d")
        end = (target_time + timedelta(days=2)).strftime("%Y-%m-%d")
        hist = t.history(start=start, end=end, interval="1d")
        if not hist.empty:
            hist.index = hist.index.tz_convert("UTC") if hist.index.tz else hist.index.tz_localize("UTC")
            target_utc = target_time if target_time.tzinfo else target_time.replace(tzinfo=timezone.utc)
            diffs = abs(hist.index - target_utc)
            closest_idx = diffs.argmin()
            price = float(hist["Close"].iloc[closest_idx])
            if math.isnan(price):
                return None
            return round(price, 2)

        return None
    except Exception as e:
        logger.debug("[트래커] %s 가격 조회 실패 (%s): %s", symbol, target_time, e)
        return None


async def track_outcomes(pool) -> dict:
    """
    미채움 체크포인트에 실제 가격을 채움.
    Returns: {"filled": N, "failed": N, "total": N}
    """
    pending = await fetch_pending_outcomes(pool)
    if not pending:
        logger.info("[트래커] 채울 체크포인트 없음")
        return {"filled": 0, "failed": 0, "total": 0}

    logger.info("[트래커] %d개 체크포인트 처리 시작", len(pending))

    # symbol별로 그룹화하여 API 호출 최소화
    by_symbol: dict[str, list[dict]] = defaultdict(list)
    for row in pending:
        by_symbol[row["symbol"]].append(row)

    filled = 0
    failed = 0
    fail_by_symbol: dict[str, int] = defaultdict(int)

    for symbol, rows in by_symbol.items():
        for row in rows:
            offset = CHECKPOINT_OFFSETS.get(row["checkpoint"])
            if not offset:
                continue
            target_time = row["signal_time"] + offset

            price = _fetch_historical_price(symbol, target_time)

            if price is not None and row["price_at_signal"] > 0:
                return_pct = round(
                    (price - row["price_at_signal"]) / row["price_at_signal"] * 100, 4
                )
                ok = await update_outcome(pool, row["outcome_id"], price, return_pct)
                if ok:
                    filled += 1
                else:
                    failed += 1
                    fail_by_symbol[symbol] += 1
            else:
                # 가격 조회 실패 → NULL로 표시하되 fetched_at 채워서 무한 재시도 방지
                await update_outcome(pool, row["outcome_id"], None, None)
                failed += 1
                fail_by_symbol[symbol] += 1

        # yfinance rate limit 대비
        await asyncio.sleep(0.3)

    result = {"filled": filled, "failed": failed, "total": len(pending)}
    logger.info(
        "[트래커] 완료 — 채움:%d 실패:%d 전체:%d",
        filled, failed, len(pending),
    )

    # 데이터 품질 경고: 실패율 > 30% 이면 심볼별 분류 로그
    total = len(pending)
    if total > 0 and failed / total > 0.30:
        breakdown = ", ".join(
            f"{sym}:{cnt}" for sym, cnt in sorted(fail_by_symbol.items())
        )
        logger.warning(
            "[트래커] 데이터 품질 경고 — 실패율 %.0f%% (%d/%d). 심볼별: %s",
            failed / total * 100, failed, total, breakdown,
        )

    return result


# ═════════════════════════════════════════════════════════════
# Component B: 과거 데이터 백필
# ═════════════════════════════════════════════════════════════

def _build_price_context_historical(
    symbol: str,
    ticker_name: str,
    as_of_date: datetime,
) -> Optional[PriceContext]:
    """
    as_of_date 기준 과거 시세로 PriceContext 구성.
    """
    try:
        t = yf.Ticker(symbol)
        start = (as_of_date - timedelta(days=365)).strftime("%Y-%m-%d")
        end = as_of_date.strftime("%Y-%m-%d")
        hist = t.history(start=start, end=end)
        if hist.empty or len(hist) < 2:
            return None

        closes = hist["Close"].tolist()
        volumes = hist["Volume"].tolist()
        current = closes[-1]
        prev = closes[-2]
        change_pct = round((current - prev) / prev * 100, 2) if prev else 0

        rsi = _calc_rsi(closes)

        avg_vol = sum(volumes[-20:]) / len(volumes[-20:]) if len(volumes) >= 5 else None
        vol_ratio = round(volumes[-1] / avg_vol, 2) if avg_vol else None
        volume_surge = bool(vol_ratio and vol_ratio >= 2.0)

        week52_high = round(max(closes), 2)
        week52_low = round(min(closes), 2)

        return PriceContext(
            ticker=ticker_name, symbol=symbol, source="yfinance",
            current=round(current, 2), change_pct=change_pct,
            rsi=rsi, volume_ratio=vol_ratio,
            week52_high=week52_high, week52_low=week52_low,
            volume_surge=volume_surge, success=True,
        )
    except Exception as e:
        logger.debug("[백필] %s 과거 시세 실패: %s", symbol, e)
        return None


async def cross_analyze_historical(
    direction: str,
    strength: int,
    tickers: list[str],
    ticker_symbols: dict[str, str] | None,
    as_of_date: datetime,
    _ctx_cache: Optional[dict] = None,
) -> CrossAnalysis:
    """
    과거 시점(as_of_date) 기준으로 교차분석 실행.
    cross_analyze()를 직접 호출하여 판정 로직 중복 방지.
    _ctx_cache: session-scoped dict keyed by (symbol, iso_year, iso_week) —
    avoids re-fetching 365-day history for the same symbol/week combo.
    """
    # 심볼 해석 (기존 로직 재활용)
    resolved: dict[str, str] = {}
    if ticker_symbols:
        resolved.update(ticker_symbols)
    for tk in tickers:
        if tk not in resolved:
            key = tk.lower().strip()
            if key in YFINANCE_MAP:
                resolved[tk] = YFINANCE_MAP[key]

    iso = as_of_date.isocalendar()
    cache_week = (iso.year, iso.week)
    # ISO week granularity: signals on Mon–Fri of the same week share one 365-day
    # history fetch. Price accuracy is off by up to 4 trading days for week-end
    # signals — acceptable for backfill; live path uses _build_price_context_historical
    # directly without this cache.

    contexts: list[PriceContext] = []
    for tk, sym in resolved.items():
        cache_key = (sym, cache_week[0], cache_week[1])
        if _ctx_cache is not None and cache_key in _ctx_cache:
            ctx = _ctx_cache[cache_key]
        else:
            ctx = _build_price_context_historical(sym, tk, as_of_date)
            # Only cache successful fetches — do NOT cache None (transient failures).
            # Caching None would poison all signals in the same (symbol, week).
            if _ctx_cache is not None and ctx is not None and ctx.success:
                _ctx_cache[cache_key] = ctx
            await asyncio.sleep(0.3)
        if ctx and ctx.success:
            contexts.append(ctx)

    if not contexts:
        return CrossAnalysis(
            verdict="NEUTRAL", score=strength * 2,
            summary="과거 시세 데이터 없음",
            price_contexts=[], confirm_count=0, conflict_count=0,
        )

    # cross_analyze()에 과거 컨텍스트를 직접 전달 — 판정 로직은 한 곳에만 존재
    return cross_analyze(direction, strength, tickers, ticker_symbols, _contexts=contexts)


async def backfill_historical(pool, since: Optional[str] = None) -> dict:
    """
    기존 trade_signals에 교차분석이 없는 행을 백필.
    """
    since_filter = ""
    args: list = []
    if since:
        since_filter = "AND s.detected_at >= $1"
        args.append(datetime.fromisoformat(since).replace(tzinfo=timezone.utc))

    query = f"""
        SELECT s.id AS signal_id, s.direction, s.strength,
               s.tickers, s.detected_at
        FROM   trade_signals s
        LEFT JOIN cross_analysis_results car ON car.signal_id = s.id
        WHERE  car.id IS NULL
               AND s.direction IN ('BUY', 'SELL', 'WATCH')
               {since_filter}
        ORDER BY s.detected_at ASC
    """

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *args)

    if not rows:
        logger.info("[백필] 백필 대상 없음")
        return {"processed": 0, "skipped": 0}

    logger.info("[백필] %d개 신호 백필 시작", len(rows))
    processed = 0
    skipped = 0
    _ctx_cache: dict = {}  # (symbol, iso_year, iso_week) → PriceContext | None

    for row in rows:
        tickers = row["tickers"] or []
        if not tickers:
            skipped += 1
            continue

        as_of = row["detected_at"]
        cross = await cross_analyze_historical(
            direction=row["direction"],
            strength=row["strength"],
            tickers=tickers,
            ticker_symbols=None,
            as_of_date=as_of,
            _ctx_cache=_ctx_cache,
        )

        cross_id = await save_cross_analysis(pool, row["signal_id"], cross)
        if not cross_id:
            skipped += 1
            continue

        # 과거 체크포인트 가격 즉시 채우기
        for ctx in cross.price_contexts:
            for cp, offset in CHECKPOINT_OFFSETS.items():
                target_time = as_of + offset
                price = _fetch_historical_price(ctx.symbol, target_time)
                if price is not None and ctx.current > 0:
                    return_pct = round(
                        (price - ctx.current) / ctx.current * 100, 4
                    )
                else:
                    price = None
                    return_pct = None

                # outcome 행 찾아서 업데이트
                async with pool.acquire() as conn:
                    outcome_row = await conn.fetchrow(
                        """
                        SELECT po.id FROM price_outcomes po
                        JOIN   cross_analysis_prices cap ON cap.id = po.cross_price_id
                        WHERE  cap.cross_id = $1
                          AND  cap.symbol = $2
                          AND  po.checkpoint = $3
                        """,
                        cross_id, ctx.symbol, cp,
                    )
                if outcome_row:
                    await update_outcome(pool, outcome_row["id"], price, return_pct)

            await asyncio.sleep(0.3)

        processed += 1
        if processed % 10 == 0:
            logger.info("[백필] 진행: %d/%d", processed, len(rows))

    result = {"processed": processed, "skipped": skipped}
    logger.info("[백필] 완료 — 처리:%d 스킵:%d", processed, skipped)
    return result


# ═════════════════════════════════════════════════════════════
# Component C: 지표 계산 + 리포트
# ═════════════════════════════════════════════════════════════

async def calculate_metrics(
    pool,
    verdict_filter: Optional[str] = None,
    checkpoint_filter: Optional[str] = None,
) -> dict:
    """
    교차분석 백테스팅 지표 계산.
    """
    conditions = ["po.fetched_at IS NOT NULL", "po.return_pct IS NOT NULL"]
    args: list = []
    idx = 0

    if verdict_filter:
        idx += 1
        conditions.append(f"car.verdict = ${idx}")
        args.append(verdict_filter)
    if checkpoint_filter:
        idx += 1
        conditions.append(f"po.checkpoint = ${idx}")
        args.append(checkpoint_filter)

    where = " AND ".join(conditions)

    query = f"""
        SELECT car.verdict,
               s.direction,
               car.score,
               po.checkpoint,
               po.return_pct
        FROM   price_outcomes po
        JOIN   cross_analysis_prices cap ON cap.id = po.cross_price_id
        JOIN   cross_analysis_results car ON car.id = cap.cross_id
        JOIN   trade_signals s ON s.id = car.signal_id
        WHERE  {where}
        ORDER BY car.verdict, po.checkpoint
    """

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *args)
        # Baseline always uses 1d checkpoint — it has the most data and represents
        # the primary holding period in the Telegram report. checkpoint_filter does
        # not apply here; callers wanting other checkpoints can compute separately.
        baseline_rows = await conn.fetch("""
            SELECT s.direction,
                   COUNT(*) FILTER (WHERE po.return_pct > 0) AS up_count,
                   COUNT(*) FILTER (WHERE po.return_pct < 0) AS down_count,
                   COUNT(*) AS total
            FROM price_outcomes po
            JOIN cross_analysis_prices cap ON cap.id = po.cross_price_id
            JOIN cross_analysis_results car ON car.id = cap.cross_id
            JOIN trade_signals s ON s.id = car.signal_id
            WHERE po.checkpoint = '1d' AND po.return_pct IS NOT NULL
            GROUP BY s.direction
        """)

    if not rows:
        return {"message": "데이터 없음", "rows": 0}

    # 시장 기준선 계산
    market_baseline: dict[str, float] | None = None
    if baseline_rows:
        bl: dict[str, float] = {}
        for br in baseline_rows:
            total = br["total"] if br["total"] is not None else 0
            if total == 0:
                continue
            direction = br["direction"]
            if direction == "BUY":
                bl["BUY"] = round(br["up_count"] / total * 100, 1)
            elif direction == "SELL":
                bl["SELL"] = round(br["down_count"] / total * 100, 1)
        if bl:
            market_baseline = bl

    # 그룹별 집계
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        key = (r["verdict"], r["checkpoint"])
        groups[key].append(dict(r))

    metrics = {}
    for (verdict, checkpoint), items in sorted(groups.items()):
        # NaN/None 필터링 — yfinance에서 NaN이 DB에 저장된 경우 방어
        valid = [i for i in items
                 if i["return_pct"] is not None and not math.isnan(i["return_pct"])]
        returns = [i["return_pct"] for i in valid]
        directions = [i["direction"] for i in valid]
        scores = [i["score"] for i in valid]

        # 적중 판정
        hits = 0
        has_watch = any(d == "WATCH" for d in directions)
        for ret, direction in zip(returns, directions):
            if verdict in ("CONFIRM", "NEUTRAL", "CAUTION"):
                # CAUTION: 기술적 지표가 약하게 반대이지만 원래 신호가 맞을 수 있음
                # → 원래 신호 방향과 동일 기준으로 적중 측정
                if direction == "BUY" and ret > 0:
                    hits += 1
                elif direction == "SELL" and ret < 0:
                    hits += 1
                # WATCH는 방향성 없음 — hit_rate 계산 제외
            elif verdict == "FILTER":
                # FILTER가 맞으려면 원래 신호 방향 반대로 가야 함
                if direction == "BUY" and ret <= 0:
                    hits += 1
                elif direction == "SELL" and ret >= 0:
                    hits += 1

        n = len(returns)
        avg_ret = round(sum(returns) / n, 4) if n else 0
        sorted_rets = sorted(returns)
        median_ret = sorted_rets[n // 2] if n else 0
        # WATCH 방향은 적중률 의미 없음 (방향성 없는 모니터링 신호)
        directional_n = sum(1 for d in directions if d != "WATCH")
        if has_watch and directional_n == 0:
            hit_rate = None
        elif directional_n > 0:
            hit_rate = round(hits / directional_n * 100, 1)
        else:
            hit_rate = None
        avg_score = round(sum(scores) / n, 1) if n else 0

        metrics[(verdict, checkpoint)] = {
            "count": n,
            "hit_rate": hit_rate,
            "avg_return": avg_ret,
            "median_return": median_ret,
            "min_return": sorted_rets[0] if sorted_rets else 0,
            "max_return": sorted_rets[-1] if sorted_rets else 0,
            "avg_score": avg_score,
        }

    # 점수 구간별 수익률 (NaN 제외)
    score_buckets: dict[str, list[float]] = {"0-3": [], "4-6": [], "7-10": []}
    for r in rows:
        ret = r["return_pct"]
        if ret is None or math.isnan(ret):
            continue
        sc = r["score"]
        if sc <= 3:
            score_buckets["0-3"].append(ret)
        elif sc <= 6:
            score_buckets["4-6"].append(ret)
        else:
            score_buckets["7-10"].append(ret)

    score_corr = {}
    for bucket, rets in score_buckets.items():
        if rets:
            score_corr[bucket] = {
                "count": len(rets),
                "avg_return": round(sum(rets) / len(rets), 4),
            }

    return {
        "rows": len(rows),
        "by_verdict_checkpoint": metrics,
        "score_correlation": score_corr,
        "market_baseline": market_baseline,
    }


def _print_report(metrics: dict) -> None:
    """지표를 터미널에 깔끔하게 출력."""
    if metrics.get("message"):
        print(f"\n  {metrics['message']}")
        return

    print(f"\n{'=' * 80}")
    print(f"  교차분석 백테스팅 리포트  (데이터: {metrics['rows']}건)")
    print(f"{'=' * 80}")

    # 판정별 체크포인트 테이블
    print(f"\n{'판정':<10} {'체크포인트':<10} {'건수':>6} {'적중률':>8} "
          f"{'평균수익':>10} {'중앙값':>10} {'최소':>10} {'최대':>10} {'평균점수':>8}")
    print("-" * 80)

    by_vc = metrics.get("by_verdict_checkpoint", {})
    for (verdict, checkpoint), m in sorted(by_vc.items()):
        print(
            f"{verdict:<10} {checkpoint:<10} {m['count']:>6} "
            f"{m['hit_rate']:>7.1f}% "
            f"{m['avg_return']:>9.4f}% "
            f"{m['median_return']:>9.4f}% "
            f"{m['min_return']:>9.4f}% "
            f"{m['max_return']:>9.4f}% "
            f"{m['avg_score']:>7.1f}"
        )

    # 점수 상관관계
    sc = metrics.get("score_correlation", {})
    if sc:
        print(f"\n{'─' * 40}")
        print("  점수 구간별 평균 수익률")
        print(f"{'─' * 40}")
        for bucket, data in sorted(sc.items()):
            print(f"  점수 {bucket:<6}  건수:{data['count']:>4}  "
                  f"평균수익:{data['avg_return']:>8.4f}%")

    print(f"\n{'=' * 80}\n")


# ═════════════════════════════════════════════════════════════
# Component D: 리포트 파일 저장
# ═════════════════════════════════════════════════════════════

REPORT_DIR = Path(__file__).parent / "reports"


def _ensure_report_dir() -> Path:
    """reports/ 디렉터리 생성 (없으면)."""
    REPORT_DIR.mkdir(exist_ok=True)
    return REPORT_DIR


def _report_filename(fmt: str) -> str:
    """타임스탬프 기반 파일명 생성."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"backtest_{ts}.{fmt}"


def save_report_csv(metrics: dict, path: Optional[str] = None) -> str:
    """
    리포트를 CSV 파일로 저장.
    Returns: 저장된 파일 경로.
    """
    if metrics.get("message"):
        logger.warning("[리포트] 저장할 데이터 없음: %s", metrics["message"])
        return ""

    report_dir = _ensure_report_dir()
    filepath = Path(path) if path else report_dir / _report_filename("csv")

    by_vc = metrics.get("by_verdict_checkpoint", {})
    sc = metrics.get("score_correlation", {})

    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)

        # 판정별 체크포인트 테이블
        writer.writerow([
            "판정", "체크포인트", "건수", "적중률(%)",
            "평균수익(%)", "중앙값(%)", "최소(%)", "최대(%)", "평균점수",
        ])
        for (verdict, checkpoint), m in sorted(by_vc.items()):
            writer.writerow([
                verdict, checkpoint, m["count"], m["hit_rate"],
                m["avg_return"], m["median_return"],
                m["min_return"], m["max_return"], m["avg_score"],
            ])

        # 빈 행 구분
        writer.writerow([])

        # 점수 구간별 수익률
        writer.writerow(["점수구간", "건수", "평균수익(%)"])
        for bucket, data in sorted(sc.items()):
            writer.writerow([bucket, data["count"], data["avg_return"]])

        # 시장 기준선
        baseline = metrics.get("market_baseline")
        if baseline:
            writer.writerow([])
            writer.writerow(["시장기준선", "BUY(%)", "SELL(%)"])
            writer.writerow([
                "랜덤기준선",
                baseline.get("BUY", ""),
                baseline.get("SELL", ""),
            ])

    logger.info("[리포트] CSV 저장: %s", filepath)
    return str(filepath)


def save_report_json(metrics: dict, path: Optional[str] = None) -> str:
    """
    리포트를 JSON 파일로 저장.
    Returns: 저장된 파일 경로.
    """
    if metrics.get("message"):
        logger.warning("[리포트] 저장할 데이터 없음: %s", metrics["message"])
        return ""

    report_dir = _ensure_report_dir()
    filepath = Path(path) if path else report_dir / _report_filename("json")

    # dict의 tuple 키를 문자열 키로 변환 (JSON 호환)
    by_vc = metrics.get("by_verdict_checkpoint", {})
    serializable_vc = {}
    for (verdict, checkpoint), m in by_vc.items():
        key = f"{verdict}_{checkpoint}"
        serializable_vc[key] = {"verdict": verdict, "checkpoint": checkpoint, **m}

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_rows": metrics["rows"],
        "by_verdict_checkpoint": serializable_vc,
        "score_correlation": metrics.get("score_correlation", {}),
        "market_baseline": metrics.get("market_baseline"),
    }

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    logger.info("[리포트] JSON 저장: %s", filepath)
    return str(filepath)


# ═════════════════════════════════════════════════════════════
# Component E: Telegram 리포트 포맷터
# ═════════════════════════════════════════════════════════════

def _esc(s: str) -> str:
    """Telegram MarkdownV2 이스케이프. &도 포함 (S&P500 등)."""
    for ch in r'_*[]()~`>#+-=|{}.!&':
        s = s.replace(ch, f'\\{ch}')
    return s


async def _fetch_ticker_breakdown(pool, min_signals: int = 5) -> list[dict]:
    """
    CONFIRM 판정의 종목별 적중률 (1d 체크포인트).
    min_signals 이상인 종목만 반환.
    """
    query = """
        SELECT cap.symbol,
               s.direction,
               po.return_pct
        FROM   price_outcomes po
        JOIN   cross_analysis_prices cap ON cap.id = po.cross_price_id
        JOIN   cross_analysis_results car ON car.id = cap.cross_id
        JOIN   trade_signals s ON s.id = car.signal_id
        WHERE  po.checkpoint = '1d'
          AND  po.return_pct IS NOT NULL
          AND  car.verdict = 'CONFIRM'
        ORDER  BY cap.symbol
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

    by_symbol: dict[str, list] = defaultdict(list)
    for r in rows:
        if not math.isnan(r["return_pct"]):
            by_symbol[r["symbol"]].append((r["direction"], r["return_pct"]))

    result = []
    for symbol, items in sorted(by_symbol.items()):
        n = len(items)
        if n < min_signals:
            continue
        hits = sum(
            1 for direction, ret in items
            if (direction == "BUY" and ret > 0) or (direction == "SELL" and ret < 0)
        )
        directional = sum(1 for direction, _ in items if direction != "WATCH")
        if directional == 0:
            continue
        result.append({
            "symbol": symbol,
            "count": n,
            "hit_rate": round(hits / directional * 100, 1),
        })
    return result


async def backtest_report_telegram(pool) -> str:
    """
    백테스팅 리포트를 Telegram MarkdownV2 포맷 문자열로 반환.
    pool=None 이면 에러 메시지 반환.
    """
    if pool is None:
        return "❌ DB 미연결 상태입니다\\."

    BACKTEST_MIN_SIGNALS = int(os.environ.get("BACKTEST_MIN_SIGNALS", "10"))

    metrics = await calculate_metrics(pool)

    if metrics.get("message"):
        return (
            "📊 *백테스팅 리포트*\n\n"
            "데이터 없음\\. `python backtest\\.py backfill` 을 먼저 실행해주세요\\."
        )

    # 데이터 신선도 체크
    freshness_warning = ""
    try:
        async with pool.acquire() as conn:
            latest = await conn.fetchval(
                "SELECT MAX(fetched_at) FROM price_outcomes WHERE fetched_at IS NOT NULL"
            )
        if latest:
            from datetime import timezone as _tz
            now_utc = datetime.now(_tz.utc)
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=_tz.utc)
            hours_ago = (now_utc - latest).total_seconds() / 3600
            if hours_ago > 48:
                freshness_warning = (
                    f"⚠️ *데이터 경고*: 최근 업데이트 {_esc(f'{hours_ago:.0f}')}시간 전"
                    " \\(가격 데이터가 오래되었을 수 있음\\)\n\n"
                )
    except Exception:
        pass

    today = _esc(datetime.now().strftime("%Y-%m-%d"))
    lines = []
    if freshness_warning:
        lines.append(freshness_warning)

    lines.append(f"📊 *백테스팅 리포트* \\({today}\\)")
    lines.append(f"총 데이터: {_esc(str(metrics['rows']))}건")

    baseline = metrics.get("market_baseline")
    if baseline:
        buy_str = _esc(f"{baseline['BUY']:.1f}%") if "BUY" in baseline else "N/A"
        sell_str = _esc(f"{baseline['SELL']:.1f}%") if "SELL" in baseline else "N/A"
        lines.append(f"랜덤 기준선: {buy_str} \\(BUY\\) / {sell_str} \\(SELL\\)")
    lines.append("")

    # 판정별 적중률 (1d 체크포인트)
    by_vc = metrics.get("by_verdict_checkpoint", {})
    verdict_1d = {
        verdict: m for (verdict, cp), m in by_vc.items() if cp == "1d"
    }

    verdict_icons = {
        "CONFIRM": "✅", "CAUTION": "⚠️", "FILTER": "🔴", "NEUTRAL": "➖",
    }

    if verdict_1d:
        lines.append("🎯 *판정별 적중률* \\(1d 체크포인트\\)")
        lines.append("─────────────────────────")
        for verdict in ("CONFIRM", "CAUTION", "FILTER", "NEUTRAL"):
            m = verdict_1d.get(verdict)
            if not m:
                continue
            icon = verdict_icons.get(verdict, "")
            count_str = _esc(str(m["count"]))
            avg_ret_str = _esc(f"{m['avg_return']:+.2f}%")
            if verdict == "FILTER":
                lines.append(
                    f"{icon} {verdict}   {count_str}건 차단  "
                    f"avg {avg_ret_str}"
                )
            elif m["hit_rate"] is None:
                lines.append(
                    f"{icon} {verdict}   {count_str}건  적중률 N/A  avg {avg_ret_str}"
                )
            else:
                hr_str = _esc(f"{m['hit_rate']:.1f}%")
                lines.append(
                    f"{icon} {verdict}   {hr_str} \\({count_str}건\\)  avg {avg_ret_str}"
                )
        lines.append("")

    # 체크포인트별 CONFIRM 적중률
    confirm_by_cp = {
        cp: m for (verdict, cp), m in by_vc.items()
        if verdict == "CONFIRM" and m.get("hit_rate") is not None
    }
    if confirm_by_cp:
        cp_parts = []
        for cp in ("1h", "4h", "1d", "3d"):
            m = confirm_by_cp.get(cp)
            if m:
                hr_str = "{:.1f}%".format(m["hit_rate"])
                cp_parts.append(f"{cp}: {_esc(hr_str)}")
        if cp_parts:
            lines.append("⏱️ *체크포인트별 CONFIRM 적중률*")
            lines.append(_esc(" | ").join(cp_parts))
            lines.append("")

    # 종목별 적중률 (CONFIRM, 1d, 5건 이상)
    try:
        ticker_rows = await _fetch_ticker_breakdown(pool, min_signals=5)
    except Exception:
        ticker_rows = []

    if ticker_rows:
        lines.append(f"📈 *종목별 정확도* \\(CONFIRM 1d, {_esc(str(BACKTEST_MIN_SIGNALS))}건 이상\\)")
        lines.append("─────────────────────────")
        for row in ticker_rows:
            sym = _esc(row["symbol"])
            hr = _esc(f"{row['hit_rate']:.1f}%")
            cnt = _esc(str(row["count"]))
            if row["count"] >= BACKTEST_MIN_SIGNALS:
                lines.append(f"{sym}   {hr} \\({cnt}건\\)")
            else:
                # 5~(min-1)건: 낮은 신뢰도 표시
                lines.append(f"{sym}   {hr} \\({cnt}건\\) ⚠️ 낮은 신뢰도")

    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════════════

async def main():
    parser = argparse.ArgumentParser(description="교차분석 백테스팅")
    sub = parser.add_subparsers(dest="command", required=True)

    # track
    sub.add_parser("track", help="미채움 체크포인트에 실제 가격 채우기")

    # backfill
    bf = sub.add_parser("backfill", help="기존 신호에 교차분석 백필")
    bf.add_argument("--since", help="시작일 (예: 2025-03-01)")

    # report
    rp = sub.add_parser("report", help="백테스팅 리포트 출력")
    rp.add_argument("--verdict", help="판정 필터 (CONFIRM/CAUTION/FILTER/NEUTRAL)")
    rp.add_argument("--checkpoint", help="체크포인트 필터 (1h/4h/1d/3d)")
    rp.add_argument("--output", "-o", default="both",
                     choices=["csv", "json", "both", "none"],
                     help="리포트 저장 형식 (기본: both)")

    args = parser.parse_args()

    pool = await create_pool()
    await init_db(pool)

    try:
        if args.command == "track":
            result = await track_outcomes(pool)
            print(f"트래커 완료: 채움 {result['filled']}, "
                  f"실패 {result['failed']}, 전체 {result['total']}")

        elif args.command == "backfill":
            result = await backfill_historical(pool, since=args.since)
            print(f"백필 완료: 처리 {result['processed']}, 스킵 {result['skipped']}")

        elif args.command == "report":
            metrics = await calculate_metrics(
                pool,
                verdict_filter=args.verdict,
                checkpoint_filter=args.checkpoint,
            )
            _print_report(metrics)

            # 파일 저장
            fmt = args.output
            saved = []
            if fmt in ("csv", "both"):
                p = save_report_csv(metrics)
                if p:
                    saved.append(p)
            if fmt in ("json", "both"):
                p = save_report_json(metrics)
                if p:
                    saved.append(p)
            if saved:
                print("📁 리포트 저장 완료:")
                for s in saved:
                    print(f"   {s}")
    finally:
        await pool.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    asyncio.run(main())
