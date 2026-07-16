"""모의투자 잡 — Exit Checker, EOD 샘플러, T+1 진입."""

import asyncio
import logging
import random
from datetime import date, timedelta
from typing import Optional

import httpx

from analysis.backtest.config import (
    OPTIMAL_EXIT_PARAMS          as _KOSPI_P,
    OPTIMAL_EXIT_PARAMS_KOSDAQ   as _KOSDAQ_P,
    OPTIMAL_EXIT_PARAMS_CROSS    as _CROSS_P,
    OPTIMAL_EXIT_PARAMS_ICHIMOKU as _ICHI_P,
)
from core.db import load_chart_signals_latest
from data.kiwoom_paper_trader import (
    MODEL_CONFIG,
    get_open_positions,
    update_to_closed,
    get_pending_positions,
    update_to_open,
    _qty_from_price,
    insert_pending,
    get_open_slot_count,
    init_paper_positions,
)
from telegram.telegram_notify import _get_token, _get_chat_id, _post_message

logger = logging.getLogger(__name__)


async def paper_exit_checker_job(db_pool, paper_trader) -> None:
    """정규장 마감 직전(15:20 KST) — 오픈 포지션 전체에 대해 exit 조건 판정 → 시장가 매도주문."""
    today = date.today()
    _loop = asyncio.get_running_loop()

    _open_positions = await get_open_positions(db_pool)
    if not _open_positions:
        logger.info("[paper-exit] 오픈 포지션 없음")
        return

    logger.info("[paper-exit] 오픈 포지션 %d건 exit 체크 시작", len(_open_positions))

    # 현재가 조회 — Kiwoom mock API ka10001 (정규장 중 실시간 가격)
    _all_tickers = list({p["ticker"] for p in _open_positions})
    _prices: dict[str, float] = {}
    for _tk in _all_tickers:
        await asyncio.sleep(0.5)
        _px = await _loop.run_in_executor(None, paper_trader.get_current_price, _tk)
        if _px:
            _prices[_tk] = float(_px)
    logger.info("[paper-exit] Kiwoom 현재가 조회: %d/%d 종목", len(_prices), len(_all_tickers))

    _closed, _tp1_fired, _watermark_updated = 0, 0, 0

    for _pos in _open_positions:
        _pos_id       = _pos["id"]
        _ticker       = _pos["ticker"]
        _entry        = _pos["entry_actual"] or _pos["entry_theory"]
        _hard_stop    = _pos["hard_stop_pct"]
        _tp1_pct      = _pos["tp1_pct"]
        _tp1_ratio    = _pos["tp1_ratio"]
        _trail_pct    = _pos["trail_pct"]
        _tp1_done     = _pos["tp1_date"] is not None
        _watermark    = _pos["watermark"] or _entry
        _signal_date  = _pos["signal_date"]
        _qty          = _pos["qty"] or 0

        if not _entry or _entry <= 0:
            continue

        # 현재가 룩업 (Kiwoom mock API 조회 결과)
        _close = _prices.get(_ticker)
        if not _close or _close <= 0:
            logger.warning("[paper-exit] %s 현재가 없음 — 스킵", _ticker)
            continue

        _ret = (_close - _entry) / _entry  # 수익률 (미실현)

        # 워터마크 갱신 (고점 추적)
        if _close > _watermark:
            _watermark = float(_close)
            _watermark_updated += 1
            try:
                async with db_pool.acquire() as _conn:
                    await _conn.execute(
                        "UPDATE paper_positions SET watermark=$1 WHERE id=$2",
                        _watermark, _pos_id,
                    )
            except Exception as _e:
                logger.warning("[paper-exit] %s 워터마크 업데이트 실패: %s", _ticker, _e)

        # ── exit 조건 판정 ────────────────────────────────────
        _exit_type = None
        _blended   = None

        # 1. 최대 보유일 (91일)
        if (today - _signal_date).days >= 91:
            _exit_type = "period_end"

        # 2. 하드 스탑
        elif _close <= _entry * (1 - _hard_stop):
            _exit_type = "hard_stop"

        # 3. 1차 익절 (TP1 미발동 시)
        elif not _tp1_done and _close >= _entry * (1 + _tp1_pct):
            # TP1 발동: 절반 청산 기록만 (잔여분은 계속 보유)
            _exit_type = None   # 전량 청산 아님
            _tp1_fired += 1
            try:
                async with db_pool.acquire() as _conn:
                    await _conn.execute(
                        "UPDATE paper_positions SET tp1_date=$1, tp1_price=$2 WHERE id=$3",
                        today, float(_close), _pos_id,
                    )
                logger.info("[paper-exit] %s TP1 발동 +%.1f%% (잔여분 트레일링 계속)",
                            _ticker, _ret * 100)
            except Exception as _e:
                logger.warning("[paper-exit] %s TP1 기록 실패: %s", _ticker, _e)

        # 4. 트레일링 스탑 (TP1 발동 후)
        elif _tp1_done and _close <= _watermark * (1 - _trail_pct):
            _exit_type = "trail"

        if _exit_type is None:
            continue

        # ── 매도주문 제출 ─────────────────────────────────────
        _sell_ord = ""
        if _qty > 0:
            try:
                _sell_ord = await _loop.run_in_executor(
                    None, paper_trader.place_sell, _ticker, _qty
                )
            except Exception as _e:
                logger.warning("[paper-exit] %s 매도주문 실패: %s", _ticker, _e)
                _sell_ord = "FAILED"

        # blended_return 계산 (TP1 발동 시 가중평균)
        if _tp1_done:
            _tp1_ret = (_pos["tp1_price"] - _entry) / _entry if _pos.get("tp1_price") else 0
            _final   = _ret
            _blended = _tp1_ratio * _tp1_ret + (1 - _tp1_ratio) * _final
        else:
            _blended = _ret

        try:
            await update_to_closed(db_pool, _pos_id, float(_close), _exit_type, _sell_ord, _blended)
            _closed += 1
            logger.info("[paper-exit] %s 청산 [%s] 수익=%.2f%% 주문번호=%s",
                        _ticker, _exit_type, _blended * 100, _sell_ord)
        except Exception as _e:
            logger.warning("[paper-exit] %s DB 청산 업데이트 실패: %s", _ticker, _e)

    logger.info("[paper-exit] 완료 — 청산=%d TP1발동=%d 워터마크갱신=%d",
                _closed, _tp1_fired, _watermark_updated)

    if _closed > 0:
        try:
            _msg = (
                f"모의투자 청산 완료 ({today})\n"
                f"청산: {_closed}건 | TP1발동: {_tp1_fired}건"
            )
            async with httpx.AsyncClient() as _http:
                await _post_message(_http, _get_token(), _get_chat_id(),
                                    _msg, label="paper-exit", parse_mode=None)
        except Exception as _e:
            logger.warning("[paper-exit] 텔레그램 알림 실패: %s", _e)


async def paper_eod_sampler_job(db_pool, paper_trader) -> None:
    """stage_classifier(16:30) 완료 후 — 오늘 신호를 sampling → pending 삽입."""
    today = date.today()
    logger.info("[paper-sampler] EOD 샘플링 시작 (%s)", today)

    # 오늘 Stage 1 진입 종목
    try:
        async with db_pool.acquire() as _conn:
            _stage1_rows = await _conn.fetch(
                "SELECT ticker, s1_high FROM stage_classifications "
                "WHERE classified_date=$1 AND stage=1",
                today,
            )
    except Exception as _e:
        logger.warning("[paper-sampler] stage_classifications 조회 실패: %s", _e)
        return

    stage1_all = [{"ticker": r["ticker"], "price": float(r["s1_high"] or 0)}
                  for r in _stage1_rows]
    logger.info("[paper-sampler] Stage1 %d종목", len(stage1_all))

    # 최신 Ichimoku 통과 종목
    try:
        _week, _ichi_rows = await load_chart_signals_latest(db_pool)
        _ichi_tickers = {r["ticker"] for r in _ichi_rows}
        _ichi_price   = {r["ticker"]: float(r.get("close") or 0) for r in _ichi_rows}
    except Exception as _e:
        logger.warning("[paper-sampler] Ichimoku 조회 실패: %s", _e)
        _ichi_tickers, _ichi_price = set(), {}

    # Cross = Stage1 ∩ Ichimoku
    cross_signals  = [s for s in stage1_all if s["ticker"] in _ichi_tickers]
    stage_kospi    = [s for s in stage1_all if s["ticker"].endswith(".KS")]
    stage_kosdaq   = [s for s in stage1_all if s["ticker"].endswith(".KQ")]
    ichi_signals   = [{"ticker": t, "price": _ichi_price.get(t, 0)}
                      for t in _ichi_tickers]

    model_queue = [
        ("stage",    stage_kospi,   _KOSPI_P),
        ("kosdaq",   stage_kosdaq,  _KOSDAQ_P),
        ("cross",    cross_signals, _CROSS_P),
        ("ichimoku", ichi_signals,  _ICHI_P),
    ]

    total_inserted = 0
    seed_base = int(today.strftime("%Y%m%d"))

    for _model, _signals, _params in model_queue:
        if not _signals:
            continue
        _cfg       = MODEL_CONFIG.get(_model, {"max_slots": 10, "position_krw": 10_000_000})
        _open_cnt  = await get_open_slot_count(db_pool, _model)
        _available = _cfg["max_slots"] - _open_cnt
        if _available <= 0:
            logger.info("[paper-sampler] [%s] 슬롯 없음 (%d/%d)", _model, _open_cnt, _cfg["max_slots"])
            continue

        random.seed(seed_base ^ (hash(_model) & 0xFFFFFFFF))
        _selected = random.sample(_signals, min(_available, len(_signals)))

        for _sig in _selected:
            try:
                _pid = await insert_pending(
                    db_pool,
                    model=_model,
                    ticker=_sig["ticker"],
                    signal_date=today,
                    entry_theory=_sig["price"],
                    tp1_pct=_params.get("tp1_pct", 0.15),
                    tp1_ratio=_params.get("tp1_ratio", 0.50),
                    trail_pct=_params.get("trail_pct", 0.10),
                    hard_stop_pct=_params.get("hard_stop_pct", 0.10),
                )
                total_inserted += 1
                logger.info("[paper-sampler] [%s] %s pending (id=%d, theory=%.0f)",
                            _model, _sig["ticker"], _pid, _sig["price"])
            except Exception as _e:
                logger.warning("[paper-sampler] [%s] %s 삽입 실패: %s", _model, _sig["ticker"], _e)

    logger.info("[paper-sampler] 완료 — %d건 pending 삽입", total_inserted)

    if total_inserted > 0:
        try:
            _msg = (
                f"모의투자 EOD 샘플링 ({today})\n"
                f"Stage KOSPI {len(stage_kospi)}건 / KOSDAQ {len(stage_kosdaq)}건 / "
                f"Cross {len(cross_signals)}건 / Ichimoku {len(ichi_signals)}건\n"
                f"→ pending 삽입: {total_inserted}건"
            )
            async with httpx.AsyncClient() as _http:
                await _post_message(_http, _get_token(), _get_chat_id(),
                                    _msg, label="paper-sampler", parse_mode=None)
        except Exception as _e:
            logger.warning("[paper-sampler] 텔레그램 알림 실패: %s", _e)


async def paper_open_entry_job(db_pool, paper_trader) -> None:
    """장 시작(09:00) 후 시가 확정 → pending 포지션 매수주문 제출."""
    today = date.today()
    _loop = asyncio.get_running_loop()

    # 오늘 pending이 없으면 최근 4일 이내 미처리 pending 처리 (주말 포함)
    _pending = []
    _target = today
    for _delta in range(0, 4):
        _target = today - timedelta(days=_delta)
        _pending = await get_pending_positions(db_pool, _target)
        if _pending:
            break
    else:
        logger.info("[paper-entry] pending 없음")
        return

    logger.info("[paper-entry] %d건 pending → 매수주문 시작 (신호일=%s)", len(_pending), _target)

    for _pos in _pending:
        _ticker = _pos["ticker"]
        _model  = _pos["model"]
        _pos_id = _pos["id"]

        # 시가 조회 (ka10001 open_pric) — 종목 간 0.5초 딜레이로 mock API rate limit 방지
        await asyncio.sleep(0.5)
        _open_px = await _loop.run_in_executor(None, paper_trader.get_open_price, _ticker)
        if not _open_px:
            _open_px = await _loop.run_in_executor(None, paper_trader.get_current_price, _ticker)
        if not _open_px:
            logger.warning("[paper-entry] %s 가격 조회 실패 — 스킵", _ticker)
            continue

        _cfg = MODEL_CONFIG.get(_model, {"max_slots": 10, "position_krw": 10_000_000})
        _qty = _qty_from_price(_cfg["position_krw"], _open_px)
        if _qty <= 0:
            logger.warning("[paper-entry] %s qty=0 (price=%d) — 스킵", _ticker, _open_px)
            continue

        # 매수주문 제출
        try:
            _ord_no = await _loop.run_in_executor(
                None, paper_trader.place_buy, _ticker, _qty
            )
        except Exception as _e:
            logger.warning("[paper-entry] %s 매수주문 실패: %s", _ticker, _e)
            continue

        # pending → open
        try:
            await update_to_open(db_pool, _pos_id, float(_open_px), _qty, _ord_no)
            logger.info("[paper-entry] %s %d주 매수 완료 (시가=%d, 주문번호=%s)",
                        _ticker, _qty, _open_px, _ord_no)
        except Exception as _e:
            logger.warning("[paper-entry] %s DB 업데이트 실패: %s", _ticker, _e)

    logger.info("[paper-entry] 완료")
