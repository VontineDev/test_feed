"""백테스트 엔진 본체 (backtest_engine.py에서 이동, Phase C).

run_backtest 오케스트레이터와 compose 모드 러너.
"""

from __future__ import annotations

import logging
from dataclasses import replace as _dc_replace
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from analysis.backtest.config import OPTIMAL_EXIT_PARAMS_CROSS
from analysis.backtest.models import (
    BacktestConfig,
    BacktestResult,
    GroupMetrics,
    SignalRecord,
)
from analysis.backtest.helpers import (
    _build_price_lookup,
    _compute_group_metrics,
    _entry_on_or_after,
    _fill_returns,
)
from analysis.backtest.fetch import (
    _batch_fetch_ohlcv,
    _fetch_index,
    _fetch_single_ohlcv,
)
from analysis.backtest.replay import (
    _apply_cross_filter,
    _replay_ichimoku,
    _replay_stage,
    _replay_stage2,
    _replay_stage2_v11,
    _replay_stage2_v12,
    _replay_stage2_v13,
    _replay_stage_v11,
    _replay_stage_v12,
    _replay_stage_v13,
    _replay_stage_v14,
    _replay_stage_v15,
)
from analysis.backtest.exit_models import _compute_sell_signals_and_s2

logger = logging.getLogger(__name__)
_KST = ZoneInfo("Asia/Seoul")

def _run_compose(config: BacktestConfig) -> BacktestResult:
    """compose 모드 — strategy_compose 합성 신호를 SignalRecord로 변환 후
    기존 forward-return/청산/지표/HTML 머신을 재사용한다.

    replay(_replay_*)를 우회한다: 신호는 백필된 precompute 테이블 JOIN에서 온다
    (plan-eng-review JOIN+백필 아키텍처). 효율을 위해 신호가 있는 티커의
    OHLCV만 수집한다(전종목 X).

    데이터 흐름:
        strategy_compose.load_signal_frame → derive_flags → STRATEGIES[s].run()
            → [(ticker, ISO주)]
        ISO주 금요일 이후 첫 거래일에 진입(SignalRecord, mode="compose")
            → _fill_returns / _compute_sell_signals_and_s2 / _compute_group_metrics
    """
    from analysis import strategy_compose as sc
    from analysis.chart_screener import get_all_tickers, fetch_kind_sector_map

    spec = sc.STRATEGIES.get(config.strategy)
    if spec is None:
        raise ValueError(
            f"알 수 없는 전략: {config.strategy!r} (가능: {sorted(sc.STRATEGIES)})"
        )

    logger.info(
        "[compose] 전략=%s 소스=%s 기간=%s~%s 시장=%s",
        config.strategy, spec.sources, config.start, config.end, config.market,
    )

    def _empty(note: str) -> BacktestResult:
        return BacktestResult(
            config=config, signals=[], overall=GroupMetrics(),
            computed_at=datetime.now(_KST).isoformat(), note=note,
        )

    # 1. 합성 신호 (ticker, ISO주)
    frame = sc.load_signal_frame(config.dsn, config.start, config.end, spec.sources)
    if frame.empty:
        return _empty(f"compose {config.strategy}: 소스 데이터 0건")
    frame = sc.derive_flags(frame)
    sig_df = spec.run(frame)
    if sig_df.empty:
        return _empty(f"compose {config.strategy}: 합성 신호 0건")

    # 2. 시장·기간 필터 → (ticker, 금요일) 진입 후보
    sector_map  = fetch_kind_sector_map()
    all_tickers = get_all_tickers(sector_map=sector_map if sector_map else None)
    meta: dict[str, tuple[str, str]] = {t: (n, s) for t, n, s in all_tickers}

    entries: list[tuple[str, date]] = []
    for row in sig_df.itertuples(index=False):
        ticker = row.ticker
        friday = sc.week_to_friday(row.week)
        if not (config.start <= friday <= config.end):
            continue
        mkt = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"
        if config.market != "ALL" and mkt != config.market:
            continue
        entries.append((ticker, friday))
    if not entries:
        return _empty(f"compose {config.strategy}: 기간/시장 필터 후 신호 0건")

    needed = sorted({t for t, _ in entries})
    logger.info("[compose] 합성 신호 %d건 · 대상 티커 %d개", len(entries), len(needed))

    # 3. OHLCV (신호 티커만) + KOSPI 벤치마크
    fetch_start = config.start - timedelta(days=760)
    hold_buffer = (config.hold_weeks * 7 + 14) if config.hold_weeks else 0
    fetch_end   = config.end + timedelta(days=max(105, hold_buffer))

    from core.ohlcv_cache import batch_fetch_cached, fetch_index_cached
    ticker_pairs = [(t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI") for t in needed]
    ohlcv_map = batch_fetch_cached(
        ticker_pairs, fetch_start, fetch_end, config.workers, config.dsn, _fetch_single_ohlcv,
    )
    kospi_df     = fetch_index_cached("^KS11", "IDX", fetch_start, fetch_end, config.dsn, _fetch_index)
    kospi_lookup = _build_price_lookup(kospi_df) if kospi_df is not None else {}

    # 4. 수급 (청산 로직 S3 조건 공용)
    flow_lookup: Optional[dict] = None
    try:
        from core.ohlcv_cache import load_flow_data
        flow_lookup = load_flow_data(config.dsn, needed, config.start, fetch_end)
    except Exception as e:
        logger.warning("[compose] 수급 데이터 로드 실패: %s", e)

    # 5. SignalRecord 생성 — 금요일 이후 첫 거래일에 진입
    price_lookup_cache: dict[str, dict[date, float]] = {}
    seen: set[tuple[str, date]] = set()
    signals: list[SignalRecord] = []
    for ticker, friday in entries:
        df = ohlcv_map.get(ticker)
        if df is None or df.empty:
            continue
        plook = price_lookup_cache.get(ticker)
        if plook is None:
            plook = _build_price_lookup(df)
            price_lookup_cache[ticker] = plook
        entry = _entry_on_or_after(plook, friday)
        if entry is None:
            continue
        edate, eclose = entry
        if eclose <= 0 or (ticker, edate) in seen:
            continue
        seen.add((ticker, edate))
        mkt  = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"
        name = meta.get(ticker, (ticker, ""))[0]
        signals.append(SignalRecord(
            ticker=ticker, name=name, signal_date=edate,
            close_at_signal=eclose, mode="compose", market=mkt,
        ))
    if not signals:
        return _empty(f"compose {config.strategy}: 진입가 산정 후 신호 0건")

    # 6. 청산 파라미터 — 미설정 시 CROSS 최적값 적용 (조합도 교차 성격)
    cfg = config
    if config.tp1_pct == 0 and config.trail_pct == 0:
        cfg = _dc_replace(config, **OPTIMAL_EXIT_PARAMS_CROSS)
        logger.info("[compose] 기본 분할청산 파라미터 적용 (CROSS 최적값)")

    # 7. 수익률
    for sig in signals:
        plook = price_lookup_cache.get(sig.ticker)
        if plook is not None:
            _fill_returns(sig, plook, kospi_lookup, cfg.tx_cost_rt, cfg.hold_weeks)

    # 8. 업종 주입
    for sig in signals:
        sig.sector = meta.get(sig.ticker, ("", ""))[1]

    # 9. 매도 신호·MDD (S2/S3는 compose 모드에 비적용 — stage 전용)
    _compute_sell_signals_and_s2(
        signals, ohlcv_map, cfg.tx_cost_rt,
        stop_loss_pct=cfg.hard_stop_pct, flow_lookup=flow_lookup, cfg=cfg,
        stage3_peakout_map=None,
    )

    # 10. 정렬 + 집계
    signals.sort(key=lambda s: s.signal_date)
    overall = _compute_group_metrics(signals, cfg.rf_rate_annual, cfg.hold_weeks)

    note = f"compose {config.strategy} — 신호 {len(signals)}건"
    if flow_lookup is None:
        note += " | 수급 미적용(DSN/데이터 없음)"
    logger.info("[compose] 완료 — 신호:%d 승률28d:%s", overall.n,
                f"{overall.win_rate_28d*100:.1f}%" if overall.win_rate_28d is not None else "N/A")
    return BacktestResult(
        config=config, signals=signals, overall=overall,
        computed_at=datetime.now(_KST).isoformat(), note=note,
    )


def run_backtest(config: BacktestConfig) -> BacktestResult:
    """백테스트 메인 함수. CLI 및 Telegram 봇에서 동기 호출."""
    if config.mode == "compose":
        return _run_compose(config)

    from analysis.chart_screener import get_all_tickers

    logger.info(
        "[백테스트] 모드=%s 기간=%s~%s 시장=%s 최대티커=%s",
        config.mode, config.start, config.end, config.market, config.max_tickers or "전종목",
    )

    # 1. 업종 매핑 + 티커 목록
    from analysis.chart_screener import fetch_kind_sector_map
    sector_map  = fetch_kind_sector_map()
    all_tickers = get_all_tickers(sector_map=sector_map if sector_map else None)

    # 외부 API 타임아웃 등으로 목록이 비면 DB daily_flow에서 직접 로드
    if not all_tickers and config.dsn:
        try:
            from core.db_sync import connect
            conn = connect(config.dsn)
            cur  = conn.cursor()
            cur.execute("SELECT DISTINCT ticker FROM daily_flow ORDER BY ticker")
            rows = cur.fetchall()
            conn.close()
            all_tickers = [(r[0], r[0].split(".")[0], "") for r in rows]
            logger.info("[백테스트] DB fallback 티커 %d개 로드", len(all_tickers))
        except Exception as _e:
            logger.warning("[백테스트] DB fallback 실패: %s", _e)

    if config.market == "KOSPI":
        tickers = [(t, n, s) for t, n, s in all_tickers if t.endswith(".KS")]
    elif config.market == "KOSDAQ":
        tickers = [(t, n, s) for t, n, s in all_tickers if t.endswith(".KQ")]
    else:
        tickers = all_tickers

    if config.max_tickers > 0:
        tickers = tickers[:config.max_tickers]

    logger.info("[백테스트] 대상 티커 %d개", len(tickers))

    # 2. 데이터 수집 범위
    #   전방: MA120w(주봉 120주=840일) + 여유 → 2년(760일) lookback
    #   후방: 최대 보유 91일 + 여유 14일 (hold_weeks 지정 시 그 기간으로 확장)
    fetch_start = config.start - timedelta(days=760)
    hold_buffer = (config.hold_weeks * 7 + 14) if config.hold_weeks else 0
    fetch_end   = config.end + timedelta(days=max(105, hold_buffer))

    # 3. OHLCV 병렬 수집
    if config.dsn:
        from core.ohlcv_cache import batch_fetch_cached, fetch_index_cached
        ticker_pairs = [
            (t, "KOSDAQ" if t.endswith(".KQ") else "KOSPI")
            for t, _, _ in tickers
        ]
        ohlcv_map = batch_fetch_cached(
            ticker_pairs, fetch_start, fetch_end,
            config.workers, config.dsn, _fetch_single_ohlcv,
        )
        kospi_df = fetch_index_cached(
            "^KS11", "IDX", fetch_start, fetch_end,
            config.dsn, _fetch_index,
        )
    else:
        ticker_syms = [t for t, _, _ in tickers]
        ohlcv_map   = _batch_fetch_ohlcv(ticker_syms, fetch_start, fetch_end, config.workers)
        kospi_df    = _fetch_index("^KS11", fetch_start, fetch_end)

    # 4. KOSPI 벤치마크 조회
    kospi_lookup = _build_price_lookup(kospi_df) if kospi_df is not None else {}

    # 5. 수급 데이터 사전 로드 (DSN 있을 때 전 모드 공통 — S1 조건 5 + S3 조건 5 공용)
    #    S3 감지는 신호일 이후 최대 91일까지 스캔 → fetch_end까지 로드
    flow_lookup: Optional[dict] = None
    streak_lookup: Optional[dict] = None
    if config.dsn:
        try:
            from core.ohlcv_cache import load_flow_data, load_flow_streaks
            ticker_syms = [t for t, _, _ in tickers]
            flow_lookup = load_flow_data(config.dsn, ticker_syms, config.start, fetch_end)
            logger.info("[백테스트] 수급 데이터 로드: %d건", len(flow_lookup))
            if config.mode in ("stage_v12", "stage2_v12", "stage_v13", "stage2_v13", "stage_v14", "stage_v15"):
                streak_lookup = load_flow_streaks(config.dsn, ticker_syms, config.start, fetch_end)
                logger.info("[백테스트] streak 데이터 로드: %d건", len(streak_lookup))
        except Exception as e:
            logger.warning("[백테스트] 수급 데이터 로드 실패 (조건 5 생략): %s", e)

    shares_lookup: Optional[dict] = None
    if config.mode in ("stage_v12", "stage_v13", "stage_v14", "stage_v15") and config.dsn:
        try:
            from core.ohlcv_cache import load_listed_shares
            shares_lookup = load_listed_shares(config.dsn)
            logger.info("[백테스트] 상장주식수 로드: %d종목", len(shares_lookup))
        except Exception as e:
            logger.warning("[백테스트] 상장주식수 로드 실패 (조건 9 생략): %s", e)

    # 6. 신호 재현
    all_signals: list[SignalRecord] = []

    for ticker, name, _ in tickers:
        df = ohlcv_map.get(ticker)
        if df is None or df.empty:
            continue
        mkt = "KOSDAQ" if ticker.endswith(".KQ") else "KOSPI"

        if config.mode in ("ichimoku", "cross"):
            all_signals.extend(_replay_ichimoku(ticker, name, df, mkt, config))
        if config.mode in ("stage", "cross"):
            all_signals.extend(_replay_stage(ticker, name, df, mkt, config, flow_lookup))
        if config.mode == "stage2":
            all_signals.extend(_replay_stage2(ticker, name, df, mkt, config))
        if config.mode == "stage_v11":
            all_signals.extend(_replay_stage_v11(ticker, name, df, mkt, config, flow_lookup))
        if config.mode == "stage2_v11":
            all_signals.extend(_replay_stage2_v11(ticker, name, df, mkt, config))
        if config.mode == "stage_v12":
            all_signals.extend(_replay_stage_v12(ticker, name, df, mkt, config, flow_lookup, streak_lookup, shares_lookup))
        if config.mode == "stage2_v12":
            all_signals.extend(_replay_stage2_v12(ticker, name, df, mkt, config))
        if config.mode == "stage_v13":
            all_signals.extend(_replay_stage_v13(ticker, name, df, mkt, config, flow_lookup, streak_lookup, shares_lookup))
        if config.mode == "stage2_v13":
            all_signals.extend(_replay_stage2_v13(ticker, name, df, mkt, config, flow_lookup))
        if config.mode == "stage_v14":
            all_signals.extend(_replay_stage_v14(ticker, name, df, mkt, config, flow_lookup, streak_lookup, shares_lookup))
        if config.mode == "stage_v15":
            all_signals.extend(_replay_stage_v15(ticker, name, df, mkt, config, flow_lookup, streak_lookup, shares_lookup))

    # 7. Cross 필터
    if config.mode == "cross":
        all_signals = _apply_cross_filter(all_signals)

    # 8. 수익률 계산
    stock_lookup_cache: dict[str, dict[date, float]] = {}
    for sig in all_signals:
        df = ohlcv_map.get(sig.ticker)
        if df is None:
            continue
        if sig.ticker not in stock_lookup_cache:
            stock_lookup_cache[sig.ticker] = _build_price_lookup(df)
        _fill_returns(sig, stock_lookup_cache[sig.ticker], kospi_lookup, config.tx_cost_rt, config.hold_weeks)

    # 8.5. 업종 정보 주입
    ticker_sector: dict[str, str] = {t: s for t, _n, s in tickers}
    for sig in all_signals:
        sig.sector = ticker_sector.get(sig.ticker, "")

    # 8.6. 매도 신호·MDD·S2/S3 진행일 계산
    # Stage3 peakout map: 분할 청산 모드(tp1>0 or trail>0)에서만 DB 조회
    stage3_peakout_map: Optional[dict] = None
    if config.dsn and config.use_stage3_peak and (config.tp1_pct > 0 or config.trail_pct > 0):
        try:
            import asyncio as _asyncio
            from core.db import get_stage3_peakout_map as _get_peakout
            ticker_syms = [t for t, _, _ in tickers]
            stage3_peakout_map = _asyncio.run(
                _get_peakout(None, ticker_syms, config.start, fetch_end, dsn=config.dsn)
            )
        except Exception as _pe:
            logger.warning("[백테스트] Stage3 peakout 조회 실패 (use_stage3_peak 무시): %s", _pe)

    _compute_sell_signals_and_s2(
        all_signals, ohlcv_map, config.tx_cost_rt,
        stop_loss_pct=config.hard_stop_pct,
        flow_lookup=flow_lookup,
        cfg=config,
        stage3_peakout_map=stage3_peakout_map,
        streak_lookup=streak_lookup,
    )

    # 9. 날짜 순 정렬 → MDD equity curve가 시간 순서대로 누적
    all_signals.sort(key=lambda s: s.signal_date)

    # 10. 집계 지표
    overall = _compute_group_metrics(all_signals, config.rf_rate_annual, config.hold_weeks)

    note = ""
    if flow_lookup is not None:
        notes = []
        if config.mode in ("stage", "cross"):
            notes.append(f"S1 조건 5(외·기관 순매수 OR) 적용")
        notes.append(f"S3 조건 5(외·기관 동시 순매수 AND) 적용 — {len(flow_lookup)}건 기준")
        note = " | ".join(notes)
    else:
        if config.mode in ("stage", "cross"):
            note = "S1·S3 수급 조건 제외 — daily_flow 없음 (DSN 미설정)"
        else:
            note = "S3 수급 조건 제외 — daily_flow 없음 (DSN 미설정)"

    logger.info(
        "[백테스트] 완료 — 신호:%d 승률28d:%s",
        overall.n,
        f"{overall.win_rate_28d * 100:.1f}%" if overall.win_rate_28d is not None else "N/A",
    )

    return BacktestResult(
        config=config,
        signals=all_signals,
        overall=overall,
        computed_at=datetime.now(_KST).isoformat(),
        note=note,
    )
