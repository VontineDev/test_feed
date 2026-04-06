# TODOS

Items deferred from code review and planning sessions.

---

## P3: APScheduler Job Persistence for Weekly Backtest Report

**What:** Add Postgres-backed job store (`SQLAlchemyJobStore`) to APScheduler in
`run_scheduler.py` so scheduled jobs survive process restarts.

**Why:** Currently, if the process crashes before Sunday 20:00, the weekly backtest
report is silently skipped. The `/backtest` command serves as a manual fallback, so
this isn't a blocker — but it's worth having for reliability.

**How to apply:**
```python
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
scheduler = AsyncIOScheduler(
    jobstores={'default': SQLAlchemyJobStore(url=get_dsn())}
)
```

**Pros:** Missed jobs auto-retry after restart. Weekly report is reliable.
**Cons:** Adds SQLAlchemy dependency (if not already present).
**Effort:** S (human: ~2h / CC: ~10 min)
**Priority:** P3
**Blocked by:** Nothing. But do this after the /backtest command and weekly report are shipped.

---

## P3: yfinance Batching in Backfill

**What:** Use `yf.download([sym1, sym2, sym3], ...)` in `backfill_historical()` instead
of per-symbol calls inside the loop.

**Why:** Current backfill does 1 HTTP call per ticker per signal (e.g. 500 signals × 3
tickers = 1500 calls). `yf.download()` batches multiple tickers in one call. `batch_run.py`
already uses this pattern.

**Pros:** Backfill completes in minutes instead of 30+ minutes for large datasets.
**Cons:** Requires restructuring the backfill loop to group by date range, not by signal.
**Effort:** M (human: ~4h / CC: ~20 min)
**Priority:** P3
**Blocked by:** Not blocking — backfill is a one-time operation. Do this if backfill takes
too long with real data.

---

## P3: Backtest Baseline Hit Rate

**What:** Add a "random baseline" stat to `calculate_metrics()` in `backtest.py` — what %
of all directional trade signals went in the predicted direction regardless of verdict.
Show as `"market_baseline": {"BUY": 0.54, "SELL": 0.48}` in the metrics dict and as
"랜덤 기준선: 54% (BUY) / 48% (SELL)" in the Telegram report.

**Why:** Without a baseline, CONFIRM=62% looks impressive but might just be market drift.
With a baseline of 54%, CONFIRM adds 8pp of alpha — meaningful. Without it, 62% is a
number with no context.

**How to apply:**
```sql
SELECT direction,
       COUNT(*) FILTER (WHERE return_pct > 0) AS up_count,
       COUNT(*) AS total
FROM price_outcomes po
JOIN cross_analysis_prices cap ON cap.id = po.cross_price_id
JOIN cross_analysis_results car ON car.id = cap.cross_id
JOIN trade_signals s ON s.id = car.signal_id
WHERE po.checkpoint = '1d' AND po.return_pct IS NOT NULL
GROUP BY direction
```

**Pros:** Makes the backtest scientifically defensible. Adds 1 SQL query, ~5 lines in formatter.
**Cons:** Needs 2-4 weeks of accumulated data to be meaningful.
**Effort:** S (human: ~2h / CC: ~10 min)
**Priority:** P3
**Blocked by:** Needs enough signal data first. Do this after the first weekly report fires.

---

## Completed

- `/backtest` command, `backtest_report_telegram()`, weekly Sunday report, DRY fix for `cross_analyze_historical()`, `await asyncio.sleep()`, WATCH hit_rate=None, data quality log, `fetch_pending_outcomes` limit 500, `test_backtest.py` (12 tests) **Completed:** v0.1.0.0 (2026-04-04)
- ISSUE-005 Telegram routing: all articles (Korean + foreign) gated behind `signal.is_actionable`; dead `tg_send` import removed (ISSUE-006); `test_telegram_routing.py` (4 regression tests) **Completed:** v0.2.1.0 (2026-04-06)
- ISSUE-001 LM Studio health check inference probe; ISSUE-002 Qwen3 `/no_think` prefix in `_call_ollama_native`; ISSUE-003 `requirements.txt`; ISSUE-004 stale comment in `signal_detector.py:104`; `test_summarizer_regression_1.py` regression tests **Completed:** v0.2.1.0 (2026-04-06)
