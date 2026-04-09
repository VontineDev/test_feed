# TODOS

Items deferred from code review and planning sessions.

---

## P3: KOREA_BASE_RATE Staleness Warning

**What:** On scheduler startup, log the current `KOREA_BASE_RATE` value and warn if
it hasn't been updated in 90+ days (infer from `.env` file mtime or git blame on the
env.example entry).

**Why:** KOREA_BASE_RATE is a hardcoded env var that must be updated manually when
the Bank of Korea changes the base rate (up to 8x/year). If nobody updates it, the
LLM receives confidently wrong macro context — e.g., "base rate: 2.5%" when it was
cut to 2.25% two months ago. The LLM won't know it's wrong.

**How to apply:**
```python
# In main() startup in run_scheduler.py
import os, stat
env_mtime = os.stat(".env").st_mtime if os.path.exists(".env") else None
if env_mtime:
    age_days = (time.time() - env_mtime) / 86400
    if age_days > 90:
        logger.warning("KOREA_BASE_RATE may be stale — .env last modified %d days ago. Check BOK rate.", int(age_days))
```

**Pros:** Zero network calls. Catches silent stale macro context. One-time ~5 min fix.
**Cons:** .env mtime isn't reliable if the file is regenerated without a real change.
**Effort:** XS (human: ~30 min / CC: ~5 min)
**Priority:** P3
**Blocked by:** Macro signal enrichment feature must ship first.

---

## P3: Replace Deprecated asyncio.get_event_loop() in cross_analyze Call

**What:** Replace `asyncio.get_event_loop().run_in_executor(...)` with
`asyncio.get_running_loop().run_in_executor(...)` at `run_scheduler.py:393`.

**Why:** `asyncio.get_event_loop()` is deprecated inside a running coroutine in
Python 3.10+ and will raise `DeprecationWarning`. In Python 3.12+ it emits a
`DeprecationWarning` by default; future versions will break. The macro signal
enrichment feature correctly uses `get_running_loop()` — the same fix should be
applied to the existing `cross_analyze` call.

**How to apply:**
```python
# run_scheduler.py:393 — change:
cross = await asyncio.get_event_loop().run_in_executor(
# to:
cross = await asyncio.get_running_loop().run_in_executor(
```

**Pros:** One-line fix. Removes Python version fragility.
**Cons:** None.
**Effort:** XS (human: ~5 min / CC: ~2 min)
**Priority:** P3
**Blocked by:** Nothing.

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
