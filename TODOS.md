# TODOS

Items deferred from code review and planning sessions.

---


## P4: Index on price_outcomes(checkpoint) for Backtest Query Performance

**What:** Add `CREATE INDEX IF NOT EXISTS idx_price_outcomes_checkpoint ON price_outcomes(checkpoint, return_pct)` to `init_db()` in `db.py`.

**Why:** `calculate_metrics()` and the new market_baseline query both filter `WHERE po.checkpoint = '1d'`. On a small table this is fine. As `price_outcomes` accumulates months of data (weeks × signals × checkpoints), a full table scan on every `/backtest` command or weekly report will become noticeable.

**How to apply:**
```python
# In db.py init_db() — after the existing CREATE TABLE statements
await conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_price_outcomes_checkpoint
    ON price_outcomes(checkpoint, return_pct)
    WHERE return_pct IS NOT NULL
""")
```

**Pros:** Future-proof. One SQL line. `CREATE INDEX IF NOT EXISTS` is idempotent.
**Cons:** Premature optimization — no benefit until the table has 10k+ rows.
**Effort:** XS (human: ~10 min / CC: ~2 min)
**Priority:** P4
**Blocked by:** Nothing. Add after several months of accumulated backtest data confirm the query is slow.

---

## P2: Ticker Resolution Diagnostics (Phase 1 of approved design)

**What:** Add `_resolution_misses: Counter` to `market_data.py` and `get_resolution_miss_report(top_n=10)`. Log WARNING at step 5 when a ticker fully fails resolution (with fuzzy-miss guard). Call `get_resolution_miss_report(10)` at end of `collect_job()` in `run_scheduler.py`.

**Why:** We can't tune fuzzy matching without knowing which ticker names actually miss. Run for 1–2 weeks, collect the top misses, then calibrate threshold. Approved in design doc: `~/.gstack/projects/VontineDev-test_feed/Jin-feat-krx-listings-db-design-20260414-183901.md`.

**Priority:** P2
**Deferred from:** `feat/krx-listings-db` (v0.3.0.0)

---

## P2: resolve_fuzzy() in TickerCache (Phase 2 of approved design)

**What:** Add `resolve_fuzzy(name, threshold=0.82)` to `ticker_cache.py` using `difflib.SequenceMatcher`. Insert as step 3.5 in `market_data.get_price_context()` between the exact-cache lookup and the static YFINANCE_MAP. Change step 4 `elif` → `if`.

**Why:** Exact-name misses on lesser-known KRX stocks (e.g., LLM writes "셀트리온헬스케어" vs KRX "셀트리온헬스케어(주)"). Threshold 0.82 validated: 현대차 vs 현대차증권 = 0.75 < 0.82 (no false positive). Implement AFTER Phase 1 diagnostics produce real miss data.

**Priority:** P2
**Deferred from:** `feat/krx-listings-db` (v0.3.0.0)
**Blocked by:** Phase 1 diagnostics (need 1–2 weeks of real miss data to calibrate)

---

## Completed

- `/backtest` command, `backtest_report_telegram()`, weekly Sunday report, DRY fix for `cross_analyze_historical()`, `await asyncio.sleep()`, WATCH hit_rate=None, data quality log, `fetch_pending_outcomes` limit 500, `test_backtest.py` (12 tests) **Completed:** v0.1.0.0 (2026-04-04)
- ISSUE-005 Telegram routing: all articles (Korean + foreign) gated behind `signal.is_actionable`; dead `tg_send` import removed (ISSUE-006); `test_telegram_routing.py` (4 regression tests) **Completed:** v0.2.1.0 (2026-04-06)
- ISSUE-001 LM Studio health check inference probe; ISSUE-002 Qwen3 `/no_think` prefix in `_call_ollama_native`; ISSUE-003 `requirements.txt`; ISSUE-004 stale comment in `signal_detector.py:104`; `test_summarizer_regression_1.py` regression tests **Completed:** v0.2.1.0 (2026-04-06)
- P3 backlog clean sweep: asyncio fix, KOREA_BASE_RATE staleness warning, market baseline in calculate_metrics(), APScheduler SQLAlchemyJobStore persistence, dict cache with isocalendar() in backfill_historical(); `test_backtest.py` expanded to 20 tests **Completed:** 2026-04-10
