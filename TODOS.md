# TODOS

Items deferred from code review and planning sessions.

---

## P4: Rename test_feeds.py → scripts/check_feeds.py (QA-2026-04-18)

**What:** `test_feeds.py` is a standalone RSS feed connectivity script, not a pytest test file. Rename to `scripts/check_feeds.py` (or just `check_feeds.py`) and update any references.

**Why:** The `test_` prefix causes pytest to attempt collection (0 tests found — no harm, but misleading). Companion issue to ISSUE-QA-001 (fixed). Low urgency since there are no side effects.

**Effort:** XS (human: ~2 min / CC: ~1 min)
**Priority:** P4
**Completed:** v0.4.2.0 (2026-04-19)

---

## P3: HTML Screener Report — Sparklines

**What:** Add close-price sparklines (mini bar or line charts) per stock row in the HTML screener report. Each row would show a 12-week price trend alongside the existing columns.

**Why:** The screener design UI kit (`ui_kits/screener/index.html`) includes sparklines. They let you visually distinguish a recent breakout from a stale one at a glance, without opening the ticker in a charting app.

**How to apply:**
- Add `close_history: list[float] = field(default_factory=list)` to `ScreenResult` (after `ma_120w`)
- In `screen_ticker()`, pass the last 12 Close values from `df["Close"].dropna().tail(12).tolist()` into the constructor
- In `generate_html_report.py`, render as inline SVG `<polyline>` (no JS dependency) scaled to the cell height
- Tests: add a test that `close_history` is populated when rows ≥ 12, and that empty history renders as empty cell not crash

**Pros:** Matches design intent. Zero new API calls (data already fetched in `fetch_weekly_ohlcv`). SVG is inline — no JS or chart lib dependency.
**Cons:** Changes `ScreenResult` dataclass interface — requires updating all constructors in tests (but CC makes this fast). Adds ~12 floats per stock to the in-memory result set (negligible).
**Effort:** XS-S (human: ~3h / CC: ~20 min)
**Priority:** P3
**Blocked by:** ~~HTML report v1 (generate_html_report.py) must be implemented first~~ — unblocked (shipped v0.4.1.0)
**Completed:** v0.4.2.0 (2026-04-19)

---

## P3: HTML Screener Report — Sector-Grouped View

**What:** Add an alternative sector-grouped view to the HTML screener report, either as a second `<section>` at the bottom or as a toggle. v1 uses 정배열/일반 grouping; this adds grouping by KIND 업종명.

**Why:** Sector rotation is one of the core use cases for the screener. Seeing "반도체 4종목, 바이오 3종목, 에너지 2종목" in one glance shows whether the breakout is broad-based or concentrated. The existing `ScreenResult.sector` field is already populated — no new fetches.

**How to apply:**
- In `generate_html_report.py`, add `_group_by_sector(results)` that returns `dict[str, list[ScreenResult]]` sorted by sector name
- Render as a second section `<h2>업종별</h2>` below the 정배열/일반 table
- Stocks with `sector=""` go under "기타"
- Tests: add test that sector grouping collapses stocks correctly and "기타" bucket catches empty sectors

**Pros:** No data cost. Uses `ScreenResult.sector` already in hand. Adds real analytical value.
**Cons:** Longer HTML output (two tables instead of one). Sector names from KIND can be verbose and inconsistent.
**Effort:** XS (human: ~1h / CC: ~10 min)
**Priority:** P3
**Blocked by:** ~~HTML report v1 (generate_html_report.py) must be implemented first~~ — unblocked (shipped v0.4.1.0)
**Completed:** v0.4.2.0 (2026-04-19)

---

## P3: Screener v2 — Condition G NaN Calibration (after first W17 run)

**What:** After the first Sunday run with v2 (2026-W17 or later), check what fraction of passed stocks have `ma_120w IS NULL` (i.e., passed via the NaN-safe fallback, not a real 120wMA comparison):

```sql
SELECT
    COUNT(*) FILTER (WHERE ma_120w IS NULL) AS null_count,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE ma_120w IS NULL) / COUNT(*), 1) AS null_pct
FROM chart_signals
WHERE week_of = '2026-W17';
```

**Decision rule:**
- If `null_pct > 20%`: tighten condition G — NaN → G **fails** (require full data). Change `G = (not ma_120w_valid) or (close > ma_120w)` to `G = ma_120w_valid and (close > ma_120w)` in `screen_ticker()`.
- If `null_pct ≤ 20%`: current NaN-pass behavior is acceptable. No change.

**Why:** Stocks with < 100 weeks of data (recently listed, data gaps) pass condition G automatically. If these dominate the results, the 120wMA filter has no real effect on them. The 20% threshold ensures the filter is doing real work on at least 80% of passed stocks.

**How to apply:** Run the SQL after the first v2 screener run. Update `screen_ticker()` in `chart_screener.py` if threshold is breached. Re-run screener manually to confirm count change.

**Pros:** Closes the product hypothesis loop for condition G calibration.
**Cons:** Requires one full run of data before the decision can be made.
**Effort:** XS (human: ~10 min / CC: ~5 min)
**Priority:** P3
**Blocked by:** First successful v2 screener run.

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
**Completed:** v0.4.2.0 (2026-04-19)

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
- ISSUE-001 (QA) Screener Telegram formatter over-escaped tickers in code spans (`005930.KS` → `005930\\.KS`); ISSUE-002 local `esc()` missing backtick; ISSUE-003 `test_db_dsn` isolation failure (load_dotenv restoring DB_PASSWORD during reload). All 3 fixed. `test_screener_telegram_regression_1.py` (8 regression tests). **Completed:** /qa 2026-04-16
- Article type classification: `article_type` field on `TradeSignal` + `SIGNAL_PROMPT` + `_parse_signal_json()` + DB migration + `save_signal()` + `fetch_latest_signals()` + `run_scheduler.py` call site + Telegram type badges + backtest type breakdown; `test_article_type.py` (17 tests) + 2 backtest tests. **Completed:** v0.2.5.0 (2026-04-16); **QA:** ISSUE-001 (WATCH inflating type breakdown denominator) fixed + 1 regression test (2026-04-16)
- ISSUE-QA-001 `test_screener_cmd.py` missing `__main__` guard — `asyncio.run(main())` at module level connected to production DB and sent 499-ticker screener results to Telegram on every `pytest` run. Fixed by adding guard. `test_screener_cmd_regression_1.py` (1 regression test). **Completed:** /qa 2026-04-18

## P2: HIGH CONFIDENCE Integration (v2 — after screener validation)

**What:** Cross-signal confirmation: when a stock appears in `chart_signals` this week AND triggers a news signal, flag the Telegram alert as HIGH CONFIDENCE. Requires: `confidence: str = "NORMAL"` field added to `TradeSignal` (last field in dataclass), `get_chart_signals_this_week(_db_pool)` called once per `collect_job()` cycle, intersection check `set(signal.ticker_symbols.values()) & chart_candidates` in `summary_worker()`, distinct Telegram format in `telegram_notify.py`.

**Why:** Validate screener output manually first (2-3 weeks). If weekly breakout stocks don't show visible momentum, the integration adds noise without signal. This is the riskiest assumption in the screener design.

**How to apply:** After 2-3 weeks of manual review of the Sunday Telegram screener output, if the stocks are showing real momentum: implement the integration. Files: `signal_detector.py`, `run_scheduler.py`, `telegram_notify.py`.

**Pros:** Reduces false positives in news signals. Startup-differentiating feature. The actual product hypothesis.
**Cons:** Adds DB read per collect_job() cycle (fast, indexed). Requires manual validation period.
**Effort:** S (human: ~1 day / CC: ~20 min)
**Priority:** P2
**Blocked by:** Screener running for 2-3 weeks. Manual review of output.

---

## P3: Enhanced Ichimoku Conditions (v2 — after first Sunday run)

**What:** Add G/H/I conditions to `screen_ticker()` — conversion line > base line (전환선 > 기준선), both rising. Add J (ma_20w > ma_60w, 정배열) for ranking. Show "Enhanced" badge in Telegram weekly summary.

**Why:** The basic 6-condition screen may produce too many or too few candidates on the first real run. Calibrate count first, then add conditions to tighten quality.

**How to apply:** After first Sunday run, check `SELECT COUNT(*) FROM chart_signals WHERE week_of = current_week`. If > 50 candidates, add G+H+I to reduce noise. If < 5, consider loosening conditions instead.

**Pros:** Higher quality breakout signals. The `is_enhanced` column already exists in DB (zero migration cost).
**Cons:** Reduces candidate count — may over-filter in thin markets.
**Effort:** S (human: ~4h / CC: ~15 min)
**Priority:** P3
**Blocked by:** First real Sunday screener run.

---

## P3: Backtest Chart Signal Accuracy (30+ days after launch)

**What:** Add `backtest_chart_signals()` to `backtest.py`. Pull `chart_signals` from last 4 weeks. Join with `price_outcomes` on ticker + date range. Compute hit rate (positive return at 1wk, 4wk checkpoints) vs KOSPI baseline. Report: does the Ichimoku breakout filter actually identify outperformers?

**Why:** The product hypothesis (chart breakout = higher upside probability) is unvalidated. This is the quantitative validation. Without it, you're shipping a filter based on faith in Ichimoku theory.

**How to apply:** After 30+ Sundays of screener runs (need enough samples). Extend the existing `weekly_backtest_report` job or add a separate command. The `price_outcomes` table already tracks future prices for trade_signals — extend it to also track `chart_signals` tickers.

**Pros:** Evidence-based decision to keep, tune, or drop the screener. Closes the product hypothesis loop.
**Cons:** Needs 30+ data points (6+ months). Requires extending price_outcomes tracking to chart_signals tickers.
**Effort:** M (human: ~2 days / CC: ~30 min)
**Priority:** P3
**Blocked by:** price_outcomes tracking extended to chart_signals tickers; 30+ weeks of screener data.

---

## P2: USER_MANUAL.md — Ollama/LLM Install Section Depth

**What:** When writing USER_MANUAL.md section 3 (설치 가이드), give Ollama installation
especially detailed treatment: model download step (`ollama pull qwen3.5:9b` — 4-8GB,
can take 10-30 min on slow connections), port configuration, and a dedicated
troubleshooting subsection for LLM failures (model not found, port in use,
download interrupted, out of disk space).

**Why:** Outside-voice review of the user manual design identified Ollama model setup
as the highest-abandonment step in the install flow. A Korean VPS user downloading
a 7B model on limited bandwidth with no progress feedback will give up or misdiagnose
failure. Getting this section right determines whether a stranger successfully completes
the install.

**How to apply:** When Claude Code writes USER_MANUAL.md, instruct it to treat the
Ollama section as a first-class install guide (not a one-liner) and add these
troubleshooting entries to section 10: `ollama list` shows no models, `ollama serve`
port 11434 already in use, model download interrupted (resume with same pull command).

**Pros:** Reduces the most common first-time install failure. Directly improves
Success Criterion 1 (stranger installs without asking a question).
**Cons:** Adds ~1-2 pages to the manual. Minor length increase.
**Effort:** S (human: ~30 min / CC: ~5 min)
**Priority:** P2
**Blocked by:** USER_MANUAL.md writing session started.
