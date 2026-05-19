# TODOS

Items deferred from code review and planning sessions.

---

## P4: Move compare_tx_amt.py → scripts/compare_tx_amt.py (v0.7.0.0)

**What:** `compare_tx_amt.py` is a dev validation script (Naver 거래대금 vs yfinance Vol×Close 오차 검증). It lives in the project root alongside production modules.

**Why:** Avoids confusion between production code and dev tooling. The root should contain only production-runnable modules.

**How to apply:** `git mv compare_tx_amt.py scripts/compare_tx_amt.py`. Create `scripts/` if it doesn't exist. No imports reference `compare_tx_amit.py` directly.

**Effort:** XS (human: ~2 min / CC: ~1 min)
**Priority:** P4
**Completed:** v0.7.3.0 (2026-05-08) — 파일 삭제됨 (`krx_flow_sync.py` 도입 후 역할 없음)

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
**Completed:** v0.9.3.0 (2026-05-20) — `resolve_fuzzy()` 추가(`ticker_cache.py`); `_parse_signal_json()`에서 exact→fuzzy→miss 순 해석; `_resolution_misses` 연동; `test_resolve_fuzzy.py` (13 tests).

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
**Completed:** v0.9.3.0 (2026-05-20) — `confidence` 필드 추가(`TradeSignal`); 게이팅 버그 수정(`.values()` 기준 교차); `signal.confidence="HIGH"` 상향; Telegram `🔥 HIGH CONFIDENCE` 배지; `test_high_confidence.py` (7 tests).

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
**Completed:** v0.4.2.0 (2026-04-19)

---

## P3: 3-Stage Classifier — Daily Ticker Cap: Start at 150, Expand to 300 After Measurement

**What:** Sprint 2 initial deployment caps `daily_flow` fetch to 150 tickers (not 300). After 2 weeks of production runs, check p50/p99 yfinance fetch latency in logs. If p99 < 0.5s, expand to 300.

**Why:** yfinance throttles at large batch sizes. The 17:00 KST deadline (30-minute job window) is at risk if fetch latency exceeds ~0.3s/ticker with 8 workers. Starting at 150 ensures the deadline holds on launch day. Real data informs expansion — not estimates.

**How to apply:**
- In the Sprint 2 daily job, use: `cap = int(os.environ.get("DAILY_CLASSIFIER_TICKERS", "150"))`
- Log p99 fetch time per daily run: sort `[INFO] [일봉] ... → API 수집` timestamps
- When p99 < 0.5s for 5 consecutive runs: set `DAILY_CLASSIFIER_TICKERS=300` in .env

**Pros:** Safe launch. Real measurement informs scale-up. Env-var configurable at runtime.
**Cons:** Initially misses the 150-300 ticker range. Top Ichimoku candidates by score are still included.
**Effort:** XS (human: ~15 min / CC: ~5 min)
**Priority:** P3
**Blocked by:** Sprint 2 daily job implementation.

---

## P3: 3-Stage Classifier — Tighten News Gating After Sprint 2 Ships

**What:** Post-Sprint 2, change news eligibility rule from:
`ticker in Ichimoku output OR in daily_flow within 7 days`
to:
`ticker in Ichimoku output OR has active stage_classification (Stage 1/2/3) within 7 days`

**Why:** The current Sprint 1 gating is an improvement but still lets through tickers that have daily_flow data but failed all stage conditions (classified as None). True screener-first requires an active stage classification.

**How to apply:**
- In `summary_worker` eligibility check, add DB query: `SELECT 1 FROM stage_classifications WHERE ticker=$1 AND classified_date >= now()-interval '7 days' AND stage IS NOT NULL LIMIT 1`
- Replaces the `daily_flow` 7d check entirely
- Sprint 1 gating remains unchanged until Sprint 2 is in production with 7+ days of stage_classifications data

**Pros:** True screener-first. News only for stocks actively staged.
**Cons:** Requires Sprint 2 populated before tightening is meaningful.
**Effort:** XS (human: ~20 min / CC: ~5 min)
**Priority:** P3
**Blocked by:** Sprint 2 (stage_classifications table populated for ≥ 7 days).

---

## P3: backtest_engine — Non-Standard Measurement Period (on-the-fly N-week)

**What:** When `/backtest stage 8` is called and 8w is not a stored period, compute the actual return on-the-fly from the stored price snapshot instead of falling back to the nearest standard period (4w). Currently the MVP falls back to nearest standard period with a "(closest: 4w)" note.

**Why:** Users may want specific horizons (6w, 8w, 10w) that don't map cleanly to the stored 1/4/13 week checkpoints. The stored OHLCV data is already present in signals2.json — on-the-fly computation is straightforward.

**How to apply:**
- In `backtest_engine.py`, `build_comparison_report(measure_weeks=N)` checks if N is in [1, 4, 13]. If not, for each signal fetch `_nearest_price(stock_lookup, signal_date + timedelta(weeks=N))` and compute the return directly.
- Requires that signals2.json stores the 60-day OHLCV snapshot per ticker, OR re-fetches from yfinance at report time (slower but simpler).
- If re-fetching: cache results in `/tmp/backtest_ohlcv_cache/` keyed by ticker+date to avoid duplicate fetches within one report run.

**Pros:** Flexible measurement horizon for power users. No schema change to signals2.json.
**Cons:** Re-fetching OHLCV adds 5-30 seconds to report generation for non-standard periods. Acceptable if the note explains the delay.
**Effort:** XS (human: ~30 min / CC: ~5 min)
**Priority:** P3
**Blocked by:** ~~Sprint 3 (backtest_engine.py) must ship first.~~ Unblocked (shipped v0.7.0.0). Note: actual implementation uses direct yfinance OHLCV fetch per-run, not signals2.json; re-fetch approach is already in place. Just needs `--hold-weeks N` param added to `BacktestConfig` and `_fill_returns`.
**Completed:** v0.7.3.1 (2026-05-09) — `hold_weeks` added to `BacktestConfig`, `_fill_returns`, `_compute_group_metrics`; custom period shown in both reports.

---

## P3: backtest_engine — Stage 2 Replay (_replay_stage2)

**What:** Add `_replay_stage2()` to `backtest_engine.py`. Currently only Stage 1 is replayed in `stage` mode. Stage 2 walk-forward requires: (1) replaying Stage 1 signals first, (2) looking forward 14 days from each Stage 1 signal for Stage 2 conditions.

Stage 2 conditions to replay:
- Condition 1: `close` in Stage 1 high −5% ~ −20% range (uses `s1_high = close_at_signal_day`)
- Condition 2: `close >= MA20 * 0.95`
- Condition 3: `vol_today / vol_s1_day` in [0.30, 0.60] (거래량 비율)
- Condition 4: `inst_streak >= 0` — not replayable (no historical 수급), skip as with Stage 1 수급

**거래대금 vs 거래량 note:** `compare_tx_amt.py` validated (2026-04-27, 10 tickers) that `Volume × Close` approximates actual 거래대금 with 1.38% mean absolute error, 3.55% max. For the 30~60% ratio check, errors partially cancel. Use volume-based ratio for Condition 3 backtest.

**How to apply:**
- In `backtest_engine.py`, add `_replay_stage2(ticker, name, daily_df, market, config)` that first calls `_replay_stage()` (or inlines Stage 1 check) to find S1 dates, then for each S1 signal date scans the next 14 days for Stage 2 conditions.
- Add `"stage2"` to `BacktestConfig.mode` valid values.
- Add `test_replay_stage2.py` with at least: S1 prerequisite check, each condition independently, 14-day lookback boundary, condition 4 skipped gracefully.
- (Note: `run_backtest.py` was removed — `/backtest stage2` Telegram command is the intended interface)

**Pros:** Validates the full 3-stage classifier pipeline end-to-end. Can measure whether Stage 2 entries outperform raw Stage 1.
**Cons:** Replaying Stage 2 requires Stage 1 history in the same data window — increases memory footprint for long backtests. The 수급 skip means Stage 2 replay is 3/4 conditions, same limitation as Stage 1.
**Effort:** S (human: ~1 day / CC: ~20 min)
**Priority:** P3
**Blocked by:** Nothing. `compare_tx_amt.py` validation complete (2026-04-27).
**Completed:** v0.7.3.2 (2026-05-09) — `_replay_stage2`, `mode="stage2"`, `tests/test_replay_stage2.py` (25 tests).

---

## P3: /watchlist bot command — on-demand watchlist view

**What:** Add `/watchlist` to `telegram_bot.py` as an on-demand command. Calls `_watchlist_brief_job()` logic and sends the result to the requesting chat.

**Why:** The 17:00 KST brief is scheduled, but if you want to check the watchlist at 10:00 or after a news event, you have to wait. On-demand access makes the system interactive.

**Context:** `_watchlist_brief_job()` is module-level and uses `_db_pool` global. The bot command would need to call the core query/format logic without the scheduler context. Best approach: extract a `get_watchlist_entries(pool)` helper from `_watchlist_brief_job` and call it from both the scheduler job and the bot command handler.

**How to apply:**
1. Extract the data assembly logic (Steps 2-7 in `_watchlist_brief_job`) into `async def _build_watchlist_entries(pool)` in `run_scheduler.py` or a new `watchlist.py`.
2. In `telegram_bot.py`, add `/watchlist` handler that calls `_build_watchlist_entries` and sends the brief to the requesting `chat_id`.
3. Register the command in `init_bot()`.

**Pros:** Makes the system interactive — user can check status anytime. Enables manual refresh after a Stage 1 signal fires during the day.
**Cons:** Requires refactoring `_watchlist_brief_job` to extract the data logic into a reusable function. Adds one bot command to maintain.
**Effort:** XS→S (human: ~1h / CC: ~15 min after the refactor)
**Priority:** P3
**Depends on:** Watchlist brief being stable with real `stage_classifications` data (2-3 weeks post-launch).
**Completed:** v0.9.3.0 (2026-05-20) — `_build_watchlist_entries(pool)` 추출; `/watchlist` 핸들러 + 라우팅; `_register_commands()` 등록; `send_watchlist_brief(target_chat_id=)` 파라미터 추가.

---

## P3: Vol ratio delta (∆ vs yesterday) in watchlist brief

**What:** Show "+5%" or "-12%" change vs yesterday's vol ratio in each ticker line of the daily brief.

**Why:** Trend matters more than absolute ratio. A ratio of 0.8 falling from 1.2 is very different from a ratio of 0.8 rising from 0.5. The delta tells you which direction the rally is heading.

**Context:** `watchlist_vol_log` table is now created and populated by `_watchlist_brief_job`. Yesterday's ratio is available via `get_watchlist_vol_log(pool, tickers, lookback=2)`. The delta is `today_ratio - yesterday_ratio`.

**How to apply:**
1. In `_watchlist_brief_job`, load `lookback=2` from `watchlist_vol_log` to get yesterday's ratio.
2. Compute `delta = today_ratio - yesterday_ratio` (None if no prior entry).
3. Pass `vol_ratio_delta` to each entry dict.
4. In `send_watchlist_brief`, format as `+5%▲` or `-12%▼` after the ratio line.

**Pros:** Trend visibility with near-zero extra cost (one query already done). Makes the brief more actionable.
**Cons:** First day of tracking has no delta. Minor display change.
**Effort:** XS (human: ~30 min / CC: ~10 min)
**Priority:** P3
**Depends on:** `watchlist_vol_log` populated for at least 1 trading day (now being built).
**Completed:** v0.9.3.0 (2026-05-20) — `vol_ratio_delta` 계산·전달; `send_watchlist_brief`에서 `+5%▲`/`-12%▼` 포맷 표시.

---

## P3: D+10 retirement notice in watchlist brief

**What:** When a ticker's `days_since == 10` (its last tracking day), add a "[마지막 추적일]" marker in the brief and a closing line summarizing the final status.

**Why:** Currently tickers just disappear from the next brief. A retirement notice closes the loop — user knows the 10-day period ended and can make a final decision on the position.

**Context:** `days_since = (today - s1_date).days` is already computed in `_watchlist_brief_job`. When `days_since >= 10`, add a retirement marker to the entry dict and format it in `send_watchlist_brief`.

**How to apply:**
1. In `entries` assembly, add `"retiring": days_since >= 10`.
2. In `send_watchlist_brief`, when `e.get("retiring")`, append `" [마지막 추적일]"` to the ticker line.
3. Optionally send a separate "퇴장" message for retiring tickers.

**Pros:** User clarity. Prevents confusion about why a ticker disappeared.
**Cons:** `get_stage1_watchlist(days=14)` would still include it for 4 more days after D+10. Need to cap at `days <= 10` or display as "retired."
**Effort:** XS (human: ~20 min / CC: ~5 min)
**Priority:** P3
**Depends on:** Watchlist brief stable with real data.
**Completed:** v0.9.3.0 (2026-05-20) — `"retiring": days_since >= 10`; `send_watchlist_brief`에서 D+10부터 `[마지막 추적일]` 표시.
