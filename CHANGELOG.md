# Changelog

All notable changes to this project will be documented in this file.

## [0.2.2.0] - 2026-04-10

### Added
- Backtest Telegram report now shows a random baseline ("랜덤 기준선: X% (BUY) / Y% (SELL)") so CONFIRM hit rates have context — e.g., CONFIRM=62% against a baseline of 54% shows 8pp of real alpha.
- Scheduler startup logs the current KOREA_BASE_RATE value and warns when the `.env` file hasn't been updated in 90+ days, preventing stale macro context from reaching the LLM silently.
- APScheduler jobs now persist to Postgres via SQLAlchemyJobStore — a crash before Sunday 20:00 no longer silently drops the weekly backtest report. Falls back to MemoryJobStore if Postgres is unreachable at startup.
- Backtest baseline and market_baseline key included in JSON and CSV report exports.

### Changed
- Backfill (`backfill_historical`) now caches 365-day yfinance history per (symbol, ISO week), reducing HTTP calls from 1 per signal×ticker to 1 per unique symbol×week. A 500-signal backfill of 3 tickers goes from ~1500 calls to ~(unique symbol × week combos).
- `requirements.txt`: added `sqlalchemy>=2.0.0`, `psycopg2-binary>=2.9.0`; bumped `APScheduler>=3.10.4`.

### Fixed
- `asyncio.get_event_loop()` → `asyncio.get_running_loop()` in cross_analyze call, removing Python 3.12+ DeprecationWarning.
- Cache no longer stores `None` for failed yfinance fetches — transient failures no longer poison all signals in the same ISO week.
- SQLAlchemyJobStore DSN normalization handles both `postgresql://` and `postgres://` (Heroku/Render) via regex, not fragile string replace.

## [0.2.1.0] - 2026-04-06

### Fixed
- Telegram routing (ISSUE-005): all articles — Korean and foreign — now only send when signal is actionable. Previously Korean articles were forwarded unconditionally regardless of signal result.
- LM Studio health check (ISSUE-001): `/v1/models` replaced with a lightweight inference probe, so an unresponsive LM Studio no longer appears alive and blocks Ollama fallback.
- Qwen3 thinking suppression (ISSUE-002): `_call_ollama_native` now prepends `/no_think\n\n` when `enable_thinking=False`, preventing runaway reasoning tokens from Qwen3 models.

### Added
- `requirements.txt` (ISSUE-003): all Python dependencies with minimum versions for reproducible installs.
- Regression test suite: `test_summarizer_regression_1.py` (covers ISSUE-001/002 health-check and thinking-token fixes), `test_telegram_routing.py` (covers ISSUE-005 signal-gated routing for Korean and foreign categories, 4 scenarios).

### Removed
- Dead `tg_send` import alias from `run_scheduler.py` (ISSUE-006) — `send_article` was unused after routing fix.

## [0.2.0.0] - 2026-04-06

### Added
- Korean-language news feeds: 연합뉴스 (economy, market), 한국경제 (economy, finance), 매일경제 (경제, 증권) — 6 new RSS feeds, all category="korea"
- Site-specific HTML parsers for Korean sources: `_parse_yonhap()`, `_parse_hankyung()`, `_parse_mk()` in `article_fetcher.py`
- Korean articles now get full body extraction (not just RSS summary), enabling richer LLM summaries and better signal detection

### Changed
- `summarizer.py` SUMMARY_PROMPT: "English news" → "financial news" so Korean-source articles are processed correctly by Qwen3
- `yna.co.kr/rss/stock.xml` (404) replaced with `/rss/market.xml` (연합뉴스 마켓+ 최신기사)

## [0.1.0.0] - 2026-04-04

### Added
- Cross-analysis backtesting system: track verdict accuracy (CONFIRM/CAUTION/FILTER/NEUTRAL) at 1h/4h/1d/3d checkpoints using yfinance price data
- `/backtest` Telegram bot command: on-demand formatted report with per-verdict hit rates and per-ticker accuracy breakdown
- Weekly automated backtest report every Sunday 20:00 KST via APScheduler CronTrigger
- `backtest_report_telegram()`: MarkdownV2-safe formatter with 48h data freshness warning
- `test_backtest.py`: 12 pytest unit tests covering `_esc()` escaping and `calculate_metrics()` hit-rate logic (BUY/SELL/FILTER/WATCH branches, empty DB, NaN filtering)
- Volume pattern analysis tool (`volume_pattern.py`) for KR/US stocks
- Daily OHLCV export, DB cache check, and batch analysis scripts (`batch_run.py`)
- Telegram channel broadcasting support

### Changed
- `cross_analyze_historical()` now delegates to `cross_analyze()` directly — eliminates DRY drift where backfill used different WATCH threshold (2% vs 0.5%) and missing conflict logic
- All `time.sleep(0.3)` in async functions replaced with `await asyncio.sleep(0.3)`
- WATCH direction `hit_rate` is now `None` (N/A) instead of incorrect unconditional 100%
- `fetch_pending_outcomes` default limit raised from 100 to 500
- Daily log rotation with 14-day retention

### Fixed
- Summarizer repeated output and unclosed thinking blocks
- Signal detector robustness with retry and thinking token cleanup
- NaN handling in backtest metrics

### Removed
- `batchrun.py` — superseded by `batch_run.py`
