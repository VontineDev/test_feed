# Changelog

All notable changes to this project will be documented in this file.

## [0.2.7.0] - 2026-04-16

### Added

- **Screener v2: condition G — 120-week MA filter** (`chart_screener.py`): added condition G (`close > 120wMA`) to the weekly breakout screen. `fetch_weekly_ohlcv` now fetches 3 years of data (`period="3y"`) to ensure enough bars for a true 120wMA. NaN-safe: tickers with < 100 weeks of data pass automatically (`min_periods=100`). `ScreenResult` gains `ma_120w: Optional[float]` field (`None` when insufficient data).
- **Screener v2: KIND sector data** (`chart_screener.py`): `fetch_kind_sector_map()` fetches KOSPI + KOSDAQ sector (업종) mappings from KIND (한국거래소 기업공시시스템). Returns `{종목코드: 업종}` dict. Graceful on failure — returns `{}` so the screener continues without sector data. `ScreenResult` gains `sector: str` field (empty on KIND failure).
- **Screener v2: sector-grouped Telegram output** (`telegram_notify.py`): `send_weekly_screener()` now groups results by sector (top-5 sectors by count × top-3 stocks each). Sector name truncated at 20 chars. Stocks with no sector assigned fall into "기타". Shows KIND failure warning when all results have empty sector.
- **DB migration — `sector` and `ma_120w` columns** (`db.py`): `chart_signals` table gains `sector VARCHAR(80)` and `ma_120w FLOAT` columns. `init_db()` runs idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` on startup. `save_chart_signals()` and `load_chart_signals_latest()` updated accordingly.
- **Tests: `TestFetchKindSectorMap`** (`test_chart_screener.py`): 3 tests covering happy-path HTML parsing, exception handling (KIND down), and empty HTML.
- **Tests: `TestConditionG`** (`test_chart_screener.py`): 3 tests covering G pass (close > 120wMA), G fail (close < 120wMA), and NaN-pass (< 100 bars of data).
- **Tests: `TestKINDFailureWarning`** (`test_screener_telegram_regression_1.py`): 2 tests verifying KIND warning appears when all sectors are empty, and absent when sectors are populated.
- **Tests: `TestSectorGroupedFormat`** (`test_screener_telegram_regression_1.py`): 4 tests verifying sector grouping order, 기타 fallback, 20-char truncation, and top-3-per-sector limit.

## [0.2.6.0] - 2026-04-16

### Added

- **Weekly chart screener** (`chart_screener.py`): scans all KOSPI/KOSDAQ stocks (~2,770) every Sunday at 20:30 KST using Ichimoku + 20/60-week MA conditions (6-condition filter). Results sent to Telegram DM and channel simultaneously. `has_gapjum` flag (20wMA > 60wMA) marks highest-conviction candidates.
- **`/screener` Telegram command** (`telegram_bot.py`): on-demand access to the latest weekly screening results — loads from DB, no re-scan needed. Sends to both DM and channel.
- **`chart_signals` DB table** (`db.py`): stores screener output per ticker per ISO week. `save_chart_signals()` upserts results; `load_chart_signals_latest()` fetches the most recent week's full result set.
- **DM + channel simultaneous screener delivery** (`telegram_notify.py`): `send_weekly_screener()` now always sends to personal DM, and also to the channel when `TELEGRAM_CHANNEL_ID` is set. Previously it was either/or.

### Fixed

- **Ollama model name mismatch** (`summarizer.py`): default `OLLAMA_MODEL` was `Qwen3.5-9B:latest` (uppercase, hyphen) but Ollama's actual model tag is `qwen3.5:9b`. All `/api/chat` calls returned 404. Changed default to `qwen3.5:9b`.
- **`/help` MarkdownV2 parse error** (`telegram_bot.py`): `<` and `>` in `/volume <종목명|티커>` were unescaped, causing Telegram to reject the message with "Character '>' is reserved". Escaped to `\<` and `\>`.

## [0.2.5.0] - 2026-04-16

### Added

- **Article type classification** (`signal_detector.py`): every LLM signal inference call now extracts `article_type` as part of the same JSON response — zero additional API calls. Eight types supported: `earnings`, `ma`, `management`, `analyst`, `regulatory`, `product`, `macro`, `other`.
- **Telegram type badges** (`telegram_notify.py`): actionable signal alerts now show a type emoji badge (📊 earnings, 🤝 M&A, 🔍 analyst, ⚖️ regulatory, 🚀 product, 🌐 macro, 👤 management). Badge is suppressed for `other` to avoid noise.
- **Backtest type breakdown** (`backtest.py`): `/backtest` Telegram report now shows per-article-type hit rates at the 1d checkpoint. Types with fewer than 5 signals are suppressed until enough data accumulates.
- **DB migration** (`db.py`): `ALTER TABLE trade_signals ADD COLUMN IF NOT EXISTS article_type VARCHAR(20) DEFAULT 'other'` — idempotent, safe on existing deployments.

### Fixed

- **`send_signal()` esc() missing backtick** (`telegram_notify.py`): local `esc()` in `send_signal` was missing `` ` `` from the escape list, inconsistent with `send_weekly_screener`. Fixes potential MarkdownV2 rendering breakage for tickers or reasons containing backticks.
- **WATCH signals inflated article type hit rates** (`backtest.py`): `_fetch_type_breakdown` included WATCH signals in `COUNT(*)` but WATCH has no directional hit definition. This deflated hit rates for all article types. Filtered to `BUY/SELL` only, matching the existing `_fetch_ticker_breakdown` pattern.
- **`start_crawler.bat` hardcoded user path** (`start_crawler.bat`): paths were hardcoded to `C:\Users\Jin\test_feed` — won't work on any other machine. Replaced with `%~dp0` (relative to the .bat file location).

## [0.2.4.0] - 2026-04-11

### Fixed

- **Ollama empty JSON responses** (`summarizer.py`): `repeat_penalty: 1.3` penalised `{` `"` `:` tokens that already appeared in the *generated output*, making the model unable to produce multi-key JSON. Reset to `1.0` (disabled). Affects both summarisation and signal detection calls.
- **Summariser always empty after thinking block** (`summarizer.py`): `max_tokens=300` was exhausted by Qwen3's ~290-token `<think>` block, leaving no room for the Korean summary. Raised to `600`. Summary failures drop from ~50/day to near zero.
- **Signal detector echoed macro context instead of JSON** (`signal_detector.py`): `SIGNAL_PROMPT` ended with `{macro_section}` — the last text the model saw was "Note: High USD/KRW..." so it continued/echoed that instead of outputting JSON. Added explicit output instruction after the macro section. Fixes `JSON 없음 | raw: Macro context:` parse failures.
- **Silent Ollama model-not-found errors** (`summarizer.py`): Some Ollama versions return HTTP 200 with `{"error": "model not found"}` instead of 4xx. Added `data.get("error")` check before the empty-content path so the error is logged clearly instead of masquerading as two empty-response retries.
- **Misleading empty-response error when Ollama returns thinking content** (`summarizer.py`): When Ollama separates `message.thinking` from `message.content`, an empty content field now logs `think=Nchar` so operators can tune `max_tokens` or verify `think:false` is working.

### Changed

- Removed explicit `frequency_penalty: 0` / `presence_penalty: 0` from `_call_openai_compat` payload — these are defaults and caused `400 Bad Request` on strict OpenAI-compat backends.

### Tests

- Regression tests for all five fixed failure modes (ISSUE-003 through ISSUE-007) in `test_summarizer_regression_1.py` and new `test_signal_prompt.py`.

## [0.2.3.0] - 2026-04-10

### Security

- **[HIGH] Removed hardcoded default PostgreSQL password** (`db.py`): `DB_PASSWORD` now has no fallback — `get_dsn()` raises `RuntimeError` at startup if neither `DB_PASSWORD` nor `DATABASE_URL` is set. Prevents silent use of well-known credentials on misconfigured deployments.
- **[HIGH] Fixed Telegram auth bypass** (`telegram_bot.py`): `_get_allowed_ids()` now raises `RuntimeError` when both `ALLOWED_CHAT_IDS` and `TELEGRAM_CHAT_ID` are unset, instead of returning `set()` which caused the `if allowed and ...` guard to short-circuit and allow all users.
- **[MEDIUM] Removed default PostgreSQL password from `docker-compose.yml`**: `POSTGRES_PASSWORD: ${DB_PASSWORD:-news1234}` → `${DB_PASSWORD}` (no fallback). Container startup now fails explicitly if `DB_PASSWORD` is not in `.env`.
- **[MEDIUM] Bound PostgreSQL port to loopback** (`docker-compose.yml`): `"5432:5432"` → `"127.0.0.1:5432:5432"` — prevents remote access to the DB port on servers without an application-layer firewall.
- **[MEDIUM] Dockerfile runs as non-root**: Added `appuser` system account and `USER appuser` directive — container process no longer runs as root.

### Added

- `.env.example`: documents all required environment variables with placeholder values to prevent misconfiguration.

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
