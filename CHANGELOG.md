# Changelog

All notable changes to this project will be documented in this file.

## [0.5.0.0] - 2026-04-25

### Added
- **Screener-first architecture** (`run_scheduler.py`): news signals are now gated by the weekly screener. If a ticker hasn't passed Stage 1 screening this week, its news signals are suppressed. `_screener_tickers: set[str]` is a module-level cache warmed from DB at startup and refreshed after every Sunday screener job.
- **Stage 2 filter presets** (`screener_filters.py`): four named filters — `저평가`, `성장`, `배당`, `가격건전성` — that apply quantitative conditions (PER, PBR, EPS, dividend yield, volume spike) to `ScreenResult` lists. Used by the HTML report and future Telegram segmentation.
- **Backtest engine** (`chart_backtest.py`): `BacktestSignal` dataclass tracks 1w/4w/13w returns and KOSPI excess returns per screener signal. `incremental_update()` fills pending return fields using yfinance weekly OHLCV (no look-ahead via `_check_at_row`). `build_summary()` produces monthly + annual metrics (win rate, avg return, median, excess return). Runs automatically after each Sunday screener job.
- **Backtest HTML report** (`generate_backtest_html.py`): renders the backtest summary as a dark-mode HTML file written to `BACKTEST_DIR/chart_backtest_latest.html`. Shows annual and monthly breakdowns for 전체/정배열/일반 groups with sparkline trend.
- **Foreign/institutional flow columns** in HTML screener report (`generate_html_report.py`): `_flow_badge()` renders green/red badges for `foreign_net_buy` and `inst_net_buy` fields. Data shows `—` until Sprint 2 wires the Naver flow API.
- **`daily_flow` DB table** (`db.py`): stores daily foreign/institutional net-buy data per ticker (`ticker`, `trade_date`, `foreign_net`, `inst_net`, `foreign_streak`, `inst_streak`). `save_daily_flow()` upserts with ON CONFLICT. Sprint 2 will wire the scheduler.
- **`high_w` / `volume_w` in `chart_signals`** (`db.py`, `chart_screener.py`): weekly high and volume captured from yfinance OHLCV and persisted alongside each screener result. Required for Stage 2 spike-volume and S1-high conditions.
- **`fetch_daily_flow()`** (`market_data.py`): queries Naver Finance mobile `investorTrendDays` endpoint for foreign/institutional net-buy data. Synchronous (for `run_in_executor`). Handles null values from the API gracefully.
- **`target_chat_id` param** in `send_weekly_screener()` (`telegram_notify.py`): when set, sends only to that chat ID and skips channel broadcast. `/screener` bot command now passes the invoker's `chat_id` — prevents channel broadcasts triggered by individual users.
- **`SCREENER_WORKERS=8`** documented in `.env.example` for the daily 16:30 KST classifier job latency budget.

### Changed
- **`calc_rsi()` renamed from `_calc_rsi()`** (`market_data.py`): now a public function for reuse in `screener_filters.py` and future stage classifier modules.
- **`save_chart_signals()` extended to 14 positional params** (`db.py`): `$13=high_w`, `$14=volume_w` added with ON CONFLICT upsert.

### Tests
- `test_chart_backtest.py`: 31 tests — no look-ahead slice guard, return calculations, excess return, nearest-price lookup, `_group_metrics` (win rate, avg, median, None exclusion), `compute_metrics` (전체/정배열/일반), `build_summary` (monthly grouping, annual totals), JSON round-trip, `_week_label`.
- `test_screener_filters.py`: Stage 2 filter preset validation.
- `test_news_gating.py`: 5 tests — non-screener signal suppressed, screener signal allowed, empty cache disables gating, partial overlap allows, non-actionable still blocked.
- `test_screener_telegram_regression_1.py`: ISSUE-001/002 regression (ticker code span, backtick escaping).
- `test_screener_telegram_regression_2.py`: ISSUE-003 regression (`target_chat_id` channel broadcast fix).
- `test_chart_screener.py`: 2 new tests for `save_chart_signals` `$13`/`$14` params and backward compatibility.

## [0.4.2.0] - 2026-04-19

### Added
- **Sparklines in HTML screener report** (`generate_html_report.py`): each stock row now shows a 12-week close-price trend as an inline SVG sparkline. Lets you distinguish a fresh breakout from a months-old stale one at a glance, without opening a chart app. No JS or external dependencies — pure SVG `<polyline>`.
- **Sector-grouped view** (`generate_html_report.py`): a second `업종별` section now appears below the 정배열/일반 tables, grouping results by KIND 업종명. Shows at a glance whether a breakout week is broad-based or concentrated in one sector. Uses the `ScreenResult.sector` field already populated — zero new API calls.
- **Ticker resolution diagnostics** (`market_data.py`): a module-level `Counter` now tracks every ticker name that fully fails resolution (all 5 steps). `get_resolution_miss_report(top_n=10)` returns the top misses as a human-readable string. Called at the end of each `collect_job()` cycle — resolution gaps now surface as WARNING log lines in production.

### Changed
- **Step 5 resolution log level** (`market_data.py`): full ticker-miss logging upgraded from `DEBUG` to `WARNING` so it surfaces in production logs without requiring verbose mode.
- **`test_feeds.py` renamed to `scripts/check_feeds.py`**: the RSS connectivity checker is not a pytest test file. Moved to `scripts/` to stop pytest from attempting collection (0 tests found, misleading).

## [0.4.1.0] - 2026-04-19

### Added
- **HTML screener report** (`generate_html_report.py`): every Sunday screener run now writes a self-contained HTML file to `reports/screener/screener_YYYYMMDD_HHMM.html`. You get a clean two-section table (★ 정배열 / 일반), sorted by close price, with 120주선 displayed as `—` when data is insufficient (< 100 bars) — matching the footnote that explains the NaN-pass rule. Sectors default to `기타` when empty. HTML is generated as a pure function (`generate_html(results)`) so it's easy to test and easy to run standalone from the CLI.
- **Design bundle support** (`generate_html_report.py`): the generator loads `web/screener_design/colors_and_type.css` and inlines fonts as base64 when the bundle is present. Falls back to system fonts with a WARNING log if the bundle is missing — no hard dependency.
- **Print styles** (`generate_html_report.py`): the generated HTML includes `@media print` styles (white background, collapsed table borders) so you can print or save as PDF directly from the browser.
- **Scheduler integration** (`run_scheduler.py`): HTML generation runs automatically after every Sunday screener job. Failure is isolated — a broken HTML write does not affect the existing Telegram delivery.

### Tests
- `test_generate_html_report.py`: 10 tests — empty state, ★ 정배열 section rendering, 일반 section absent when all stocks are 정배열, `ma_120w=None` → `—` (not "None"), HTML escaping on stock names, no external stylesheet links, `sector=""` → "기타", close-price sort order, `lang="ko"` attribute, footer footnote.

## [0.4.0.0] - 2026-04-18

### Added
- **Article type classification** (`signal_detector.py`, `db.py`, `telegram_notify.py`): `TradeSignal` now carries an `article_type` field (EARNINGS / MACRO / TECHNICAL / GUIDANCE / SECTOR / OTHER). `SIGNAL_PROMPT` instructs the LLM to classify each article. Type is persisted in `trade_signals.article_type` (DB column added idempotently). Signals include a `TYPE_BADGE` emoji in Telegram messages (🔴 earnings, 🌐 macro, 📊 technical, etc.).
- **Per-article-type hit rate breakdown** (`backtest.py`, `generate_report.py`): backtest report now includes a breakdown of signal accuracy by article type. Identifies which categories produce the highest-conviction signals.
- **Weekly chart screener** (`chart_screener.py`): scans all KOSPI/KOSDAQ stocks (~2,770) every Sunday at 20:30 KST using Ichimoku + 20/60-week MA conditions (6-condition filter). Results stored in `chart_signals` DB table and delivered to Telegram DM + channel simultaneously.
- **`/screener` Telegram command** (`telegram_bot.py`): on-demand access to the latest weekly screening results from DB — no re-scan required.
- **Screener v2: 120-week MA filter** (`chart_screener.py`): `close > 120wMA` required (condition G). Stocks with < 100 weeks of data pass automatically. `ScreenResult` gains `ma_120w` field.
- **Screener v2: KIND sector grouping** (`chart_screener.py`, `telegram_notify.py`): each screened ticker carries its KRX sector from KIND. Sunday messages show top 5 sectors with top 3 stocks per sector.
- **DB migrations** (`db.py`): `chart_signals.sector` (VARCHAR 80), `chart_signals.ma_120w` (FLOAT), `trade_signals.article_type` (VARCHAR 40) — all added idempotently via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.
- **Signal Cross-Analysis v2: MACD + Bollinger Bands + MA trend layer** (`market_data.py`): `cross_analyze()` now factors in MACD histogram direction, MACD cross (bullish/bearish), Bollinger %B position, and price vs MA20/MA50 alignment. Zero extra API calls — all indicators computed from the existing 1-year daily Close series.
- **Richer Telegram signal messages** (`telegram_notify.py`): each price line now shows price change, RSI, MACD direction (▲▲/▼▼ for crosses), Bollinger %B, and MA20/MA50 position.
- **Fundamental layer: PER/PBR/EPS enrichment** (`market_data.py`): `cross_analyze()` factors in valuation fundamentals sourced from Naver Finance mobile API (no credentials required). `PriceContext` gains `per`, `pbr`, `eps` fields. Fundamental score adjusts signal score by −3 to +2. Startup pre-warms all Korean tickers via 5-worker thread pool.
- **Fundamental display in Telegram** (`telegram_notify.py`): price lines show PER/PBR tokens when noteworthy (`PER:12↓`, `PER:80↑`, `적자`, `PBR:0.6↓`). Neutral companies show no tokens.

### Fixed
- **WATCH signal noise** (`signal_detector.py`): `is_actionable` now requires strength ≥ 3 for WATCH signals (was ≥ 2), reducing false positives from ambiguous news.
- **`_fetch_type_breakdown` SQL** (`backtest.py`): WATCH signals excluded from article type breakdown query — WATCH is a hold signal, not a trading action, and inflated breakdown counts.
- **Ollama model name mismatch** (`summarizer.py`): default `OLLAMA_MODEL` changed from `Qwen3.5-9B:latest` to `qwen3.5:9b` — previous value caused 404 on all `/api/chat` calls.
- **`/help` MarkdownV2 parse error** (`telegram_bot.py`): `<` and `>` in `/volume <종목명|티커>` escaped to `\<` and `\>`.

### Tests (242 total)
- `test_article_type.py`: 19 tests — 17 unit tests for type classification logic, 2 backtest integration tests.
- `test_chart_screener.py`: 31 tests — KIND sector fetch, condition G (120wMA), screener pipeline.
- `test_fundamental.py`: 41 tests — `_parse_naver_value`, `_fundamental_score`, `_fetch_fundamental`, `cross_analyze` average delta, Telegram display.
- `test_macro_signal.py`: 10 new tests — MACD/BB/MA scoring (Group 8), yfinance TA computation (Group 9).
- `test_signal_prompt.py`: 7 tests — WATCH ≥ 3 threshold.
- `test_screener_telegram_regression_1.py`: 6 regression tests — KIND failure warning, sector-grouped format.
- `test_screener_cmd.py`: 4 tests — `/screener` command handler.
- `test_backtest.py`: updated with article type breakdown tests.

## [0.3.0.0] - 2026-04-14

### Added
- **KRX full listing DB** (`krx_sync.py`): fetches all KOSPI/KOSDAQ securities (2,500+ tickers) from KRX public API and upserts into `krx_listings` PostgreSQL table. Table is created automatically on first run. EUC-KR response encoding handled explicitly. KONEX and unsupported market types excluded.
- **Ticker cache singleton** (`ticker_cache.py`): in-memory `TickerCache` loaded at startup and refreshed daily at 20:00 KST. Resolves Korean stock names (including full-length names that exceed KRX's 10-character display limit) and 6-digit short codes to yfinance symbols. Safe to call before `load()` — returns `None` and falls through to static maps.
- **Automatic daily sync** (`run_scheduler.py`): APScheduler job at 20:00 KST (`Asia/Seoul`) syncs `krx_listings` and reloads the cache after market close. Startup sequence runs initial sync before the first article job fires, with `try/finally` so cache reloads from existing DB rows even if the KRX API is down.
- **Regression tests** (`test_krx_sync.py`, `test_ticker_cache_integration.py`): 38 tests covering field parsing, KONEX exclusion, ISU_ABBRV vs ISU_NM name resolution, atomic cache reload, EUC-KR decoding, empty-response error handling, clock skew guard, all-KONEX silent-wipe guard, and real `get_price_context()` integration.
- **Index on `krx_listings.updated_at`** (`db.py`): ensures the daily delisted-stock DELETE is a fast index scan, not a full table scan.

### Fixed
- **Clock skew race condition** (`krx_sync.py`): `sync_start_ts` is now fetched via `SELECT NOW()` from inside the transaction instead of Python's clock. If Python's clock was even 1 ms ahead of Postgres, the DELETE at the end of the transaction would silently delete all just-upserted rows.
- **Silent full-table wipe on all-KONEX API response** (`krx_sync.py`): if all rows in the API response were KONEX or unsupported markets, `executemany()` was a no-op and the subsequent DELETE wiped the entire table. Now raises `ValueError` before the transaction opens.

### Changed
- `market_data.py` `get_price_context()`: `ticker_cache.resolve()` queried before static `YFINANCE_MAP` fallback. Articles mentioning tickers not in the hardcoded map now receive full price context, RSI, and volume signals.
- `volume_pattern.py` `resolve_ticker()`: `ticker_cache.resolve()` queried before `KR_KOSDAQ`/`KR_KOSPI` static dicts. Return type unchanged — still returns `(yfinance_ticker, display_name, market_code)` tuple.
## [0.2.9.0] - 2026-04-18

### Added

- **Fundamental layer: PER/PBR/EPS enrichment** (`market_data.py`): `cross_analyze()` now factors in valuation fundamentals. Data sourced from Naver Finance mobile API (no credentials required; replaces pykrx which requires KRX login under Python 3.14). `PriceContext` gains `per`, `pbr`, `eps` fields. Fundamental score adjusts the signal score by −3 to +2: loss-making tickers (EPS < 0) penalized −2, high PER (>50) or high PBR (>5) penalized −1 each, cheap tickers (PER < 15 or PBR < 1) rewarded +1 each. Multi-ticker score uses average delta (not sum) to prevent one bad ticker from dominating.
- **Fundamental display in Telegram** (`telegram_notify.py`): price lines now show PER/PBR tokens when noteworthy — `PER:12↓` for cheap, `PER:80↑` for expensive, `적자` for loss-making, `PBR:0.6↓` or `PBR:7.0↑` at extremes. Neutral companies show no fundamental tokens, keeping the price line clean for normal cases.
- **Startup pre-warm** (`run_scheduler.py`): all Korean tickers in `YFINANCE_MAP` are pre-warmed via `prewarm_fundamentals()` at startup using a 5-worker thread pool. Avoids 150ms cold-miss latency on first signal of the day.
- **Daily cache** (`market_data.py`): fundamental results cached by date key (`_fund_cache`), reset on process restart. Cache hits cost ~0ms; misses cost ~150ms (Naver API). Thread-safe via `dict.setdefault()`.

### For contributors

- `test_fundamental.py` (41 tests): `_parse_naver_value` (10 edge cases including Korean unit suffixes), `_to_krx_code` (4 cases), `_fundamental_score` (11 cases including boundary values per==15/50, pbr==1/5, eps==0), `_fetch_fundamental` (6 cases with mocked httpx: cache hit, HTTPX_OK=False, success, loss-maker, PER>200 cap, API exception), `cross_analyze` average delta (3 cases), Telegram per_str/pbr_str display (7 cases).

## [0.2.8.0] - 2026-04-17

### Added

- **Signal Cross-Analysis v2: MACD + Bollinger Bands + MA trend layer** (`market_data.py`): `cross_analyze()` now factors in five new technical indicators per ticker — MACD histogram direction, MACD cross (bullish/bearish), Bollinger %B position, and price vs MA20/MA50 alignment. Zero extra API calls: all indicators computed from the same 1-year daily Close series already fetched for RSI. `PriceContext` gains five new Optional fields (`macd_hist`, `macd_cross`, `bb_pct`, `above_ma20`, `above_ma50`) with `None` defaults for full backward compatibility.
- **Richer Telegram signal messages** (`telegram_notify.py`): each price line in a signal alert now shows up to five data points — price change, RSI, MACD direction (▲/▼/▲▲/▼▼ for crosses), Bollinger %B, and MA20/MA50 position (↑↓). All indicators are skipped gracefully if data is unavailable (illiquid tickers, short history).
- **WATCH noise reduction** (`signal_detector.py`): `is_actionable` now requires strength ≥ 3 for WATCH signals (previously ≥ 2), reducing false positives from ambiguous news.

### For contributors

- `TestCrossAnalyzeMACDBBMA` (Group 8, `test_macro_signal.py`): 8 unit tests covering BUY + bullish_cross, BUY + overbought BB (regression guard), BUY + below both MAs, BUY + oversold BB, SELL + bearish_cross, SELL + above both MAs, all-None fields (no scoring change), Telegram price line format.
- `TestFetchYfinanceTAComputation` (Group 9, `test_macro_signal.py`): 2 integration tests — sufficient data (60 bars) produces valid float indicators; insufficient data (10 bars) returns `bb_pct=None` from NaN guard.
- `TestIsActionableThreshold` (`test_signal_prompt.py`): 7 tests for the WATCH ≥ 3 threshold.

## [0.2.7.0] - 2026-04-17

### Added

- **Screener v2: 120-week MA filter (condition G)** (`chart_screener.py`): the weekly breakout screen now requires close price above the 120-week moving average. Stocks with less than 100 weeks of data pass automatically so recently-listed tickers aren't excluded. `ScreenResult` gains a `ma_120w` field showing the computed average (or `None` when data is insufficient). OHLCV fetch extended to 3 years (`period="3y"`) to supply enough bars.
- **Screener v2: KIND sector data** (`chart_screener.py`): each screened ticker now carries its exchange sector (업종) from KIND (한국거래소 기업공시시스템). Fetched at scan time, gracefully skipped on network failure — the screener still runs, just without sector labels.
- **Screener v2: sector-grouped Telegram output** (`telegram_notify.py`): Sunday screener messages now show the top 5 sectors by candidate count, with the top 3 stocks per sector. Previously a flat list of 20 stocks. Sector names longer than 20 characters are truncated. Shows a warning when KIND data was unavailable.
- **DB migration — `sector` and `ma_120w`** (`db.py`): `chart_signals` table gains two columns. `init_db()` adds them idempotently on startup so existing deployments upgrade automatically.

### For contributors

- `TestFetchKindSectorMap` (`test_chart_screener.py`): 3 tests — happy-path HTML parse, KIND down (exception), empty HTML (no `<tr>`).
- `TestConditionG` (`test_chart_screener.py`): 3 tests — G passes (close > 120wMA), G fails (close < 120wMA), NaN-pass (< 100 bars).
- `TestKINDFailureWarning` (`test_screener_telegram_regression_1.py`): 2 tests — warning shown when all sectors empty, absent when sectors populated.
- `TestSectorGroupedFormat` (`test_screener_telegram_regression_1.py`): 4 tests — sector order, 기타 fallback, 20-char truncation, top-3 limit.

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
