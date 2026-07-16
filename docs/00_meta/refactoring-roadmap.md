# 리팩토링 로드맵 (완료 기록 + 향후 계획)

구조 리팩토링의 완료 기록과 향후 계획의 단일 기준점. 개별 백로그 항목의 상세 필드(Effort/Depends on 등)는 [TODOS.md](TODOS.md)가 관리하고, 이 문서는 전체 그림과 우선순위·방법론을 담는다.

---

## 원칙 (core/db.py 분리에서 확립한 방법론)

1. **순수 코드 이동(pure code motion)** — 함수 본문은 byte-identical하게 옮긴다. 동작 개선·헬퍼 도입은 별도 커밋/작업으로 분리한다.
2. **facade re-export** — 옛 모듈은 새 모듈에서 이름을 re-import해 기존 `from X import Y` 경로와 `mock.patch("X.Y")` 도트 경로를 그대로 보존한다. 호출부 수정 0건이 목표.
3. **커밋마다 게이트** — 전체 pytest green(기존 실패 베이스라인 대비 신규 실패 0) + 공개 표면 diff(`vars(module)` 스냅샷 비교) 무변화.
4. **진입점 불변** — Task Scheduler/배치가 실행하는 파일명·argparse 인터페이스는 바꾸지 않는다.
5. **검증 도구** — AST 기반 함수 본문 비교, import 스모크(`python -c "import ..."`), `git diff --color-moved=dimmed-zebra`, pyright.

---

## 완료 기록

| 날짜 | 작업 | 규모 | 커밋 |
|---|---|---|---|
| 2026-07-15 | Phase A~C: core/dates·tor·db_sync·env 신설, backtest_engine(3,360줄) → analysis/backtest/ 8모듈 | L | (TODOS.md P3 항목 참조) |
| 2026-07-16 | 대시보드 백엔드 라우터 분리: main.py → routers_* 10개 + market_snap 데이터 계층 | L | `b591f19`~`54d5001` (7커밋) |
| 2026-07-16 | core/db.py 도메인 분리 (아래 상세) | M | `96bfb3c`~`7b31706` (5커밋) |

### 2026-07-16 core/db.py 도메인 분리 상세

1,689줄 단일 파일 → 인프라 facade(514줄) + 도메인 모듈 6개. 33개 함수 본문 전부 byte-identical 이동, 호출부 수정 0건.

| 모듈 | 담당 테이블 | 줄수 |
|---|---|---|
| `core/db.py` (facade) | get_dsn/create_pool/init_db + 전체 DDL 상수 + RLS 목록 + re-export | 514 |
| `core/db_news.py` | news_articles | 124 |
| `core/db_signals.py` | trade_signals | 119 |
| `core/db_market.py` | ticker_names, intraday_volumes, daily_ohlcv, daily_flow | 303 |
| `core/db_screener.py` | chart_signals | 120 |
| `core/db_stage.py` | stage_classifications, watchlist_vol_log, sector_daily_stats | 383 |
| `core/db_trades.py` | trade_log | 248 |

**facade가 보존하는 하위호환 제약 4종:**
1. `mock.patch("core.db.<name>")` 도트 경로 (get_dsn, create_pool, init_db, save_chart_signals, get_prev_streak, load_chart_signals_latest 등 테스트 6곳)
2. `reload(core.db)` (test_db_dsn.py) — get_dsn·load_dotenv 블록이 db.py에 남아있어야 함
3. private `_CREATE_TRADE_LOG` import (test_trade_integration.py) — DDL 상수는 db.py에 유지
4. `import core.db as db` 속성 접근 (data/market_data.py, data/krx_flow_sync.py, analysis/volume_pattern.py)

**검증 결과:** pytest 826 passed(베이스라인 동일, 기존 실패 4건은 test_scan_cmd.py — 아래 Phase D 참조) / `vars(core.db)` 표면 diff 무변화(미사용 stdlib import 3개 제거만) / AST 비교 33함수 byte-identical / import 스모크 전 소비자 통과 / pyright 신규 오류 0.

**의도적 보류:** `_CREATE_TABLE` DDL 메가블록 + `init_db` 분리 — 멀티스테이트먼트 DDL이 단일 implicit transaction으로 실행되는 원자성을 보존하기 위해 이번 범위에서 제외. → Phase D 항목.

---

## 향후 로드맵

우선순위·리스크 순으로 Phase D→G. 각 Phase 내 항목은 독립적이라 개별 진행 가능.

### Phase D — 저위험 정리 (워밍업)

| 항목 | What | 제약 | Effort |
|---|---|---|---|
| test_scan_cmd.py 정리 | 제거된 `_handle_scan`을 import하는 dead test 4건(현재 스위트의 유일한 실패). 삭제 또는 `_handle_screener`(telegram_bot.py, 동일 `_scan_lock` 가드)로 재작성 | 없음 | XS |
| jobs/ 보일러플레이트 dedupe | stage_backfill·screener_backfill의 동일한 `get_pool()`, CLI 잡 3개(stage_backfill/screener_backfill/ohlcv_warm)의 `sys.path.insert + load_dotenv + logging.basicConfig` 삼중복 → `jobs/_common.py` | 표준 CLI 인터페이스 유지 | S |
| core/db.py 후속: DDL 분리 | `_CREATE_TABLE` 등 DDL 상수 + init_db → `core/db_schema.py`, db.py facade에서 re-export 유지 | `_CREATE_TRADE_LOG` import 경로, init_db 런타임 호출(run_scheduler main/_run_once_*), DDL 단일 트랜잭션 원자성 | S |
| Phase A~C 심(shim) 삭제 | TODOS.md P3 항목 그대로 — `_last_trading_day` 등 별칭, `analysis/backtest_engine.py` 79줄 심 | 스케줄러 ~1주 정상 가동 후, grep으로 잔여 importer 0 확인 | S |

### Phase E — 대시보드 백엔드 마무리

| 항목 | What | 제약 | Effort |
|---|---|---|---|
| routers_portfolio.py 분리 | 3개 도메인 혼재 해소: 포트폴리오 CRUD + `/api/ticker/lookup` + `/api/dart/summary/{ticker}` → 뒤 2개를 별도 라우터로 | 없음 (라우터 등록만 main.py에서 갱신) | M |
| 가격 조회 로직 통합 | portfolio 자체 `_get_current_prices`(aftermarket_snap→yfinance 폴백)가 common의 `_fetch_current_prices`와 다른 구현 — 요구사항 차이 분석 후 common으로 단일화 | 동작 차이가 의도적인지 먼저 확인 (순수 이동 아님 — 동작 통합) | M |
| routers_report.py SQL 추출 | 통합 스크리너 SQL 상수 ~130줄(`_UNIFIED_*`, `_AS_OF_*`) → `report_queries.py` | 없음 | S |
| krx_listings JOIN 헬퍼 | `LEFT JOIN krx_listings k ON k.yfinance_symbol=...` 종목명 해석 패턴이 report×2·paper×2·heatmap×3·portfolio·market_snap에 반복 — 공용 SQL 프래그먼트/헬퍼로 | name-resolution.md의 COALESCE 폴백 체인 표준 준수 | S |

### Phase F — 백필 플러밍 통합

stage_backfill(421줄)·screener_backfill(351줄)은 분류 알고리즘 자체는 라이브 잡과 공유한다(`classify_stage_v15`/`chart_screener` — single source of truth 유지됨). 중복은 오케스트레이션 계층: OHLCV fetch, ThreadPoolExecutor prefetch 루프, ISO-week 슬라이싱(`iso_fridays`/`slice_until`/`week_of_to_monday`), flow/listed-shares 벌크 로드. 이를 공용 모듈로 통합.

- 제약: 두 파일 모두 standalone CLI(argparse), `test_stage_backfill.py`가 stage_backfill을 직접 테스트.
- Effort: M

### Phase G — run_scheduler.py 분해 (최후순위 · 최고위험)

1,130줄 = 진입점 + 스케줄 배선 + 실비즈니스 로직 + 위임 심 혼재. 자연 분할:

- **collector 모듈**: FEEDS 설정, `fetch_feed`/`collect_job`, `summary_worker` (~470줄 실로직)
- **job_wrappers 모듈**: jobs/를 lazy-import하는 1~3줄 위임 심 ~25개
- **run_scheduler.py 잔류**: `main()`(scheduler.add_job 배선 ~30개), argparse CLI, `_trigger_watcher_job`, facade re-export

**하드 제약 (이것 때문에 마지막):**
- Task Scheduler 진입점 — `scripts/register_tasks.ps1`·`scripts/start_crawler.bat`이 `python run_scheduler.py --interval/--incremental/--once` 실행. 파일명·플래그 불변.
- 테스트 6개 파일이 `run_scheduler.*`를 mock.patch/직접 참조: `get_macro_context`, `_macro_cache`, `_macro_cache_ts`, `MACRO_CACHE_TTL`, `_get_macro`, `tg_send_signal`, `_screener_tickers`, `_build_watchlist_entries` → **db.py와 동일한 facade 패턴 필수**, 심볼을 옮기지 말고 re-export.
- 모듈 전역 상태(`_seen_hashes`, `_summary_queue`, `_db_pool`, `_paper_trader`)를 collect_job/summary_worker가 공유 — 이동 시 참조 공유 의미론 주의 (재할당 금지, dashboard common.py 캐시 dict와 같은 제약).
- Effort: L

### 비대상 (현상 유지)

| 모듈 | 사유 |
|---|---|
| dashboard/backend/common.py (277줄) | 의도적으로 스코프된 공유 인프라 허브 — 의존 규칙 문서화됨(라우터→common 단방향). 캐시 dict는 main.py가 참조를 공유하므로 재할당 금지 |
| core/article_fetcher.py (355줄) | 응집도 양호 — 소스별 파서 10개 + 단일 공개 함수. 파서가 더 늘면 그때 분리 |
| dashboard/backend/routers_macro.py (416줄) | 단일 도메인으로 응집. `_kiwoom_to_yfinance`만 공용화 후보 (기회적) |
| core/ohlcv_cache.py (434줄) | OHLCV 캐시 vs flow/aftermarket 로더 두 책임이지만 데이터소스 기준으로는 응집 — 낮은 우선순위 |
