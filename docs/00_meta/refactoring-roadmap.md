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
| 2026-07-16 | Phase D 저위험 정리 4/4 항목 완료 (아래 상세) | S+M | test_scan_cmd.py 교체 + jobs/_common.py + db_schema 분리 + 심 삭제 (5커밋) |
| 2026-07-17 | Phase E 대시보드 백엔드 마무리 3/4 항목 완료 (아래 상세) | M | routers_portfolio 분리 + report_queries 추출 + JOIN 헬퍼 (3커밋) |
| 2026-07-17 | Phase F 백필 플러밍 통합, 범위 재조정 후 완료 (아래 상세) | S | jobs/stage_shared.py 추출 (1커밋) |
| 2026-07-17 | Phase G run_scheduler.py 분해, 안전한 부분만 완료 (아래 상세) | L | jobs/scheduler_collect.py + jobs/scheduler_wrappers.py 추출 (2커밋) |
| 2026-07-17 | Phase F 잔여: 프리페치 스켈레톤 + load_flow_range 정책 통일 (아래 상세) | S | stage_shared.prefetch_ohlcv/load_flow_range 추가 (1커밋) |
| 2026-07-17 | stage_classifier 레거시 분리 (9단계 계획 잔여, 아래 상세) | S | analysis/stage_classifier_legacy.py 신설 (1커밋) |
| 2026-07-17 | 텔레그램 계층 정리 (9단계 계획 마지막 항목, 아래 상세) | M | notify 중복 통합 + telegram/bot_handlers.py 분리 (2커밋) |

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

**의도적 보류:** `_CREATE_TABLE` DDL 메가블록 + `init_db` 분리 — 멀티스테이트먼트 DDL이 단일 implicit transaction으로 실행되는 원자성을 보존하기 위해 이번 범위에서 제외. → Phase D 항목(아래에서 완료).

### 2026-07-16 Phase D 저위험 정리 (4/4 완료)

첫 세션에서 3개 실행, 1개(심 삭제)는 예상보다 범위가 커 디스코프했다가 같은 날 후속 세션에서 마이그레이션 후 완료.

1. **test_scan_cmd.py → test_run_screener_cmd.py**: `_handle_scan`은 완전히 죽은 심볼(리포 전체에서 이 파일 4곳 외 참조 없음, 스위트의 유일한 실패 4건이었음). 동일한 4개 시나리오(no-pool/busy-lock/live-run-save/invoker-only-send)를 현재 실제 동작하는 함수(`_handle_run_screener` 가드 + `_run_screener_task` 실행, telegram_bot.py)를 대상으로 재작성. 결과: 830 passed(기존 실패 0).
2. **jobs/_common.py 추출**: `stage_backfill.py`·`screener_backfill.py`의 byte-identical `get_pool()` + 부트스트랩 블록(load_dotenv+logging.basicConfig)을 통합. `sys.path.insert(ROOT)`만 각 파일에 남김 — `jobs` 패키지 자체를 import하려면 그 앞에 선행돼야 하는 최소 코드라 공유 모듈로 옮길 수 없었음(import 순서 제약). `ohlcv_warm.py`는 이중 용도(임포터블 job + 독립 CLI)라 범위 제외.
3. **core/db_schema.py 분리**: `_CREATE_*` DDL 상수 7개, `_RLS_ALWAYS`/`_RLS_IF_EXISTS`, `init_db(pool)`을 byte-identical 이동(AST 비교로 검증). db.py는 get_dsn/create_pool만 남기고 facade에서 전체 재수출 — `_CREATE_TRADE_LOG` import(test_trade_integration.py), `patch("core.db.init_db")`(test_volume_integration.py) 모두 하위호환 유지.
4. **Phase A~C 심 마이그레이션 + 삭제 (완료)**: 5개 날짜/Tor 별칭(`_last_trading_day`/`_jittered_delay`/`_tor_new_identity`/`_prev_business_day`)과 `analysis/backtest_engine.py` 79줄 심을 두 커밋으로 처리.
   - **별칭 4종**: 소스 3곳(`data/krx_flow_sync.py`, `analysis/chart_screener.py`, `data/kiwoom_aftermarket_sync.py`)에서 별칭 정의 제거 + 내부 호출부를 canonical 함수명으로 개명. 테스트 2곳(`test_krx_flow_sync.py`, `test_chart_screener.py`)은 import를 `core.dates`/`core.tor` 직접 참조로 전환(로컬 변수명 유지로 테스트 본문 무변경). `test_krx_flow_sync.py`의 `mock.patch("data.krx_flow_sync._tor_new_identity", ...)` 11곳은 내부 개명에 맞춰 `new_identity`로 갱신. `test_core_dates.py`/`test_core_tor.py`의 `TestAliasesStillResolve`(별칭이 canonical과 동일 객체인지만 확인하던 테스트)는 삭제 — 별칭 자체가 없어지므로 무의미. 결과: 826 passed(830→4개 회귀테스트 삭제, 신규 실패 0).
   - **backtest_engine 심**: 프로덕션 6곳(`jobs/paper_jobs.py`, `telegram/telegram_bot.py`×2, `scripts/` 4개)과 테스트 6곳을 `analysis.backtest.{config,models,helpers,fetch,replay,exit_models,engine}` 직접 import로 전환 후 심 파일 삭제. **핵심 위험 포인트**: `telegram_bot.py`의 함수-로컬 import와 `test_backtest_compose_bot.py`의 `mock.patch` 문자열 타깃은 반드시 같은 커밋에서 함께 이동 — 따로 옮기면 패치가 조용히 무효화돼 테스트가 실제 `run_backtest`를 호출하는 위험(탐색 단계에서 발견, lockstep으로 처리해 회피). `test_backtest_reexports.py`(심의 re-export 완전성만 검증하던 33개 테스트)는 심 삭제와 함께 제거. 결과: 793 passed(826→33개 제거, 신규 실패 0).
   - 최초 Effort 추정(S)이 실제로는 M 상당이었음 — 탐색에 두 서브에이전트, 마이그레이션에 파일 15개 이상 관여.

### 2026-07-17 Phase E 대시보드 백엔드 마무리 (3/4 완료)

4개 항목 중 3개 실행, 가격조회 로직 통합은 탐색 결과 순수 리팩토링 대상이 아님이 확인돼 디스코프.

1. **routers_portfolio.py 분리**: `/api/ticker/lookup` → `routers_ticker.py`, `/api/dart/summary/{ticker}` → `routers_dart.py`로 byte-identical 이동. routers_portfolio.py는 포트폴리오 CRUD 3개 엔드포인트만 남음. main.py에 두 라우터 import+include_router+재수출 추가(기존 sibling 라우터와 동일 컨벤션). 유일한 importer가 main.py였고 직접 테스트하는 파일도 없어 저위험 확인 후 진행.
2. **가격조회 로직 통합 — 디스코프**: `common._fetch_current_prices`(paper trading용, yfinance 배치 조회+공유 TTL 캐시, KR-only, yfinance 형식 티커 전제)와 `routers_portfolio._get_current_prices`(수동 포트폴리오용, DB aftermarket_snap 우선+yfinance 폴백, 캐시 없음, KR+US 지원, bare 티커 코드 전제)를 비교한 결과 **의도적으로 다른 구현** — 티커 포맷·가격 소스 우선순위·시장 커버리지 3가지가 전부 실제 요구사항 차이. 강제 통합은 순수 이동이 아니라 새 로직 작성이 필요하고, 두 함수 모두 직접 테스트가 없어 회귀 위험만 키움. "순수 코드 이동" 원칙에 어긋나 범위에서 제외 — 필요해지면 별도 기능 작업(새 테스트 포함)으로 재검토.
3. **routers_report.py SQL 추출**: `_UNIFIED_TAIL`/`_UNIFIED_TODAY_SQL`/`_UNIFIED_HISTORY_SQL`/`_AS_OF_SQL`/`_AS_OF_HISTORY_SQL`(~130줄, `get_unified_screener` 전용) → `report_queries.py` byte-identical 이동(git diff로 확인).
4. **krx_listings JOIN 헬퍼**: 탐색 결과 종목명 해석 JOIN 패턴이 그룹별로 실제 다름을 확인 — byte-identical한 3-way JOIN(`ticker_names`→`krx_listings`→`chart_signals` LATERAL, `p.ticker` 기준)만 5곳(routers_heatmap.py get_positions, routers_paper.py×2, routers_report.py×2)에서 발견돼 `common._NAME_RESOLUTION_JOIN` 상수로 통합. 나머지는 의도적으로 범위 제외: `routers_report._STAGE_QUERY`(섹터까지 해석, superset), `_UNIFIED_TAIL`의 kl CTE(4단계 폴백+정규화, 별개 패턴), `routers_portfolio.lookup_ticker`(JOIN이 아닌 Python 순차 조회), `routers_paper.py`의 "krx_listings는 이 환경에서 항상 비어있어 제외"라는 주석이 달린 예외 케이스(전제가 최신인지 별도 확인 필요). 검증: whitespace 정규화 SQL 텍스트 비교로 3개 파일 무변화 확인.
5. 결과: 전체 pytest 793 passed(3개 커밋 전부 신규 실패 0).

### 2026-07-17 Phase F 백필 플러밍 통합 (범위 재조정 후 완료)

탐색 결과 로드맵의 원래 전제("stage_backfill·screener_backfill 간 오케스트레이션 중복")가 대부분 false lead였음을 확인:
- OHLCV fetch는 두 스크립트가 일봉/주봉으로 cadence 자체가 다름(의도적) — screener_backfill은 daily_flow를 아예 쓰지 않음.
- `iso_fridays`/`week_label`/`slice_until`(stage_backfill 전용)과 `week_of_to_monday`(screener_backfill 전용)는 중복이 아니라 서로 다른 방향의 변환 — 상호보완적.

진짜 byte-identical 중복은 **stage_backfill.py와 라이브 stage_job.py 사이**에 있었음(둘 다 백필 스크립트가 아닌데도) — 정확히 그 부분만 `jobs/stage_shared.py`로 추출:
1. `normalize_ohlcv(df)`: MultiIndex 평탄화+컬럼선택+UTC tz 인덱스 3줄 스니펫 — stage_backfill/stage_job/screener_backfill **3곳 전부**에서 동일하게 반복되던 것을 통일.
2. `load_listed_shares(pool)`: krx_listings SQL byte-identical (stage_backfill ↔ stage_job).
3. `build_row(...)`: stage_classifications upsert 행 조립 — stage_backfill ↔ stage_job 간 byte-identical(매개변수명만 `friday`→`as_of_date`로 일반화). `tests/test_stage_backfill.py`가 `jobs.stage_backfill.build_row`를 직접 import하므로 facade 패턴으로 재수출.
4. 부수 정리: stage_backfill.py가 `jobs._common.get_pool`을 이미 import해두고도 동일 내용으로 로컬 재정의해 shadow하던 죽은 코드 삭제.

**범위 제외(judgment call 필요)**: ThreadPoolExecutor 프리페치 스켈레톤(예외처리 유무가 다름), `load_flow_range`(윈도우 상한 유무가 다름) — 둘 다 "같은 목적, 다른 안전장치"라 강제 통합 전 정책 결정 필요. 위 향후 로드맵 표에 남김.

검증: AST 비교로 `build_row`/`load_listed_shares` 로직 무변화 확인, 두 백필 CLI `--help` 스모크, `test_stage_backfill.py` 19 passed, 전체 pytest 793 passed(신규 실패 0).

### 2026-07-17 Phase F 잔여 항목 정리 (judgment call 2건 해소)

범위 제외로 남겨뒀던 "같은 목적, 다른 안전장치" 2건을 정책 결정 후 통일. byte-identical 이동이 아니라 정책 결정을 동반한 통합이므로 별도 작업으로 처리.

1. **프리페치 스켈레톤 → `stage_shared.prefetch_ohlcv(tickers, fetch_fn, workers, log_tag)`**: try/except **있는** 형태(screener 정책)로 통일. stage_backfill의 `fut.result()` bare 호출은 잠재 버그였음 — 현재는 두 fetcher가 예외를 삼켜 발현 안 되지만, fetcher가 바뀌면 한 종목 실패가 전체 백필을 죽이는 구조. `batch_fetch_ohlcv`/`prefetch_all`은 시그니처를 유지한 채 위임 wrapper로 전환(호출부 무변경). 종목별 수집 로직(일봉 윈도우 vs 주봉 3y)은 `fetch_fn` 주입으로 분리 — cadence 차이는 의도대로 유지.
2. **`load_flow_range` → stage_shared로 이동**: 상한 **있는** 형태(stage_backfill 버전)로 통일 — stage_job은 `today` 기준이라 `<= today` 추가는 no-op. **에러 정책은 함수에 박지 않고 호출부에 유지**: stage_job은 호출부 try/except로 "실패 시 빈 map으로 계속"(라이브 잡 생존 우선), stage_backfill은 전파(CLI fail-fast). 이로써 강제 정책 통일 문제 자체가 해소됨. stage_job.py의 27줄 인라인 블록이 7줄로 축소.

검증: 두 백필 CLI `--help` + import 스모크, `test_stage_backfill.py` 19 passed, 전체 pytest 793 passed(신규 실패 0). 세 함수 모두 직접 테스트/mock.patch 타깃 없음을 grep으로 확인(이동 자체의 테스트 파손 위험 0). 라이브 스케줄러 프로세스 재시작은 이번에도 범위 밖.

### 2026-07-17 stage_classifier 레거시 분리 (TODOS.md 9단계 계획 잔여)

859줄 → 573줄(-286) + `analysis/stage_classifier_legacy.py`(339줄) 신설.

**탐색에서 확정한 핵심 사실 — 버전 체인은 누적 구조**: v15는 `_check_stage1_v14→v13→v11→_check_stage1(v1.0)`, `_check_stage2_v13→v12→v11`, `_check_stage3_v12→v11`을 그대로 호출한다. 즉 v11~v14의 조건 헬퍼 대부분은 레거시가 아니라 **v15의 라이브 의존성** — 옮길 수 있는 것은 어떤 라이브 경로도 참조하지 않는 심볼뿐이다.

**legacy로 이동한 8개 (전부 zero 라이브 소비자, AST byte-identical 확인):** 구버전 디스패처 5개(`classify_stage` v1.0, `classify_stage_v11`~`v14`) + v1.0 전용 `_check_stage2`/`_check_stage3` + v1.2 전용 `_check_stage1_v12`(v13이 v11을 직접 호출해 우회하므로 legacy-only).

**본 모듈에 남긴 15개:** `classify_stage_v15`/`check_peakout`/`compute_*`(프로덕션 표면) + v15 누적 체인이 쓰는 조건 헬퍼 전부.

**의존 방향**: legacy → stage_classifier 단방향(공유 헬퍼 import). 역방향 facade 재수출은 순환 import를 만들므로 두지 않고, 유일한 소비자였던 `test_stage_classifier.py`의 import를 legacy 직접 참조로 전환(Phase D 별칭 마이그레이션과 동일 패턴). `test_compose_parity.py`는 주석 언급뿐 실 의존 없음. 프로덕션 소비자(stage_job/stage_backfill/stage_shared)는 v15/check_peakout/compute_*만 써서 무변경.

검증: AST 비교 8 moved + 15 kept 전부 identical, import 스모크, `test_stage_classifier.py`+`test_compose_parity.py`+`test_stage_backfill.py` 74 passed, 전체 pytest 793 passed(신규 실패 0), pyright 대상 파일 0 errors.

### 2026-07-17 텔레그램 계층 정리 (9단계 계획 마지막 항목)

2단계로 처리. telegram_trade.py는 이미 깨끗한 구조(전역 0, 테스트 타깃 1개)라 무변경.

**1단계 — 파일 내 중복 정리 (1커밋):**
- telegram_notify: send_* 5개 함수에 중첩 정의돼 있던 byte-identical `esc()` 사본 5개 + `esc_code()` 사본 2개를 모듈 레벨 단일 정의로 통합.
- telegram_bot: /paper·/paper_perf에 중복된 `MODEL_ICON` dict를 모듈 상수로 호이스팅.
- **의도적 비통합**: telegram_bot의 `esc()`는 백틱 미포함 charset으로 notify 버전과 동작이 다름 — 강제 통일은 동작 변경이라 제외. `_get_token`/`_send` 계열 3종(bot/notify/trade)도 재시도·parse_mode·시그니처가 서로 달라(의도적) 통합 대상 아님.

**2단계 — telegram_bot.py 핸들러 분리 (1커밋):** 1,272줄 → 293줄 + `telegram/bot_handlers.py`(1,064줄) 신설.
- 이동: 명령어 핸들러/수동트리거 태스크 21개 + `esc`/`_fmt_kst`/`MODEL_ICON`. telegram_bot이 top-level 재수출(facade)해 `from telegram.telegram_bot import X` 경로(테스트 4파일) 보존.
- **telegram_bot에 남긴 것**: 폴링 루프·라우팅(`_process_update`)·전송(`_send`/`_send_plain`)·락 5개·전역(`_last_update_id`/`_start_time`/`_seen_hashes_ref`)·config 유틸. 이유: (a) `global` 재바인딩 3개(Phase G와 동일 제약), (b) 테스트가 `mock.patch("telegram.telegram_bot._send_plain/_backtest_lock/_handle_backtest_compose")`로 모듈 속성을 통째로 교체, (c) **test_run_screener_cmd가 `telegram_bot._scan_lock = asyncio.Lock()`으로 락 자체를 재바인딩** — facade 재수출로는 재바인딩이 핸들러에 전파되지 않으므로 락은 반드시 telegram_bot 소유 + 속성 접근이어야 함(탐색 단계에서 발견한 함정).
- 핸들러는 함수 본문 `import telegram.telegram_bot as bot` 지연 import + `bot._send_plain(...)` 속성 접근으로 항상 최신 바인딩을 읽음 — jobs/scheduler_wrappers.py(Phase G)와 동일 패턴. bot_handlers는 top-level에서 telegram_bot을 import하지 않아 어느 방향으로 먼저 import돼도 순환 없음.
- 함수-로컬 import(telegram_notify/telegram_trade/run_scheduler/jobs.*)는 함수와 함께 이동 — 패치 문자열이 소스 모듈을 타깃하므로 lockstep 문제 없음.

검증: AST 비교(지연 import 제거 + `bot.` 접두어 정규화 후) 23 moved + 10 kept 전부 identical, 양방향 import 스모크 + facade 동일성 확인, `run_scheduler.py --help`, 텔레그램 테스트 12파일 130 passed, 전체 pytest 793 passed(신규 실패 0), pyright 오류 수 베이스라인 동일. 라이브 스케줄러 프로세스 재시작은 범위 밖.

**이로써 2026-07-15 시작한 구조 리팩토링 9단계 계획 전체 종료.** 남은 것은 위 Phase E/F/G 표의 기능 작업 재분류 항목(가격조회 통합 등)과 비대상 목록뿐.

---

## 향후 로드맵

우선순위·리스크 순으로 Phase D→G. 각 Phase 내 항목은 독립적이라 개별 진행 가능. **Phase D~G 전부 완료** — 아래는 각 Phase 진행 중 발견된, case-by-case 판단이 필요해 범위 제외한 잔여 항목들이다.

### Phase D — 저위험 정리 (워밍업) — ✅ 완료 (4/4)

위 완료 기록 참조. 다음은 Phase E부터.

### Phase E — 대시보드 백엔드 마무리 — ✅ 완료 (3/4 + 잔여 통합 완료)

위 완료 기록 참조.

~~가격 조회 로직 통합~~ — **완료(2026-07-17, 기능 작업)**. 2단계로 처리:
1. 선행 회귀 테스트: `tests/test_price_lookup.py` 특성화 테스트 20개로 두 함수의 현재 동작 고정(quirk 2건 포함).
2. 통합: `common.fetch_current_prices(tickers, *, pool=None, use_cache=True)` 단일 구현 — ① bare KR+pool → aftermarket_snap, ② yfinance 형식 → 배치 다운로드, ③ 잔여 bare → fast_info(US 직접/KR .KS→.KQ). quirk 2건 수정: 전역 스냅샷 캐시 → **티커별 (price, expires) 엔트리**, 단일 티커 플랫 컬럼 응답 → Close 정규화 반환. 기존 두 이름은 얇은 위임 wrapper로 유지(호출부 5곳·test_paper_analytics patch 타깃 무변경). 부수 동작 변경 2건: `update_cache=False`는 이제 캐시 읽기도 건너뜀(항상 신선), 포트폴리오 경로에 티커별 5분 캐시 신규 적용. 테스트 26개로 갱신, 전체 pytest 819 passed.

~~나머지 krx_listings 패턴 조정~~ — **완료(2026-07-17, case-by-case 판정)**:
- **routers_paper "krx_listings 항상 비어있음" 주석**: 실 DB 확인 결과 **전제가 거짓**(전 종목 2,765행, name_ko/listed_shares 100%). 이 전제로 tn-only로 축소돼 있던 curve 종목명 맵 + export CSV 쿼리 2개를 공용 `_NAME_RESOLUTION_JOIN`으로 승격 — 실측으로 모의투자 종목 62개 중 38개의 한글명 해석이 복구됨.
- **`_STAGE_QUERY`**: sector까지 해석하는 superset + 별칭 `sc` — 통합하려면 별칭 파라미터화·다른 LATERAL 필요. **의도적 비대상 확정.**
- **`_UNIFIED_TAIL` kl CTE**: bare 코드 정규화 4단계 폴백, 별개 패턴. **의도적 비대상 확정.**

**Phase E 잔여 항목 전부 종료.**

### Phase F — 백필 플러밍 통합 — ✅ 완료 (범위 재조정)

위 완료 기록 참조. 원래 가정("두 백필 스크립트 간 중복")은 탐색 결과 대부분 false lead였음 — 실제 중복은 stage_backfill.py와 라이브 stage_job.py 사이에 있었고, 그 부분만 `jobs/stage_shared.py`로 추출 완료.

남은 항목: ~~ThreadPoolExecutor 프리페치 통합~~, ~~`load_flow_range` 윈도우 정책 통일~~ — 둘 다 2026-07-17 후속 세션에서 정책 결정 후 완료(위 "Phase F 잔여 항목 정리" 상세 참조). 채택 정책: 프리페치는 try/except 통일(`stage_shared.prefetch_ohlcv`), load_flow_range는 상한 통일 + 에러 정책은 호출부 유지.

| 항목 | What | 제약 | Effort |
|---|---|---|---|
| OHLCV fetch cadence | 일봉(daily/60d) vs 주봉(3y/1wk) — 의도적으로 다른 그레인이라 통합 대상 아님 | 통합 시도하지 말 것 | — |

### Phase G — run_scheduler.py 분해 (최후순위 · 최고위험) — ✅ 완료 (안전한 부분만)

938줄로 축소(1,130→938, -192줄). 탐색 결과 전역 상태 **재할당**(global 재바인딩)이 여러 함수·모듈 경계에 걸쳐 있어 순수 이동만으로는 위험(다른 모듈로 옮긴 뒤 `from x import y`로 이름을 가져오면 재할당이 반영 안 돼 조용히 깨짐)함을 확인 — 사용자와 논의해 범위를 "안전한 부분만"으로 확정.

**이동 완료:**
- `jobs/scheduler_collect.py`(105줄): `_url_hash`/`_parse_dt`/`_fmt_date`/`_is_fresh`/`fetch_feed` + `MAX_AGE_HOURS`/`FETCH_RETRY_COUNT`/`FETCH_RETRY_DELAY` — 전역 상태 완전 무관(http/cfg 매개변수만 사용), byte-identical.
- `jobs/scheduler_wrappers.py`(159줄): 순수 위임 잡 19개. 그중 8개(`_youtube_*`×3, `_daily_market_snap_job`, `_daily_aftermarket_sync_job`, `_daily_flow_sync_job`, `_daily_ohlcv_warm_job`, `_build_watchlist_entries`)는 전역 상태 완전 무관이라 byte-identical. 나머지 11개는 `_db_pool`/`_paper_trader`를 bare-name으로 읽어서, 함수 본문에 `import run_scheduler`(이 파일이 이미 쓰는 지연 import 스타일과 동일)를 추가하고 `run_scheduler._db_pool`/`run_scheduler._paper_trader` 속성 접근으로 전환 — 모듈은 싱글턴이라 속성 접근은 매번 최신값을 읽으므로 `main()`의 재할당이 정상 반영됨(`from run_scheduler import _db_pool` 같은 이름 import는 값을 스냅샷해서 재할당을 못 봄 — 이 프로젝트에서 처음 만난 종류의 위험).

**run_scheduler.py에 남긴 것(전역 재할당 또는 오케스트레이션):**
`main()`, argparse CLI, `_run_once_watchlist`/`_run_once_stage`, `collect_job`/`summary_worker`(전역상태 R/W), `_get_macro`+매크로 캐시(자기완결적, 유일한 호출자 summary_worker도 남음), `_daily_stage_job`/`_weekly_screener_job`(전역 재할당+체이닝), `_trigger_watcher_job`(DB 트랜잭션+5개 잡 디스패치 오케스트레이션).

**facade**: run_scheduler.py 상단의 `from jobs.scheduler_wrappers import (...)` 19개 블록 자체가 facade 역할 — `main()`의 `scheduler.add_job(...)`과 `_trigger_watcher_job`의 디스패치가 이 이름들을 참조하려면 어차피 top-level import가 필요해 별도 재수출 코드 불필요.

**검증**: AST 비교로 8개 byte-identical + 11개는 `run_scheduler.` 치환 외 무변화 확인, import/`--help` 스모크, 타깃 테스트 6개 파일(`test_macro_signal`/`test_news_gating`/`test_telegram_routing`/`test_watchlist_features`/`test_high_confidence`/`test_p3_remaining`) 76 passed, 전체 pytest 793 passed(신규 실패 0). Task Scheduler 진입점(`register_tasks.ps1`→`start_crawler.bat`→`python run_scheduler.py --interval N [--no-summary]`) 파일명·CLI 불변. **실제 스케줄러 프로세스 재시작은 이번 범위 밖** — 코드/테스트 검증까지만.

**범위 제외(추가 분리하려면 공유 state 모듈 필요):**
`_db_pool`/`_paper_trader` 재할당까지 다른 모듈로 옮기려면 `import state; state.x = ...` 패턴의 공유 state 모듈을 도입해 ~30-40군데 read/write 사이트를 전부 고쳐야 함 — 기계적이지만 광범위한 수정이라 이번 범위에서 제외. `_daily_stage_job`/`_weekly_screener_job`/`_trigger_watcher_job`/`collect_job`/`summary_worker`는 그런 이유로 run_scheduler.py에 남아있음.

### 비대상 (현상 유지)

| 모듈 | 사유 |
|---|---|
| dashboard/backend/common.py (277줄) | 의도적으로 스코프된 공유 인프라 허브 — 의존 규칙 문서화됨(라우터→common 단방향). 캐시 dict는 main.py가 참조를 공유하므로 재할당 금지 |
| core/article_fetcher.py (355줄) | 응집도 양호 — 소스별 파서 10개 + 단일 공개 함수. 파서가 더 늘면 그때 분리 |
| dashboard/backend/routers_macro.py (416줄) | 단일 도메인으로 응집. `_kiwoom_to_yfinance`만 공용화 후보 (기회적) |
| core/ohlcv_cache.py (434줄) | OHLCV 캐시 vs flow/aftermarket 로더 두 책임이지만 데이터소스 기준으로는 응집 — 낮은 우선순위 |
